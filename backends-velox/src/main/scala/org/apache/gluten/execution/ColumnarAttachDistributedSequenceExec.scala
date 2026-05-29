/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarAttachDistributedSequenceBaseExec, SparkPlan}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Velox implementation of [[ColumnarAttachDistributedSequenceBaseExec]] that prepends a contiguous,
 * globally increasing `Long` id column to its child output while keeping the columnar pipeline
 * intact.
 *
 * Mirrors Spark's `AttachDistributedSequenceExec` semantics with two passes over the child:
 *   1. A first pass executes the child plan over partitions `[0, numPartitions - 1)` and sums the
 *      `numRows` of every produced batch -- the last partition's count is not needed for the
 *      prefix-sum. The batches are closed immediately; no native data is materialized for the count
 *      pass beyond what the child operator naturally produces.
 *   2. The per-partition prefix-sum is broadcast and a second pass executes the child plan again,
 *      prepending the new id column. Each output column (id + copies of the input columns) is a
 *      freshly allocated [[ArrowWritableColumnVector]] so the output batch has a uniform reference
 *      count -- required by the downstream `OffloadArrowDataExec`'s `getRefCntHeavy` check. Input
 *      values are copied via Arrow's `ValueVector.copyFromSafe`; the upstream input batch is left
 *      untouched and closed by the upstream iterator.
 *
 * Why no cache? The natural choice would be to wrap the child output in
 * [[org.apache.spark.sql.execution.ColumnarCachedBatchSerializer]] and `persist` once, so the child
 * plan is computed only once. That works for ordinary columnar batches but fails for zero-column
 * batches that can result from column pruning when only the new id column is selected
 * (`df.select("id")` projects away every input column): the cache serializer's
 * `ensureVeloxBatch -> isVeloxBatch -> getIndicatorVector` path throws on zero-column input. The
 * two-pass approach trades one extra child execution for robustness across all valid plans, and
 * matches vanilla Spark's behavior when the pandas-on-Spark cache option is `NONE`.
 *
 * For the trivial single-partition case the count pass is skipped and the assignment runs directly
 * with `startOffset = 0`.
 */
case class ColumnarAttachDistributedSequenceExec(
    sequenceAttr: Attribute,
    override val child: SparkPlan)
  extends ColumnarAttachDistributedSequenceBaseExec(sequenceAttr, child) {

  override def batchType(): Convention.BatchType = ArrowJavaBatchType

  override def requiredChildConvention(): Seq[ConventionReq] = Seq(
    ConventionReq.ofBatch(ConventionReq.BatchType.Is(ArrowJavaBatchType)))

  private val outputSchema: StructType =
    StructType(
      StructField(sequenceAttr.name, LongType, nullable = false) +:
        child.output.map(a => StructField(a.name, a.dataType, a.nullable)))

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRdd = child.executeColumnar()
    val numPartitions = childRdd.getNumPartitions

    if (numPartitions <= 1) {
      // Fast path: at most one partition, no need to count.
      return childRdd.mapPartitions(it => assignIds(it, startOffset = 0L))
    }

    // First pass: execute the child plan and count rows per partition for partitions
    // [0, numPartitions - 1). The last partition's count is unused for the prefix-sum.
    // Each batch is closed immediately after reading numRows so off-heap buffers are released.
    val frontCounts: Array[Long] = sparkContext.runJob(
      childRdd,
      (it: Iterator[ColumnarBatch]) => {
        var sum = 0L
        while (it.hasNext) {
          val cb = it.next()
          sum += cb.numRows().toLong
          cb.close()
        }
        sum
      },
      0 until (numPartitions - 1)
    )
    val offsets = frontCounts.scanLeft(0L)(_ + _)
    val bcOffsets = sparkContext.broadcast(offsets)

    // Second pass: re-execute the child plan and prepend the id column.
    childRdd.mapPartitionsWithIndex {
      (pid, it) => assignIds(it, bcOffsets.value(pid))
    }
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): ColumnarAttachDistributedSequenceExec =
    copy(child = newChild)

  /**
   * Prepends a `Long` id column to each input batch starting from `startOffset` and incrementing by
   * row index. The output is a fresh heavy [[ColumnarBatch]] whose columns are all freshly
   * allocated [[ArrowWritableColumnVector]]s with reference count 1. Input column values are copied
   * via Arrow's `ValueVector.copyFromSafe` rather than retained zero-copy so the resulting batch
   * satisfies the uniform-reference-count invariant required by downstream
   * `ColumnarBatches.offload` / `getRefCntHeavy`. The original input batch is left untouched and is
   * closed by the upstream iterator's recycling logic.
   */
  private def assignIds(
      batches: Iterator[ColumnarBatch],
      startOffset: Long): Iterator[ColumnarBatch] = {
    val attached = new Iterator[ColumnarBatch] {
      private var running: Long = startOffset

      override def hasNext: Boolean = batches.hasNext

      override def next(): ColumnarBatch = {
        val inputCb = batches.next()
        ColumnarBatches.checkLoaded(inputCb)
        val numRows = inputCb.numRows()
        val outCols = ArrowWritableColumnVector.allocateColumns(numRows, outputSchema)
        try {
          val idVec = outCols(0)
          var i = 0
          while (i < numRows) {
            idVec.putLong(i, running + i)
            i += 1
          }
          idVec.setValueCount(numRows)

          // Copy each input column into its corresponding freshly-allocated output column. Using
          // Arrow's per-row `copyFromSafe` keeps the implementation type-agnostic and ensures every
          // output column has reference count 1, matching the id column. This avoids the uniform
          // ref-count check enforced by `ColumnarBatches.getRefCntHeavy` when the planner inserts
          // an `OffloadArrowDataExec` between us and the next Velox consumer.
          var j = 0
          while (j < inputCb.numCols()) {
            val src = inputCb.column(j).asInstanceOf[ArrowWritableColumnVector].getValueVector
            val dst = outCols(j + 1).getValueVector
            var r = 0
            while (r < numRows) {
              dst.copyFromSafe(r, r, src)
              r += 1
            }
            dst.setValueCount(numRows)
            j += 1
          }

          running += numRows
          new ColumnarBatch(outCols.asInstanceOf[Array[ColumnVector]], numRows)
        } catch {
          case t: Throwable =>
            outCols.foreach(_.close())
            throw t
        }
      }
    }
    Iterators
      .wrap(attached)
      .recyclePayload(_.close())
      .create()
  }
}
