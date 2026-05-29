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
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.{ColumnarAttachDistributedSequenceBaseExec, ColumnarCachedBatchSerializer, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel

/**
 * Velox implementation of [[ColumnarAttachDistributedSequenceBaseExec]] that prepends a contiguous,
 * globally increasing `Long` id column to its child output while keeping the columnar pipeline
 * intact.
 *
 * Mirrors Spark's `AttachDistributedSequenceExec` semantics:
 *   1. The child columnar output is materialized once into a cached `RDD[CachedBatch]` using
 *      Gluten's [[ColumnarCachedBatchSerializer]] (Velox native serialization, persisted at
 *      `MEMORY_AND_DISK_SER`). This prevents the inherent two-pass nature of `zipWithIndex` from
 *      re-running the child plan twice.
 *   2. A first pass over the cached partitions `[0, numPartitions - 1)` reads only the `numRows`
 *      field of each `CachedColumnarBatch` -- no native deserialization is required to count.
 *   3. The per-partition prefix-sum is broadcast and a second pass deserializes the cached batches
 *      back to columnar form, ensures they are Arrow-loaded, and prepends the new id column by
 *      retaining the input column vectors (zero-copy) and allocating one
 *      [[ArrowWritableColumnVector]] for the id column.
 *
 * For the trivial single-partition case the cache step is skipped and the assignment is done
 * directly over the child output with `startOffset = 0`.
 */
case class ColumnarAttachDistributedSequenceExec(
    sequenceAttr: Attribute,
    override val child: SparkPlan)
  extends ColumnarAttachDistributedSequenceBaseExec(sequenceAttr, child) {

  override def batchType(): Convention.BatchType = ArrowJavaBatchType

  override def requiredChildConvention(): Seq[ConventionReq] = Seq(
    ConventionReq.ofBatch(ConventionReq.BatchType.Is(ArrowJavaBatchType)))

  private val idSchema: StructType =
    StructType(Seq(StructField(sequenceAttr.name, LongType, nullable = false)))

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRdd = child.executeColumnar()
    val numPartitions = childRdd.getNumPartitions

    if (numPartitions <= 1) {
      // Fast path: at most one partition, no need to count or cache.
      return childRdd.mapPartitions(it => assignIds(it, startOffset = 0L, alreadyLoaded = true))
    }

    val sqlConf = SQLConf.get
    val cacheAttrs = child.output
    val cached = getOrBuildCache(childRdd, cacheAttrs, sqlConf)

    // First pass: count rows per partition for partitions [0, numPartitions - 1) -- the last
    // partition's count is unused. Reading `CachedColumnarBatch.numRows` is a case-class field
    // access; no native deserialization is required.
    val frontCounts: Array[Long] = sparkContext.runJob(
      cached,
      (it: Iterator[CachedBatch]) => {
        var sum = 0L
        while (it.hasNext) {
          sum += it.next().numRows.toLong
        }
        sum
      },
      0 until (numPartitions - 1)
    )
    val offsets = frontCounts.scanLeft(0L)(_ + _)
    val bcOffsets = sparkContext.broadcast(offsets)

    // Second pass: deserialize cached batches back to Velox-native ColumnarBatches and prepend
    // the id column. `convertCachedBatchToColumnarBatch` already wraps its output with
    // Iterators.recyclePayload, so closing happens correctly.
    val rehydrated = new ColumnarCachedBatchSerializer()
      .convertCachedBatchToColumnarBatch(cached, cacheAttrs, cacheAttrs, sqlConf)

    rehydrated.mapPartitionsWithIndex {
      (pid, it) => assignIds(it, bcOffsets.value(pid), alreadyLoaded = false)
    }
  }

  // Idempotent cache materialization. doExecuteColumnar() may be called more than once during
  // planning / fallback handling; we want to persist only once and have a single handle to
  // unpersist in cleanupResources().
  @transient @volatile private var cachedRdd: RDD[CachedBatch] = _

  private def getOrBuildCache(
      childRdd: RDD[ColumnarBatch],
      cacheAttrs: Seq[Attribute],
      sqlConf: SQLConf): RDD[CachedBatch] = {
    val existing = cachedRdd
    if (existing != null) {
      return existing
    }
    synchronized {
      if (cachedRdd == null) {
        // Serialize child batches into on-heap Velox-native byte blobs (CachedColumnarBatch) and
        // persist them so that the count pass and the assign pass each read from the cache
        // instead of recomputing the child plan.
        cachedRdd = new ColumnarCachedBatchSerializer()
          .convertColumnarBatchToCachedBatch(
            childRdd,
            cacheAttrs,
            StorageLevel.MEMORY_AND_DISK_SER,
            sqlConf)
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      cachedRdd
    }
  }

  override protected def doColumnarCleanup(): Unit = {
    val toRelease = synchronized {
      val r = cachedRdd
      cachedRdd = null
      r
    }
    if (toRelease != null) {
      try {
        toRelease.unpersist(blocking = false)
      } catch {
        case _: Throwable => // best-effort; do not propagate from cleanup
      }
    }
  }

  /**
   * Prepends a `Long` id column to each input batch starting from `startOffset` and incrementing by
   * row index. If `alreadyLoaded` is false the input batch is brought into Arrow-loaded (heavy)
   * form first via [[ColumnarBatches#ensureLoaded]] -- this is a no-op when the batch is already
   * heavy.
   */
  private def assignIds(
      batches: Iterator[ColumnarBatch],
      startOffset: Long,
      alreadyLoaded: Boolean): Iterator[ColumnarBatch] = {
    val attached = new Iterator[ColumnarBatch] {
      private var running: Long = startOffset

      override def hasNext: Boolean = batches.hasNext

      override def next(): ColumnarBatch = {
        val rawCb = batches.next()
        val inputCb =
          if (alreadyLoaded) {
            ColumnarBatches.checkLoaded(rawCb)
            rawCb
          } else {
            // After `convertCachedBatchToColumnarBatch` the batch is Velox-native (light); load
            // it into Arrow-Java (heavy) form via a zero-copy ABI handoff so the id column
            // (an ArrowWritableColumnVector) can sit next to the input columns in the output
            // batch. `load` closes the light input on success.
            ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), rawCb)
          }
        try {
          val numRows = inputCb.numRows()

          val idVec = ArrowWritableColumnVector
            .allocateColumns(numRows, idSchema)
            .head
          try {
            var i = 0
            while (i < numRows) {
              idVec.putLong(i, running + i)
              i += 1
            }
            idVec.setValueCount(numRows)

            // Retain input columns once so that closing both the input batch (by upstream) and
            // the output batch (by the wrapping iterator) leaves the underlying Arrow buffers'
            // ref-counts at zero.
            ColumnarBatches.retain(inputCb)

            val outCols = new Array[ColumnVector](inputCb.numCols() + 1)
            outCols(0) = idVec
            var j = 0
            while (j < inputCb.numCols()) {
              outCols(j + 1) = inputCb.column(j)
              j += 1
            }
            running += numRows
            new ColumnarBatch(outCols, numRows)
          } catch {
            case t: Throwable =>
              idVec.close()
              throw t
          }
        } catch {
          case t: Throwable =>
            // If we loaded the input ourselves and failed before constructing the output
            // batch (which would otherwise own the retain), release it here so the underlying
            // Arrow buffers are freed. When alreadyLoaded == true the upstream iterator still
            // owns rawCb, so we must not close it here.
            if (!alreadyLoaded) {
              try {
                inputCb.close()
              } catch {
                case _: Throwable => // swallow secondary failure
              }
            }
            throw t
        }
      }
    }
    Iterators
      .wrap(attached)
      .recyclePayload(_.close())
      .create()
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): ColumnarAttachDistributedSequenceExec =
    copy(child = newChild)
}
