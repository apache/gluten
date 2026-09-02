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
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.vector.FieldVector

import org.lance.spark.internal.LanceArrowStreamScanner
import org.lance.spark.read.{LanceInputPartition, LanceScan}

import scala.collection.JavaConverters._

/**
 * A Gluten leaf that reads a Lance table through the Arrow C Data Interface and produces
 * [[ArrowJavaBatchType]] columnar batches, which Gluten then offloads to Velox via its automatic
 * ArrowJava -> ArrowNative transition.
 *
 * This replaces a vanilla [[BatchScanExec]] over a [[LanceScan]]. Each Spark partition maps to one
 * Lance fragment. For every fragment the leaf asks lance-spark to export the planned scan as an
 * [[ArrowArrayStream]] ([[LanceArrowStreamScanner.export]]) and hands back only the C-struct
 * address. The address is re-wrapped with Gluten's own Arrow build and imported here, so lance-spark
 * and Gluten never share Arrow Java objects across their classloaders -- only the version-stable C
 * ABI struct crosses the boundary. This is the consumer side of lance-core's
 * {@code LanceScanner#exportArrowStream(long)} (lance#7259).
 *
 * Each imported batch is transferred out of the reader's root into independent Arrow buffers so the
 * emitted [[ColumnarBatch]] owns its data: Gluten's offload transition C-exports then closes each
 * batch, and the recycle callback closes it again, exactly as for [[ColumnarRangeExec]]. The reader
 * and the lance-spark handle are closed on task completion, which releases the native scan and the
 * caller-owned stream struct.
 */
case class LanceScanTransformer(@transient batchScan: BatchScanExec)
  extends LeafExecNode
  with ValidatablePlan {

  @transient private lazy val lanceScan: LanceScan =
    batchScan.scan.asInstanceOf[LanceScan]

  @transient private lazy val inputPartitions: Array[InputPartition] =
    lanceScan.toBatch.planInputPartitions()

  override def output: Seq[Attribute] = batchScan.output

  override def outputPartitioning: Partitioning = UnknownPartitioning(inputPartitions.length)

  override def batchType(): Convention.BatchType = ArrowJavaBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(s"$nodeName does not support row-based execution.")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    new LanceColumnarRDD(sparkContext, inputPartitions)
}

object LanceScanTransformer {

  /** Whether this DSv2 scan is a Lance scan that can be offloaded to Velox through Arrow C. */
  def supportsBatchScan(scan: Scan): Boolean = scan.isInstanceOf[LanceScan]

  /**
   * Whether the planned Lance scan can actually be exported through the Arrow C stream.
   *
   * A pushed aggregation (e.g. {@code COUNT(*)}) or a full-text query makes the native fragment
   * scan's output diverge from the partition's declared Spark schema: for an aggregation the
   * partition's output is the aggregate result while the fragment scan returns data rows, and a
   * full-text query auto-projects a {@code _score} column. [[LanceArrowStreamScanner.export]]
   * rejects both at runtime (checkNoPushedAggregation / checkNativeSchemaMatchesPartition), but by
   * then the node is already offloaded to Velox, so the throw fails the query instead of falling
   * back. Detecting them here keeps such scans on vanilla Spark (e.g. the fast COUNT(*) metadata
   * reader) rather than offloading a scan that can only crash.
   */
  def isExportable(batchScan: BatchScanExec): Boolean =
    batchScan.inputPartitions.forall {
      case p: LanceInputPartition =>
        !p.getPushedAggregation.isPresent && p.getReadOptions.getFullTextQuery == null
      case _ => false
    }
}

/** One RDD partition per planned Lance [[InputPartition]] (one Lance fragment each). */
private class LancePartition(override val index: Int, val inputPartition: LanceInputPartition)
  extends Partition

/**
 * Executes the planned Lance partitions, importing each fragment's exported Arrow C stream with
 * Gluten's Arrow build and yielding offloadable [[ArrowWritableColumnVector]] batches.
 */
private class LanceColumnarRDD(
    @transient private val sc: SparkContext,
    private val inputPartitions: Array[InputPartition])
  extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] =
    inputPartitions.zipWithIndex.map {
      case (p, i) => new LancePartition(i, p.asInstanceOf[LanceInputPartition])
    }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val lancePartition = split.asInstanceOf[LancePartition].inputPartition
    val allocator = ArrowBufferAllocators.contextInstance()

    val batches: Iterator[ColumnarBatch] =
      lancePartition.getLanceSplit.getFragments.asScala.iterator.flatMap {
        fragId =>
          // Plan + export the fragment scan on the lance-spark side; only the C-struct address
          // crosses over. wrap() views that struct with Gluten's Arrow, and importArrayStream()
          // moves it into a reader that owns and drains the native scan.
          val handle = LanceArrowStreamScanner.export(fragId.intValue(), lancePartition)
          val stream = ArrowArrayStream.wrap(handle.streamAddress())
          val reader = Data.importArrayStream(allocator, stream)
          context.addTaskCompletionListener[Unit] {
            _ =>
              try {
                // Drains + runs the C release callback, tearing down the native scan.
                reader.close()
              } finally {
                // Frees the caller-owned stream struct and closes the scanner + dataset.
                handle.close()
              }
          }

          new Iterator[ColumnarBatch] {
            private var advanced = false
            private var hasMore = false

            private def advance(): Unit =
              if (!advanced) {
                hasMore = reader.loadNextBatch()
                advanced = true
              }

            override def hasNext: Boolean = {
              advance()
              hasMore
            }

            override def next(): ColumnarBatch = {
              advance()
              if (!hasMore) {
                throw new NoSuchElementException()
              }
              advanced = false
              val root = reader.getVectorSchemaRoot
              val rowCount = root.getRowCount
              // Transfer (zero-copy move) each column out of the reader's reused root so the emitted
              // batch owns its buffers and is safe for the offload transition to close.
              val transferred = new java.util.ArrayList[FieldVector](root.getFieldVectors.size())
              root.getFieldVectors.asScala.foreach {
                fv =>
                  val pair = fv.getTransferPair(allocator)
                  pair.transfer()
                  transferred.add(pair.getTo.asInstanceOf[FieldVector])
              }
              val vectors = ArrowWritableColumnVector.loadColumns(rowCount, transferred)
              new ColumnarBatch(vectors.asInstanceOf[Array[ColumnVector]], rowCount)
            }
          }
      }

    Iterators
      .wrap(batches)
      .recyclePayload((batch: ColumnarBatch) => batch.close())
      .create()
  }
}
