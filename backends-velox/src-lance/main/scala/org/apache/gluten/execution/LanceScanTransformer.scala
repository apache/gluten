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
import org.apache.gluten.vectorized.ArrowColumnVectors

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.LanceArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{BaseRepeatedValueVector, FixedSizeListVector}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.lance.spark.LanceConstant
import org.lance.spark.internal.LanceArrowStreamScanner
import org.lance.spark.read.{LanceInputPartition, LanceScan}
import org.lance.spark.utils.{BlobUtils, JsonUtils}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * A Gluten leaf that reads a Lance table through the Arrow C Data Interface and produces
 * [[ArrowJavaBatchType]] columnar batches, which Gluten then offloads to Velox via its automatic
 * ArrowJava -> ArrowNative transition.
 *
 * This replaces a vanilla [[BatchScanExec]] over a [[LanceScan]]. Each Spark partition maps to one
 * Lance fragment. For every fragment the leaf asks lance-spark to export the planned scan as an
 * [[ArrowArrayStream]] ([[LanceArrowStreamScanner.export]]) and hands back only the C-struct
 * address. The address is re-wrapped with Gluten's own Arrow build and imported here, so
 * lance-spark and Gluten never share Arrow Java objects across their classloaders -- only the
 * version-stable C ABI struct crosses the boundary. This is the consumer side of lance-core's
 * {@code LanceScanner#exportArrowStream(long)} (lance#7259).
 *
 * Each imported batch is transferred out of the reader's root into independent Arrow buffers so the
 * emitted [[ColumnarBatch]] owns its data: Gluten's offload transition C-exports then closes each
 * batch, and the recycle callback closes it again, exactly as for [[ColumnarRangeExec]]. The reader
 * and the lance-spark handle are closed as soon as a fragment is exhausted (or on task completion
 * if the task ends early), which releases the native scan and the caller-owned stream struct.
 */
case class LanceScanTransformer(@transient batchScan: BatchScanExec)
  extends LeafExecNode
  with ValidatablePlan {

  @transient private lazy val lanceScan: LanceScan =
    batchScan.scan.asInstanceOf[LanceScan]

  @transient private lazy val inputPartitions: Array[InputPartition] =
    lanceScan.toBatch.planInputPartitions()

  // Captured eagerly: `batchScan` is transient, but `output` is also read on executors (e.g. by
  // DeserializeToObjectExec) after this node has been serialized.
  override val output: Seq[Attribute] = batchScan.output

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
   * reader) rather than offloading a scan that can only crash. The same holds for the columns
   * checked by [[hasExportableColumns]].
   */
  def isExportable(batchScan: BatchScanExec): Boolean =
    batchScan.inputPartitions.forall {
      case p: LanceInputPartition =>
        !p.getPushedAggregation.isPresent && p.getReadOptions.getFullTextQuery == null
      case _ => false
    } && hasExportableColumns(batchScan.scan.readSchema())

  /**
   * Whether the native fragment scan returns the columns of `schema` as declared, each as an Arrow
   * vector that Spark's [[ArrowColumnVector]], which each imported vector is wrapped in, reads as
   * the declared type.
   *
   * [[LanceArrowStreamScanner.export]] streams the native scan as is, and only if its schema equals
   * the one lance-spark's Spark-to-Arrow mapping ([[LanceArrowUtils]]) derives from the declared
   * schema. The native scan differs for a scan that reads no columns, where it returns `_rowid`,
   * and for the columns in [[needsLanceReader]]. Every other column arrives with its mapped type,
   * with vectors rewritten as Lists ([[FixedSizeLists]]), and ArrowColumnVector has no accessor for
   * some of these types, e.g. FixedSizeBinary, Date(MILLISECOND) and the elements of a Float16
   * vector. All of these fail at execution, after the scan was offloaded. A column's type is
   * checked by wrapping an empty vector of its field as the scan emits it, which resolves the type
   * as the imported batches will.
   */
  private def hasExportableColumns(schema: StructType): Boolean =
    schema.nonEmpty && !schema.fields.exists(needsLanceReader) &&
      // The mapping throws for types it rejects (e.g. Float16 without Arrow's Float2Vector), and
      // the export runs the same mapping.
      Try(LanceArrowUtils.toArrowSchema(schema, "UTC", true)).toOption.exists {
        arrowSchema =>
          schema.fields.zip(arrowSchema.getFields.asScala).forall {
            case (field, arrowField) =>
              isWrappable(FixedSizeLists.toListField(arrowField), field.dataType)
          }
      }

  /**
   * Whether only lance-spark's own reader returns `field` as declared, by fixing up the native scan
   * after import. The native scan returns blob columns as blob descriptions alongside `_rowaddr`,
   * and JSON columns as JSONB bytes. Lance's metadata columns, and the blob position and size
   * columns, are left out of the native scan's projection: lance-spark requests, computes or
   * reorders them itself.
   */
  private def needsLanceReader(field: StructField): Boolean =
    BlobUtils.isBlobReadColumn(field) || JsonUtils.isJsonSparkField(field) ||
      metadataColumns.contains(field.name) ||
      field.name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX) ||
      field.name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX)

  private val metadataColumns = Set(
    LanceConstant.ROW_ID,
    LanceConstant.ROW_ADDRESS,
    LanceConstant.FRAGMENT_ID,
    LanceConstant.ROW_CREATED_AT_VERSION,
    LanceConstant.ROW_LAST_UPDATED_AT_VERSION,
    LanceConstant.SCORE
  )

  private def isWrappable(field: Field, dataType: DataType): Boolean = {
    val vector = field.createVector(ArrowBufferAllocators.globalInstance())
    try {
      Try(new ArrowColumnVector(vector).dataType()).toOption.exists(
        DataType.equalsStructurally(_, dataType, ignoreNullability = true))
    } finally {
      vector.close()
    }
  }
}

/** One RDD partition per planned Lance [[InputPartition]] (one Lance fragment each). */
private class LancePartition(override val index: Int, val inputPartition: LanceInputPartition)
  extends Partition

/**
 * Executes the planned Lance partitions, importing each fragment's exported Arrow C stream with
 * Gluten's Arrow build and yielding offloadable Java Arrow batches.
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
          val fragment = new LanceFragmentIterator(fragId.intValue(), lancePartition, allocator)
          // Safety net for early task termination; normally the fragment closes itself once
          // exhausted, and close() is idempotent.
          context.addTaskCompletionListener[Unit](_ => fragment.close())
          fragment
      }

    Iterators
      .wrap(batches)
      .recyclePayload((batch: ColumnarBatch) => batch.close())
      .create()
  }
}

/**
 * Reads one Lance fragment through the Arrow C stream. Native resources are released as soon as the
 * fragment is exhausted, so a partition spanning many fragments holds at most one open scan.
 */
private class LanceFragmentIterator(
    fragId: Int,
    partition: LanceInputPartition,
    allocator: BufferAllocator)
  extends Iterator[ColumnarBatch]
  with AutoCloseable {

  // Plan + export the fragment scan on the lance-spark side; only the C-struct address crosses
  // over. wrap() views that struct with Gluten's Arrow, and importArrayStream() moves it into a
  // reader that owns and drains the native scan.
  private val handle = LanceArrowStreamScanner.export(fragId, partition)

  private val reader: ArrowReader =
    try {
      Data.importArrayStream(allocator, ArrowArrayStream.wrap(handle.streamAddress()))
    } catch {
      case t: Throwable =>
        try handle.close()
        catch { case s: Throwable => t.addSuppressed(s) }
        throw t
    }

  private var closed = false
  private var advanced = false
  private var hasMore = false

  private def advance(): Unit =
    if (!advanced) {
      hasMore = reader.loadNextBatch()
      advanced = true
      if (!hasMore) {
        close()
      }
    }

  override def hasNext: Boolean = !closed && {
    advance()
    hasMore
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    advanced = false
    val root = reader.getVectorSchemaRoot
    val rowCount = root.getRowCount
    val vectors = ArrowColumnVectors.wrap(LanceFragmentIterator.takeColumns(root, allocator))
    new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), rowCount)
  }

  override def close(): Unit =
    if (!closed) {
      closed = true
      try {
        // Drains + runs the C release callback, tearing down the native scan.
        reader.close()
      } finally {
        // Frees the caller-owned stream struct and closes the scanner + dataset.
        handle.close()
      }
    }
}

private object LanceFragmentIterator {

  /**
   * Transfers (zero-copy moves) each column out of the reader's reused `root` so the emitted batch
   * owns its buffers: it stays valid after the reader is closed, and is safe for the offload
   * transition to close. Columns holding a FixedSizeList (Lance vectors) are rebuilt as Lists on
   * the way out, still without copying their values. If a column fails, the columns already taken
   * are released.
   */
  def takeColumns(
      root: VectorSchemaRoot,
      allocator: BufferAllocator): java.util.List[FieldVector] = {
    val taken = new java.util.ArrayList[FieldVector](root.getFieldVectors.size())
    try {
      root.getFieldVectors.asScala.foreach {
        fv =>
          if (FixedSizeLists.contains(fv.getField)) {
            taken.add(FixedSizeLists.toLists(fv, allocator))
          } else {
            val pair = fv.getTransferPair(allocator)
            pair.transfer()
            taken.add(pair.getTo.asInstanceOf[FieldVector])
          }
      }
      taken
    } catch {
      case t: Throwable =>
        taken.asScala.foreach(_.close())
        throw t
    }
  }
}

/**
 * Rewrites FixedSizeList vectors, which is how Lance stores vector (embedding) columns, as
 * variable-size Lists. Neither Spark's [[org.apache.spark.sql.vectorized.ArrowColumnVector]], which
 * wraps the emitted batches, nor Velox's Arrow import supports FixedSizeList, while a List holds
 * the same values and matches the ArrayType that lance-spark declares for the column. Element `i`
 * of a FixedSizeList of size `n` is child range `[i * n, (i + 1) * n)` also for null entries, so
 * the offsets are `i * n` throughout and the child values are reused as is: only the offsets buffer
 * is new. Nested FixedSizeLists, e.g. a vector inside a struct or a list of vectors, are rewritten
 * too.
 */
private object FixedSizeLists {

  /** Whether `field` is a FixedSizeList or has one among its descendants. */
  def contains(field: Field): Boolean =
    field.getType.isInstanceOf[ArrowType.FixedSizeList] ||
      field.getChildren.asScala.exists(contains)

  /**
   * Builds a vector in `allocator` holding the data of `source`, with every FixedSizeList rewritten
   * as a List. Buffers are shared or moved rather than copied, so `source` may be left without its
   * child data; its owner still closes it.
   */
  def toLists(source: FieldVector, allocator: BufferAllocator): FieldVector = {
    val target = toListField(source.getField).createVector(allocator)
    try {
      load(source, target, allocator)
      target
    } catch {
      case t: Throwable =>
        target.close()
        throw t
    }
  }

  /** `field` with every FixedSizeList rewritten as a List, as [[toLists]] emits it. */
  def toListField(field: Field): Field =
    if (!contains(field)) {
      field
    } else {
      val fieldType = field.getType match {
        case _: ArrowType.FixedSizeList =>
          new FieldType(
            field.isNullable,
            ArrowType.List.INSTANCE,
            field.getDictionary,
            field.getMetadata)
        case _ => field.getFieldType
      }
      new Field(field.getName, fieldType, field.getChildren.asScala.map(toListField).asJava)
    }

  /** Loads `source` into `target`, whose field is `toListField(source.getField)`. */
  private def load(source: FieldVector, target: FieldVector, allocator: BufferAllocator): Unit =
    if (!contains(source.getField)) {
      // Same type on both sides: move the buffers.
      source.makeTransferPair(target).transfer()
    } else {
      val node = new ArrowFieldNode(source.getValueCount, source.getNullCount)
      source match {
        case fixed: FixedSizeListVector =>
          val width = BaseRepeatedValueVector.OFFSET_WIDTH
          val offsets = allocator.buffer((fixed.getValueCount + 1).toLong * width)
          try {
            (0 to fixed.getValueCount).foreach {
              i => offsets.setInt(i.toLong * width, i * fixed.getListSize)
            }
            target.loadFieldBuffers(node, java.util.Arrays.asList(fixed.getValidityBuffer, offsets))
          } finally {
            // loadFieldBuffers took its own reference.
            offsets.close()
          }
        case _ =>
          target.loadFieldBuffers(node, source.getFieldBuffers)
      }
      source.getChildrenFromFields.asScala.zip(target.getChildrenFromFields.asScala).foreach {
        case (s, t) => load(s, t, allocator)
      }
    }
}
