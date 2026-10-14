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
package org.apache.iceberg.spark.source

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.{BaseTable, BatchScan, FileFormat, FileScanTask, MetadataColumns, PlanningMode, ScanTask, Snapshot, SnapshotSummary}
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.spark.{SparkReadConf, SparkReadOptions, SparkSchemaUtil}
import org.apache.iceberg.util.TableScanUtil

import java.io.{Closeable, IOException, UncheckedIOException}
import java.util.{Collections, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Owns encoded native work and scalar statistics, without filling Iceberg's task caches. The
 * ordinary Batch interface remains a lazy, pinned Iceberg fallback. Only the native transformer
 * requests nativeBatch, so a later physical-plan fallback can still use Spark.
 */
class CompactIcebergBatchScan private (
    spark: SparkSession,
    original: SparkBatchQueryScan,
    pinnedScan: BatchScan,
    readConf: SparkReadConf,
    private val snapshot: Snapshot,
    val partitionSchema: StructType)
  extends SparkBatchQueryScan(
    spark,
    original.table(),
    pinnedScan,
    readConf,
    original.expectedSchema(),
    original.filterExpressions(),
    () => null) with CompactIcebergScan {

  private case class PlanningResult(partitions: Array[InputPartition], rows: Long)
  private var result: Option[PlanningResult] = None
  private var useLegacy = false

  // Never retain partially encoded work if planning fails or an unsupported task is encountered.
  private def plan(): Option[PlanningResult] = synchronized {
    if (useLegacy || result.nonEmpty) {
      return result
    }
    if (Thread.currentThread().isInterrupted) {
      throw new InterruptedException("Iceberg task encoding was interrupted")
    }
    val encoded = ArrayBuffer.empty[InputPartition]
    var rows = 0L
    val raw = pinnedScan.planFiles()
    var owner: Closeable = raw
    try {
      try {
        val files = CloseableIterable.transform[ScanTask, FileScanTask](
          raw.asInstanceOf[CloseableIterable[ScanTask]],
          task => {
            if (!task.isFileScanTask || task.asFileScanTask().isDataTask) {
              throw new CompactIcebergBatchScan.UnsupportedTask
            }
            val file = task.asFileScanTask()
            if (file.file().format() != FileFormat.PARQUET || !file.deletes().isEmpty) {
              throw new CompactIcebergBatchScan.UnsupportedTask
            }
            file
          }
        )
        val groups = TableScanUtil.planTaskGroups(
          files,
          pinnedScan.targetSplitSize(),
          pinnedScan.splitLookback(),
          pinnedScan.splitOpenFileCost())
        // The grouping iterable owns the file iterable. Close that chain exactly once;
        // Iceberg's scan timer and metrics reporter are not idempotently closeable.
        owner = groups
        groups.asScala.foreach {
          group =>
            if (Thread.currentThread().isInterrupted) {
              throw new InterruptedException("Iceberg task encoding was interrupted")
            }
            encoded += new CompactIcebergInputPartition(
              GlutenIcebergSourceUtil.encodeTaskGroup(group, encoded.size, partitionSchema),
              group.tasks().size())
            rows = Math.addExact(rows, group.estimatedRowsCount())
        }
      } finally {
        owner.close()
      }
      // Coalescing changes the partition index encoded in each file item. Keep the existing
      // planner for such scans until encoded batches can be regrouped without decoding them.
      if (encoded.size > GlutenConfig.get.batchScanMaxInputPartitions) {
        useLegacy = true
      } else {
        result = Some(PlanningResult(encoded.toArray, rows))
      }
      result
    } catch {
      case _: CompactIcebergBatchScan.UnsupportedTask =>
        useLegacy = true
        None
      case e: IOException => throw new UncheckedIOException(e)
    }
  }

  def usesCompactPlanning: Boolean = plan().nonEmpty

  private def fallbackBatch(): Batch = synchronized {
    // A native Batch may already refer to the published result. Keep it immutable even if
    // another physical alternative requests a vanilla reader for this scan.
    super.toBatch()
  }

  // Spark may ask for a Batch before deciding whether the scan can be offloaded. Merely
  // obtaining the Batch must not populate the legacy caches.
  private lazy val vanillaBatch: Batch = new Batch {
    private lazy val delegate = fallbackBatch()
    override def planInputPartitions(): Array[InputPartition] = delegate.planInputPartitions()
    override def createReaderFactory(): PartitionReaderFactory = delegate.createReaderFactory()
  }

  override def toBatch(): Batch = vanillaBatch

  private lazy val encodedBatch: Batch = synchronized {
    plan() match {
      case Some(planned) => new Batch {
          override def planInputPartitions(): Array[InputPartition] = planned.partitions.clone()
          override def createReaderFactory(): PartitionReaderFactory =
            CompactIcebergBatchScan.NativeReaderFactory
        }
      case None => vanillaBatch
    }
  }

  def nativeBatch(): Batch = encodedBatch

  override def estimateStatistics(): Statistics = synchronized {
    if (useLegacy) {
      return super.estimateStatistics(snapshot)
    }
    if (!table().spec().isUnpartitioned && filterExpressions().isEmpty) {
      // Preserve Iceberg's cheap snapshot-summary path: do not plan files for this estimate.
      val rows = snapshot.summary().getOrDefault(
        SnapshotSummary.TOTAL_RECORDS_PROP,
        Long.MaxValue.toString).toLong
      return new Stats(
        SparkSchemaUtil.estimateSize(readSchema(), rows),
        rows,
        Collections.emptyMap())
    }
    plan() match {
      case Some(planned) =>
        new Stats(
          SparkSchemaUtil.estimateSize(readSchema(), planned.rows),
          planned.rows,
          Collections.emptyMap())
      case None => super.estimateStatistics(snapshot)
    }
  }

  override def outputPartitioning(): Partitioning = plan() match {
    case Some(planned) => new UnknownPartitioning(planned.partitions.length)
    case None => super.outputPartitioning()
  }

  override def filterAttributes(): Array[NamedReference] = Array.empty

  override def filter(filters: Array[Predicate]): Unit = {
    require(filters.isEmpty, "Runtime filters must use the ordinary Iceberg scan")
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = original.reportDriverMetrics()

  override def equals(other: Any): Boolean = other match {
    case that: CompactIcebergBatchScan =>
      super.equals(that) && snapshot.snapshotId() == that.snapshot.snapshotId()
    case _ => false
  }

  override def hashCode(): Int =
    31 * super.hashCode() + java.lang.Long.hashCode(snapshot.snapshotId())
}

object CompactIcebergBatchScan {
  private class UnsupportedTask extends RuntimeException

  private object NativeReaderFactory extends PartitionReaderFactory {
    override def supportColumnarReads(partition: InputPartition): Boolean = true
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
      throw new UnsupportedOperationException("Encoded splits are read by the native transformer")
    override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
      throw new UnsupportedOperationException("Encoded splits are read by the native transformer")
  }

  def wrap(spark: SparkSession, scan: Scan, options: JMap[String, String]): Scan = scan match {
    case original: SparkBatchQueryScan if original.getClass == classOf[SparkBatchQueryScan] =>
      val table = original.table()
      val conf = new SparkReadConf(spark, table, original.branch(), options)
      val eligible = original.scan().isInstanceOf[BatchScan] &&
        table.isInstanceOf[BaseTable] && table.specs().size() == 1 && table.schemas().size() == 1 &&
        table.asInstanceOf[BaseTable].operations().current().formatVersion() < 3 &&
        !conf.preserveDataGrouping() && !conf.localityEnabled() &&
        (!conf.adaptiveSplitSizeEnabled() || conf.splitSizeOption() != null) &&
        conf.dataPlanningMode() == PlanningMode.LOCAL &&
        conf.branch() == null && conf.tag() == null &&
        !spark.sessionState.conf.cboEnabled &&
        !GlutenIcebergSourceUtil.hasVendedCredentials(original) &&
        original.expectedSchema().columns().asScala.forall(
          f =>
            f.`type`().isPrimitiveType && !MetadataColumns.isMetadataColumn(f.fieldId()))
      if (!eligible) {
        return scan
      }
      val batch = original.scan().asInstanceOf[BatchScan]
      val snapshot = batch.snapshot()
      if (snapshot == null) {
        return scan
      }
      // Use the same selected snapshot for statistics, native work, and any later fallback.
      val pinned = if (original.snapshotId() != null || conf.asOfTimestamp() != null) {
        batch
      } else {
        batch.useSnapshot(snapshot.snapshotId())
      }
      val fixedOptions = new JHashMap[String, String]()
      options.asScala.foreach {
        case (key, value) =>
          fixedOptions.put(key.toLowerCase(java.util.Locale.ROOT), value)
      }
      fixedOptions.remove(SparkReadOptions.AS_OF_TIMESTAMP)
      fixedOptions.put(SparkReadOptions.SNAPSHOT_ID, snapshot.snapshotId().toString)
      val fixedConf = new SparkReadConf(spark, table, null, fixedOptions)
      val schema =
        try {
          GlutenIcebergSourceUtil.readPartitionSchema(
            table.spec(),
            table.spec(),
            original.readSchema())
        } catch {
          case _: UnsupportedOperationException => return scan
        }
      new CompactIcebergBatchScan(spark, original, pinned, fixedConf, snapshot, schema)
    case _ => scan
  }
}
