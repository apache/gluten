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

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.{PaimonLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan}
import org.apache.paimon.table.{DataTable, FileStoreTable}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.InternalRowPartitionComputer

import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class PaimonScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    override val pushDownFilters: Option[Seq[Expression]] = None)
  extends BatchScanExecTransformerBase(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues
  ) {

  private lazy val coreOptions: CoreOptions = scan match {
    case scan: PaimonScan =>
      scan.table match {
        case dataTable: DataTable =>
          dataTable.coreOptions()
        case _ =>
          throw new GlutenNotSupportException("Only support Paimon DataTable.")
      }
    case _ =>
      throw new GlutenNotSupportException("Only support PaimonScan.")
  }

  override def getPartitionSchema: StructType = scan match {
    case paimonScan: PaimonScan =>
      val partitionKeys = paimonScan.table.partitionKeys()
      StructType(scan.readSchema().filter(field => partitionKeys.contains(field.name)))
    case _ =>
      throw new GlutenNotSupportException("Only support PaimonScan.")
  }

  override def getDataSchema: StructType = new StructType()

  override def withNewPushdownFilters(filters: Seq[Expression]): PaimonScanTransformer = {
    this.copy(pushDownFilters = Some(filters))
  }

  override def withOutput(newOutput: Seq[AttributeReference]): BatchScanExecTransformerBase = {
    this.copy(output = newOutput)
  }

  override lazy val fileFormat: ReadFileFormat = {
    val formatStr = coreOptions.fileFormatString()
    if ("parquet".equalsIgnoreCase(formatStr)) {
      ReadFileFormat.ParquetReadFormat
    } else if ("orc".equalsIgnoreCase(formatStr)) {
      ReadFileFormat.OrcReadFormat
    } else {
      ReadFileFormat.UnknownFormat
    }
  }

  override def doValidateInternal(): ValidationResult = {
    scan match {
      case paimonScan: PaimonScan =>
        paimonScan.table match {
          case table: FileStoreTable =>
            if (fileFormat == ReadFileFormat.UnknownFormat) {
              return ValidationResult.failed("Only support parquet/orc Paimon table.")
            }
            if (!table.primaryKeys().isEmpty || coreOptions.deletionVectorsEnabled()) {
              return ValidationResult.failed("Not support Paimon PK/DV table.")
            }
          case table =>
            return ValidationResult.failed(
              s"Not support Paimon ${table.getClass.getSimpleName} table.")
        }
      case _ =>
        return ValidationResult.failed("Only support PaimonScan.")
    }
    super.doValidateInternal()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = throw new UnsupportedOperationException()

  override def getSplitInfosFromPartitions(
      partitions: Seq[(Partition, ReadFileFormat)]): Seq[SplitInfo] = {
    val partitionComputer = PaimonScanTransformer.getRowDataPartitionComputer(scan)
    partitions.map { case (partition, _) => partitionToSplitInfo(partition, partitionComputer) }
  }

  private def partitionToSplitInfo(
      partition: Partition,
      partitionComputer: InternalRowPartitionComputer): SplitInfo = {
    partition match {
      case p: SparkDataSourceRDDPartition =>
        val paths = mutable.ListBuffer.empty[String]
        val starts = mutable.ListBuffer.empty[JLong]
        val lengths = mutable.ListBuffer.empty[JLong]
        val partitionColumns = mutable.ListBuffer.empty[JMap[String, String]]

        p.inputPartitions.foreach {
          case partition: PaimonInputPartition =>
            partition.splits.foreach {
              split =>
                val rawFilesOpt = split.convertToRawFiles()
                if (rawFilesOpt.isPresent) {
                  val partitionRow = split.asInstanceOf[DataSplit].partition()
                  val partitionCols = partitionComputer.generatePartValues(partitionRow)
                  val rawFiles = rawFilesOpt.get().asScala
                  paths ++= rawFiles.map(_.path())
                  starts ++= rawFiles.map(file => JLong.valueOf(file.offset()))
                  lengths ++= rawFiles.map(file => JLong.valueOf(file.length()))
                  partitionColumns ++= mutable.ArrayBuffer.fill(rawFiles.size)(partitionCols)
                } else {
                  throw new GlutenNotSupportException(
                    "Cannot get raw files from paimon SparkInputPartition.")
                }
            }
          case o =>
            throw new GlutenNotSupportException(s"Unsupported input partition type: $o")
        }

        PaimonLocalFilesBuilder.makePaimonLocalFiles(
          p.index,
          paths.asJava,
          starts.asJava,
          lengths.asJava,
          partitionColumns.asJava,
          fileFormat,
          SoftAffinity
            .getFilePartitionLocations(paths.toArray, p.preferredLocations())
            .toList
            .asJava,
          new JHashMap[String, String]()
        )
      case _ => throw new GlutenNotSupportException()
    }
  }

  override def doCanonicalize(): PaimonScanTransformer = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output),
      pushDownFilters = pushDownFilters.map(QueryPlan.normalizePredicates(_, output))
    )
  }

  override protected[this] def supportsBatchScan(scan: Scan): Boolean =
    PaimonScanTransformer.supportsBatchScan(scan)
}

object PaimonScanTransformer {
  def apply(batchScan: BatchScanExec): PaimonScanTransformer = {
    new PaimonScanTransformer(
      batchScan.output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan),
      keyGroupedPartitioning = SparkShimLoader.getSparkShims.getKeyGroupedPartitioning(batchScan),
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan)
    )
  }

  private def getRowDataPartitionComputer(scan: Scan): InternalRowPartitionComputer = {
    scan match {
      case paimonScan: PaimonScan =>
        val table = paimonScan.table.asInstanceOf[FileStoreTable]
        // use __HIVE_DEFAULT_PARTITION__ because velox using this
        new InternalRowPartitionComputer(
          ExternalCatalogUtils.DEFAULT_PARTITION_NAME,
          table.schema().logicalPartitionType(),
          table.partitionKeys.asScala.toArray,
          false
        )
      case _ =>
        throw new GlutenNotSupportException("Only support PaimonScan.")
    }
  }

  def supportsBatchScan(scan: Scan): Boolean = {
    scan.getClass == classOf[PaimonScan]
  }
}
