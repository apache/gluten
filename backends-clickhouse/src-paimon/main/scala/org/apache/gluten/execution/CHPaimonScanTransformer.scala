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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.paimon.CoreOptions
import org.apache.paimon.io.DataOutputSerializer
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import scala.collection.JavaConverters._

case class CHPaimonScanTransformer(
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

  private lazy val paimonScanTable: FileStoreTable = scan match {
    case paimonScan: PaimonScan =>
      paimonScan.table match {
        case dataTable: FileStoreTable => dataTable
        case _ => throw new GlutenNotSupportException("Only support Paimon DataTable.")
      }
    case _ => throw new GlutenNotSupportException("Only support PaimonScan.")
  }

  private lazy val coreOptions: CoreOptions = paimonScanTable.coreOptions()

  // Paimon folds query options into the scan table, including its schema options.
  private[execution] lazy val effectiveReadOptions: Map[String, String] =
    paimonScanTable.options().asScala.toMap

  override def getPartitionSchema: StructType = scan match {
    case paimonScan: PaimonScan =>
      val partitionKeys = paimonScan.table.partitionKeys()
      StructType(scan.readSchema().filter(field => partitionKeys.contains(field.name)))
    case _ => throw new GlutenNotSupportException("Only support PaimonScan.")
  }

  override def getDataSchema: StructType = scan match {
    case paimonScan: PaimonScan =>
      val partitionKeys = paimonScan.table.partitionKeys()
      StructType(scan.readSchema().filterNot(field => partitionKeys.contains(field.name)))
    case _ => throw new GlutenNotSupportException("Only support PaimonScan.")
  }

  override def withNewPushdownFilters(filters: Seq[Expression]): CHPaimonScanTransformer =
    copy(pushDownFilters = Some(filters))

  override def withOutput(newOutput: Seq[AttributeReference]): BatchScanExecTransformerBase =
    copy(output = newOutput)

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
          case _: FileStoreTable =>
            if (!paimonScan.inputSplits.forall(_.isInstanceOf[DataSplit])) {
              return ValidationResult.failed("Native Paimon reader only supports DataSplit.")
            }
          case table =>
            return ValidationResult.failed(
              s"Not support Paimon ${table.getClass.getSimpleName} table.")
        }
      case _ => return ValidationResult.failed("Only support PaimonScan.")
    }
    super.doValidateInternal()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = throw new UnsupportedOperationException()

  override def getSplitInfosFromPartitions(
      partitions: Seq[(Partition, ReadFileFormat)]): Seq[SplitInfo] = {
    partitions.map { case (partition, _) =>
      partition match {
        case p: SparkDataSourceRDDPartition =>
          val splits = p.inputPartitions.flatMap {
            case input: PaimonInputPartition =>
              input.splits.map {
                case split: DataSplit =>
                  val out = new DataOutputSerializer(1024)
                  split.serialize(out)
                  out.getCopyOfBuffer
                case other =>
                  throw new GlutenNotSupportException(s"Unsupported Paimon split: $other")
              }
            case other =>
              throw new GlutenNotSupportException(s"Unsupported input partition type: $other")
          }
          val node = PaimonLocalFilesBuilder.makeNativePaimonLocalFiles(
            p.index,
            coreOptions.path().toString,
            coreOptions.branch(),
            effectiveReadOptions.asJava,
            splits.asJava,
            p.preferredLocations().toList.asJava)
          node.setFileSchema(getDataSchema)
          node
        case _ => throw new GlutenNotSupportException()
      }
    }
  }

  override def doCanonicalize(): CHPaimonScanTransformer = {
    copy(
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

object CHPaimonScanTransformer {
  def apply(batchScan: BatchScanExec): CHPaimonScanTransformer = {
    new CHPaimonScanTransformer(
      batchScan.output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = batchScan.table,
      keyGroupedPartitioning = batchScan.keyGroupedPartitioning,
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan))
  }
}
