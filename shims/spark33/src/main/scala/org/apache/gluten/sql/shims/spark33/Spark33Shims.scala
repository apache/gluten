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
package org.apache.gluten.sql.shims.spark33

import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.expression.{ExpressionNames, Sig}
import org.apache.gluten.sql.shims.SparkShims
import org.apache.gluten.utils.ExceptionUtils

import org.apache.spark._
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.DecimalPrecision
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil, QueryExecution, SparkPlan, SparkPlanner}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.crypto.ParquetCryptoRuntimeException
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.MessageType

import java.time.ZoneOffset

import scala.collection.mutable

class Spark33Shims extends SparkShims {

  override def scalarExpressionMappings: Seq[Sig] = {
    Seq(Sig[Empty2Null](ExpressionNames.EMPTY2NULL))
  }

  override def aggregateExpressionMappings: Seq[Sig] = Seq()

  override def runtimeReplaceableExpressionMappings: Seq[Sig] = Seq()

  override def isNullIntolerant(expr: Expression): Boolean = expr.isInstanceOf[NullIntolerant]

  override def generateFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      fileSourceScanExec: FileSourceScanExec): FileScanRDD = {
    new FileScanRDD(
      sparkSession,
      readFunction,
      filePartitions,
      new StructType(
        fileSourceScanExec.requiredSchema.fields ++
          fileSourceScanExec.relation.partitionSchema.fields),
      fileSourceScanExec.metadataColumns
    )
  }

  override def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions
      .flatMap {
        p => p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values))
      }
      .groupBy {
        f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath).getName)
            .getOrElse(throw invalidBucketFile(f.filePath))
      }
  }

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table = null

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, filePath, start, length, locations)

  override def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String]): Map[String, String] = {
    val originMetadataColumn = super.generateMetadataColumns(file, metadataColumnNames)
    val metadataColumn: mutable.Map[String, String] = mutable.Map(originMetadataColumn.toSeq: _*)
    val path = new Path(file.filePath)
    for (columnName <- metadataColumnNames) {
      columnName match {
        case FileFormat.FILE_PATH => metadataColumn += (FileFormat.FILE_PATH -> path.toString)
        case FileFormat.FILE_NAME => metadataColumn += (FileFormat.FILE_NAME -> path.getName)
        case FileFormat.FILE_SIZE =>
          metadataColumn += (FileFormat.FILE_SIZE -> file.fileSize.toString)
        case FileFormat.FILE_MODIFICATION_TIME =>
          val fileModifyTime = TimestampFormatter
            .getFractionFormatter(ZoneOffset.UTC)
            .format(file.modificationTime * 1000L)
          metadataColumn += (FileFormat.FILE_MODIFICATION_TIME -> fileModifyTime)
        case _ =>
      }
    }
    metadataColumn.toMap
  }

  private def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Array(path),
      cause = null)
  }

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(session => GlutenFormatFactory.getExtendedColumnarPostRule(session))
  }

  def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    // Setup a job group here so later it may get cancelled by groupId if necessary.
    sc.setJobGroup(
      broadcastExchange.runId.toString,
      s"broadcast exchange (runId ${broadcastExchange.runId})",
      interruptOnCancel = true)
  }

  def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    sc.cancelJobGroup(broadcastExchange.runId.toString)
  }

  def getFileStatus(partition: PartitionDirectory): Seq[(FileStatus, Map[String, Any])] =
    partition.files.map(f => (f, Map.empty[String, Any]))

  def isFileSplittable(
      relation: HadoopFsRelation,
      filePath: Path,
      sparkSchema: StructType): Boolean = true

  def isRowIndexMetadataColumn(name: String): Boolean = false

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int = -1

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow,
      metadata: Map[String, Any] = Map.empty): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(
      sparkSession,
      file,
      filePath,
      isSplitable,
      maxSplitBytes,
      partitionValues)
  }

  def structFromAttributes(attrs: Seq[Attribute]): StructType = {
    StructType(attrs.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  def attributesFromStruct(structType: StructType): Seq[Attribute] = {
    structType.fields.map {
      field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }
  }

  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] = {
    ae.plan
  }

  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = {
    batchScan.keyGroupedPartitioning
  }

  override def extractExpressionTimestampAddUnit(exp: Expression): Option[Seq[String]] = {
    exp match {
      case timestampAdd: TimestampAdd =>
        Option.apply(Seq(timestampAdd.unit, timestampAdd.timeZoneId.getOrElse("")))
      case _ => Option.empty
    }
  }

  override def createParquetFilters(
      conf: SQLConf,
      schema: MessageType,
      caseSensitive: Option[Boolean] = None): ParquetFilters = {
    new ParquetFilters(
      schema,
      conf.parquetFilterPushDownDate,
      conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal,
      conf.parquetFilterPushDownStringStartWith,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED)
    )
  }

  override def getOperatorId(plan: QueryPlan[_]): Option[Int] = {
    plan.getTagValue(QueryPlan.OP_ID_TAG)
  }

  override def setOperatorId(plan: QueryPlan[_], opId: Int): Unit = {
    plan.setTagValue(QueryPlan.OP_ID_TAG, opId)
  }

  override def unsetOperatorId(plan: QueryPlan[_]): Unit = {
    plan.unsetTagValue(QueryPlan.OP_ID_TAG)
  }
  override def isParquetFileEncrypted(footer: ParquetMetadata): Boolean = {
    try {
      footer.toString
      false
    } catch {
      case e: Exception if ExceptionUtils.hasCause(e, classOf[ParquetCryptoRuntimeException]) =>
        true
      case e: Throwable =>
        e.printStackTrace()
        false
    }
  }

  override def extractExpressionTimestampDiffUnit(exp: Expression): Option[String] = {
    exp match {
      case timestampDiff: TimestampDiff =>
        Some(timestampDiff.unit)
      case _ => Option.empty
    }
  }

  override def widerDecimalType(d1: DecimalType, d2: DecimalType): DecimalType = {
    DecimalPrecision.widerDecimalType(d1, d2)
  }

  override def getErrorMessage(raiseError: RaiseError): Option[Expression] = {
    Some(raiseError.child)
  }

  /**
   * Shim layer for QueryExecution to maintain compatibility across different Spark versions.
   *
   * @since Spark
   *   4.1
   */
  override def createSparkPlan(
      sparkSession: SparkSession,
      planner: SparkPlanner,
      plan: LogicalPlan): SparkPlan =
    QueryExecution.createSparkPlan(sparkSession, planner, plan)

  override def isFinalAdaptivePlan(p: AdaptiveSparkPlanExec): Boolean = {
    val args = p.argString(Int.MaxValue)
    val index = args.indexOf("isFinalPlan=")
    assert(index >= 0)
    args.substring(index + "isFinalPlan=".length).trim.toBoolean
  }
}
