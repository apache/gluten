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
package org.apache.gluten.sql.shims.spark35

import org.apache.gluten.execution.PartitionedFileUtilShim
import org.apache.gluten.expression.{ExpressionNames, Sig}
import org.apache.gluten.sql.shims.SparkShims

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{ExtendedAnalysisException, InternalRow}
import org.apache.spark.sql.catalyst.analysis.DecimalPrecision
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{KeyGroupedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{InternalRowComparableWrapper, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, Scan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetFilters}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.window.{Final, GlutenFinal, GlutenPartial, Partial, WindowGroupLimitExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StructField, StructType}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.MessageType

import java.time.ZoneOffset
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

class Spark35Shims extends SparkShims {

  override def scalarExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[SplitPart](ExpressionNames.SPLIT_PART),
      Sig[Sec](ExpressionNames.SEC),
      Sig[Csc](ExpressionNames.CSC),
      Sig[KnownNullable](ExpressionNames.KNOWN_NULLABLE),
      Sig[Empty2Null](ExpressionNames.EMPTY2NULL),
      Sig[Mask](ExpressionNames.MASK),
      Sig[TimestampAdd](ExpressionNames.TIMESTAMP_ADD),
      Sig[TimestampDiff](ExpressionNames.TIMESTAMP_DIFF),
      Sig[RoundFloor](ExpressionNames.FLOOR),
      Sig[RoundCeil](ExpressionNames.CEIL),
      Sig[ArrayInsert](ExpressionNames.ARRAY_INSERT),
      Sig[CheckOverflowInTableInsert](ExpressionNames.CHECK_OVERFLOW_IN_TABLE_INSERT),
      Sig[ArrayAppend](ExpressionNames.ARRAY_APPEND),
      Sig[UrlEncode](ExpressionNames.URL_ENCODE),
      Sig[UrlDecode](ExpressionNames.URL_DECODE),
      Sig[ToPrettyString](ExpressionNames.TO_PRETTY_STRING)
    )
  }

  override def aggregateExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[RegrR2](ExpressionNames.REGR_R2),
      Sig[RegrSlope](ExpressionNames.REGR_SLOPE),
      Sig[RegrIntercept](ExpressionNames.REGR_INTERCEPT),
      Sig[RegrSXY](ExpressionNames.REGR_SXY),
      Sig[RegrReplacement](ExpressionNames.REGR_REPLACEMENT)
    )
  }

  override def runtimeReplaceableExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[ArrayCompact](ExpressionNames.ARRAY_COMPACT),
      Sig[ArrayPrepend](ExpressionNames.ARRAY_PREPEND),
      Sig[ArraySize](ExpressionNames.ARRAY_SIZE),
      Sig[EqualNull](ExpressionNames.EQUAL_NULL),
      Sig[ILike](ExpressionNames.ILIKE),
      Sig[MapContainsKey](ExpressionNames.MAP_CONTAINS_KEY),
      Sig[Get](ExpressionNames.GET),
      Sig[Luhncheck](ExpressionNames.LUHN_CHECK)
    )
  }

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
      fileSourceScanExec.fileConstantMetadataColumns
    )
  }

  override def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions
      .flatMap(p => p.files.map(f => PartitionedFileUtilShim.getPartitionedFile(f, p.values)))
      .groupBy {
        f =>
          BucketingUtils
            .getBucketId(f.toPath.getName)
            .getOrElse(throw invalidBucketFile(f.urlEncodedPath))
      }
  }

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table = batchScan.table

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, SparkPath.fromPathString(filePath), start, length, locations)

  override def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String]): Map[String, String] = {
    val originMetadataColumn = super.generateMetadataColumns(file, metadataColumnNames)
    val metadataColumn: mutable.Map[String, String] = mutable.Map(originMetadataColumn.toSeq: _*)
    val path = new Path(file.filePath.toString)
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
        case FileFormat.FILE_BLOCK_START =>
          metadataColumn += (FileFormat.FILE_BLOCK_START -> file.start.toString)
        case FileFormat.FILE_BLOCK_LENGTH =>
          metadataColumn += (FileFormat.FILE_BLOCK_LENGTH -> file.length.toString)
        case _ =>
      }
    }
    metadataColumn.toMap
  }

  // https://issues.apache.org/jira/browse/SPARK-40400
  private def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Map("path" -> path),
      cause = null)
  }

  private def getLimit(limit: Int, offset: Int): Int = {
    if (limit == -1) {
      // Only offset specified, so fetch the maximum number rows
      Int.MaxValue
    } else {
      assert(limit > offset)
      limit - offset
    }
  }

  override def getLimitAndOffsetFromGlobalLimit(plan: GlobalLimitExec): (Int, Int) = {
    (getLimit(plan.limit, plan.offset), plan.offset)
  }

  override def isWindowGroupLimitExec(plan: SparkPlan): Boolean = plan match {
    case _: WindowGroupLimitExec => true
    case _ => false
  }

  override def getWindowGroupLimitExecShim(plan: SparkPlan): WindowGroupLimitExecShim = {
    val windowGroupLimitPlan = plan.asInstanceOf[WindowGroupLimitExec]
    val mode = windowGroupLimitPlan.mode match {
      case Partial => GlutenPartial
      case Final => GlutenFinal
    }
    WindowGroupLimitExecShim(
      windowGroupLimitPlan.partitionSpec,
      windowGroupLimitPlan.orderSpec,
      windowGroupLimitPlan.rankLikeFunction,
      windowGroupLimitPlan.limit,
      mode,
      windowGroupLimitPlan.child
    )
  }

  override def getWindowGroupLimitExec(
      windowGroupLimitExecShim: WindowGroupLimitExecShim): SparkPlan = {
    val mode = windowGroupLimitExecShim.mode match {
      case GlutenPartial => Partial
      case GlutenFinal => Final
    }
    WindowGroupLimitExec(
      windowGroupLimitExecShim.partitionSpec,
      windowGroupLimitExecShim.orderSpec,
      windowGroupLimitExecShim.rankLikeFunction,
      windowGroupLimitExecShim.limit,
      mode,
      windowGroupLimitExecShim.child
    )
  }

  override def getLimitAndOffsetFromTopK(plan: TakeOrderedAndProjectExec): (Int, Int) = {
    (getLimit(plan.limit, plan.offset), plan.offset)
  }

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = List()

  override def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {
    GlutenFileFormatWriter.writeFilesExecuteTask(
      description,
      jobTrackerID,
      sparkStageId,
      sparkPartitionId,
      sparkAttemptNumber,
      committer,
      iterator
    )
  }

  override def enableNativeWriteFilesByDefault(): Boolean = true

  override def broadcastInternal[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    SparkContextUtils.broadcastInternal(sc, value)
  }

  override def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    // Setup a job tag here so later it may get cancelled by tag if necessary.
    sc.addJobTag(broadcastExchange.jobTag)
    sc.setInterruptOnCancel(true)
  }

  override def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    sc.cancelJobsWithTag(broadcastExchange.jobTag)
  }

  override def getShuffleAdvisoryPartitionSize(shuffle: ShuffleExchangeLike): Option[Long] =
    shuffle.advisoryPartitionSize

  def getFileStatus(partition: PartitionDirectory): Seq[(FileStatus, Map[String, Any])] =
    partition.files.map(f => (f.fileStatus, f.metadata))

  def isFileSplittable(
      relation: HadoopFsRelation,
      filePath: Path,
      sparkSchema: StructType): Boolean = {
    relation.fileFormat
      .isSplitable(relation.sparkSession, relation.options, filePath)
  }

  def isRowIndexMetadataColumn(name: String): Boolean =
    name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int = {
    sparkSchema.fields.zipWithIndex.find {
      case (field: StructField, _: Int) =>
        field.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
    } match {
      case Some((field: StructField, idx: Int)) =>
        if (field.dataType != LongType && field.dataType != IntegerType) {
          throw new RuntimeException(
            s"${ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} " +
              "must be of LongType or IntegerType")
        }
        idx
      case _ => -1
    }
  }

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow,
      metadata: Map[String, Any] = Map.empty): Seq[PartitionedFile] = {
    PartitionedFileUtilShim.splitFiles(
      sparkSession,
      FileStatusWithMetadata(file, metadata),
      isSplitable,
      maxSplitBytes,
      partitionValues)
  }

  def structFromAttributes(attrs: Seq[Attribute]): StructType = {
    DataTypeUtils.fromAttributes(attrs)
  }

  def attributesFromStruct(structType: StructType): Seq[Attribute] = {
    DataTypeUtils.toAttributes(structType)
  }

  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] = {
    ae match {
      case eae: ExtendedAnalysisException =>
        eae.plan
      case _ =>
        None
    }
  }
  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = {
    batchScan.keyGroupedPartitioning
  }

  override def getCommonPartitionValues(
      batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] = {
    batchScan.spjParams.commonPartitionValues
  }

  override def orderPartitions(
      batchScan: DataSourceV2ScanExecBase,
      scan: Scan,
      keyGroupedPartitioning: Option[Seq[Expression]],
      filteredPartitions: Seq[Seq[InputPartition]],
      outputPartitioning: Partitioning,
      commonPartitionValues: Option[Seq[(InternalRow, Int)]],
      applyPartialClustering: Boolean,
      replicatePartitions: Boolean,
      joinKeyPositions: Option[Seq[Int]] = None): Seq[Seq[InputPartition]] = {
    scan match {
      case _ if keyGroupedPartitioning.isDefined =>
        var finalPartitions = filteredPartitions

        outputPartitioning match {
          case p: KeyGroupedPartitioning =>
            if (
              SQLConf.get.v2BucketingPushPartValuesEnabled &&
              SQLConf.get.v2BucketingPartiallyClusteredDistributionEnabled
            ) {
              assert(
                filteredPartitions.forall(_.size == 1),
                "Expect partitions to be not grouped when " +
                  s"${SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key} " +
                  "is enabled"
              )

              val groupedPartitions = batchScan
                .groupPartitions(finalPartitions.map(_.head), true)
                .getOrElse(Seq.empty)

              // This means the input partitions are not grouped by partition values. We'll need to
              // check `groupByPartitionValues` and decide whether to group and replicate splits
              // within a partition.
              if (commonPartitionValues.isDefined && applyPartialClustering) {
                // A mapping from the common partition values to how many splits the partition
                // should contain. Note this no longer maintain the partition key ordering.
                val commonPartValuesMap = commonPartitionValues.get
                  .map(t => (InternalRowComparableWrapper(t._1, p.expressions), t._2))
                  .toMap
                val nestGroupedPartitions = groupedPartitions.map {
                  case (partValue, splits) =>
                    // `commonPartValuesMap` should contain the part value since it's the super set.
                    val numSplits = commonPartValuesMap
                      .get(InternalRowComparableWrapper(partValue, p.expressions))
                    assert(
                      numSplits.isDefined,
                      s"Partition value $partValue does not exist in " +
                        "common partition values from Spark plan")

                    val newSplits = if (replicatePartitions) {
                      // We need to also replicate partitions according to the other side of join
                      Seq.fill(numSplits.get)(splits)
                    } else {
                      // Not grouping by partition values: this could be the side with partially
                      // clustered distribution. Because of dynamic filtering, we'll need to check
                      // if the final number of splits of a partition is smaller than the original
                      // number, and fill with empty splits if so. This is necessary so that both
                      // sides of a join will have the same number of partitions & splits.
                      splits.map(Seq(_)).padTo(numSplits.get, Seq.empty)
                    }
                    (InternalRowComparableWrapper(partValue, p.expressions), newSplits)
                }

                // Now fill missing partition keys with empty partitions
                val partitionMapping = nestGroupedPartitions.toMap
                finalPartitions = commonPartitionValues.get.flatMap {
                  case (partValue, numSplits) =>
                    // Use empty partition for those partition values that are not present.
                    partitionMapping.getOrElse(
                      InternalRowComparableWrapper(partValue, p.expressions),
                      Seq.fill(numSplits)(Seq.empty))
                }
              } else {
                // either `commonPartitionValues` is not defined, or it is defined but
                // `applyPartialClustering` is false.
                val partitionMapping = groupedPartitions.map {
                  case (row, parts) =>
                    InternalRowComparableWrapper(row, p.expressions) -> parts
                }.toMap

                // In case `commonPartitionValues` is not defined (e.g., SPJ is not used), there
                // could exist duplicated partition values, as partition grouping is not done
                // at the beginning and postponed to this method. It is important to use unique
                // partition values here so that grouped partitions won't get duplicated.
                finalPartitions = p.uniquePartitionValues.map {
                  partValue =>
                    // Use empty partition for those partition values that are not present
                    partitionMapping.getOrElse(
                      InternalRowComparableWrapper(partValue, p.expressions),
                      Seq.empty)
                }
              }
            } else {
              val partitionMapping = finalPartitions.map {
                parts =>
                  val row = parts.head.asInstanceOf[HasPartitionKey].partitionKey()
                  InternalRowComparableWrapper(row, p.expressions) -> parts
              }.toMap
              finalPartitions = p.partitionValues.map {
                partValue =>
                  // Use empty partition for those partition values that are not present
                  partitionMapping.getOrElse(
                    InternalRowComparableWrapper(partValue, p.expressions),
                    Seq.empty)
              }
            }

          case _ =>
        }
        finalPartitions
      case _ =>
        filteredPartitions
    }
  }

  override def withTryEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.TRY
      case s: Subtract => s.evalMode == EvalMode.TRY
      case d: Divide => d.evalMode == EvalMode.TRY
      case m: Multiply => m.evalMode == EvalMode.TRY
      case c: Cast => c.evalMode == EvalMode.TRY
      case _ => false
    }
  }

  override def withAnsiEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.ANSI
      case s: Subtract => s.evalMode == EvalMode.ANSI
      case d: Divide => d.evalMode == EvalMode.ANSI
      case m: Multiply => m.evalMode == EvalMode.ANSI
      case i: IntegralDivide => i.evalMode == EvalMode.ANSI
      case _ => false
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
      conf.parquetFilterPushDownStringPredicate,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED)
    )
  }

  override def extractExpressionArrayInsert(arrayInsert: Expression): Seq[Expression] = {
    val expr = arrayInsert.asInstanceOf[ArrayInsert]
    Seq(expr.srcArrayExpr, expr.posExpr, expr.itemExpr, Literal(expr.legacyNegativeIndex))
  }

  override def withOperatorIdMap[T](idMap: java.util.Map[QueryPlan[_], Int])(body: => T): T = {
    val prevIdMap = QueryPlan.localIdMap.get()
    try {
      QueryPlan.localIdMap.set(idMap)
      body
    } finally {
      QueryPlan.localIdMap.set(prevIdMap)
    }
  }

  override def getOperatorId(plan: QueryPlan[_]): Option[Int] = {
    Option(QueryPlan.localIdMap.get().get(plan))
  }

  override def setOperatorId(plan: QueryPlan[_], opId: Int): Unit = {
    val map = QueryPlan.localIdMap.get()
    assert(!map.containsKey(plan))
    map.put(plan, opId)
  }

  override def unsetOperatorId(plan: QueryPlan[_]): Unit = {
    QueryPlan.localIdMap.get().remove(plan)
  }

  override def isParquetFileEncrypted(footer: ParquetMetadata): Boolean = {
    footer.getFileMetaData.getEncryptionType match {
      // UNENCRYPTED file has a plaintext footer and no file encryption,
      // We can leverage file metadata for this check and return unencrypted.
      case EncryptionType.UNENCRYPTED =>
        false
      // PLAINTEXT_FOOTER has a plaintext footer however the file is encrypted.
      // In such cases, read the footer and use the metadata for encryption check.
      case EncryptionType.PLAINTEXT_FOOTER =>
        true
      case _ =>
        false
    }
  }

  override def getOtherConstantMetadataColumnValues(file: PartitionedFile): JMap[String, Object] =
    file.otherConstantMetadataColumnValues.asJava.asInstanceOf[JMap[String, Object]]

  override def getCollectLimitOffset(plan: CollectLimitExec): Int = {
    plan.offset
  }

  override def unBase64FunctionFailsOnError(unBase64: UnBase64): Boolean = unBase64.failOnError

  override def extractExpressionTimestampAddUnit(exp: Expression): Option[Seq[String]] = {
    exp match {
      case timestampAdd: TimestampAdd =>
        Option.apply(Seq(timestampAdd.unit, timestampAdd.timeZoneId.getOrElse("")))
      case _ => Option.empty
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
    p.isFinalPlan
  }
}
