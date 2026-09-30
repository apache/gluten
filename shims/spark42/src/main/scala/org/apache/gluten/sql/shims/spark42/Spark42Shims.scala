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
package org.apache.gluten.sql.shims.spark42

import org.apache.gluten.execution.PartitionedFileUtilShim
import org.apache.gluten.expression.{ExpressionNames, Sig}
import org.apache.gluten.sql.shims.SparkShims

import org.apache.spark._
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{ExtendedAnalysisException, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftSingle}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{CollationFactory, InternalRowComparableWrapper, MapData}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, Scan}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetFilters}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.window.{Final, Partial, _}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.storage.{GlutenShuffleBlockFetcherIterator, GlutenShuffleBlockFetcherIteratorBase, ShuffleBlockFetcherIteratorParams}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.metadata.{CompressionCodecName, ParquetMetadata}
import org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType}

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._

class Spark42Shims extends SparkShims {

  override def getSampleSeed(plan: SampleExec): Long = plan.resolvedSeed

  override def isKeyGroupedPartitioning(partitioning: Partitioning): Boolean =
    partitioning.isInstanceOf[KeyedPartitioning]

  override def getLocalTableScanStream(plan: LocalTableScanExec): Option[SparkDataStream] =
    plan.stream

  override def scalarExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[Empty2Null](ExpressionNames.EMPTY2NULL),
      Sig[Mask](ExpressionNames.MASK),
      Sig[ArrayInsert](ExpressionNames.ARRAY_INSERT),
      Sig[CheckOverflowInTableInsert](ExpressionNames.CHECK_OVERFLOW_IN_TABLE_INSERT),
      Sig[ArrayAppend](ExpressionNames.ARRAY_APPEND),
      Sig[UrlEncode](ExpressionNames.URL_ENCODE),
      Sig[KnownNotContainsNull](ExpressionNames.KNOWN_NOT_CONTAINS_NULL),
      Sig[UrlDecode](ExpressionNames.URL_DECODE),
      Sig[ToPrettyString](ExpressionNames.TO_PRETTY_STRING),
      Sig[RandStr](ExpressionNames.RANDSTR),
      Sig[RegExpInStr](ExpressionNames.REGEXP_INSTR),
      Sig[DayName](ExpressionNames.DAY_NAME),
      Sig[MonthName](ExpressionNames.MONTH_NAME)
    )
  }

  override def aggregateExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[RegrSlope](ExpressionNames.REGR_SLOPE),
      Sig[RegrIntercept](ExpressionNames.REGR_INTERCEPT),
      Sig[RegrSXY](ExpressionNames.REGR_SXY),
      Sig[RegrReplacement](ExpressionNames.REGR_REPLACEMENT),
      Sig[BitmapConstructAgg](ExpressionNames.BITMAP_CONSTRUCT_AGG)
    )
  }

  override def runtimeReplaceableExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[ArrayCompact](ExpressionNames.ARRAY_COMPACT),
      Sig[ArrayPrepend](ExpressionNames.ARRAY_PREPEND),
      Sig[EqualNull](ExpressionNames.EQUAL_NULL),
      Sig[Get](ExpressionNames.GET),
      Sig[Luhncheck](ExpressionNames.LUHN_CHECK)
    )
  }

  override def isNullIntolerant(expr: Expression): Boolean = expr.nullIntolerant

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

  // https://issues.apache.org/jira/browse/SPARK-40400
  private def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Map("path" -> path),
      cause = null)
  }

  override def isWindowGroupLimitExec(plan: SparkPlan): Boolean = plan match {
    case _: WindowGroupLimitExec => true
    case _ => false
  }

  override def isEmptyRelationExec(plan: SparkPlan): Boolean = plan match {
    case _: EmptyRelationExec => true
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
    name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME ||
      name.equalsIgnoreCase("__delta_internal_is_row_deleted")

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
  override def getCommonPartitionValues(
      batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] = {
    // Spark 4.2 removed `StoragePartitionJoinParams` (and `BatchScanExec.spjParams`), so the
    // "common partition values" that a partially-clustered storage-partitioned join used to expose
    // on the scan node are no longer available here -- Spark 4.2 computes and applies them in
    // `EnsureRequirements`/`GroupPartitionsExec` instead. There is no equivalent accessor on the
    // 4.2 `BatchScanExec`, so we conservatively return `None`, which simply disables the
    // partially-clustered-distribution refinement in Gluten's own scan planner (DEGRADED: see the
    // note in `orderPartitions`). This does not affect the base (fully-clustered) SPJ path.
    None
  }

  // please ref BatchScanExec::inputRDD
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
        outputPartitioning match {
          case p: KeyedPartitioning =>
            val partExpressions = keyGroupedPartitioning.get

            // DEGRADED (Spark 4.2 port): Spark 4.2 removed `KeyGroupedPartitioning` and
            // `StoragePartitionJoinParams`, and moved the storage-partitioned-join refinements that
            // used to run here into `EnsureRequirements`/`GroupPartitionsExec`:
            //   - subset-of-join-keys projection (`joinKeyPositions`),
            //   - compatible partition-expression reduction (`reducers`),
            //   - partially-clustered replication (`commonPartitionValues` /
            //     `applyPartialClustering` / `replicatePartitions`).
            // Gluten never populates `joinKeyPositions`/`reducers`, and `getCommonPartitionValues`
            // returns `None` on 4.2, so `commonPartitionValues` is always empty here. These
            // parameters therefore have no 4.2 equivalent that can be reproduced on the scan node
            // and are intentionally NOT applied; only the base key-grouped ordering is reproduced.
            // The base (fully-clustered) SPJ path is unaffected.
            val groupedPartitions = filteredPartitions.map {
              splits =>
                assert(splits.nonEmpty && splits.head.isInstanceOf[HasPartitionKey])
                (splits.head.asInstanceOf[HasPartitionKey].partitionKey(), splits)
            }

            val partitionMapping = groupedPartitions.map {
              case (partValue, splits) =>
                InternalRowComparableWrapper(partValue, partExpressions) -> splits
            }.toMap

            // Use the unique, sorted partition keys as the canonical partition order (Spark 4.2's
            // `KeyedPartitioning.toGrouped` returns distinct keys sorted ascending), filling absent
            // keys with empty split groups so both sides of a storage-partitioned join stay
            // aligned. This mirrors the old `KeyGroupedPartitioning.uniquePartitionValues` path.
            p.toGrouped.partitionKeys.map {
              keyWrapper =>
                // Use empty partition for those partition values that are not present
                partitionMapping.getOrElse(keyWrapper, Seq.empty)
            }

          case _ => filteredPartitions
        }
      case _ =>
        filteredPartitions
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

  override def shouldFallbackForParquetVariantAnnotation(footer: ParquetMetadata): Boolean = {
    if (SQLConf.get.getConf(SQLConf.PARQUET_IGNORE_VARIANT_ANNOTATION)) {
      false
    } else {
      containsVariantAnnotation(footer.getFileMetaData.getSchema)
    }
  }

  private def containsVariantAnnotation(groupType: GroupType): Boolean = {
    groupType.getFields.asScala.exists {
      field =>
        Option(field.getLogicalTypeAnnotation)
          .exists(_.isInstanceOf[LogicalTypeAnnotation.VariantLogicalTypeAnnotation]) ||
        (!field.isPrimitive && containsVariantAnnotation(field.asGroupType()))
    }
  }

  override def getOtherConstantMetadataColumnValues(file: PartitionedFile): JMap[String, Object] =
    file.otherConstantMetadataColumnValues.asJava.asInstanceOf[JMap[String, Object]]

  override def extractExpressionTimestampAddUnit(exp: Expression): Option[Seq[String]] = {
    exp match {
      // Velox does not support quantity larger than Int.MaxValue.
      case TimestampAdd(_, LongLiteral(quantity), _, _) if quantity > Integer.MAX_VALUE =>
        Option.empty
      case timestampAdd: TimestampAdd =>
        Option.apply(Seq(timestampAdd.unit, timestampAdd.timeZoneId.getOrElse("")))
      case _ => Option.empty
    }
  }

  override def decimalAllowPrecisionLoss(expr: BinaryArithmetic): Boolean = expr match {
    case a: Add => a.evalContext.allowDecimalPrecisionLoss
    case s: Subtract => s.evalContext.allowDecimalPrecisionLoss
    case m: Multiply => m.evalContext.allowDecimalPrecisionLoss
    case d: Divide => d.evalContext.allowDecimalPrecisionLoss
    // Remainder and Pmod do not carry evalContext in Spark 4.1. They also throw
    // GlutenNotSupportException in DecimalArithmeticUtil.getResultType, so they never
    // reach Velox execution; SQLConf.get is a safe fallback for the name-lookup path.
    case _ => SQLConf.get.decimalOperationsAllowPrecisionLoss
  }

  override def getErrorMessage(raiseError: RaiseError): Option[Expression] = {
    raiseError.errorParms match {
      case CreateMap(children, _)
          if children.size == 2 && children.head.isInstanceOf[Literal]
            && children.head.asInstanceOf[Literal].value.toString == "errorMessage" =>
        Some(children(1))
      case lit: Literal if lit.value.isInstanceOf[MapData] =>
        // Constant-folded CreateMap: look up "errorMessage" in the MapData
        val mapData = lit.value.asInstanceOf[MapData]
        (0 until mapData.numElements())
          .find(i => mapData.keyArray().getUTF8String(i).toString == "errorMessage")
          .map(i => Literal(mapData.valueArray().getUTF8String(i), StringType))
      case _ => None
    }
  }

  override def throwExceptionInWrite(
      t: Throwable,
      writePath: String,
      descriptionPath: String): Unit = {
    throw t
  }

  override def enrichWriteException(cause: Throwable, path: String): Nothing = {
    GlutenFileFormatWriter.wrapWriteError(cause, path)
  }
  override def getFileSourceScanStream(scan: FileSourceScanExec): Option[SparkDataStream] = {
    scan.stream
  }

  override def unsupportedCodec: Seq[CompressionCodecName] = {
    Seq(CompressionCodecName.LZO, CompressionCodecName.BROTLI, CompressionCodecName.LZ4_RAW)
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
    QueryExecution.createSparkPlan(planner, plan)

  override def isLeftSingleJoinType(joinType: JoinType): Boolean = {
    joinType == LeftSingle
  }

  override def isBinaryCollationString(dt: StringType): Boolean =
    dt.collationId == CollationFactory.UTF8_BINARY_COLLATION_ID

  override def getShuffleBlockFetcherIterator(params: ShuffleBlockFetcherIteratorParams)
      : GlutenShuffleBlockFetcherIteratorBase = {
    new GlutenShuffleBlockFetcherIterator(
      params.context,
      params.shuffleClient,
      params.blockManager,
      params.mapOutputTracker,
      params.blocksByAddress,
      params.streamWrapper,
      params.maxBytesInFlight,
      params.maxReqsInFlight,
      params.maxBlocksInFlightPerAddress,
      params.maxReqSizeShuffleToMem,
      params.maxAttemptsOnNettyOOM,
      params.detectCorrupt,
      params.detectCorruptUseExtraMemory,
      params.checksumEnabled,
      params.checksumAlgorithm,
      params.shuffleMetrics,
      params.doBatchFetch,
      params.clock
    )
  }
}
