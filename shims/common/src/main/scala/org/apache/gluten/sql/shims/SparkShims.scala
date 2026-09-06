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
package org.apache.gluten.sql.shims

import org.apache.gluten.GlutenBuildInfo.SPARK_COMPILE_VERSION
import org.apache.gluten.expression.Sig

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, BinaryArithmetic, Cast, Divide, EvalMode, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, IntegralDivide, Multiply, RaiseError, SortOrder, Subtract, UnBase64}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.window.WindowGroupLimitExecShim
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.storage.{GlutenShuffleBlockFetcherIteratorBase, ShuffleBlockFetcherIteratorParams}
import org.apache.spark.util.SparkShimVersionUtil

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.metadata.{CompressionCodecName, ParquetMetadata}
import org.apache.parquet.schema.MessageType

import java.time.ZoneOffset
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

case class SparkShimDescriptor(major: Int, minor: Int, patch: Int) {
  override def toString(): String = s"$major.$minor.$patch"

  def matches(other: SparkShimDescriptor): Boolean = {
    major == other.major && minor == other.minor
  }
}

object SparkShimDescriptor {
  def apply(version: String): SparkShimDescriptor = {
    SparkShimVersionUtil.sparkMajorMinorPatchVersion(version) match {
      case Some((major, minor, patch)) => SparkShimDescriptor(major, minor, patch)
      case None =>
        val (major, minor) = SparkShimVersionUtil.sparkMajorMinorVersion(version)
        SparkShimDescriptor(major, minor, 0)
    }
  }

  // Default shim descriptor being detected from the Spark version at compile time
  val DESCRIPTOR: SparkShimDescriptor = SparkShimDescriptor(SPARK_COMPILE_VERSION)
}

trait SparkShims {

  def scalarExpressionMappings: Seq[Sig]

  def aggregateExpressionMappings: Seq[Sig]

  def runtimeReplaceableExpressionMappings: Seq[Sig]

  def generateFileScanRDD(
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

  def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]]

  def getBatchScanExecTable(batchScan: BatchScanExec): Table = batchScan.table

  def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, SparkPath.fromPathString(filePath), start, length, locations)

  def isWindowGroupLimitExec(plan: SparkPlan): Boolean = false

  def getWindowGroupLimitExecShim(plan: SparkPlan): WindowGroupLimitExecShim = null

  def getWindowGroupLimitExec(windowGroupLimitExecShim: WindowGroupLimitExecShim): SparkPlan = null

  def getLimitAndOffsetFromGlobalLimit(plan: GlobalLimitExec): (Int, Int)

  def getLimitAndOffsetFromTopK(plan: TakeOrderedAndProjectExec): (Int, Int)

  def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult

  def enableNativeWriteFilesByDefault(): Boolean

  def getV1WriteRequiredOrdering(
      outputColumns: Seq[Attribute],
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      numStaticPartitionCols: Int): Seq[SortOrder]

  def broadcastInternal[T: ClassTag](sc: SparkContext, value: T): Broadcast[T]

  // To be compatible with Spark-3.5 and later
  // See https://github.com/apache/spark/pull/41440
  def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit
  def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit

  // Compatible with Spark-3.5 and later
  def getShuffleAdvisoryPartitionSize(shuffle: ShuffleExchangeLike): Option[Long] = None

  def getFileStatus(partition: PartitionDirectory): Seq[(FileStatus, Map[String, Any])]

  def isFileSplittable(relation: HadoopFsRelation, filePath: Path, sparkSchema: StructType): Boolean

  def isRowIndexMetadataColumn(name: String): Boolean

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow,
      metadata: Map[String, Any] = Map.empty): Seq[PartitionedFile]

  def structFromAttributes(attrs: Seq[Attribute]): StructType

  def attributesFromStruct(structType: StructType): Seq[Attribute]

  def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String] = Seq.empty): Map[String, String] = {
    val requested = metadataColumnNames.toSet
    val originMetadataColumn = Seq(
      InputFileName().prettyName -> file.filePath.toString,
      InputFileBlockStart().prettyName -> file.start.toString,
      InputFileBlockLength().prettyName -> file.length.toString
    ).collect { case (name, value) if requested.contains(name) => name -> value }.toMap
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

  // For compatibility with Spark-3.5.
  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan]

  def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = {
    batchScan.keyGroupedPartitioning
  }

  def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]]

  /**
   * Most of the code in this method is copied from
   * [[org.apache.spark.sql.execution.datasources.v2.BatchScanExec.inputRDD]].
   */
  def orderPartitions(
      batchScan: DataSourceV2ScanExecBase,
      scan: Scan,
      keyGroupedPartitioning: Option[Seq[Expression]],
      filteredPartitions: Seq[Seq[InputPartition]],
      outputPartitioning: Partitioning,
      commonPartitionValues: Option[Seq[(InternalRow, Int)]],
      applyPartialClustering: Boolean,
      replicatePartitions: Boolean,
      joinKeyPositions: Option[Seq[Int]] = None): Seq[Seq[InputPartition]]

  def extractExpressionTimestampAddUnit(timestampAdd: Expression): Option[Seq[String]]

  def withTryEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.TRY
      case s: Subtract => s.evalMode == EvalMode.TRY
      case d: Divide => d.evalMode == EvalMode.TRY
      case m: Multiply => m.evalMode == EvalMode.TRY
      case c: Cast => c.evalMode == EvalMode.TRY
      case _ => false
    }
  }

  def withAnsiEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.ANSI
      case s: Subtract => s.evalMode == EvalMode.ANSI
      case d: Divide => d.evalMode == EvalMode.ANSI
      case m: Multiply => m.evalMode == EvalMode.ANSI
      case c: Cast => c.evalMode == EvalMode.ANSI
      case i: IntegralDivide => i.evalMode == EvalMode.ANSI
      case _ => false
    }
  }

  def isNullIntolerant(expr: Expression): Boolean

  def createParquetFilters(
      conf: SQLConf,
      schema: MessageType,
      caseSensitive: Option[Boolean] = None): ParquetFilters

  def extractExpressionArrayInsert(arrayInsert: Expression): Seq[Expression]

  /** Shim method for usages from GlutenExplainUtils.scala. */
  def withOperatorIdMap[T](idMap: java.util.Map[QueryPlan[_], Int])(body: => T): T = {
    body
  }

  /** Shim method for usages from GlutenExplainUtils.scala. */
  def getOperatorId(plan: QueryPlan[_]): Option[Int]

  /** Shim method for usages from GlutenExplainUtils.scala. */
  def setOperatorId(plan: QueryPlan[_], opId: Int): Unit

  /** Shim method for usages from GlutenExplainUtils.scala. */
  def unsetOperatorId(plan: QueryPlan[_]): Unit

  /**
   * Returns the streaming source associated with a [[LocalTableScanExec]], if any. The `stream`
   * field only exists on Spark 4.0+ (where `LocalTableScanExec` mixes in
   * `StreamSourceAwareSparkPlan`); on Spark 3.x local relations have no streaming concept, so the
   * default implementation returns None.
   */
  def getLocalTableScanStream(plan: LocalTableScanExec): Option[SparkDataStream] = None

  def isParquetFileEncrypted(footer: ParquetMetadata): Boolean

  def shouldFallbackForParquetVariantAnnotation(footer: ParquetMetadata): Boolean = false

  def getOtherConstantMetadataColumnValues(file: PartitionedFile): JMap[String, Object] =
    Map.empty[String, Any].asJava.asInstanceOf[JMap[String, Object]]

  def getCollectLimitOffset(plan: CollectLimitExec): Int

  def unBase64FunctionFailsOnError(unBase64: UnBase64): Boolean

  def widerDecimalType(d1: DecimalType, d2: DecimalType): DecimalType

  // Spark 4.1+ (SPARK-53968) embeds allowDecimalPrecisionLoss in each arithmetic expression's
  // evalContext at analysis time. Spark41Shims overrides this to read from the expression.
  // All earlier versions have no evalContext field, so reading SQLConf.get here is correct.
  def decimalAllowPrecisionLoss(expr: BinaryArithmetic): Boolean =
    SQLConf.get.decimalOperationsAllowPrecisionLoss

  def getRewriteCreateTableAsSelect(session: SparkSession): SparkStrategy = _ => Seq.empty

  /** Shim method for get the "errorMessage" value for Spark 4.0 and above */
  def getErrorMessage(raiseError: RaiseError): Option[Expression]

  def throwExceptionInWrite(t: Throwable, writePath: String, descriptionPath: String): Unit = {
    throw new SparkException(
      s"Task failed while writing rows to staging path: $writePath, " +
        s"output path: $descriptionPath",
      t)
  }

  // Compatibility method for Spark 4.0: rethrows the exception cause to maintain API compatibility
  def enrichWriteException(cause: Throwable, path: String): Nothing = {
    throw cause
  }

  def getFileSourceScanStream(scan: FileSourceScanExec): Option[SparkDataStream] = {
    None
  }

  def unsupportedCodec: Seq[CompressionCodecName] = {
    Seq(CompressionCodecName.LZO, CompressionCodecName.BROTLI)
  }

  /**
   * Shim layer for QueryExecution to maintain compatibility across different Spark versions.
   * @since Spark
   *   4.1
   */
  def createSparkPlan(
      sparkSession: SparkSession,
      planner: SparkPlanner,
      plan: LogicalPlan): SparkPlan

  /**
   * Checks if the given JoinType is LeftSingle. LeftSingle is a Spark 4.0+ join type, semantically
   * similar to LeftOuter. Default implementation returns false for Spark 3.x compatibility.
   */
  def isLeftSingleJoinType(joinType: JoinType): Boolean = false

  /**
   * Returns true iff the given StringType uses the UTF8_BINARY collation (id == 0).
   *
   * Spark 4.0 introduced collation-aware StringType. Bound computation in gluten cached batch
   * partition-stats uses unsigned byte order, which only matches Spark's predicate semantics for
   * UTF8_BINARY. Non-binary collations must be gated out of the dispatch fast path;
   * deserializeStats fills a sentinel bound so vanilla
   * SimpleMetricsCachedBatchSerializer.buildFilter pass-throughs them.
   *
   * Default returns true (Spark 3.x has no collation concept; all StringType is binary). Any future
   * Spark 4.0+ shim MUST override and consult collationId, otherwise the binary-only invariant
   * degrades silently to "accept any collation".
   */
  def isBinaryCollationString(dt: StringType): Boolean = true

  def getShuffleBlockFetcherIterator(params: ShuffleBlockFetcherIteratorParams)
      : GlutenShuffleBlockFetcherIteratorBase
}
