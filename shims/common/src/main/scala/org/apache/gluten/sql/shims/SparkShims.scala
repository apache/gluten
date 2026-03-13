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
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, RaiseError, UnBase64}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.window.WindowGroupLimitExecShim
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.util.SparkShimVersionUtil

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.metadata.{CompressionCodecName, ParquetMetadata}
import org.apache.parquet.schema.MessageType

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
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
      fileSourceScanExec: FileSourceScanExec): FileScanRDD

  def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]]

  // Spark3.4 new add table parameter in BatchScanExec.
  def getBatchScanExecTable(batchScan: BatchScanExec): Table

  // The PartitionedFile API changed in spark 3.4
  def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile

  def isWindowGroupLimitExec(plan: SparkPlan): Boolean = false

  def getWindowGroupLimitExecShim(plan: SparkPlan): WindowGroupLimitExecShim = null

  def getWindowGroupLimitExec(windowGroupLimitExecShim: WindowGroupLimitExecShim): SparkPlan = null

  def getLimitAndOffsetFromGlobalLimit(plan: GlobalLimitExec): (Int, Int) = (plan.limit, 0)

  def getLimitAndOffsetFromTopK(plan: TakeOrderedAndProjectExec): (Int, Int) = (plan.limit, 0)

  def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]]

  def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {
    throw new UnsupportedOperationException()
  }

  def enableNativeWriteFilesByDefault(): Boolean = false

  def broadcastInternal[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    // Since Spark 3.4, the `sc.broadcast` has been optimized to use `sc.broadcastInternal`.
    // More details see SPARK-39983.
    // TODO, remove this shim once we drop Spark3.3 and previous
    sc.broadcast(value)
  }

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
    Map(
      InputFileName().prettyName -> file.filePath.toString,
      InputFileBlockStart().prettyName -> file.start.toString,
      InputFileBlockLength().prettyName -> file.length.toString
    )
  }

  // For compatibility with Spark-3.5.
  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan]

  def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = Option(Seq())

  def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] =
    Option(Seq())

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
      joinKeyPositions: Option[Seq[Int]] = None): Seq[Seq[InputPartition]] =
    filteredPartitions

  def extractExpressionTimestampAddUnit(timestampAdd: Expression): Option[Seq[String]] =
    Option.empty

  def extractExpressionTimestampDiffUnit(timestampDiff: Expression): Option[String] =
    Option.empty

  def withTryEvalMode(expr: Expression): Boolean = false

  def withAnsiEvalMode(expr: Expression): Boolean = false

  def createParquetFilters(
      conf: SQLConf,
      schema: MessageType,
      caseSensitive: Option[Boolean] = None): ParquetFilters

  def extractExpressionArrayInsert(arrayInsert: Expression): Seq[Expression] = {
    throw new UnsupportedOperationException("ArrayInsert not supported.")
  }

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

  def isParquetFileEncrypted(footer: ParquetMetadata): Boolean

  def shouldFallbackForParquetVariantAnnotation(footer: ParquetMetadata): Boolean = false

  def getOtherConstantMetadataColumnValues(file: PartitionedFile): JMap[String, Object] =
    Map.empty[String, Any].asJava.asInstanceOf[JMap[String, Object]]

  def getCollectLimitOffset(plan: CollectLimitExec): Int = 0

  def unBase64FunctionFailsOnError(unBase64: UnBase64): Boolean = false

  def widerDecimalType(d1: DecimalType, d2: DecimalType): DecimalType

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

  def isFinalAdaptivePlan(p: AdaptiveSparkPlanExec): Boolean

  /**
   * Checks if the given JoinType is LeftSingle. LeftSingle is a Spark 4.0+ join type, semantically
   * similar to LeftOuter. Default implementation returns false for Spark 3.x compatibility.
   */
  def isLeftSingleJoinType(joinType: JoinType): Boolean = false
}
