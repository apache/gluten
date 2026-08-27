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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.utils.FileIndexUtil

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.{ExplainUtils, FileSourceScanExecShim}
import org.apache.spark.sql.execution.adaptive.InputStats
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SparkVersionUtil
import org.apache.spark.util.collection.BitSet

import org.apache.commons.lang3.StringUtils

case class FileSourceScanExecTransformer(
    @transient override val relation: HadoopFsRelation,
    @transient stream: Option[SparkDataStream],
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false,
    override val pushDownFilters: Option[Seq[Expression]] = None,
    inputStats: Option[InputStats] = None)
  extends FileSourceScanExecTransformerBase(
    relation,
    stream,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan
  ) {

  override def getInputStats: Option[InputStats] = {
    inputStats
  }

  override def doCanonicalize(): FileSourceScanExecTransformer = {
    FileSourceScanExecTransformer(
      relation,
      // remove stream on canonicalization; this is needed for reused shuffle to be effective in
      // self-join
      None,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan,
      pushDownFilters.map(QueryPlan.normalizePredicates(_, output)),
      inputStats
    )
  }

  override def withNewPushdownFilters(filters: Seq[Expression]): FileSourceScanExecTransformer =
    copy(pushDownFilters = Some(filters))
}

abstract class FileSourceScanExecTransformerBase(
    @transient override val relation: HadoopFsRelation,
    @transient stream: Option[SparkDataStream],
    override val output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends FileSourceScanExecShim(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan)
  with DatasourceScanTransformer {

  // Executor-side metrics only (excludes driverMetricsAlias).
  @transient private lazy val executorSideScanMetrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance
      .genFileSourceScanTransformerMetrics(sparkContext)
      .filter(m => !driverMetricsAlias.contains(m._1))

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    executorSideScanMetrics ++ driverMetricsAlias

  override def scanFilters: Seq[Expression] = dataFilters

  override def getMetadataColumns(): Seq[AttributeReference] = metadataColumns

  override def getPartitions: Seq[Partition] = {
    if (SparkVersionUtil.gteSpark40) {
      getPartitionsSeq()
    } else {
      BackendsApiManager.getTransformerApiInstance
        .genPartitionSeq(
          relation,
          requiredSchema,
          getPartitionArray,
          output,
          bucketedScan,
          optionalBucketSet,
          optionalNumCoalescedBuckets,
          disableBucketedScan,
          filterExprs()
        )
    }
  }

  override def getPartitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)] =
    getPartitions.map((_, fileFormat))

  override def getPartitionSchema: StructType = relation.partitionSchema

  override def getDataSchema: StructType = relation.dataSchema

  override def getRootPathsInternal: Seq[String] = {
    FileIndexUtil.getRootPath(relation.location)
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (
      !metadataColumns.isEmpty && !BackendsApiManager.getSettings.supportNativeMetadataColumns()
    ) {
      return ValidationResult.failed(s"Unsupported metadata columns scan in native.")
    }

    if (
      SparkShimLoader.getSparkShims.findRowIndexColumnIndexInSchema(schema) > 0 &&
      !BackendsApiManager.getSettings.supportNativeRowIndexColumn()
    ) {
      return ValidationResult.failed("Unsupported row index column scan in native.")
    }

    if (hasUnsupportedColumns) {
      return ValidationResult.failed(s"Unsupported columns scan in native.")
    }

    if (hasFieldIds) {
      // Spark read schema expects field Ids , the case didn't support yet by native.
      return ValidationResult.failed(
        s"Unsupported matching schema column names " +
          s"by field ids in native scan.")
    }
    super.doValidateInternal()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance
      .genFileSourceScanTransformerMetricsUpdater(executorSideScanMetrics)

  override val nodeName: String = {
    s"${getClass.getSimpleName} $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  override def getProperties: Map[String, String] = {
    this.fileFormat match {
      case ReadFileFormat.TextReadFormat =>
        var options: Map[String, String] = Map()
        relation.options.foreach {
          case ("delimiter", v) => options += ("field_delimiter" -> v)
          case ("quote", v) => options += ("quote" -> v)
          case ("header", v) =>
            val cnt = if (v == "true") 1 else 0
            options += ("header" -> cnt.toString)
          case ("escape", v) => options += ("escape" -> v)
          case ("nullvalue", v) => options += ("nullValue" -> v)
          case (_, _) =>
        }
        options
      case _ => Map.empty
    }
  }

  @transient override lazy val fileFormat: ReadFileFormat =
    BackendsApiManager.getSettings.getSubstraitReadFileFormatV1(relation.fileFormat)

  // A `*` on a `PushedFilters` entry claims the scan itself fully evaluates that filter, so Spark
  // needs no post-scan Filter. That only holds when the backend actually accepts Gluten's full
  // filter pushdown. ClickHouse deliberately declines it for Parquet
  // (CHSparkPlanExecApi.supportPushDownFilterToScan) to keep vanilla-Spark best-effort semantics;
  // there `BasicScanExecTransformer.filterExprs()` silently drops filters the backend cannot
  // evaluate, leaving a real (non-no-op) FilterExecTransformer above the scan, so marking would be
  // a false claim. Only mark when every filter belonging to the scan is scan-supported.
  private def nativeScanHandlesPushedFilters: Boolean = {
    val api = BackendsApiManager.getSparkPlanExecApiInstance
    api.supportPushDownFilterToScan(this) &&
    FileSourceScanExecTransformerBase.allScanFiltersHandled(
      scanFilters,
      api.isSupportedScanFilter(_, this))
  }

  // Keep Spark's execution metadata unchanged and customize only the map used by explain output.
  private def metadataForDisplay: Map[String, String] = {
    metadata.updated(
      "PushedFilters",
      FileSourceScanExecTransformerBase.renderPushedFilters(
        pushedFilterStringsForDisplay,
        nativeScanHandlesPushedFilters))
  }

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadataForDisplay.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    val nativeFiltersString = s"NativeFilters: ${filterExprs().mkString("[", ",", "]")}"
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr" +
        s" $nativeFiltersString")
  }

  override def verboseStringWithOperatorId(): String = {
    // Render the structured display map directly instead of parsing the string returned by super.
    val metadataStr = metadataForDisplay.toSeq.sorted.filterNot {
      case (_, value) if value.isEmpty || value == "[]" => true
      case (key, _) if key == "DataFilters" || key == "Format" => true
      case _ => false
    }.map {
      case ("Location", _) =>
        val location = relation.location
        val numPaths = location.rootPaths.length
        val abbreviatedLocation = if (numPaths <= 1) {
          location.rootPaths.mkString("[", ", ", "]")
        } else {
          "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
        }
        s"Location: ${location.getClass.getSimpleName} ${redact(abbreviatedLocation)}"
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  // The "override" keyword is omitted to maintain compatibility with earlier Spark versions.
  def getStream: Option[SparkDataStream] = {
    stream
  }
}

object FileSourceScanExecTransformerBase {
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  /** Renders already-structured pushed-filter strings without parsing their contents. */
  private[execution] def renderPushedFilters(
      filters: Seq[String],
      markAsHandled: Boolean): String = {
    val prefix = if (markAsHandled) "*" else ""
    filters.map(prefix + _).mkString("[", ", ", "]")
  }

  /** Returns true when every filter belonging to the scan is scan-supported. */
  private[execution] def allScanFiltersHandled(
      scanFilters: Seq[Expression],
      isSupported: Expression => Boolean): Boolean =
    scanFilters.forall(isSupported)
}
