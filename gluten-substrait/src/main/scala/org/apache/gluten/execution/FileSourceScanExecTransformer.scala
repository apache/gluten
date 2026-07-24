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
import org.apache.spark.sql.execution.FileSourceScanExecShim
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

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case ("PushedFilters", value) =>
        "PushedFilters: " + StringUtils.abbreviate(
          redact(FileSourceScanExecTransformerBase.starPushedFilters(value)),
          maxMetadataValueLength)
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    val nativeFiltersString = s"NativeFilters: ${filterExprs().mkString("[", ",", "]")}"
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr" +
        s" $nativeFiltersString")
  }

  // Formatted explain (used by the plan-stability golden files) renders `PushedFilters` from
  // `metadata` via Spark's verboseStringWithOperatorId, which the simpleString override above does
  // not touch. Mark it there too so both rendering paths agree.
  override def verboseStringWithOperatorId(): String =
    FileSourceScanExecTransformerBase.markPushedFilters(super.verboseStringWithOperatorId())

  // The "override" keyword is omitted to maintain compatibility with earlier Spark versions.
  def getStream: Option[SparkDataStream] = {
    stream
  }
}

object FileSourceScanExecTransformerBase {
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  // Mark each pushed filter with a `*` in the rendered `PushedFilters` list, mirroring Spark's
  // RowDataSourceScanExec convention for a filter the source evaluates itself (so no separate
  // post-scan Filter is needed). Gluten pushes every conjunct into the native scan via
  // PushDownFilterToScan and applies them as exact row-level filters, so the paired
  // FilterExecTransformer is a no-op (FilterExecTransformerBase.isNoop) -- the same end state a `*`
  // advertises. `metadata` is a lazy val and cannot be super-overridden, so the mark is applied to
  // the rendered plan string.

  /**
   * Mark every entry of the `PushedFilters: [...]` list in a rendered plan-node string with a `*`.
   * Returns the text unchanged when no (untruncated) `PushedFilters` list is present.
   */
  private[execution] def markPushedFilters(text: String): String =
    rewritePushedFiltersList(text, starPushedFilters)

  /**
   * Locate the `PushedFilters: [...]` list in a rendered plan-node string and replace it with
   * `rewrite(list)` (where `list` is the whole `"[...]"` including brackets). Returns the text
   * unchanged when no (untruncated) `PushedFilters` list is present. Used to layer independent
   * display rewrites (e.g. `*`-marking, Delta CDF backtick removal) over the same list.
   */
  private[execution] def rewritePushedFiltersList(
      text: String,
      rewrite: String => String): String = {
    val marker = "PushedFilters: ["
    val markerAt = text.indexOf(marker)
    if (markerAt < 0) {
      return text
    }
    val listStart = markerAt + marker.length - 1 // index of the opening '['
    var depth = 0
    var i = listStart
    var listEnd = -1
    while (i < text.length && listEnd < 0) {
      text.charAt(i) match {
        case '[' | '(' => depth += 1
        case ']' | ')' =>
          depth -= 1
          if (depth == 0) {
            listEnd = i
          }
        case _ =>
      }
      i += 1
    }
    if (listEnd < 0) {
      return text // truncated/abbreviated list; leave as-is
    }
    val rewritten = rewrite(text.substring(listStart, listEnd + 1))
    text.substring(0, listStart) + rewritten + text.substring(listEnd + 1)
  }

  /**
   * Prefix each top-level entry of a rendered `PushedFilters` list (`"[f1, f2, ...]"`) with `*`.
   * Depth-aware: commas nested inside a single filter (e.g. `In(id, [1,2,3])`) are not separators.
   */
  private[execution] def starPushedFilters(rendered: String): String = {
    if (rendered.length < 2 || rendered.head != '[' || rendered.last != ']') {
      return rendered
    }
    val inner = rendered.substring(1, rendered.length - 1)
    if (inner.isEmpty) {
      return rendered
    }
    val entries = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var depth = 0
    var i = 0
    while (i < inner.length) {
      val c = inner.charAt(i)
      if (c == '[' || c == '(') {
        depth += 1
        current.append(c)
        i += 1
      } else if (c == ']' || c == ')') {
        depth -= 1
        current.append(c)
        i += 1
      } else if (c == ',' && depth == 0 && i + 1 < inner.length && inner.charAt(i + 1) == ' ') {
        entries += current.toString
        current.clear()
        i += 2
      } else {
        current.append(c)
        i += 1
      }
    }
    entries += current.toString
    entries.map("*" + _).mkString("[", ", ", "]")
  }
}
