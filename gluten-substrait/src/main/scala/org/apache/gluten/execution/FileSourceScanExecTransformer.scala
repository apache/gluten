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
import org.apache.spark.sql.catalyst.csv.CSVExprUtils
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

import scala.util.Try

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
    inputStats: Option[InputStats] = None,
    override val requiredMapSubfields: Map[String, Seq[SubfieldPath]] = Map.empty)
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
      inputStats,
      requiredMapSubfields
    )
  }

  override def withNewPushdownFilters(filters: Seq[Expression]): FileSourceScanExecTransformer =
    copy(pushDownFilters = Some(filters))

  override def supportsMapKeyPruning: Boolean = true

  override def withRequiredMapSubfields(
      subfields: Map[String, Seq[SubfieldPath]]): FileSourceScanExecTransformer =
    copy(requiredMapSubfields = subfields)
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

  /** Format-specific metrics that should be displayed with the native file scan. */
  protected def additionalScanMetrics: Map[String, SQLMetric] = Map.empty

  // Metrics attached to the native file scan. Format-specific additions may be updated on the
  // driver or executors; driver-only aliases are excluded.
  @transient private lazy val nativeScanMetrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance
      .genFileSourceScanTransformerMetrics(sparkContext)
      .filter(m => !driverMetricsAlias.contains(m._1)) ++ additionalScanMetrics

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    nativeScanMetrics ++ driverMetricsAlias

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
    if (fileFormat == ReadFileFormat.TextReadFormat) {
      csvFieldDelimiter match {
        case Some(d) if isSingleAscii(d) =>
        case Some(d) =>
          return ValidationResult.failed(
            "Only a single ASCII character field delimiter is supported in native text scan, " +
              s"got '$d'.")
        case None =>
          return ValidationResult.failed(
            s"Cannot decode the CSV field delimiter '$rawCsvFieldDelimiter'.")
      }
    }
    super.doValidateInternal()
  }

  // Spark's CSVOptions takes `sep` over the legacy `delimiter` alias.
  private def rawCsvFieldDelimiter: String =
    relation.options.getOrElse("sep", relation.options.getOrElse("delimiter", ","))

  // The delimiter after decoding escapes such as `\t`, as CSVOptions does. None when it cannot be
  // decoded, for which Spark raises an error.
  private def csvFieldDelimiter: Option[String] =
    Try(CSVExprUtils.toDelimiterStr(rawCsvFieldDelimiter)).toOption

  // The native reader splits on the first byte of the delimiter.
  private def isSingleAscii(delimiter: String): Boolean =
    delimiter.length == 1 && delimiter.charAt(0) <= 0x7f

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance
      .genFileSourceScanTransformerMetricsUpdater(nativeScanMetrics)

  override val nodeName: String = {
    s"${getClass.getSimpleName} $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  override def getProperties: Map[String, String] = {
    this.fileFormat match {
      case ReadFileFormat.TextReadFormat =>
        var options: Map[String, String] = Map()
        csvFieldDelimiter.foreach(d => options += ("field_delimiter" -> d))
        relation.options.foreach {
          case ("quote", v) => options += ("quote" -> v)
          case ("header", v) =>
            val cnt = if ("true".equalsIgnoreCase(v)) 1 else 0
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
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    val nativeFiltersString = s"NativeFilters: ${filterExprs().mkString("[", ",", "]")}"
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr" +
        s" $nativeFiltersString")
  }

  // The "override" keyword is omitted to maintain compatibility with earlier Spark versions.
  def getStream: Option[SparkDataStream] = {
    stream
  }
}

object FileSourceScanExecTransformerBase {
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined
}
