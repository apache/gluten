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
package org.apache.spark.sql.hive

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.BasicScanExecTransformer
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveTableScanExecTransformer._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.execution.{AbstractHiveTableScanExec, HiveTableScanExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapred.TextInputFormat

import java.net.URI

case class HiveTableScanExecTransformer(
    requestedAttributes: Seq[Attribute],
    relation: HiveTableRelation,
    partitionPruningPred: Seq[Expression],
    prunedOutput: Seq[Attribute] = Seq.empty[Attribute])(@transient newSession: SparkSession)
  extends AbstractHiveTableScanExec(
    requestedAttributes,
    relation,
    partitionPruningPred,
    prunedOutput)(newSession)
  with BasicScanExecTransformer {

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(relation.tableMeta)

  @transient private lazy val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  override def scanFilters: Seq[Expression] = Seq.empty

  override def supportPushDownFilters: Boolean = false

  override def pushDownFilters: Option[Seq[Expression]] = None

  override def getMetadataColumns(): Seq[AttributeReference] = Seq.empty

  override def getPartitions: Seq[Partition] = partitions

  override def getPartitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)] =
    partitionWithReadFileFormats

  override def getDistinctPartitionReadFileFormats: Set[ReadFileFormat] =
    if (
      relation.isPartitioned &&
      basePrunedPartitions.exists(_.getInputFormatClass != tableDesc.getInputFileFormatClass)
    ) {
      basePrunedPartitions.map {
        partition => getReadFileFormat(HiveClientImpl.fromHivePartition(partition).storage)
      }.toSet
    } else {
      Set(fileFormat)
    }

  override def getPartitionSchema: StructType = relation.tableMeta.partitionSchema

  override def getDataSchema: StructType = relation.tableMeta.dataSchema

  // TODO: get root paths from hive table.
  override def getRootPathsInternal: Seq[String] = Seq.empty

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetricsUpdater(metrics)

  @transient private lazy val hivePartitionConverter =
    new HivePartitionConverter(session.sessionState.newHadoopConf(), session)

  @transient private lazy val existsMixedInputFormat: Boolean =
    prunedPartitions.exists(_.getInputFormatClass != tableDesc.getInputFileFormatClass)

  @transient private lazy val partitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)] =
    if (!relation.isPartitioned) {
      val tableLocation: URI = relation.tableMeta.storage.locationUri.getOrElse {
        throw new UnsupportedOperationException("Table path not set.")
      }

      hivePartitionConverter.createFilePartition(tableLocation).map((_, fileFormat))
    } else if (existsMixedInputFormat) {
      val readFileFormats = prunedPartitions.map {
        partition => getReadFileFormat(HiveClientImpl.fromHivePartition(partition).storage)
      }

      hivePartitionConverter.createFilePartition(
        prunedPartitions,
        relation.partitionCols.map(_.dataType),
        readFileFormats)
    } else {
      val filePartitions = hivePartitionConverter
        .createFilePartition(prunedPartitions, relation.partitionCols.map(_.dataType))

      filePartitions.map((_, fileFormat))
    }

  @transient private lazy val partitions: Seq[Partition] = partitionWithReadFileFormats.unzip._1

  @transient override lazy val fileFormat: ReadFileFormat =
    getReadFileFormat(relation.tableMeta.storage)

  private def getReadFileFormat(storage: CatalogStorageFormat): ReadFileFormat = {
    storage.inputFormat match {
      case Some(inputFormat)
          if TEXT_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") | Some(
                "org.apache.hive.hcatalog.data.JsonSerDe") =>
            ReadFileFormat.JsonReadFormat
          case _ => ReadFileFormat.TextReadFormat
        }
      case Some(inputFormat)
          if ORC_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        ReadFileFormat.OrcReadFormat
      case Some(inputFormat)
          if PARQUET_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        ReadFileFormat.ParquetReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  private def createDefaultTextOption(): Map[String, String] = {
    var options: Map[String, String] = Map()
    relation.tableMeta.storage.serde match {
      case Some("org.apache.hadoop.hive.serde2.OpenCSVSerde") =>
        options += ("field_delimiter" -> ",")
        options += ("quote" -> "\"")
        options += ("escape" -> "\\")
      case _ =>
        options += ("field_delimiter" -> DEFAULT_FIELD_DELIMITER.toString)
        options += ("nullValue" -> NULL_VALUE.toString)
        options += ("escape" -> "\\")
    }

    options
  }

  override def getProperties: Map[String, String] = {
    var properties: Map[String, String] = Map()
    tableDesc.getProperties
      .entrySet()
      .forEach(e => properties += (e.getKey.toString -> e.getValue.toString))

    var options: Map[String, String] = createDefaultTextOption()
    // property key string read from org.apache.hadoop.hive.serde.serdeConstants
    properties.foreach {
      case ("separatorChar", v) =>
        // If separatorChar, we should use default separatorChar
        // for org.apache.hadoop.hive.serde2.OpenCSVSerde
        // It fixed issue: https://github.com/oap-project/gluten/issues/3108
        val nv = if (v.isEmpty) "," else v
        options += ("field_delimiter" -> nv)
      case ("field.delim", v) =>
        // If field.delim is empty, we should use default field delimiter
        // for org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
        // It fixed issue: https://github.com/oap-project/gluten/issues/3108
        val nv = if (v.isEmpty) DEFAULT_FIELD_DELIMITER.toString else v
        options += ("field_delimiter" -> nv)
      case ("quoteChar", v) => options += ("quote" -> v)
      case ("quote.delim", v) => options += ("quote" -> v)
      case ("skip.header.line.count", v) => options += ("header" -> v)
      case ("escapeChar", v) => options += ("escape" -> v)
      case ("escape.delim", v) => options += ("escape" -> v)
      case ("serialization.null.format", v) => options += ("nullValue" -> v)
      case (_, _) =>
    }
    options
  }

  override def nodeName: String = s"NativeScan hive ${relation.tableMeta.qualifiedName}"

  override def doCanonicalize(): HiveTableScanExecTransformer = {
    val input: AttributeSeq = relation.output
    HiveTableScanExecTransformer(
      requestedAttributes.map(QueryPlan.normalizeExpressions(_, input)),
      relation.canonicalized.asInstanceOf[HiveTableRelation],
      QueryPlan.normalizePredicates(partitionPruningPred, input)
    )(sparkSession)
  }
}

object HiveTableScanExecTransformer {

  private val NULL_VALUE: Char = 0x00
  private val DEFAULT_FIELD_DELIMITER: Char = 0x01
  val TEXT_INPUT_FORMAT_CLASS: Class[TextInputFormat] =
    Utils.classForName("org.apache.hadoop.mapred.TextInputFormat")
  val ORC_INPUT_FORMAT_CLASS: Class[OrcInputFormat] =
    Utils.classForName("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
  val PARQUET_INPUT_FORMAT_CLASS: Class[MapredParquetInputFormat] =
    Utils.classForName("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
  def isHiveTableScan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[HiveTableScanExec]
  }

  def apply(plan: SparkPlan): HiveTableScanExecTransformer = {
    plan match {
      case hiveTableScan: HiveTableScanExec =>
        new HiveTableScanExecTransformer(
          hiveTableScan.requestedAttributes,
          hiveTableScan.relation,
          hiveTableScan.partitionPruningPred)(hiveTableScan.session)
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't transform HiveTableScanExecTransformer from ${plan.getClass.getSimpleName}")
    }
  }
}
