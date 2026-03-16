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
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions._

import com.google.protobuf.StringValue
import io.substrait.proto.NamedStruct

import scala.collection.JavaConverters._

trait BasicScanExecTransformer extends LeafTransformSupport with BaseDataSource {
  import org.apache.spark.sql.catalyst.util._

  /** Returns the filters that can be pushed down to native file scan */
  final def filterExprs(): Seq[Expression] = {
    if (pushDownFilters.nonEmpty) {
      val (_, scanFiltersNotInPushDownFilters) =
        scanFilters.partition(pushDownFilters.get.contains(_))
      // For filters that only exists in scan, we need to check if they are supported.
      val unsupportedFilters = scanFiltersNotInPushDownFilters.filter(
        !BackendsApiManager.getSparkPlanExecApiInstance.isSupportedScanFilter(_, this))
      if (unsupportedFilters.nonEmpty) {
        throw new UnsupportedOperationException(
          "Found unsupported filter in scan " + unsupportedFilters.mkString(", "))
      }
      val supportedPushDownFilters = pushDownFilters.get
        .filter(BackendsApiManager.getSparkPlanExecApiInstance.isSupportedScanFilter(_, this))
      FilterHandler.combineFilters(supportedPushDownFilters, scanFiltersNotInPushDownFilters)
    } else {
      // todo: When PushDownFilterToScan is not performed, find a way to throw an
      //  exception to trigger scan fallback when encountering unsupported scan filters,
      //  instead of simply filtering them out.
      scanFilters.filter(
        BackendsApiManager.getSparkPlanExecApiInstance.isSupportedScanFilter(_, this))
    }
  }

  /** Returns the filters that already exists in scan. */
  def scanFilters: Seq[Expression]

  /** Whether the scan supports push down filters. */
  def supportPushDownFilters: Boolean = true

  /**
   * Returns the filters that pushed by
   * [[org.apache.gluten.extension.columnar.PushDownFilterToScan]].
   */
  def pushDownFilters: Option[Seq[Expression]]

  /** Copy the scan with filters that pushed by filterNode. */
  def withNewPushdownFilters(filters: Seq[Expression]): BasicScanExecTransformer = {
    throw new UnsupportedOperationException(
      s"${getClass.toString} does not support push down filters.")
  }

  def getMetadataColumns(): Seq[AttributeReference]

  /** This can be used to report FileFormat for a file based scan operator. */
  val fileFormat: ReadFileFormat

  def getRootFilePaths: Seq[String] = {
    if (GlutenConfig.get.scanFileSchemeValidationEnabled) {
      getRootPathsInternal
    } else {
      Seq.empty
    }
  }

  /** Returns the file format properties. */
  def getProperties: Map[String, String] = Map.empty

  override def getSplitInfos: Seq[SplitInfo] = {
    getSplitInfosFromPartitions(getPartitionWithReadFileFormats)
  }

  def getSplitInfosFromPartitions(partitions: Seq[(Partition, ReadFileFormat)]): Seq[SplitInfo] = {
    partitions.map {
      case (partition, readFileFormat) => partitionToSplitInfo(partition, readFileFormat)
    }
  }

  private def partitionToSplitInfo(
      partition: Partition,
      readFileFormat: ReadFileFormat): SplitInfo = {
    val part = partition match {
      case sp: SparkDataSourceRDDPartition => sp.inputPartitions.map(_.asInstanceOf[Partition])
      case _ => Seq(partition)
    }

    BackendsApiManager.getIteratorApiInstance
      .genSplitInfo(
        partition.index,
        part,
        getPartitionSchema,
        getDataSchema,
        readFileFormat,
        getMetadataColumns().map(_.name),
        getProperties)
  }

  override protected def doValidateInternal(): ValidationResult = {
    val validationResult = BackendsApiManager.getSettings
      .validateScanExec(
        fileFormat,
        schema.fields,
        getDataSchema,
        getRootFilePaths,
        getProperties,
        sparkContext.hadoopConfiguration,
        getDistinctPartitionReadFileFormats
      )
    if (!validationResult.ok()) {
      return validationResult
    }

    val substraitContext = new SubstraitContext
    val relNode = transform(substraitContext).root

    doNativeValidation(substraitContext, relNode)
  }

  private def makeColumnTypeNode(attr: Attribute): ColumnTypeNode = {
    if (getPartitionSchema.exists(_.name.equals(attr.name))) {
      new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
    } else if (BackendsApiManager.getSparkPlanExecApiInstance.isRowIndexMetadataColumn(attr.name)) {
      new ColumnTypeNode(NamedStruct.ColumnType.ROWINDEX_COL)
    } else if (attr.isMetadataCol) {
      new ColumnTypeNode(NamedStruct.ColumnType.METADATA_COL)
    } else {
      new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
    }
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map(makeColumnTypeNode).asJava
    // Will put all filter expressions into an AND expression
    val exprNode = filterExprs()
      .map(ExpressionConverter.replaceAttributeReference)
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))
      .map(_.doTransform(context))
      .orNull

    // used by CH backend
    val mergeTreeFlag = if (this.fileFormat == ReadFileFormat.MergeTreeReadFormat) "1" else "0"
    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue(s"isMergeTree=$mergeTreeFlag\n").build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, null)

    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      exprNode,
      columnTypeNodes,
      extensionNode,
      context,
      context.nextOperatorId(this.nodeName))
    TransformContext(output, readNode)
  }
}
