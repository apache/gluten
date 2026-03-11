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
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.metrics.{CHRangeMetricsUpdater, MetricsUpdater}
import org.apache.gluten.substrait.`type`._
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.plans.logical.{Range => LogicalRange}
import org.apache.spark.sql.execution.{ColumnarRangeBaseExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.clickhouse.ExtensionTableBuilder
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import com.google.protobuf.StringValue
import io.substrait.proto.NamedStruct

import scala.collection.JavaConverters;

case class CHRangeExecTransformer(range: LogicalRange)
  extends ColumnarRangeBaseExec
  with LeafTransformSupport {

  override def doCanonicalize(): SparkPlan = {
    CHRangeExecTransformer(range.canonicalized.asInstanceOf[LogicalRange])
  }

  override def getSplitInfos: Seq[SplitInfo] = {
    (0 until numSlices).map {
      sliceIndex =>
        ExtensionTableBuilder.makeExtensionTable(start, end, step, numSlices, sliceIndex)
    }
  }

  override def getPartitions: Seq[Partition] = {
    (0 until numSlices).map {
      sliceIndex => GlutenRangeExecPartition(start, end, step, numSlices, sliceIndex)
    }
  }

  override def getPartitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)] =
    getPartitions.map((_, ReadFileFormat.UnknownFormat))

  @transient
  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "time")
    )

  override def metricsUpdater(): MetricsUpdater = new CHRangeMetricsUpdater(metrics)

  // No need to do native validation for CH backend. It is validated in [[doTransform]].
  override protected def doValidateInternal(): ValidationResult = ValidationResult.succeeded

  override def doTransform(context: SubstraitContext): TransformContext = {
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = JavaConverters
      .seqAsJavaListConverter(
        output.map(attr => new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)))
      .asJava

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue("isRange=1\n").build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, null)
    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      null,
      columnTypeNodes,
      extensionNode,
      context,
      context.nextOperatorId(this.nodeName))

    TransformContext(output, readNode)
  }
}
