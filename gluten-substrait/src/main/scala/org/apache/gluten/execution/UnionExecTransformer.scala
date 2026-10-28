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
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.proto.SetRel.SetOp

import scala.collection.JavaConverters._

/** Transformer for UnionExec. Note: Spark's UnionExec represents a SQL UNION ALL. */
case class UnionExecTransformer(children: Seq[SparkPlan]) extends TransformSupport {
  private val union = UnionExec(children)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genUnionTransformerMetrics(sparkContext)

  override def output: Seq[Attribute] = union.output

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = children.flatMap(getColumnarInputRDDs)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genUnionTransformerMetricsUpdater(metrics)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(children = newChildren)

  override protected def doValidateInternal(): ValidationResult = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getRelNode(context, operatorId, children.map(_.output), null, true)
    doNativeValidation(context, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childrenCtx = children.map(_.asInstanceOf[TransformSupport].transform(context))
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode =
      getRelNode(context, operatorId, children.map(_.output), childrenCtx.map(_.root), false)
    TransformContext(output, relNode)
  }

  private def getRelNode(
      context: SubstraitContext,
      operatorId: Long,
      inputAttributes: Seq[Seq[Attribute]],
      inputs: Seq[RelNode],
      validation: Boolean): RelNode = {
    if (validation) {
      // Use the second level of nesting to represent N way inputs.
      val inputTypeNodes =
        inputAttributes.map(
          attributes =>
            attributes.map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)).asJava)
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder
            .makeStruct(
              false,
              inputTypeNodes.map(nodes => TypeBuilder.makeStruct(false, nodes)).asJava)
            .toProtobuf))
      return RelBuilder.makeSetRel(
        inputs.asJava,
        SetOp.SET_OP_UNION_ALL,
        extensionNode,
        context,
        operatorId)
    }
    RelBuilder.makeSetRel(inputs.asJava, SetOp.SET_OP_UNION_ALL, context, operatorId)
  }
}
