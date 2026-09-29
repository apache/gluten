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
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._

import java.util.{ArrayList => JArrayList, List => JList}

case class ExpandExecTransformer(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode
  with UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetrics(sparkContext)

  @transient
  override lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(projections.flatten.map(_.references))
  }

  override def isNoop: Boolean = projections == null || projections.isEmpty

  override def metricsUpdater(): MetricsUpdater = if (isNoop) {
    MetricsUpdater.None
  } else {
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetricsUpdater(metrics)
  }

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  def getRelNode(
      context: SubstraitContext,
      projections: Seq[Seq[Expression]],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val projectSetExprNodes = new JArrayList[JList[ExpressionNode]]()
    projections.foreach {
      projectSet =>
        val projectExprNodes = new JArrayList[ExpressionNode]()
        projectSet.foreach {
          project =>
            val projectExprNode = ExpressionConverter
              .replaceWithExpressionTransformer(project, originalInputAttributes)
              .doTransform(context)
            projectExprNodes.add(projectExprNode)
        }
        projectSetExprNodes.add(projectExprNodes)
    }

    if (!validation) {
      RelBuilder.makeExpandRel(input, projectSetExprNodes, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeExpandRel(input, projectSetExprNodes, extensionNode, context, operatorId)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (projections.isEmpty) {
      return ValidationResult.failed("Current backend does not support empty projections in expand")
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode =
      getRelNode(substraitContext, projections, child.output, operatorId, null, validation = true)

    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    if (isNoop) {
      // The computing for this Expand is not needed.
      return childCtx
    }

    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, projections, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Expand Rel should be valid")
    TransformContext(output, currRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExpandExecTransformer =
    copy(child = newChild)
}
