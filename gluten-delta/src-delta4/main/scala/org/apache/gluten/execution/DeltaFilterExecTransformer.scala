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
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.JavaConverters._

case class DeltaFilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecTransformerBase(condition, child) {

  private var extraMetrics: Seq[(String, SQLMetric)] = Seq.empty

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(
      metrics,
      extraMetrics.toSeq)

  override def getRelNode(
      context: SubstraitContext,
      condExpr: Expression,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    assert(condExpr != null)
    val condExprNode = condExpr match {
      case IncrementMetric(child, metric) =>
        extraMetrics :+= (condExpr.prettyName, metric)
        ExpressionConverter
          .replaceWithExpressionTransformer(child, attributeSeq = originalInputAttributes)
          .doTransform(context)
      case _ =>
        ExpressionConverter
          .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
          .doTransform(context)
    }

    if (!validation) {
      RelBuilder.makeFilterRel(input, condExprNode, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeFilterRel(input, condExprNode, extensionNode, context, operatorId)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DeltaFilterExecTransformer =
    copy(child = newChild)
}
