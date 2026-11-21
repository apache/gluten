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
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.extension.IncrementMetricOffload
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.BooleanType

import scala.collection.JavaConverters._
import scala.collection.mutable

case class DeltaFilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecTransformerBase(condition, child) {

  // The metrics carried by the IncrementMetric stack of the condition. The stack wraps a literal
  // true, so the filter keeps every row and the output row count equals the evaluation count.
  // Derived from the condition once, so validation and execution cannot register a metric twice.
  private lazy val incrementMetrics: Seq[(String, SQLMetric)] =
    DeltaFilterExecTransformer.stripIncrementMetrics(condition)._2

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(
      metrics,
      incrementMetrics)

  override def getRelNode(
      context: SubstraitContext,
      condExpr: Expression,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    assert(condExpr != null)
    val nativeCondition = DeltaFilterExecTransformer.stripIncrementMetrics(condExpr)._1
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(nativeCondition, attributeSeq = originalInputAttributes)
      .doTransform(context)
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

object DeltaFilterExecTransformer {

  /**
   * Removes the stack of [[IncrementMetric]] wrapping `condition` and returns the condition that
   * goes to the native filter together with the metrics that were removed.
   *
   * Only a stack over a literal `true` is accepted: any other condition drops rows, so the output
   * row count credited to the metrics would no longer be Spark's evaluation count.
   * [[org.apache.gluten.extension.OffloadDeltaFilter]] keeps such filters on Spark, and this method
   * refuses them so validation falls back if one slips through.
   */
  private[gluten] def stripIncrementMetrics(
      condition: Expression): (Expression, Seq[(String, SQLMetric)]) = {
    val metrics = mutable.ArrayBuffer.empty[(String, SQLMetric)]
    var expr = condition
    while (expr.isInstanceOf[IncrementMetric]) {
      val increment = expr.asInstanceOf[IncrementMetric]
      metrics += ((increment.prettyName, increment.metric))
      expr = increment.child
    }
    if (metrics.nonEmpty) {
      expr match {
        case Literal(true, BooleanType) =>
        case _ =>
          throw new GlutenNotSupportException(IncrementMetricOffload.conditionalFilterReason)
      }
    }
    (expr, metrics.toSeq)
  }
}
