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
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer, IncrementMetricCall}
import org.apache.gluten.extension.DeltaPostTransformRules.containsIncrementMetricExpr
import org.apache.gluten.extension.IncrementMetricOffload
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.JavaConverters._
import scala.collection.mutable

case class DeltaProjectExecTransformer(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExecTransformerBase(projectList, child) {

  // The metrics behind the project list's IncrementMetric expressions, keyed the way the metrics
  // updater credits them: by counter function name when the backend counts natively, else by
  // "increment_metric" for a stack at the root of an alias, evaluated once per output row.
  // Derived from the project list once, so validation and execution cannot register a metric
  // twice.
  private lazy val incrementMetrics: Seq[(String, SQLMetric)] =
    DeltaProjectExecTransformer
      .stripIncrementMetrics(projectList, IncrementMetricOffload.nativeCounting)
      ._2

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(
      metrics,
      incrementMetrics)

  override def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val newProjectList = DeltaProjectExecTransformer
      .stripIncrementMetrics(projectList, IncrementMetricOffload.nativeCounting)
      ._1
    val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
      .replaceWithExpressionTransformer(newProjectList, attributeSeq = originalInputAttributes)
    val projExprNodeList = columnarProjExprs.map(_.doTransform(context)).asJava
    val emitStartIndex = originalInputAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(input, projExprNodeList, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        projExprNodeList,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DeltaProjectExecTransformer =
    copy(child = newChild)
}

object DeltaProjectExecTransformer {

  /**
   * Rewrites the project list for the backend and returns it together with the metrics it carries,
   * keyed the way the metrics updater credits them.
   *
   * With `nativeCounting`, every [[IncrementMetric]] becomes an [[IncrementMetricCall]] on one of
   * the backend's counter slots, one slot per distinct metric of the projection, so the backend
   * counts the rows it is evaluated on wherever it sits. A projection with more distinct metrics
   * than slots falls back. Counters inside `AND` or `OR` also fall back because native operand
   * reordering can change their evaluation count.
   *
   * Without it, only the stack of [[IncrementMetric]] at the root of an alias is removed and
   * credited with the output row count. An [[IncrementMetric]] anywhere else is evaluated only for
   * some rows on Spark, so it cannot be represented by the output row count;
   * [[org.apache.gluten.extension.OffloadDeltaProject]] keeps such projects on Spark, and this
   * method refuses them so validation falls back if one slips through.
   */
  private[gluten] def stripIncrementMetrics(
      projectList: Seq[NamedExpression],
      nativeCounting: Boolean): (Seq[NamedExpression], Seq[(String, SQLMetric)]) = {
    if (nativeCounting) {
      if (!IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = true)) {
        throw new GlutenNotSupportException(IncrementMetricOffload.nativeConjunctionProjectReason)
      }
      return rewriteToNativeCounters(projectList)
    }
    val metrics = mutable.ArrayBuffer.empty[(String, SQLMetric)]
    val stripped = projectList.map {
      case alias: Alias =>
        var expr: Expression = alias.child
        while (expr.isInstanceOf[IncrementMetric]) {
          val increment = expr.asInstanceOf[IncrementMetric]
          metrics += ((increment.prettyName, increment.metric))
          expr = increment.child
        }
        if (containsIncrementMetricExpr(expr)) {
          throw new GlutenNotSupportException(IncrementMetricOffload.conditionalProjectReason)
        }
        if (expr eq alias.child) alias else alias.withNewChildren(Seq(expr)).asInstanceOf[Alias]
      case other =>
        if (containsIncrementMetricExpr(other)) {
          throw new GlutenNotSupportException(IncrementMetricOffload.conditionalProjectReason)
        }
        other
    }
    (stripped, metrics.toSeq)
  }

  private def rewriteToNativeCounters(
      projectList: Seq[NamedExpression]): (Seq[NamedExpression], Seq[(String, SQLMetric)]) = {
    // One slot per distinct metric, in order of first appearance. SQLMetric has identity equality,
    // so the same metric reached from several places shares its slot and the backend sums the rows
    // of every call to it, which is how Spark increments it too.
    val slots = mutable.LinkedHashMap.empty[SQLMetric, String]
    def toCall(expr: Expression): Expression = expr.transformUp {
      case increment: IncrementMetric =>
        val functionName = slots.getOrElseUpdate(
          increment.metric, {
            if (slots.size >= IncrementMetricCall.maxCounters) {
              throw new GlutenNotSupportException(
                s"More than ${IncrementMetricCall.maxCounters} distinct metrics in one projection " +
                  "cannot be counted natively")
            }
            IncrementMetricCall.functionName(slots.size)
          }
        )
        IncrementMetricCall(increment.child, functionName)
    }
    val rewritten = projectList.map {
      case alias: Alias =>
        val child = toCall(alias.child)
        if (child eq alias.child) alias else alias.withNewChildren(Seq(child)).asInstanceOf[Alias]
      case other => other
    }
    (rewritten, slots.toSeq.map { case (metric, name) => (name, metric) })
  }
}
