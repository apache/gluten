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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.events.GlutenPlanFallbackEvent
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.execution.ui.GlutenUIUtils

import scala.collection.mutable.ArrayBuffer

/**
 * Collects fallback information from a finalized columnar plan and, depending on configuration:
 *   1. logs the fallback reason for every non-Gluten operator 2. throws a [[GlutenException]] when
 *      `spark.gluten.sql.columnar.failOnFallback` is enabled and any fallback is present 3. posts a
 *      single [[GlutenPlanFallbackEvent]] for the SQL UI
 *
 * The set of operators that count as a fallback is shared with [[GlutenExplainUtils]] via
 * [[GlutenExplainUtils.visitFallbackNodes]], so the reporter, the SQL UI, and any future consumers
 * always agree on what "fallback" means.
 */
case class GlutenFallbackReporter(glutenConf: GlutenConfig, spark: SparkSession)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  override def apply(plan: SparkPlan): SparkPlan = {
    val report = glutenConf.enableFallbackReport
    val fail = glutenConf.failOnFallback
    if (!report && !fail) {
      return plan
    }
    val fallbacks = collectAndLogFallbacks(plan, log = report)
    if (fail) {
      throwIfFallback(fallbacks)
    }
    if (report && GlutenUIUtils.uiEnabled(spark.sparkContext)) {
      postFallbackReason(plan)
    }
    plan
  }

  private def logFallbackReason(logLevel: String, nodeName: String, reason: String): Unit = {
    val executionIdInfo = Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(id => s"[QueryId=$id]")
      .getOrElse("")
    logOnLevel(logLevel, s"Validation failed for plan: $nodeName$executionIdInfo, due to: $reason")
  }

  private def collectAndLogFallbacks(plan: SparkPlan, log: Boolean): Seq[(String, String)] = {
    val validationLogLevel = glutenConf.validationLogLevel
    val fallbacks = ArrayBuffer[(String, String)]()
    GlutenExplainUtils.visitFallbackNodes(
      plan,
      onFallback = (node, reason) => {
        if (log) {
          logFallbackReason(validationLogLevel, node.nodeName, reason)
          // Within the next AQE stage, the physical plan would be a new instance that does not
          // preserve the tag, so we propagate the fallback reason to the logical plan.
          // If a logical plan maps to several physical plans, we add all reasons onto that
          // logical plan to make sure we do not lose any fallback reason.
          FallbackTags
            .getOption(node)
            .foreach(tag => node.logicalLink.foreach(FallbackTags.add(_, tag)))
        }
        fallbacks += ((node.nodeName, reason))
      }
    )
    fallbacks.toSeq
  }

  private def throwIfFallback(fallbacks: Seq[(String, String)]): Unit = {
    if (fallbacks.nonEmpty) {
      val reasons = fallbacks.map { case (name, reason) => s"  $name: $reason" }.mkString("\n")
      throw new GlutenException(
        s"Gluten fallback detected (${GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key} is enabled). " +
          s"${fallbacks.size} operator(s) fell back to Spark:\n$reasons")
    }
  }

  private def postFallbackReason(plan: SparkPlan): Unit = {
    val sc = spark.sparkContext
    val executionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId == null) {
      logDebug(s"Unknown execution id for plan: $plan")
      return
    }
    val concat = new PlanStringConcat()
    concat.append("== Physical Plan ==\n")
    val (numGlutenNodes, fallbackNodeToReason) = GlutenExplainUtils.processPlan(plan, concat.append)

    val event = GlutenPlanFallbackEvent(
      executionId.toLong,
      numGlutenNodes,
      fallbackNodeToReason.size,
      concat.toString(),
      fallbackNodeToReason
    )
    GlutenUIUtils.postEvent(sc, event)
  }
}
