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
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.execution.ui.GlutenUIUtils

/**
 * This rule is used to collect all fallback reason.
 *   1. print fallback reason for each plan node 2. post all fallback reason using one event
 */
case class GlutenFallbackReporter(glutenConf: GlutenConfig, spark: SparkSession)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableFallbackReport) {
      return plan
    }
    printFallbackReason(plan)
    if (GlutenUIUtils.uiEnabled(spark.sparkContext)) {
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

  private def printFallbackReason(plan: SparkPlan): Unit = {
    val validationLogLevel = glutenConf.validationLogLevel
    def printPlan(p: SparkPlan): Unit = {
      p.foreachUp {
        case _: GlutenPlan => // ignore
        case cmd: CommandResultExec =>
          printPlan(cmd.commandPhysicalPlan)
        case p: SparkPlan if FallbackTags.nonEmpty(p) =>
          val tag = FallbackTags.get(p)
          logFallbackReason(validationLogLevel, p.nodeName, tag.reason())
          // With in next round stage in AQE, the physical plan would be a new instance that
          // can not preserve the tag, so we need to set the fallback reason to logical plan.
          // Then we can be aware of the fallback reason for the whole plan.
          // If a logical plan mapping to several physical plan, we add all reason into
          // that logical plan to make sure we do not lose any fallback reason.
          p.logicalLink.foreach(logicalPlan => FallbackTags.add(logicalPlan, tag))
        case _ =>
      }
    }
    printPlan(plan)
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
