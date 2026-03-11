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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._

case class FallbackOnANSIMode(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (GlutenConfig.get.enableAnsiMode && GlutenConfig.get.enableAnsiFallback) {
      plan.foreach(FallbackTags.add(_, "does not support ansi mode"))
    }
    plan
  }
}

case class FallbackMultiCodegens(session: SparkSession) extends Rule[SparkPlan] {
  lazy val glutenConf: GlutenConfig = GlutenConfig.get
  lazy val physicalJoinOptimize = glutenConf.enablePhysicalJoinOptimize
  lazy val optimizeLevel: Integer = glutenConf.physicalJoinOptimizationThrottle
  lazy val outputSize: Integer = glutenConf.physicalJoinOptimizationOutputSize

  def existsMultiCodegens(plan: SparkPlan, count: Int = 0): Boolean =
    plan match {
      case plan: CodegenSupport if plan.supportCodegen =>
        if (
          (count + 1) >= optimizeLevel && plan.output.map(_.dataType.defaultSize).sum >= outputSize
        ) {
          return true
        }
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case plan: ShuffledHashJoinExec =>
        if (
          (count + 1) >= optimizeLevel && plan.output.map(_.dataType.defaultSize).sum >= outputSize
        ) {
          return true
        }

        plan.children.exists(existsMultiCodegens(_, count + 1))
      case plan: SortMergeJoinExec if GlutenConfig.get.forceShuffledHashJoin =>
        if (
          (count + 1) >= optimizeLevel && plan.output.map(_.dataType.defaultSize).sum >= outputSize
        ) {
          return true
        }

        plan.children.exists(existsMultiCodegens(_, count + 1))
      case _ => false
    }

  def addFallbackTag(plan: SparkPlan): SparkPlan = {
    FallbackTags.add(plan, "fallback multi codegens")
    plan
  }

  def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport =>
      plan.supportCodegen
    case _ => false
  }

  def isAQEShuffleReadExec(plan: SparkPlan): Boolean = {
    plan match {
      case _: AQEShuffleReadExec => true
      case _ => false
    }
  }

  def addFallbackTagRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        addFallbackTag(p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens)))
      case p: BroadcastExchangeExec =>
        addFallbackTag(p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens)))
      case p: ShuffledHashJoinExec =>
        addFallbackTag(p.withNewChildren(p.children.map(addFallbackTagRecursive)))
      case p if !supportCodegen(p) =>
        p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens))
      case p if isAQEShuffleReadExec(p) =>
        p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens))
      case p: QueryStageExec => p
      case p => addFallbackTag(p.withNewChildren(p.children.map(addFallbackTagRecursive)))
    }
  }

  def tagOnFallbackForMultiCodegens(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan if existsMultiCodegens(plan) =>
        addFallbackTagRecursive(plan)
      case other =>
        other.withNewChildren(other.children.map(tagOnFallbackForMultiCodegens))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (physicalJoinOptimize) {
      tagOnFallbackForMultiCodegens(plan)
    } else plan
  }
}
