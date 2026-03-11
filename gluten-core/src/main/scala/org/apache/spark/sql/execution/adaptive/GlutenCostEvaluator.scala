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
package org.apache.spark.sql.execution.adaptive

import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.Utils

/**
 * This [[CostEvaluator]] is to force use the new physical plan when cost is equal.
 *
 * Since https://github.com/apache/gluten/pull/6143.
 */
case class GlutenCostEvaluator() extends CostEvaluator with SQLConfHelper {

  private val vanillaCostEvaluator: CostEvaluator = {
    val forceOptimizeSkewedJoin =
      conf.getConfString("spark.sql.adaptive.forceOptimizeSkewedJoin").toBoolean
    val clazz = Utils.classForName("org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator")
    val ctor = clazz.getConstructor(classOf[Boolean])
    ctor.newInstance(forceOptimizeSkewedJoin.asInstanceOf[Object]).asInstanceOf[CostEvaluator]
  }

  override def evaluateCost(plan: SparkPlan): Cost = {
    if (GlutenCoreConfig.get.enableGluten) {
      new GlutenCost(vanillaCostEvaluator, plan)
    } else {
      vanillaCostEvaluator.evaluateCost(plan)
    }
  }
}
