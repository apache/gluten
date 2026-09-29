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
package org.apache.gluten.extension.columnar.cost

import org.apache.gluten.component.Component

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.SparkReflectionUtil

/**
 * The cost model API of Gluten. Used by the transition graph for choosing transition paths.
 */
trait GlutenCostModel {
  def costOf(node: SparkPlan): GlutenCost
  def costComparator(): Ordering[GlutenCost]
  def makeZeroCost(): GlutenCost
  def makeInfCost(): GlutenCost
  // Returns cost value of one + other.
  def sum(one: GlutenCost, other: GlutenCost): GlutenCost
  // Returns cost value of one - other.
  def diff(one: GlutenCost, other: GlutenCost): GlutenCost
}

object GlutenCostModel extends Logging {
  private val costModelRegistry = {
    val r = LongCostModel.registry()
    // Components should override Backend's costers. Hence, reversed registration order is applied.
    Component
      .sorted()
      .reverse
      .flatMap(_.costers())
      .foreach(coster => r.register(coster))
    r
  }

  def find(aliasOrClass: String): GlutenCostModel = {
    val costModel = find(costModelRegistry, aliasOrClass)
    costModel
  }

  private def find(registry: LongCostModel.Registry, aliasOrClass: String): GlutenCostModel = {
    if (LongCostModel.Kind.values().contains(aliasOrClass)) {
      val kind = LongCostModel.Kind.values()(aliasOrClass)
      val model = registry.get(kind)
      return model
    }
    val clazz = SparkReflectionUtil.classForName(aliasOrClass)
    logInfo(s"Using user cost model: $aliasOrClass")
    val ctor = clazz.getDeclaredConstructor()
    ctor.setAccessible(true)
    val model: GlutenCostModel = ctor.newInstance().asInstanceOf[GlutenCostModel]
    model
  }
}
