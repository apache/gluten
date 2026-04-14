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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.test.SharedSparkSession

/** Basic trait for Gluten SQL test cases. */
trait GlutenSQLTestsBaseTrait extends SharedSparkSession with GlutenTestsBaseTrait {

  override def sparkConf: SparkConf = {
    GlutenSQLTestsBaseTrait.nativeSparkConf(super.sparkConf, warehouse)
  }

  /**
   * Get all the children plan of plans.
   *
   * @param plans
   *   : the input plans.
   * @return
   */
  private def getChildrenPlan(plans: Seq[SparkPlan]): Seq[SparkPlan] = {
    if (plans.isEmpty) {
      return Seq()
    }

    val inputPlans: Seq[SparkPlan] = plans.map {
      case stage: QueryStageExec => stage.plan
      case plan => plan
    }

    var newChildren: Seq[SparkPlan] = Seq()
    inputPlans.foreach {
      plan =>
        newChildren = newChildren ++ getChildrenPlan(plan.children)
        // To avoid duplication of WholeStageCodegenXXX and its children.
        if (!plan.nodeName.startsWith("WholeStageCodegen")) {
          newChildren = newChildren :+ plan
        }
    }
    newChildren
  }

  /**
   * Get the executed plan of a data frame.
   *
   * @param df
   *   : dataframe.
   * @return
   *   A sequence of executed plans.
   */
  def getExecutedPlan(df: DataFrame): Seq[SparkPlan] = {
    df.queryExecution.executedPlan match {
      case exec: AdaptiveSparkPlanExec =>
        getChildrenPlan(Seq(exec.executedPlan))
      case plan =>
        getChildrenPlan(Seq(plan))
    }
  }
}

object GlutenSQLTestsBaseTrait {
  def nativeSparkConf(origin: SparkConf, warehouse: String): SparkConf = {
    // Native SQL configs
    val conf = origin
      .setAppName("Gluten-UT")
      .set("spark.driver.memory", "1G")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
    // Avoid static evaluation by spark catalyst. But there are some UT issues
    // coming from spark, e.g., expecting SparkException is thrown, but the wrapped
    // exception is thrown.
    // .set("spark.sql.optimizer.excludedRules", ConstantFolding.ruleName + "," +
    //     NullPropagation.ruleName)

    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.io.compression.codec", "LZ4")
        .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
        .set("spark.sql.files.openCostInBytes", "134217728")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    } else {
      conf.set("spark.unsafe.exceptionOnMemoryLeak", "true")
    }
  }
}
