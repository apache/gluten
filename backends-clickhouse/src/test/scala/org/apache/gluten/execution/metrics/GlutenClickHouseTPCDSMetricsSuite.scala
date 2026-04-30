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
package org.apache.gluten.execution.metrics

import org.apache.gluten.execution.{ColumnarNativeIterator, GlutenClickHouseTPCDSAbstractSuite, WholeStageTransformer}
import org.apache.gluten.execution.GlutenPlan

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.InputIteratorTransformer
import org.apache.spark.task.TaskResources

import scala.collection.JavaConverters._

class GlutenClickHouseTPCDSMetricsSuite extends GlutenClickHouseTPCDSAbstractSuite {

  protected val substraitPlansDatPath: String = resPath + "substrait-plans"
  protected val metricsJsonFilePath: String = resPath + "metrics-json"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.memory.offHeap.size", "4g")
  }

  test("test tpcds q47 metrics") {
    TaskResources.runUnsafe {
      // Test getting metrics
      val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator](
        Array(0).map(iter => new ColumnarNativeIterator(Iterator.empty.asJava)).toSeq.asJava)
      val outputAttributes = new java.util.ArrayList[Attribute](0)

      val nativeMetricsList = GlutenClickHouseMetricsUTUtils
        .executeSubstraitPlan(
          substraitPlansDatPath + "/tpcds-q47-wholestage-9.json",
          dataHome,
          inBatchIters,
          outputAttributes
        )

      assert(nativeMetricsList.size == 1)
      val nativeMetricsData = nativeMetricsList(0)
      assert(nativeMetricsData.metricsDataList.size() == 7)

      assert(nativeMetricsData.metricsDataList.get(1).getName.equals("kSort"))
      assert(
        nativeMetricsData.metricsDataList
          .get(1)
          .getSteps
          .get(0)
          .getProcessors
          .get(0)
          .getOutputRows == 0)
      assert(nativeMetricsData.metricsDataList.get(2).getName.equals("kWindow"))
      assert(nativeMetricsData.metricsDataList.get(4).getName.equals("kWindow"))
    }
    // Test metrics update
    val df = GlutenClickHouseMetricsUTUtils.getTPCDSQueryExecution(spark, "q47", tpcdsQueries)
    val allWholeStageTransformers = df.queryExecution.executedPlan.collect {
      case wholeStage: WholeStageTransformer => wholeStage
    }
    assert(allWholeStageTransformers.size == 9)

    val wholeStageTransformer = allWholeStageTransformers(1)

    GlutenClickHouseMetricsUTUtils.executeMetricsUpdater(
      wholeStageTransformer,
      metricsJsonFilePath + "/tpcds-q47-wholestage-9-metrics.json"
    ) {
      () =>
        val allGlutenPlans = wholeStageTransformer.collect {
          case g: GlutenPlan if !g.isInstanceOf[InputIteratorTransformer] => g
        }

        assert(allGlutenPlans.size == 30)

        val windowPlan0 = allGlutenPlans(3)
        assert(windowPlan0.metrics("totalTime").value == 2)
        assert(windowPlan0.metrics("inputWaitTime").value == 12)
        assert(windowPlan0.metrics("outputWaitTime").value == 0)
        assert(windowPlan0.metrics("numOutputRows").value == 10717)
        assert(windowPlan0.metrics("outputBytes").value == 1224479)
        assert(windowPlan0.metrics("numInputRows").value == 10717)
        assert(windowPlan0.metrics("inputBytes").value == 1128026)

        val windowPlan1 = allGlutenPlans(5)
        assert(windowPlan1.metrics("totalTime").value == 2)
        assert(windowPlan1.metrics("extraTime").value == 1)
        assert(windowPlan1.metrics("inputWaitTime").value == 23)
        assert(windowPlan1.metrics("outputWaitTime").value == 2)
        assert(windowPlan1.metrics("numOutputRows").value == 12333)
        assert(windowPlan1.metrics("outputBytes").value == 1360484)
        assert(windowPlan1.metrics("numInputRows").value == 12333)
        assert(windowPlan1.metrics("inputBytes").value == 1261820)

        val sortPlan = allGlutenPlans(6)
        assert(sortPlan.metrics("totalTime").value == 3)
        assert(sortPlan.metrics("inputWaitTime").value == 30)
        assert(sortPlan.metrics("outputWaitTime").value == 1)
        assert(sortPlan.metrics("numOutputRows").value == 12333)
        assert(sortPlan.metrics("outputBytes").value == 1261820)
        assert(sortPlan.metrics("numInputRows").value == 12333)
        assert(sortPlan.metrics("inputBytes").value == 1261820)
    }
  }
}
