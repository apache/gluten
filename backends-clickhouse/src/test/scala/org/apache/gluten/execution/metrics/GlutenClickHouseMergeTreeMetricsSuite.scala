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

import org.apache.gluten.execution.{ColumnarNativeIterator, GlutenClickHouseWholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Attribute

import org.scalatest.DoNotDiscover

import scala.collection.JavaConverters._

@DoNotDiscover
class GlutenClickHouseMergeTreeMetricsSuite extends GlutenClickHouseWholeStageTransformerSuite {

  protected val substraitPlansDatPath: String = resPath + "substrait-plans"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "1")
  }

  // There are some changes in the extension part of the substrait plan,
  // ignore first, and fix these unit cases after all the change done.
  ignore("test all substrait plan in one wholestagetransformer") {
    // Copy Substrait Plan from GlutenClickHouseDSV2ColumnarShuffleSuite.'TPCH Q2'
    val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator](0)
    val outputAttributes = new java.util.ArrayList[Attribute](0)
    val nativeMetricsList = GlutenClickHouseMetricsUTUtils
      .executeSubstraitPlan(
        substraitPlansDatPath + "/tpch-q2-in-one-wholestage.json",
        dataHome,
        inBatchIters,
        outputAttributes
      )

    assert(nativeMetricsList.size == 1)
    val nativeMetricsData = nativeMetricsList.head
    assert(nativeMetricsData.metricsDataList.size() == 44)

    assert(nativeMetricsData.metricsDataList.get(42).getName.equals("kSort"))
    assert(nativeMetricsData.metricsDataList.get(42).getSteps.get(0).getProcessors.size() == 3)
    assert(
      nativeMetricsData.metricsDataList
        .get(42)
        .getSteps
        .get(0)
        .getProcessors
        .get(2)
        .getOutputRows == 44)

    // Post Projection after join
    assert(nativeMetricsData.metricsDataList.get(40).getName.equals("kProject"))
    assert(
      nativeMetricsData.metricsDataList
        .get(40)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 44)
    assert(nativeMetricsData.metricsDataList.get(39).getName.equals("kJoin"))
    assert(
      nativeMetricsData.metricsDataList
        .get(39)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 44)

    // Hash join
    assert(nativeMetricsData.metricsDataList.get(27).getName.equals("kProject"))
    assert(nativeMetricsData.metricsDataList.get(26).getName.equals("kJoin"))
    assert(
      nativeMetricsData.metricsDataList
        .get(26)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 44)
    assert(
      nativeMetricsData.metricsDataList
        .get(26)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getInputRows == 11985)
    assert(nativeMetricsData.metricsDataList.get(25).getName.equals("kProject"))
    assert(
      nativeMetricsData.metricsDataList
        .get(25)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 292)
    assert(
      nativeMetricsData.metricsDataList
        .get(25)
        .getSteps
        .get(0)
        .getProcessors
        .get(1)
        .getName
        .equals("FillingRightJoinSide"))
    assert(nativeMetricsData.metricsDataList.get(24).getName.equals("kProject"))

    assert(
      nativeMetricsData.metricsDataList
        .get(24)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 292)

    assert(
      nativeMetricsData.metricsDataList
        .get(18)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 11985)

    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(1)
        .getName
        .equals("FillingRightJoinSide"))
    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 1000)
    assert(nativeMetricsData.metricsDataList.get(0).getName.equals("kRead"))
    assert(
      nativeMetricsData.metricsDataList
        .get(0)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 80000)
  }

  ignore("test final agg stage") {
    // Copy Substrait Plan from TPCH Q1 second agg stage
    val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator](
      Array(0).map(iter => new ColumnarNativeIterator(Iterator.empty.asJava)).toSeq.asJava)
    val outputAttributes = new java.util.ArrayList[Attribute](0)

    val nativeMetricsList = GlutenClickHouseMetricsUTUtils
      .executeSubstraitPlan(
        substraitPlansDatPath + "/tpch-q1-final-agg-stage.json",
        dataHome,
        inBatchIters,
        outputAttributes
      )

    assert(nativeMetricsList.size == 1)
    val nativeMetricsData = nativeMetricsList.head
    assert(nativeMetricsData.metricsDataList.size() == 3)

    // Post Projection
    assert(nativeMetricsData.metricsDataList.get(2).getName.equals("kProject"))
    assert(
      nativeMetricsData.metricsDataList
        .get(2)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 0)
    // Aggregating
    assert(nativeMetricsData.metricsDataList.get(1).getName.equals("kAggregate"))
    // SourceFromJavaIter
    assert(nativeMetricsData.metricsDataList.get(0).getName.equals("kRead"))
  }

  ignore("test shj stage") {
    // Copy Substrait Plan from TPCH Q1 second agg stage
    val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator](
      Array(0, 1).map(iter => new ColumnarNativeIterator(Iterator.empty.asJava)).toSeq.asJava)
    val outputAttributes = new java.util.ArrayList[Attribute](0)

    val nativeMetricsList = GlutenClickHouseMetricsUTUtils
      .executeSubstraitPlan(
        substraitPlansDatPath + "/tpch-q4-shj-stage.json",
        dataHome,
        inBatchIters,
        outputAttributes
      )

    assert(nativeMetricsList.size == 1)
    val nativeMetricsData = nativeMetricsList.head
    assert(nativeMetricsData.metricsDataList.size() == 6)

    assert(nativeMetricsData.metricsDataList.get(5).getName.equals("kAggregate"))

    // Post Projection of the join
    assert(nativeMetricsData.metricsDataList.get(3).getName.equals("kProject"))
    assert(nativeMetricsData.metricsDataList.get(2).getName.equals("kJoin"))
    assert(
      nativeMetricsData.metricsDataList
        .get(2)
        .getSteps
        .get(1)
        .getProcessors
        .get(0)
        .getOutputRows == 0)
    assert(
      nativeMetricsData.metricsDataList
        .get(2)
        .getSteps
        .get(0)
        .getProcessors
        .get(1)
        .getName
        .equals("FillingRightJoinSide"))
    assert(
      nativeMetricsData.metricsDataList
        .get(2)
        .getSteps
        .get(1)
        .getProcessors
        .get(0)
        .getName
        .equals("JoiningTransform"))
    assert(nativeMetricsData.metricsDataList.get(1).getName.equals("kRead"))
    assert(nativeMetricsData.metricsDataList.get(0).getName.equals("kRead"))
  }
}
