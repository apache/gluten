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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.{SparkConf, TaskKilledException}
import org.apache.spark.sql.GlutenQueryTest
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources

import scala.collection.JavaConverters._

class VeloxFinalStageIteratorSuite extends GlutenQueryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key, "2")
      // TaskResources.runUnsafe uses stage -1, which otherwise matches the default dump stage.
      .set(GlutenConfig.BENCHMARK_TASK_STAGEID.key, "0")
  }

  test("final-stage iterator checks cancellation between native calls") {
    assume(BackendsApiManager.getBackendName == "velox")

    val substraitContext = new SubstraitContext
    val read = RelBuilder.makeReadRelForInputIterator(
      Seq.empty[Attribute].asJava,
      substraitContext,
      substraitContext.nextOperatorId("input"))
    val project = RelBuilder.makeProjectRel(
      read,
      Seq[ExpressionNode](ExpressionBuilder.makeLongLiteral(1L)).asJava,
      substraitContext,
      substraitContext.nextOperatorId("project"),
      0)
    val plan = PlanBuilder.makePlan(substraitContext, Seq(project).asJava, Seq("value").asJava)
    val pipelineTime = SQLMetrics.createTimingMetric(sparkContext, "pipeline time")
    var metricsUpdates = 0

    val usage = TaskResources.runUnsafe {
      val context = TaskResources.getLocalTaskContext()
      // Zero-column batches supply row counts for the native constant projection. Multiple
      // small batches leave output available without a Spark input iterator checking cancellation.
      val input = Seq.fill(3)(new ColumnarBatch(Array.empty[ColumnVector], 2)).iterator
      val iterator = BackendsApiManager.getIteratorApiInstance.genFinalStageIterator(
        context,
        Seq(input),
        sparkContext.getConf,
        plan,
        pipelineTime,
        metrics => {
          assert(metrics != null)
          metricsUpdates += 1
        },
        partitionIndex = 0,
        materializeInput = false)

      assert(iterator.hasNext)
      val batch = iterator.next()
      assert(batch.numCols() == 1)
      assert(batch.numRows() == 2)
      val reason = "cancel final-stage iterator"
      context.markInterrupted(reason)
      val killed = intercept[TaskKilledException] {
        iterator.hasNext
      }
      assert(killed.reason == reason)
      assert(metricsUpdates == 0)
      // Leave the current output and native iterator open to exercise task-resource cleanup.
      TaskResources.getSharedUsage()
    }

    assert(metricsUpdates == 1)
    assert(usage.current() == 0)
    assert(!TaskResources.inSparkTask())
  }
}
