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
package org.apache.gluten.benchmarks

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BoltWholeStageTransformerSuite, WholeStageTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.commons.io.FileUtils
import org.scalatest.Tag

import java.io.File

object GenerateExample extends Tag("org.apache.gluten.tags.GenerateExample")

class NativeBenchmarkPlanGenerator extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"
  val generatedPlanDir = getClass.getResource("/").getPath + "../../../generated-native-benchmark/"
  val outputFileFormat = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val dir = new File(generatedPlanDir)
    if (dir.exists()) {
      FileUtils.forceDelete(dir)
    }
    FileUtils.forceMkdir(dir)
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.session.timeZone", "GMT+08:00")
  }

  test("Test plan json non-empty - AQE off") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.CACHE_WHOLE_STAGE_TRANSFORMER_CONTEXT.key -> "true") {
      val df = spark
        .sql("""
               |select * from lineitem
               |""".stripMargin)
      val executedPlan = df.queryExecution.executedPlan
      val lastStageTransformer = executedPlan.find(_.isInstanceOf[WholeStageTransformer])
      assert(lastStageTransformer.nonEmpty)
      var planJson = lastStageTransformer.get.asInstanceOf[WholeStageTransformer].substraitPlanJson
      assert(planJson.nonEmpty)
      executedPlan.execute()
      planJson = lastStageTransformer.get.asInstanceOf[WholeStageTransformer].substraitPlanJson
      assert(planJson.nonEmpty)
    }
  }

  test("Test plan json non-empty - AQE on") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.CACHE_WHOLE_STAGE_TRANSFORMER_CONTEXT.key -> "true") {
      val df = spark
        .sql("""
               |select * from lineitem join orders on l_orderkey = o_orderkey
               |""".stripMargin)
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
      executedPlan.execute()

      val finalPlan = executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      val lastStageTransformer = finalPlan.find(_.isInstanceOf[WholeStageTransformer])
      assert(lastStageTransformer.nonEmpty)
      val planJson = lastStageTransformer.get.asInstanceOf[WholeStageTransformer].substraitPlanJson
      assert(planJson.nonEmpty)
    }
  }

  test("generate example", GenerateExample) {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      GlutenConfig.BENCHMARK_SAVE_DIR.key -> generatedPlanDir,
      GlutenConfig.BENCHMARK_TASK_STAGEID.key -> "12",
      GlutenConfig.BENCHMARK_TASK_PARTITIONID.key -> "0"
    ) {
      logWarning(s"Generating inputs for micro benchmark to $generatedPlanDir")
      spark
        .sql("""
               |select /*+ REPARTITION(1) */
               |  o_orderpriority,
               |  count(*) as order_count
               |from
               |  orders
               |where
               |  o_orderdate >= date '1993-07-01'
               |  and o_orderdate < date '1993-07-01' + interval '3' month
               |  and exists (
               |    select /*+ REPARTITION(1) */
               |      *
               |    from
               |      lineitem
               |    where
               |      l_orderkey = o_orderkey
               |      and l_commitdate < l_receiptdate
               |  )
               |group by
               |  o_orderpriority
               |order by
               |  o_orderpriority
               |""".stripMargin)
        .foreach(_ => ())
    }
  }
}
