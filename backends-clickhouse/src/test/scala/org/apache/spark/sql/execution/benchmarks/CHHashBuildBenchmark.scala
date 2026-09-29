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
package org.apache.spark.sql.execution.benchmarks

import org.apache.gluten.execution.BroadcastJoinContext
import org.apache.gluten.vectorized.StorageJoinBuilder

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{RemoveTopColumnarToRow, SparkPlan}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.utils.CHExecUtil

import java.util

object CHHashBuildBenchmark extends SqlBasedBenchmark with CHSqlBasedBenchmark with Logging {

  protected lazy val appName = "CHHashBuildBenchmark"
  protected lazy val thrdNum = "1"
  protected lazy val memorySize = "4G"
  protected lazy val offheapSize = "4G"

  override def getSparkSession: SparkSession = {

    val conf = getSparkConf
      .set("spark.driver.maxResultSize", "0")
    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // /home/chang/test/tpch/parquet/s100/supplier * 200
    val (parquetDir, scanSchema, executedCnt) =
      if (mainArgs.isEmpty) {
        ("/data/tpch-data/parquet/lineitem", "l_orderkey,l_receiptdate", 5)
      } else {
        (mainArgs(0), mainArgs(1), mainArgs(2).toInt)
      }

    val chParquet = spark.sql(s"""
                                 |select $scanSchema from parquet.`$parquetDir`
                                 |
                                 |""".stripMargin)
    val rowCount = chParquet.count()

    val runs = Seq(1, 2, 4, 8, 16, 32, 64).reverse
      .map(num => rowCount / num)
      .map(num => s"select $scanSchema from parquet.`$parquetDir` order by 2 limit $num")
      .map {
        s =>
          logWarning(s"Running benchmark: $s")
          RemoveTopColumnarToRow(spark.sql(s).queryExecution.executedPlan)
      }
      .map(plan => createBroadcastRelation(plan))
    val benchmark =
      new Benchmark(s"Build Broadcast Hash Table with $rowCount rows", rowCount, output = output)
    runs
      .foreach {
        case (bytes, num, relation) =>
          val iteration = rowCount / num
          benchmark.addCase(
            s"build hash table with $num rows with $iteration hash tables",
            executedCnt) {
            _ =>
              for (i <- 0L until iteration) {
                val table = StorageJoinBuilder.build(
                  bytes,
                  num,
                  relation,
                  new util.ArrayList[Expression](),
                  new util.ArrayList[Attribute](),
                  false)
                StorageJoinBuilder.nativeCleanBuildHashTable("", table)
              }
          }
      }
    benchmark.run()
  }

  private def createBroadcastRelation(
      child: SparkPlan): (Array[Byte], Long, BroadcastJoinContext) = {
    val dataSize = SQLMetrics.createSizeMetric(spark.sparkContext, "size of files read")

    val countsAndBytes = child
      .executeColumnar()
      .mapPartitionsInternal(iter => CHExecUtil.toBytes(dataSize, iter))
      .collect()
    (
      countsAndBytes.flatMap(_._2),
      countsAndBytes.map(_._1).sum,
      BroadcastJoinContext(
        Seq(child.output.head),
        Inner,
        true,
        false,
        false,
        child.output,
        "",
        true)
    )
  }
}
