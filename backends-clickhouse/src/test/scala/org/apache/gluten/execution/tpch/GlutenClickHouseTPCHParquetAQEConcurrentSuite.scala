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
package org.apache.gluten.execution.tpch

import org.apache.gluten.execution.ParquetTPCHSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType

import java.util.concurrent.ForkJoinPool

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector

class GlutenClickHouseTPCHParquetAQEConcurrentSuite extends ParquetTPCHSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  private def checkResultWithoutDouble(queryNum: Int): Unit = {
    withDataFrame(queryNum) {
      df =>
        val result = df.collect()
        val schema = df.schema
        if (schema.exists(_.dataType == DoubleType)) {}
        else {
          compareResultStr(s"q$queryNum", result, queriesResults)
        }
        checkDataFrame(noFallBack = true, NOOP, df)
    }
  }

  test("fix race condition at the global variable of ColumnarOverrideRules::isAdaptiveContext") {
    val queries = ParVector((1 to 22) ++ (1 to 22) ++ (1 to 22) ++ (1 to 22): _*)
    queries.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(22))
    queries.map(queryId => checkResultWithoutDouble(queryId))
  }
}
