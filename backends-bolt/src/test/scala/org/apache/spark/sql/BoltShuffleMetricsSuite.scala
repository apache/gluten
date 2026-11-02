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

import org.apache.gluten.backendsapi.bolt.BoltMetricsApi
import org.apache.gluten.config.{HashShuffleWriterType, RssSortShuffleWriterType, SortShuffleWriterType}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

class BoltShuffleMetricsSuite extends SparkFunSuite with LocalSparkContext {
  test("Bolt RSS writers have timing metrics for hash and sort shuffle") {
    sc = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("bolt-rss-metrics")
      .set("spark.ui.enabled", "false"))
    val api = new BoltMetricsApi
    Seq(HashShuffleWriterType, RssSortShuffleWriterType, SortShuffleWriterType).foreach {
      writerType =>
        val metrics = api.genColumnarShuffleExchangeMetrics(sc, writerType)
        Seq("splitTime", "rssWriteTime", "rssPushTime", "rssCloseWaitTime").foreach {
          key =>
            assert(metrics(key).metricType == "nsTiming")
            metrics(key).add(1000L)
            assert(metrics(key).value == 1000L)
        }
    }
  }
}
