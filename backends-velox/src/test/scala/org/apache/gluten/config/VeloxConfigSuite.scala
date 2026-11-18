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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SparkVersionUtil

import org.scalatest.funsuite.AnyFunSuite

class VeloxConfigSuite extends AnyFunSuite {

  private def veloxConfig(entries: (String, String)*): VeloxConfig = {
    val conf = new SQLConf
    entries.foreach { case (k, v) => conf.setConfString(k, v) }
    new VeloxConfig(conf)
  }

  private val buildOncePerExecutor =
    VeloxConfig.VELOX_BROADCAST_BUILD_HASHTABLE_ONCE_PER_EXECUTOR.key

  test("build hash table once per executor is enabled by default") {
    val conf = veloxConfig()
    assert(!conf.benchmarkDumpRequested)
    assert(conf.enableBroadcastBuildOncePerExecutor)
  }

  test("whole stage dumping builds the broadcast hash table per task") {
    // GLUTEN-12504: the micro benchmark can only replay a broadcast hash join whose build side
    // was streamed through the input iterator, and so dumped.
    Seq(
      GlutenConfig.BENCHMARK_TASK_STAGEID.key -> "1",
      GlutenConfig.BENCHMARK_TASK_TASK_ID.key -> "3"
    ).foreach {
      dumpTrigger =>
        val conf = veloxConfig(dumpTrigger, buildOncePerExecutor -> "true")
        assert(conf.benchmarkDumpRequested, dumpTrigger)
        assert(!conf.enableBroadcastBuildOncePerExecutor, dumpTrigger)
    }
  }

  test("build hash table once per executor can still be disabled explicitly") {
    assert(!veloxConfig(buildOncePerExecutor -> "false").enableBroadcastBuildOncePerExecutor)
  }

  private val nullStructKey = "spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing"

  test("native session conf passes Spark's default for returnNullStructIfAllFieldsMissing") {
    // Spark 4.1 added the conf with default false (SPARK-53535). Earlier versions behave as if it
    // is true.
    val native = GlutenConfig.getNativeSessionConf("velox", Map.empty)
    assert(native.get(nullStructKey).contains((!SparkVersionUtil.gteSpark41).toString))
  }

  test("returnNullStructIfAllFieldsMissing set in the session overrides the default") {
    Seq("true", "false").foreach {
      value =>
        val native = GlutenConfig.getNativeSessionConf("velox", Map(nullStructKey -> value))
        assert(native.get(nullStructKey).contains(value))
    }
  }
}
