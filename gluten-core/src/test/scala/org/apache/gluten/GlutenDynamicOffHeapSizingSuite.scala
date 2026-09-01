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
package org.apache.gluten

import org.apache.gluten.component.WithDummyBackend
import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.funsuite.AnyFunSuite

class GlutenDynamicOffHeapSizingSuite extends AnyFunSuite with WithDummyBackend {

  private val MIB = 1024L * 1024L

  test("dynamic off-heap sizing derives the budget from a suffix-less executor memory") {
    // spark.executor.memory is MiB-unless-suffixed. Reading 8192 as bytes made the budget
    // negative, so drive the real driver-init path and assert the derived budget. spark.testing
    // zeroes Spark's reserved memory; without it UnifiedMemoryManager rejects 8192 bytes before
    // the plugin runs, which is the very read this test pins down.
    val conf = new SparkConf(false)
      .setAppName("GlutenDynamicOffHeapSizingSuite")
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[GlutenPlugin].getName)
      .set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .set(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, "true")
      .set("spark.executor.memory", "8192")
    val sc = new SparkContext(conf)
    try {
      val expected = ((8192L * MIB - 300L * MIB) * 0.6d).toLong
      assert(expected == 4965217075L)
      assert(
        sc.getConf.getLong(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key, -1L) == expected)
      assert(
        sc.getConf.getLong(GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key, -1L) > 0L)
    } finally {
      sc.stop()
    }
  }
}
