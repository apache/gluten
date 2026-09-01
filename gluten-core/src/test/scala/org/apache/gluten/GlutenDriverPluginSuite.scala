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

import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.SparkConf

import org.scalatest.funsuite.AnyFunSuite

class GlutenDriverPluginSuite extends AnyFunSuite {

  test("setPredefinedConfigs does not throw in untracked mode without an off-heap size") {
    // Untracked memory mode skips the off-heap size requirement in checkOffHeapSettings, so
    // spark.memory.offHeap.size may be absent. setPredefinedConfigs must not read it with the
    // single-arg getSizeAsBytes, which throws NoSuchElementException on a missing key.
    val conf = new SparkConf(false)
      .set("spark.master", "local[1]")
      .set(GlutenCoreConfig.COLUMNAR_MEMORY_UNTRACKED.key, "true")
    GlutenDriverPlugin.setPredefinedConfigs(conf)
    assert(conf.getLong(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key, -1L) == 0L)
    // The raw key is normalized to 0 so downstream readers (e.g. VeloxListenerApi.onDriverStart)
    // that call the single-arg getSizeAsBytes don't hit NoSuchElementException later.
    assert(conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY) == 0L)
  }

  test("setPredefinedConfigs reads the configured off-heap size in the normal path") {
    val conf = new SparkConf(false)
      .set("spark.master", "local[1]")
      .set(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, "true")
      .set(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, "512m")
    GlutenDriverPlugin.setPredefinedConfigs(conf)
    assert(
      conf.getLong(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key, -1L) == 512L * 1024 * 1024)
  }
}
