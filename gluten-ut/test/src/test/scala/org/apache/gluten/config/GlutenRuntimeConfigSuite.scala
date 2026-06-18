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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession

class GlutenRuntimeConfigSuite extends GlutenQueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
  }

  test("Gluten configs report correct runtime modifiability") {
    val conf = SparkSession.active.conf
    assert(conf.isModifiable(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key))
    assert(!conf.isModifiable(GlutenConfig.GLUTEN_UI_ENABLED.key))
  }

  test("GlutenConfig reads active SparkSession runtime configs") {
    val conf = SparkSession.active.conf
    val key = GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key
    val original = conf.get(key)
    try {
      conf.set(key, false)
      assert(!GlutenConfig.get.enableColumnarFileScan)
      conf.set(key, true)
      assert(GlutenConfig.get.enableColumnarFileScan)
    } finally {
      conf.set(key, original)
    }
  }

  test("Memory manager capacity ratio config validation") {

    assert(GlutenConfig.MEMORY_MANAGER_CAPACITY_RATIO.defaultValue.get == 0.75)

    withSQLConf(GlutenConfig.MEMORY_MANAGER_CAPACITY_RATIO.key -> "0.8") {
      assert(GlutenConfig.get.getConf(GlutenConfig.MEMORY_MANAGER_CAPACITY_RATIO) == 0.8)
    }

    intercept[IllegalArgumentException] {
      withSQLConf(GlutenConfig.MEMORY_MANAGER_CAPACITY_RATIO.key -> "1.5") {
        GlutenConfig.get.getConf(GlutenConfig.MEMORY_MANAGER_CAPACITY_RATIO)
      }
    }
  }
}
