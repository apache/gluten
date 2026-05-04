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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.classic.RuntimeConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.shim.GlutenTestsTrait

class GlutenRuntimeConfigSuite extends RuntimeConfigSuite with GlutenTestsTrait {
  test("Gluten configs report correct runtime modifiability") {
    val conf = new RuntimeConfig(new SQLConf)
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
}
