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
import org.apache.spark.sql.internal.{SparkConfigUtil, SQLConf}
import org.apache.spark.sql.internal.SparkConfigUtil._

import org.scalatest.funsuite.AnyFunSuiteLike

class SparkConfigUtilSuite extends AnyFunSuiteLike {

  private object TestMode extends Enumeration {
    val FIRST, LAST = Value
  }

  private object TestConfigs extends ConfigRegistry {
    val mode = buildConf("spark.gluten.test.enum.mode")
      .enumConf(TestMode)
      .createWithDefault(TestMode.FIRST)

    override def get: GlutenCoreConfig = GlutenCoreConfig.get
  }

  test("SparkConfigUtil.get and set methods") {
    val conf = new SparkConf()
    conf.set(GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE, Some(1024 * 1024))
    assert(conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD) === 10L * 1024 * 1024)
    assert(conf.get(GlutenConfig.GLUTEN_UI_ENABLED) === true)
    assert(conf.get(GlutenConfig.TEXT_INPUT_ROW_MAX_BLOCK_SIZE) === 8L * 1024)
    assert(conf.get(GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE) === Some(1024 * 1024))
    assert(conf.get(GlutenConfig.GLUTEN_LOAD_LIB_OS).isEmpty)
  }

  test("SparkConfigUtil.get for java.util.Map conf") {
    val conf = new java.util.HashMap[String, String]
    conf.put(GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE.key, (1024 * 1024).toString)
    assert(SparkConfigUtil.get(conf, SQLConf.AUTO_BROADCASTJOIN_THRESHOLD) === 10L * 1024 * 1024)
    assert(SparkConfigUtil.get(conf, GlutenConfig.GLUTEN_UI_ENABLED) === true)
    assert(SparkConfigUtil.get(conf, GlutenConfig.TEXT_INPUT_ROW_MAX_BLOCK_SIZE) === 8L * 1024)
    assert(SparkConfigUtil.get(conf, GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE) === Some(1024 * 1024))
    assert(SparkConfigUtil.get(conf, GlutenConfig.GLUTEN_LOAD_LIB_OS).isEmpty)
  }

  test("enum config through SQLConf") {
    val conf = new SQLConf
    val glutenConf = new GlutenCoreConfig(conf)
    val entry = TestConfigs.mode

    assert(glutenConf.getConf(entry) == TestMode.FIRST)
    TestMode.values.foreach {
      mode =>
        conf.setConfString(entry.key, mode.toString.toLowerCase(java.util.Locale.ROOT))
        assert(glutenConf.getConf(entry) == mode)
    }
    conf.unsetConf(entry.key)
    assert(glutenConf.getConf(entry) == TestMode.FIRST)

    val error = intercept[IllegalArgumentException] {
      conf.setConfString(entry.key, "invalid")
    }
    assert(error.getMessage.contains(entry.key))
  }
}
