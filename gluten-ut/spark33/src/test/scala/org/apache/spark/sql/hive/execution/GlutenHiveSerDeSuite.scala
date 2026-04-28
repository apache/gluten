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
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.GlutenTestSetWithSystemPropertyTrait
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.tags.SlowHiveTest

@SlowHiveTest
class GlutenHiveSerDeSuite
  extends HiveSerDeSuite
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveComparisonTestSupport {

  /**
   * Mostly copied from Spark's [[HiveSerDeSuite]] and [[GlutenTestSetWithSystemPropertyTrait]], and
   * customized so `sales.txt` is loaded from the workspace-backed Hive test resources instead of
   * via [[TestHive.getHiveFile]].
   */
  override def beforeAll(): Unit = {
    System.setProperty("spark.plugins", "org.apache.gluten.GlutenPlugin")
    System.setProperty("spark.memory.offHeap.enabled", "true")
    System.setProperty("spark.memory.offHeap.size", "1024MB")
    System.setProperty(
      "spark.shuffle.manager",
      "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

    import TestHive._
    import org.apache.hadoop.hive.serde2.RegexSerDe

    TestHive.setCacheTables(false)
    sql(s"""CREATE TABLE IF NOT EXISTS sales (key STRING, value INT)
           |ROW FORMAT SERDE '${classOf[RegexSerDe].getCanonicalName}'
           |WITH SERDEPROPERTIES ("input.regex" = "([^ ]*)\t([^ ]*)")
           """.stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH '${hiveResourcePath("data/files/sales.txt").toFile.toURI}'
           |INTO TABLE sales""".stripMargin)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      System.clearProperty("spark.plugins")
      System.clearProperty("spark.memory.offHeap.enabled")
      System.clearProperty("spark.memory.offHeap.size")
      System.clearProperty("spark.shuffle.manager")
    }
  }
}
