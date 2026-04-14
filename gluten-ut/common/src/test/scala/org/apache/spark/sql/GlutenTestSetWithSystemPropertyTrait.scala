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

/**
 * A test trait for suites whose parent creates per-test SparkContext/SparkSession (via
 * LocalSparkContext, LocalSparkSession, or direct `new SparkContext`).
 *
 * Injects GlutenPlugin config via system properties so per-test sessions inherit it. SparkConf
 * reads system properties prefixed with "spark." as defaults during construction.
 *
 * Do NOT use GlutenTestsTrait or GlutenSQLTestsTrait for these suites -- they create a shared
 * SparkSession in beforeAll() that conflicts with per-test SparkContext creation.
 */
trait GlutenTestSetWithSystemPropertyTrait extends GlutenTestsCommonTrait {

  override def beforeAll(): Unit = {
    System.setProperty("spark.plugins", "org.apache.gluten.GlutenPlugin")
    System.setProperty("spark.memory.offHeap.enabled", "true")
    System.setProperty("spark.memory.offHeap.size", "1024MB")
    System.setProperty(
      "spark.shuffle.manager",
      "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    System.clearProperty("spark.plugins")
    System.clearProperty("spark.memory.offHeap.enabled")
    System.clearProperty("spark.memory.offHeap.size")
    System.clearProperty("spark.shuffle.manager")
  }
}
