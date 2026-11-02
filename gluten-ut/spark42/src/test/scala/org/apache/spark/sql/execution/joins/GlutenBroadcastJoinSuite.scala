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
package org.apache.spark.sql.execution.joins

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.sql.{GlutenTestsCommonTrait, SparkSession}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.internal.SQLConf

/**
 * This test needs setting for spark test home (its source code), e.g., appending the following
 * setting for `mvn test`: -DargLine="-Dspark.test.home=/home/sparkuser/spark/".
 *
 * In addition, you also need build spark source code before running this test, e.g., with
 * `./build/mvn -DskipTests clean package`.
 */
class GlutenBroadcastJoinSuite extends BroadcastJoinSuite with GlutenTestsCommonTrait {

  /**
   * Create a new [[SparkSession]] running in local-cluster mode with unsafe and codegen enabled.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkBuilder = SparkSession
      .builder()
      .master("local-cluster[2,1,1024]")
      .appName("Gluten-UT")
      .master(s"local[2]")
      .config(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .config("spark.driver.memory", "1G")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.files.maxPartitionBytes", "134217728")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "1024MB")
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .config("spark.sql.warehouse.dir", warehouse)
      // Avoid static evaluation for literal input by spark catalyst.
      .config(
        "spark.sql.optimizer.excludedRules",
        ConstantFolding.ruleName + "," +
          NullPropagation.ruleName)
      // Avoid the code size overflow error in Spark code generation.
      .config("spark.sql.codegen.wholeStage", "false")

    spark = if (BackendTestUtils.isCHBackendLoaded()) {
      sparkBuilder
        .config("spark.io.compression.codec", "LZ4")
        .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
        .config("spark.sql.files.openCostInBytes", "134217728")
        .config("spark.unsafe.exceptionOnMemoryLeak", "true")
        .getOrCreate()
    } else {
      sparkBuilder
        .config("spark.unsafe.exceptionOnMemoryLeak", "true")
        .getOrCreate()
    }
  }
}
