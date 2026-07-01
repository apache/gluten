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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, GlutenTestsCommonTrait, SparkSession}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
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
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val conf = new SparkConf()
      .setMaster("local-cluster[2,1,1024]")

    val sparkBuilder = SparkSession
      .builder()
      .config(GlutenSQLTestsBaseTrait.nativeSparkConf(conf, warehouse))
    spark = sparkBuilder.getOrCreate()

  }
}
