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
package org.apache.gluten.execution

import org.apache.gluten.config.{GlutenConfig, GlutenCoreConfig}

import org.apache.spark.SparkConf

class BoltRoughCostModelSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "id as c2", "array(id, id + 1) as c3")
      .write
      .format("parquet")
      .saveAsTable("tmp1")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table tmp1")
    super.afterAll()
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(GlutenCoreConfig.GLUTEN_COST_MODEL.key, "rough")
    .set(GlutenConfig.VANILLA_VECTORIZED_READERS_ENABLED.key, "false")

  test("fallback trivial project if its neighbor nodes fell back") {
    withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
      runQueryAndCompare("select c1 as c3 from tmp1") {
        checkFallbackOperators(_, 0)
      }
    }
  }

  test("avoid adding r2c whose schema contains complex data types") {
    withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
      runQueryAndCompare("select array_contains(c3, 0) as list from tmp1") {
        checkFallbackOperators(_, 0)
      }
    }
  }
}
