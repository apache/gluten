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
package org.apache.spark.sql.extension

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsTrait

class GlutenCustomerExtensionSuite extends GlutenSQLTestsTrait {
  // These configs only take effect on ClickHouse backend.
  private val ExtendedColumnarTransformRulesKey =
    "spark.gluten.sql.columnar.extended.columnar.transform.rules"
  private val ExtendedColumnarPostRulesKey =
    "spark.gluten.sql.columnar.extended.columnar.post.rules"

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
      .set(
        ExtendedColumnarTransformRulesKey,
        "org.apache.spark.sql" +
          ".extension.CustomerColumnarPreRules")
      .set(ExtendedColumnarPostRulesKey, "")
  }

  testGluten("test customer column rules") {
    withSQLConf((GlutenConfig.GLUTEN_ENABLED.key, "false")) {
      sql("create table my_parquet(id int) using parquet")
      sql("insert into my_parquet values (1)")
      sql("insert into my_parquet values (2)")
    }
    withSQLConf((GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, "false")) {
      val df = sql("select * from my_parquet")
      val testFileSourceScanExecTransformer = df.queryExecution.executedPlan.collect {
        case f: TestFileSourceScanExecTransformer => f
      }
      assert(testFileSourceScanExecTransformer.nonEmpty)
      assert(testFileSourceScanExecTransformer.head.nodeNamePrefix.equals("TestFile"))
    }
  }
}
