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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf

import java.util.Locale

class GlutenFailOnFallbackSuite extends WholeStageTransformerSuite {

  protected val resourcePath: String = null
  protected val fileFormat: String = null

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf.set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    }
    conf
  }

  test("failOnFallback throws only when a fallback is present") {
    withTable("t") {
      spark.range(10).write.format("parquet").saveAsTable("t")

      // failOnFallback explicitly disabled: a fallback must NOT throw.
      withSQLConf(
        GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "false",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        sql("SELECT * FROM t").collect()
      }

      // failOnFallback enabled, fully native query (no fallback): must NOT throw.
      withSQLConf(GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true") {
        sql("SELECT * FROM t").collect()
      }

      // failOnFallback enabled, fallback present: must throw with the config key
      // and the offending operator/reason in the message.
      withSQLConf(
        GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        val ex = intercept[GlutenException] {
          sql("SELECT * FROM t").collect()
        }
        assert(ex.getMessage.contains(GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key))
        assert(ex.getMessage.contains("fell back to Spark"))
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("scan"))
      }
    }
  }
}
