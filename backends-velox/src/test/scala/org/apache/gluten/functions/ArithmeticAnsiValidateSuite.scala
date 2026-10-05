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
package org.apache.gluten.functions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal

class ArithmeticAnsiValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("add") {
    runQueryAndCompare("SELECT int_field1 + 100 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    val df = sql("SELECT 2147483647 + 1")

    if (isSparkVersionGE("4.0")) {
      intercept[SparkException] {
        df.collect()
      }
    } else {
      intercept[ArithmeticException] {
        df.collect()
      }
    }
  }

  test("subtract") {
    runQueryAndCompare("SELECT int_field1 - 50 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("multiply") {
    runQueryAndCompare("SELECT int_field1 * 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    val df = sql("SELECT 2147483647 + 1")
    if (isSparkVersionGE("4.0")) {
      intercept[SparkException] {
        df.collect()
      }
    } else {
      intercept[ArithmeticException] {
        df.collect()
      }
    }
  }

  test("divide") {
    runQueryAndCompare("SELECT int_field1 / 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Spark 3.4+ throws exception for division by zero in ANSI mode
    intercept[SparkException] {
      sql("SELECT 1 / 0").collect()
    }
  }

  test("div") {
    runQueryAndCompare("SELECT int_field1 div 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    intercept[SparkException] {
      sql("SELECT 1 div 0 ").collect()
    }
  }

  testWithMinSparkVersion("try_make_timestamp returns NULL for invalid input", "4.0") {
    withTempPath {
      path =>
        Seq((2024, 13, 1, 0, 0, Decimal(0, 18, 6)), (2024, 1, 1, 6, 30, Decimal(45678000, 18, 6)))
          .toDF("year", "month", "day", "hour", "min", "sec")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("try_make_timestamp_tbl")

        runQueryAndCompare("""
                             |select try_make_timestamp(year, month, day, hour, min, sec),
                             |  try_make_timestamp_ltz(year, month, day, hour, min, sec)
                             |from try_make_timestamp_tbl
                             |""".stripMargin) {
          checkGlutenPlan[ProjectExecTransformer]
        }

        intercept[SparkException] {
          sql("select make_timestamp(year, month, day, hour, min, sec) from try_make_timestamp_tbl")
            .collect()
        }
    }
  }
}
