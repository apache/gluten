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

class ArithmeticAnsiValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

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
    if (isSparkVersionGE("3.4")) {
      // Spark 3.4+ throws exception for division by zero in ANSI mode
      intercept[SparkException] {
        sql("SELECT 1 / 0").collect()
      }
    } else {
      // Spark 3.3 doesn't throw exception for division by zero in ANSI mode
      sql("SELECT 1 / 0").collect()
    }
  }

  test("div") {
    runQueryAndCompare("SELECT int_field1 div 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    if (isSparkVersionGE("3.4")) {
      intercept[SparkException] {
        sql("SELECT 1 div 0 ").collect()
      }
    }
  }

  test("decimal add overflow") {
    // Normal decimal add should succeed and match Spark results
    runQueryAndCompare(
      "SELECT CAST(1.0 AS DECIMAL(10,2)) + CAST(2.0 AS DECIMAL(10,2))") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // Overflow: max DECIMAL(38,0) + 1 should throw in ANSI mode
    if (isSparkVersionGE("4.0")) {
      intercept[SparkException] {
        sql("SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) + " +
          "CAST(1 AS DECIMAL(38,0))").collect()
      }
    } else {
      intercept[ArithmeticException] {
        sql("SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) + " +
          "CAST(1 AS DECIMAL(38,0))").collect()
      }
    }
  }

  test("decimal subtract overflow") {
    // Normal decimal subtract should succeed and match Spark results
    runQueryAndCompare(
      "SELECT CAST(5.0 AS DECIMAL(10,2)) - CAST(2.0 AS DECIMAL(10,2))") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // Overflow: -max DECIMAL(38,0) - 1 should throw in ANSI mode
    if (isSparkVersionGE("4.0")) {
      intercept[SparkException] {
        sql("SELECT CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0)) - " +
          "CAST(1 AS DECIMAL(38,0))").collect()
      }
    } else {
      intercept[ArithmeticException] {
        sql("SELECT CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0)) - " +
          "CAST(1 AS DECIMAL(38,0))").collect()
      }
    }
  }

  test("decimal try_add") {
    // Normal case should match Spark results
    runQueryAndCompare(
      "SELECT try_add(CAST(1.0 AS DECIMAL(10,2)), CAST(2.0 AS DECIMAL(10,2)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Overflow should return null
    runQueryAndCompare(
      "SELECT try_add(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), " +
        "CAST(1 AS DECIMAL(38,0)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("decimal try_subtract") {
    // Normal case should match Spark results
    runQueryAndCompare(
      "SELECT try_subtract(CAST(5.0 AS DECIMAL(10,2)), CAST(2.0 AS DECIMAL(10,2)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Overflow should return null
    runQueryAndCompare(
      "SELECT try_subtract(CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0)), " +
        "CAST(1 AS DECIMAL(38,0)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("decimal multiply overflow") {
    // Normal decimal multiply should succeed and match Spark results
    runQueryAndCompare(
      "SELECT CAST(2.0 AS DECIMAL(10,2)) * CAST(3.0 AS DECIMAL(10,2))") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // Overflow: max DECIMAL(38,0) * 2 should throw in ANSI mode
    if (isSparkVersionGE("4.0")) {
      intercept[SparkException] {
        sql("SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) * " +
          "CAST(2 AS DECIMAL(38,0))").collect()
      }
    } else {
      intercept[ArithmeticException] {
        sql("SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) * " +
          "CAST(2 AS DECIMAL(38,0))").collect()
      }
    }
  }

  test("decimal try_multiply") {
    // Normal case should match Spark results
    runQueryAndCompare(
      "SELECT try_multiply(CAST(2.0 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Overflow should return null
    runQueryAndCompare(
      "SELECT try_multiply(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), " +
        "CAST(2 AS DECIMAL(38,0)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

}
