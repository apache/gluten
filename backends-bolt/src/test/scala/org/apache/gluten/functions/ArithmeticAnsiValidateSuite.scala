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
    intercept[ArithmeticException] {
      sql("SELECT 2147483647 + 1").collect()
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
    intercept[ArithmeticException] {
      sql("SELECT 2147483647 * 2").collect()
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
      // Spark 3.2 and 3.3 don't throw exception for division by zero in ANSI mode
      sql("SELECT 1 / 0").collect()
    }
  }

}
