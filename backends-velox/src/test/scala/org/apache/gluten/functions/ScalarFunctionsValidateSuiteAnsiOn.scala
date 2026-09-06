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

class ScalarFunctionsValidateSuiteAnsiOn extends FunctionsValidateSuite {

  disableFallbackCheck

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("elt") {
    // int_field1 is 1, 2, 3, so every index is within range here.
    runQueryAndCompare("SELECT elt(int_field1, 'a', 'b', 'c') FROM datatab") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // A NULL index gives NULL rather than an error, and a NULL selected input stays NULL.
    runQueryAndCompare("SELECT elt(NULL, 'a', 'b'), elt(1, string_field1, 'b') FROM datatab") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // An out-of-range index raises an error in ANSI mode. int_field1 - 1 is 0 for the
    // first row, and int_field1 + 3 is beyond the number of inputs for every row.
    intercept[SparkException] {
      sql("SELECT elt(int_field1 - 1, 'a', 'b', 'c') FROM datatab").collect()
    }
    intercept[SparkException] {
      sql("SELECT elt(int_field1 + 3, 'a', 'b', 'c') FROM datatab").collect()
    }
  }

  test("element_at") {
    // In-bound indices, including the negative ones counting from the end of the array,
    // are unaffected by ANSI mode.
    runQueryAndCompare(
      "SELECT element_at(array(l_orderkey, l_partkey), 1)," +
        " element_at(array(l_orderkey, l_partkey), -1) FROM lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // A key the map does not contain gives NULL, in ANSI mode as well.
    runQueryAndCompare("SELECT element_at(map(1, 'a', 2, 'b'), 3) FROM lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // An index past either end of the array raises an error in ANSI mode.
    intercept[SparkException] {
      sql("SELECT element_at(array(l_orderkey, l_partkey), 3) FROM lineitem").collect()
    }
    intercept[SparkException] {
      sql("SELECT element_at(array(l_orderkey, l_partkey), -3) FROM lineitem").collect()
    }
    // An index of 0 is an error whatever the ANSI mode is.
    intercept[SparkException] {
      sql("SELECT element_at(array(l_orderkey, l_partkey), 0) FROM lineitem").collect()
    }
  }

  test("size") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, 2, 3), Seq.empty, null)
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("size_tbl")

        // Spark's legacySizeOfNull is 'spark.sql.legacy.sizeOfNull AND NOT ANSI mode', so
        // size(null) is NULL here whatever spark.sql.legacy.sizeOfNull says.
        Seq("true", "false").foreach {
          legacySizeOfNull =>
            withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> legacySizeOfNull) {
              runQueryAndCompare("SELECT size(i) FROM size_tbl") {
                checkGlutenPlan[ProjectExecTransformer]
              }
            }
        }
    }
  }
}
