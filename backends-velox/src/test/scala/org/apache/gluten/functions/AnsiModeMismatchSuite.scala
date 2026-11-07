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
import org.apache.gluten.execution.{HashAggregateExecTransformer, ProjectExecTransformer, WholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf

import scala.reflect.ClassTag

/**
 * A view keeps the ANSI mode it was created with, so a query can run some expressions with ANSI
 * mode on and others with it off. Velox functions that read the mode from the session config cannot
 * follow that, so those expressions fall back to Spark; functions with a name per mode stay native.
 */
class AnsiModeMismatchSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  disableFallbackCheck

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
  }

  // Reads the rows from a Parquet file so the expressions are evaluated in Velox and not folded
  // by the optimizer. The values never trigger an error in either mode, so both modes return the
  // same rows and the test only has to look at where the query ran.
  private def withIntTable(f: => Unit): Unit = {
    withTempPath {
      path =>
        sql("SELECT * FROM VALUES (10, 3), (-7, 2) AS t(c, d)")
          .write
          .parquet(path.getCanonicalPath)
        withTempView("int_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("int_tab")
          f
        }
    }
  }

  // Checks that no operator of type `T` was offloaded.
  private def checkFallback[T: ClassTag](df: DataFrame): Unit = {
    val tag = implicitly[ClassTag[T]]
    assert(
      !getExecutedPlan(df).exists(p => tag.runtimeClass.isInstance(p)),
      df.queryExecution.executedPlan)
  }

  // Creates a temporary view with ANSI mode set to `ansi`. The view captures that setting and
  // keeps it when it is read under another setting.
  private def createView(name: String, body: String, ansi: Boolean): Unit = {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
      sql(s"CREATE OR REPLACE TEMP VIEW $name AS $body")
    }
  }

  test("view created with ANSI off, read with ANSI on") {
    withIntTable {
      withTempView("legacy_view") {
        createView("legacy_view", "SELECT abs(c) AS a, c % d AS m, c + d AS s FROM int_tab", false)
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          // abs and % read the session config in Velox: the project falls back.
          runQueryAndCompare("SELECT a, m FROM legacy_view", noFallBack = false) {
            df => checkFallback[ProjectExecTransformer](df)
          }
          // + has a function per mode: the project stays native.
          runQueryAndCompare("SELECT s FROM legacy_view") {
            checkGlutenPlan[ProjectExecTransformer]
          }
        }
      }
    }
  }

  test("view created with ANSI on, read with ANSI off") {
    withIntTable {
      withTempView("ansi_view") {
        createView("ansi_view", "SELECT abs(c) AS a, c % d AS m, c + d AS s FROM int_tab", true)
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
          runQueryAndCompare("SELECT a, m FROM ansi_view", noFallBack = false) {
            df => checkFallback[ProjectExecTransformer](df)
          }
          runQueryAndCompare("SELECT s FROM ansi_view") {
            checkGlutenPlan[ProjectExecTransformer]
          }
        }
      }
    }
  }

  test("view created with ANSI on still throws under the default ANSI fallback") {
    // The default ANSI fallback looks only at the session, so with the session's ANSI mode off
    // it leaves the plan native, and the view's abs must still throw on overflow.
    withSQLConf(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "true") {
      withTempPath {
        path =>
          sql("SELECT CAST(-2147483648 AS INT) AS c").write.parquet(path.getCanonicalPath)
          withTempView("min_tab", "abs_view") {
            spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("min_tab")
            createView("abs_view", "SELECT abs(c) AS a FROM min_tab", true)
            withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
              val e = intercept[Exception](sql("SELECT a FROM abs_view").collect())
              assert(e.getMessage.contains("overflow"), e.getMessage)
            }
          }
      }
    }
  }

  test("view created and read in the same mode stays native") {
    withIntTable {
      Seq(true, false).foreach {
        ansi =>
          withTempView("same_mode_view") {
            createView("same_mode_view", "SELECT abs(c) AS a, c % d AS m FROM int_tab", ansi)
            withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
              runQueryAndCompare("SELECT a, m FROM same_mode_view") {
                checkGlutenPlan[ProjectExecTransformer]
              }
            }
          }
      }
    }
  }

  test("aggregate in a view keeps its mode") {
    withIntTable {
      withTempView("sum_view") {
        createView("sum_view", "SELECT sum(c) AS total FROM int_tab", false)
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          runQueryAndCompare("SELECT total FROM sum_view", noFallBack = false) {
            df => checkFallback[HashAggregateExecTransformer](df)
          }
        }
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
          runQueryAndCompare("SELECT total FROM sum_view") {
            checkGlutenPlan[HashAggregateExecTransformer]
          }
        }
      }
    }
  }

  test("array subscript with ANSI on falls back until Velox can throw") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTempPath {
        path =>
          sql("SELECT array(1, 2, 3) AS arr, 1 AS i").write.parquet(path.getCanonicalPath)
          withTempView("arr_tab") {
            spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("arr_tab")
            runQueryAndCompare("SELECT arr[i] FROM arr_tab", noFallBack = false) {
              df => checkFallback[ProjectExecTransformer](df)
            }
            // get() never throws and maps to Velox's get.
            runQueryAndCompare("SELECT get(arr, i) FROM arr_tab") {
              checkGlutenPlan[ProjectExecTransformer]
            }
          }
      }
    }
  }

  // Spark throws on ceil / floor overflow of a double only since 4.3 (SPARK-58052). The suite
  // compiles only against earlier versions, which saturate in both modes, so Velox matches Spark
  // only with ANSI mode off.
  private def withDoubleTable(f: => Unit): Unit = {
    withTempPath {
      path =>
        sql("SELECT * FROM VALUES (1.5D, 7L), (-2.5D, -7L), (1e20D, 1L), (-1e20D, -1L) AS t(c, l)")
          .write
          .parquet(path.getCanonicalPath)
        withTempView("double_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("double_tab")
          f
        }
    }
  }

  test("ceil and floor of a double fall back with ANSI on before Spark 4.3") {
    withDoubleTable {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        runQueryAndCompare("SELECT ceil(c), floor(c) FROM double_tab", noFallBack = false) {
          df => checkFallback[ProjectExecTransformer](df)
        }
        // Only doubles can overflow: ceil and floor of a bigint stay native.
        runQueryAndCompare("SELECT ceil(l), floor(l) FROM double_tab") {
          checkGlutenPlan[ProjectExecTransformer]
        }
      }
    }
  }

  test("ceil and floor of a double stay native with ANSI off") {
    withDoubleTable {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        runQueryAndCompare("SELECT ceil(c), floor(c) FROM double_tab") {
          checkGlutenPlan[ProjectExecTransformer]
        }
      }
    }
  }
}
