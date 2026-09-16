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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.internal.SQLConf

class VeloxInsertSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "placeholder"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
  }

  test("storeAssignmentPolicy default ANSI is independent from ANSI mode") {
    withTable("store_assignment_ansi_src", "store_assignment_ansi") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assert(SQLConf.get.storeAssignmentPolicy == SQLConf.StoreAssignmentPolicy.ANSI)

        createTableWithValue("store_assignment_ansi_src", "STRING", "'2147483648'")
        createTable("store_assignment_ansi", "INT")
        assertUnsafeCastAnalysisException("STRING", "INT") {
          insertIntoFrom("store_assignment_ansi", "store_assignment_ansi_src").collect()
        }

        withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.LEGACY.toString) {
          val insert = insertIntoFrom("store_assignment_ansi", "store_assignment_ansi_src")
          insert.collect()
          checkGlutenPlan[ProjectExecTransformer](insert)
          checkAnswer(spark.table("store_assignment_ansi"), Row(null))
        }
      }
    }
  }

  test("storeAssignmentPolicy preserves configured cast modes") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withTable("store_assignment_ansi_src", "store_assignment_ansi") {
        createTableWithValue("store_assignment_ansi_src", "STRING", "'2147483648'")
        createTable("store_assignment_ansi", "INT")

        withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
          assertUnsafeCastAnalysisException("STRING", "INT") {
            insertIntoFrom("store_assignment_ansi", "store_assignment_ansi_src").collect()
          }
          checkAnswer(spark.table("store_assignment_ansi"), Seq.empty[Row])
        }
      }
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("store_assignment_legacy_src", "store_assignment_legacy") {
        createTableWithValue("store_assignment_legacy_src", "STRING", "'2147483648'")
        createTable("store_assignment_legacy", "INT")

        // Disable the whole-plan ANSI fallback so the legacy store-assignment cast
        // gets a chance to offload while the session runs in ANSI mode.
        withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.LEGACY.toString,
          GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false"
        ) {
          val insert = insertIntoFrom("store_assignment_legacy", "store_assignment_legacy_src")
          insert.collect()
          checkGlutenPlan[ProjectExecTransformer](insert)
          checkAnswer(spark.table("store_assignment_legacy"), Row(null))
        }
      }
    }
  }

  test("storeAssignmentPolicy strict rejects unsafe insert casts") {
    withTable("store_assignment_strict_src", "store_assignment_strict") {
      withSQLConf(
        SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.STRICT.toString) {
        createTableWithValue("store_assignment_strict_src", "INT", "1")
        createTable("store_assignment_strict", "TINYINT")

        assertUnsafeCastAnalysisException("INT", "TINYINT") {
          insertIntoFrom("store_assignment_strict", "store_assignment_strict_src").collect()
        }
        checkAnswer(spark.table("store_assignment_strict"), Seq.empty[Row])
      }
    }
  }

  private def createTable(table: String, dataType: String): Unit =
    spark.sql(s"CREATE TABLE $table (c $dataType) USING PARQUET")

  private def createTableWithValue(table: String, dataType: String, value: String): Unit = {
    createTable(table, dataType)
    spark.sql(s"INSERT INTO $table VALUES ($value)").collect()
  }

  private def insertIntoFrom(target: String, source: String) =
    spark.sql(s"INSERT INTO $target SELECT c FROM $source")

  private def assertUnsafeCastAnalysisException(
      fromType: String,
      toType: String)(f: => Unit): Unit = {
    val exception = intercept[AnalysisException](f)
    // Spark 3.3/3.4 report the types in lower case, e.g. "string to int". Since Spark 3.5,
    // the types in the exception message are quoted in upper case, e.g. "STRING" to "INT".
    // The case conversion can be removed once Spark 3.4 and earlier are no longer supported.
    val message = exceptionMessages(exception).toLowerCase()
    assert(message.contains(fromType.toLowerCase()), message)
    assert(message.contains(toType.toLowerCase()), message)
    assert(message.contains("cast"), message)
  }

  private def exceptionMessages(e: Throwable): String = {
    val message = Option(e.getMessage).getOrElse("")
    if (e.getCause == null) {
      message
    } else {
      message + "\n" + exceptionMessages(e.getCause)
    }
  }
}
