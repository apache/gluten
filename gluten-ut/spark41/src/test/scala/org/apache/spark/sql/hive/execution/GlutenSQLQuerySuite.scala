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
package org.apache.spark.sql.hive.execution

import org.apache.spark.{SparkException, TestUtils}
import org.apache.spark.sql.{GlutenTestSetWithSystemPropertyTrait, Row}
import org.apache.spark.sql.execution.TestUncaughtExceptionHandler
import org.apache.spark.tags.SlowHiveTest

trait GlutenSQLQuerySuiteBase
  extends SQLQuerySuiteBase
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveResourcePathSupport {

  override def testNameBlackList: Seq[String] = Seq(
    // Rewritten with workspace-backed resource paths because the upstream tests resolve
    // resources from Spark test jars in gluten-ut.
    "script",
    "SPARK-25158: " +
      "Executor accidentally exit because ScriptTransformationWriterThread throw Exception"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    GlutenTestHiveTables.registerHiveQTestUtilsTables(hiveTestResourceDir)
  }

  testGluten(
    "SPARK-25158: " +
      "Executor accidentally exit because ScriptTransformationWriterThread throw Exception") {
    assume(TestUtils.testCommandAvailable("python3"))
    withTempView("test") {
      import spark.implicits._

      val defaultUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler
      try {
        val uncaughtExceptionHandler = new TestUncaughtExceptionHandler
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)

        // Use a bad udf to generate failed inputs.
        val badUDF = org.apache.spark.sql.functions.udf(
          (x: Int) => {
            if (x < 1) x
            else throw new RuntimeException("Failed to produce data.")
          })
        spark
          .range(5)
          .select(badUDF($"id").as("a"))
          .createOrReplaceTempView("test")
        val scriptFilePath = hiveResourcePath("data").toFile.getCanonicalPath
        val e = intercept[SparkException] {
          sql(
            s"""FROM test SELECT TRANSFORM(a)
               |USING 'python3 $scriptFilePath/scripts/test_transform.py "\t"'
              """.stripMargin).collect()
        }
        assert(e.getMessage.contains("Failed to produce data."))
        assert(uncaughtExceptionHandler.exception.isEmpty)
      } finally {
        Thread.setDefaultUncaughtExceptionHandler(defaultUncaughtExceptionHandler)
      }
    }
  }

  testGluten("script") {
    withTempView("script_table") {
      import spark.implicits._

      assume(TestUtils.testCommandAvailable("/bin/bash"))
      assume(TestUtils.testCommandAvailable("echo"))
      assume(TestUtils.testCommandAvailable("sed"))
      val scriptFilePath = hiveResourcePath("test-script.sh").toFile.getCanonicalPath
      val df = Seq(("x1", "y1", "z1"), ("x2", "y2", "z2")).toDF("c1", "c2", "c3")
      df.createOrReplaceTempView("script_table")
      val query = sql(s"""
                         |SELECT col1 FROM (from(SELECT c1, c2, c3 FROM script_table) tempt_table
                         |REDUCE c1, c2, c3 USING 'bash $scriptFilePath' AS
                         |(col1 STRING, col2 STRING)) script_test_table""".stripMargin)
      checkAnswer(query, Row("x1_y1") :: Row("x2_y2") :: Nil)
    }
  }
}

@SlowHiveTest
class GlutenSQLQuerySuite extends SQLQuerySuite with GlutenSQLQuerySuiteBase {}

@SlowHiveTest
class GlutenSQLQuerySuiteAE extends SQLQuerySuiteAE with GlutenSQLQuerySuiteBase {}
