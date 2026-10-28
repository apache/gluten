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

import org.apache.spark.TestUtils
import org.apache.spark.sql.{GlutenTestSetWithSystemPropertyTrait, Row}
import org.apache.spark.tags.SlowHiveTest

trait GlutenSQLQuerySuiteBase
  extends SQLQuerySuiteBase
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveResourcePathSupport {

  override def testNameBlackList: Seq[String] = Seq(
    // Rewritten with a workspace-backed script path because getTestResourcePath resolves
    // test-script.sh from the spark-hive tests jar in gluten-ut.
    "script"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    GlutenTestHiveTables.registerHiveQTestUtilsTables(hiveTestResourceDir)
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
