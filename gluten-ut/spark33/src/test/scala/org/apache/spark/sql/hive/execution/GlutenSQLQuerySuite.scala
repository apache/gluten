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

import java.sql.Date

trait GlutenSQLQuerySuiteBase
  extends SQLQuerySuiteBase
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveResourcePathSupport {

  override def testNameBlackList: Seq[String] = Seq(
    // Rewritten with workspace-backed jar paths because TestHive.getHiveFile resolves
    // these jars from the Spark test jars in gluten-ut.
    "script",
    "describe functions - user defined functions",
    "describe functions - temporary user defined functions",
    "SPARK-32668: HiveGenericUDTF initialize UDTF should use StructObjectInspector method"
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
      val scriptFilePath = hiveResourcePath("test_script.sh").toFile.getCanonicalPath
      val df = Seq(("x1", "y1", "z1"), ("x2", "y2", "z2")).toDF("c1", "c2", "c3")
      df.createOrReplaceTempView("script_table")
      val query = sql(s"""
                         |SELECT col1 FROM (from(SELECT c1, c2, c3 FROM script_table) tempt_table
                         |REDUCE c1, c2, c3 USING 'bash $scriptFilePath' AS
                         |(col1 STRING, col2 STRING)) script_test_table""".stripMargin)
      checkAnswer(query, Row("x1_y1") :: Row("x2_y2") :: Nil)
    }
  }

  testGluten("describe functions - user defined functions") {
    withUserDefinedFunction("udtf_count" -> false) {
      sql(s"""
             |CREATE FUNCTION udtf_count
             |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
             |USING JAR '${hiveResourcePath("TestUDTF.jar").toUri}'
        """.stripMargin)

      checkKeywordsExist(
        sql("describe function udtf_count"),
        s"Function: default.udtf_count",
        s"Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A"
      )
      checkAnswer(
        sql("SELECT udtf_count(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
      checkKeywordsExist(
        sql("describe function udtf_count"),
        s"Function: default.udtf_count",
        s"Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A"
      )
    }
  }

  testGluten("describe functions - temporary user defined functions") {
    withUserDefinedFunction("udtf_count_temp" -> true) {
      sql(s"""
             |CREATE TEMPORARY FUNCTION udtf_count_temp
             |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
             |USING JAR '${hiveResourcePath("TestUDTF.jar").toUri}'
        """.stripMargin)

      checkKeywordsExist(
        sql("describe function udtf_count_temp"),
        "Function: udtf_count_temp",
        s"Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A")
      checkAnswer(
        sql("SELECT udtf_count_temp(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
      checkKeywordsExist(
        sql("describe function udtf_count_temp"),
        "Function: udtf_count_temp",
        s"Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A")
    }
  }

  testGluten(
    "SPARK-32668: HiveGenericUDTF initialize UDTF should use StructObjectInspector method") {
    withUserDefinedFunction("udtf_stack1" -> true, "udtf_stack2" -> true) {
      sql(s"""
             |CREATE TEMPORARY FUNCTION udtf_stack1
             |AS 'org.apache.spark.sql.hive.execution.UDTFStack'
             |USING JAR '${hiveResourcePath("SPARK-21101-1.0.jar").toUri}'
        """.stripMargin)
      sql(s"""
             |CREATE TEMPORARY FUNCTION udtf_stack2
             |AS 'org.apache.spark.sql.hive.execution.UDTFStack2'
             |USING JAR '${hiveResourcePath("SPARK-21101-1.0.jar").toUri}'
        """.stripMargin)

      Seq("udtf_stack1", "udtf_stack2").foreach {
        udf =>
          checkAnswer(
            sql(s"SELECT $udf(2, 'A', 10, date '2015-01-01', 'B', 20, date '2016-01-01')"),
            Seq(Row("A", 10, Date.valueOf("2015-01-01")), Row("B", 20, Date.valueOf("2016-01-01")))
          )
      }
    }
  }
}

@SlowHiveTest
class GlutenSQLQuerySuite extends SQLQuerySuite with GlutenSQLQuerySuiteBase {}

@SlowHiveTest
class GlutenSQLQuerySuiteAE extends SQLQuerySuiteAE with GlutenSQLQuerySuiteBase {}
