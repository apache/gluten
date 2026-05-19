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

import org.apache.spark.sql.{GlutenTestSetWithSystemPropertyTrait, Row}
import org.apache.spark.tags.SlowHiveTest

@SlowHiveTest
class GlutenHiveUDFSuite
  extends HiveUDFSuite
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveResourcePathSupport {

  override def testNameBlackList: Seq[String] = Seq(
    "UDTF",
    "permanent UDTF"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    GlutenTestHiveTables.registerHiveQTestUtilsTables(hiveTestResourceDir)
  }

  testGluten("UDTF") {
    withUserDefinedFunction("udtf_count2" -> true) {
      sql(s"ADD JAR ${hiveResourcePath("TestUDTF.jar").toFile.getCanonicalPath}")
      sql("""
            |CREATE TEMPORARY FUNCTION udtf_count2
            |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
        """.stripMargin)

      checkAnswer(
        sql("SELECT key, cc FROM src LATERAL VIEW udtf_count2(value) dd AS cc"),
        Row(97, 500) :: Row(97, 500) :: Nil)

      checkAnswer(
        sql("SELECT udtf_count2(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
    }
  }

  testGluten("permanent UDTF") {
    withUserDefinedFunction("udtf_count_temp" -> false) {
      sql(s"""
             |CREATE FUNCTION udtf_count_temp
             |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
             |USING JAR '${hiveResourcePath("TestUDTF.jar").toUri}'
        """.stripMargin)

      checkAnswer(
        sql("SELECT key, cc FROM src LATERAL VIEW udtf_count_temp(value) dd AS cc"),
        Row(97, 500) :: Row(97, 500) :: Nil)

      checkAnswer(
        sql("SELECT udtf_count_temp(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
    }
  }
}
