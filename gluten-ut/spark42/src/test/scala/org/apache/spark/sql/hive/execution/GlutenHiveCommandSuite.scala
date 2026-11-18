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

import org.apache.spark.sql.{AnalysisException, GlutenTestSetWithSystemPropertyTrait, Row}
import org.apache.spark.tags.SlowHiveTest

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption

@SlowHiveTest
class GlutenHiveCommandSuite extends HiveCommandSuite with GlutenTestSetWithSystemPropertyTrait {

  override def testNameBlackList: Seq[String] = super.testNameBlackList ++ Seq(
    // Rewritten with a workspace-backed file because TestHive.getHiveFile resolves this resource
    // from the spark-hive tests jar in gluten-ut.
    "LOAD DATA LOCAL",
    "LOAD DATA"
  )

  Seq(true, false).foreach {
    local =>
      val loadQuery = if (local) "LOAD DATA LOCAL" else "LOAD DATA"
      testGluten(loadQuery) {
        testLoadData(loadQuery, local)
      }
  }

  private def testLoadData(loadQuery: String, local: Boolean): Unit = {
    // employee.dat has two columns separated by '|', the first is an int, the second is a string.
    // Its content looks like:
    // 16|john
    // 17|robert
    val testData = getWorkspaceFilePath(
      "sql",
      "hive",
      "src",
      "test",
      "resources",
      "data",
      "files",
      "employee.dat").toFile.getCanonicalFile

    def withInputFile(fn: File => Unit): Unit = {
      if (local) {
        fn(testData)
      } else {
        val tmp = File.createTempFile(testData.getName(), ".tmp")
        Files.copy(testData.toPath, tmp.toPath, StandardCopyOption.REPLACE_EXISTING)
        try {
          fn(tmp)
        } finally {
          tmp.delete()
        }
      }
    }

    withTable("non_part_table", "part_table") {
      sql("""
            |CREATE TABLE non_part_table (employeeID INT, employeeName STRING)
            |ROW FORMAT DELIMITED
            |FIELDS TERMINATED BY '|'
            |LINES TERMINATED BY '\n'
        """.stripMargin)

      // LOAD DATA INTO non-partitioned table can't specify partition
      intercept[AnalysisException] {
        sql(
          s"""$loadQuery INPATH "${testData.toURI}" INTO TABLE non_part_table PARTITION(ds="1")""")
      }

      withInputFile {
        path =>
          sql(s"""$loadQuery INPATH "${path.toURI}" INTO TABLE non_part_table""")

          // Non-local mode is expected to move the file, while local mode is expected to copy it.
          // Check once here that the behavior is the expected.
          assert(local === path.exists())
      }

      checkAnswer(sql("SELECT * FROM non_part_table WHERE employeeID = 16"), Row(16, "john") :: Nil)

      // Incorrect URI.
      // file://path/to/data/files/employee.dat
      //
      // TODO: need a similar test for non-local mode.
      if (local) {
        val incorrectUri = "file://path/to/data/files/employee.dat"
        intercept[AnalysisException] {
          sql(s"""LOAD DATA LOCAL INPATH "$incorrectUri" INTO TABLE non_part_table""")
        }
      }

      // Use URI as inpath:
      // file:/path/to/data/files/employee.dat
      withInputFile {
        path => sql(s"""$loadQuery INPATH "${path.toURI}" INTO TABLE non_part_table""")
      }

      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Row(16, "john") :: Nil)

      // Overwrite existing data.
      withInputFile {
        path => sql(s"""$loadQuery INPATH "${path.toURI}" OVERWRITE INTO TABLE non_part_table""")
      }

      checkAnswer(sql("SELECT * FROM non_part_table WHERE employeeID = 16"), Row(16, "john") :: Nil)

      sql("""
            |CREATE TABLE part_table (employeeID INT, employeeName STRING)
            |PARTITIONED BY (c STRING, d STRING)
            |ROW FORMAT DELIMITED
            |FIELDS TERMINATED BY '|'
            |LINES TERMINATED BY '\n'
        """.stripMargin)

      // LOAD DATA INTO partitioned table must specify partition
      withInputFile {
        f =>
          val path = f.toURI
          intercept[AnalysisException] {
            sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table""")
          }

          intercept[AnalysisException] {
            sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table PARTITION(c="1")""")
          }
          intercept[AnalysisException] {
            sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table PARTITION(d="1")""")
          }
          intercept[AnalysisException] {
            sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table PARTITION(c="1", k="2")""")
          }
      }

      withInputFile {
        f =>
          sql(s"""$loadQuery INPATH "${f.toURI}" INTO TABLE part_table PARTITION(c="1", d="2")""")
      }
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1' AND d = '2'"),
        sql("SELECT * FROM non_part_table").collect())

      // Different order of partition columns.
      withInputFile {
        f =>
          sql(s"""$loadQuery INPATH "${f.toURI}" INTO TABLE part_table PARTITION(d="1", c="2")""")
      }
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '2' AND d = '1'"),
        sql("SELECT * FROM non_part_table"))
    }
  }
}
