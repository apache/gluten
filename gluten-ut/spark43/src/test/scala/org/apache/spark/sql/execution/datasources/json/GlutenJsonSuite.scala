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
package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.SparkConf
import org.apache.spark.sql.{sources, GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, NoopCache}
import org.apache.spark.sql.execution.datasources.v2.json.JsonScanBuilder
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types.{DateType, IntegerType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.scalatest.exceptions.TestFailedException

import java.sql.{Date, Timestamp}

class GlutenJsonSuite extends JsonSuite with GlutenSQLTestsBaseTrait {

  /** Returns full path to the given file in the resource folder */
  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + fileName
  }
}

class GlutenJsonV1Suite extends GlutenJsonSuite with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "json")
}

class GlutenJsonV2Suite extends GlutenJsonSuite with GlutenSQLTestsBaseTrait {

  import testImplicits._
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  testGluten("get pushed filters") {
    val attr = "col"
    def getBuilder(path: String): JsonScanBuilder = {
      val fileIndex = new InMemoryFileIndex(
        spark,
        Seq(new org.apache.hadoop.fs.Path(path, "file.json")),
        Map.empty,
        None,
        NoopCache)
      val schema = new StructType().add(attr, IntegerType)
      val options = CaseInsensitiveStringMap.empty()
      new JsonScanBuilder(spark, fileIndex, schema, schema, options)
    }
    val filters: Array[sources.Filter] = Array(sources.IsNotNull(attr))
    withSQLConf(SQLConf.JSON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath {
        file =>
          val scanBuilder = getBuilder(file.getCanonicalPath)
          assert(scanBuilder.pushDataFilters(filters) === filters)
      }
    }

    withSQLConf(SQLConf.JSON_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      withTempPath {
        file =>
          val scanBuilder = getBuilder(file.getCanonicalPath)
          assert(scanBuilder.pushDataFilters(filters) === Array.empty[sources.Filter])
      }
    }
  }

  testGluten("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern") {
    withTempPath {
      path =>
        Seq(
          """{"date": "2020011", "ts": "2020011"}""",
          """{"date": "20201203", "ts": "20201203"}""")
          .toDF()
          .repartition(1)
          .write
          .text(path.getAbsolutePath)
        val schema = new StructType()
          .add("date", DateType)
          .add("ts", TimestampType)
        val output = spark.read
          .schema(schema)
          .option("dateFormat", "yyyyMMdd")
          .option("timestampFormat", "yyyyMMdd")
          .json(path.getAbsolutePath)

        def check(mode: String, res: Seq[Row]): Unit = {
          withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> mode) {
            checkAnswer(output, res)
          }
        }

        check(
          "legacy",
          Seq(
            Row(Date.valueOf("2020-01-01"), Timestamp.valueOf("2020-01-01 00:00:00")),
            Row(Date.valueOf("2020-12-03"), Timestamp.valueOf("2020-12-03 00:00:00"))
          )
        )

        check(
          "corrected",
          Seq(
            Row(null, null),
            Row(Date.valueOf("2020-12-03"), Timestamp.valueOf("2020-12-03 00:00:00"))
          )
        )

        val err = intercept[TestFailedException] {
          check("exception", Nil)
        }
        assert(err.message.get.contains("org.apache.spark.SparkUpgradeException"))
    }
  }
}

class GlutenJsonLegacyTimeParserSuite extends GlutenJsonSuite with GlutenSQLTestsBaseTrait {

  override def excluded: Seq[String] =
    Seq("Write timestamps correctly in ISO8601 format by default")

  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.LEGACY_TIME_PARSER_POLICY, LegacyBehaviorPolicy.LEGACY)
}
