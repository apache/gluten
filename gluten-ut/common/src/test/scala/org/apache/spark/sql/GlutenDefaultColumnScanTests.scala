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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.FileSourceScanExecTransformerBase

import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf

trait GlutenDefaultColumnScanTests extends GlutenSQLTestsBaseTrait {

  private def checkDefaultColumnScan(
      query: String,
      expected: Seq[Row],
      native: Boolean): Unit = {
    val df = sql(query)
    GlutenQueryTest.checkAnswer(df, expected)
    val plan = getExecutedPlan(df)
    assert(
      plan.count(_.isInstanceOf[FileSourceScanExecTransformerBase]) == (if (native) 1 else 0),
      df.queryExecution.executedPlan)
    assert(
      plan.count(_.isInstanceOf[FileSourceScanExec]) == (if (native) 0 else 1),
      df.queryExecution.executedPlan)
  }

  testGluten("file scan preserves existence defaults and stored nulls") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false"
    ) {
      withTable("existence_defaults") {
        sql("CREATE TABLE existence_defaults (id INT) USING parquet")
        sql("INSERT INTO existence_defaults VALUES (0)")
        sql("ALTER TABLE existence_defaults ADD COLUMN value INT DEFAULT 7")
        sql("ALTER TABLE existence_defaults ALTER COLUMN value SET DEFAULT 29")
        sql("INSERT INTO existence_defaults VALUES (1, NULL), (2, 23), (3, DEFAULT)")
        sql("ALTER TABLE existence_defaults ALTER COLUMN value DROP DEFAULT")

        checkDefaultColumnScan(
          "SELECT id, value FROM existence_defaults",
          Seq(Row(0, 7), Row(1, null), Row(2, 23), Row(3, 29)),
          native = false)
        checkDefaultColumnScan(
          "SELECT id FROM existence_defaults WHERE value = 7",
          Seq(Row(0)),
          native = false)
        checkDefaultColumnScan(
          "SELECT id FROM existence_defaults",
          Seq(Row(0), Row(1), Row(2), Row(3)),
          native = true)
        checkDefaultColumnScan(
          "SELECT count(*) FROM existence_defaults",
          Seq(Row(4L)),
          native = true)
      }
    }
  }

  testGluten("file scan keeps null existence defaults native after changing the write default") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false"
    ) {
      withTable("null_existence_defaults") {
        sql("CREATE TABLE null_existence_defaults (id INT) USING parquet")
        sql("INSERT INTO null_existence_defaults VALUES (0)")
        checkDefaultColumnScan(
          "SELECT id FROM null_existence_defaults",
          Seq(Row(0)),
          native = true)

        sql("ALTER TABLE null_existence_defaults ADD COLUMN value INT DEFAULT CAST(NULL AS INT)")
        sql("ALTER TABLE null_existence_defaults ALTER COLUMN value SET DEFAULT 9")
        sql("INSERT INTO null_existence_defaults VALUES (1, DEFAULT)")
        checkDefaultColumnScan(
          "SELECT id, value FROM null_existence_defaults",
          Seq(Row(0, null), Row(1, 9)),
          native = true)
      }
    }
  }
}
