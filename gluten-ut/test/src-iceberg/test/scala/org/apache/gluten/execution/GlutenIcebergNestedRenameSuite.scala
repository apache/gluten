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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.commons.io.FileUtils

import java.nio.file.Files

class GlutenIcebergNestedRenameSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/"
  override protected val fileFormat: String = "parquet"

  private lazy val warehouse = Files.createTempDirectory("gluten-iceberg-nested-rename").toFile

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      FileUtils.deleteDirectory(warehouse)
    }
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set("spark.sql.catalog.spark_catalog.warehouse", warehouse.toURI.toString)
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.iceberg.aggregate-push-down.enabled", "false")

  for {
    column <- Seq("_row_id", "_last_updated_sequence_number")
    (path, projection) <- Seq("s.x" -> "s", "a.element.x" -> "a[0]", "m.value.x" -> "m['key']")
  } {
    test(s"iceberg rename $path to $column falls back") {
      assume(!BackendsApiManager.getSettings.supportIcebergEqualityDeleteRead())
      val name = "iceberg_nested_rename"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name
                       |(s STRUCT<x: BIGINT>, a ARRAY<STRUCT<x: BIGINT>>,
                       |m MAP<STRING, STRUCT<x: BIGINT>>) USING iceberg
                       |TBLPROPERTIES ('format-version' = '2',
                       |'write.format.default' = 'parquet')""".stripMargin)
          spark.sql(s"""INSERT INTO $name VALUES
                       |(named_struct('x', 7L), array(named_struct('x', 7L)),
                       |map('key', named_struct('x', 7L)))""".stripMargin)
        }
        runQueryAndCompare(s"SELECT $projection.x FROM $name") {
          df =>
            checkAnswer(df, Seq(Row(7L)))
            checkGlutenPlan[IcebergScanTransformer](df)
        }
        spark.sql(s"ALTER TABLE $name RENAME COLUMN $path TO $column")
        runQueryAndCompare(s"SELECT $projection.$column FROM $name", noFallBack = false) {
          df =>
            checkAnswer(df, Seq(Row(7L)))
            checkSparkPlan[BatchScanExec](df)
            checkGlutenPlanCount[IcebergScanTransformer](df, 0)
        }
      }
    }
  }
}
