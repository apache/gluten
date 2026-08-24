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

import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.iceberg.UpdateSchema
import org.apache.iceberg.expressions.Literal
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.{Type, Types}

class VeloxIcebergSuite extends IcebergSuite {
  testWithMinSparkVersion("iceberg scan on an IBM COS Stocator-style path falls back", "3.4") {
    withTempDir {
      dir =>
        // RawLocalFileSystem stands in for a real IBM COS endpoint so this test can exercise
        // the fallback decision and the vanilla read path without live credentials.
        withSQLConf(
          "spark.hadoop.fs.cos.impl" -> classOf[RawLocalFileSystem].getName,
          "spark.hadoop.fs.cos.myservice.endpoint" -> "http://localhost:0") {
          withTable("iceberg_cos_stocator_tb") {
            spark.sql(s"""
                         |CREATE TABLE iceberg_cos_stocator_tb (id INT, data STRING)
                         |USING iceberg
                         |LOCATION 'cos://mybucket.myservice${dir.getCanonicalPath}'
                         |""".stripMargin)
            spark.sql("INSERT INTO iceberg_cos_stocator_tb VALUES (1, 'a'), (2, 'b')")

            runQueryAndCompare(
              "SELECT * FROM iceberg_cos_stocator_tb ORDER BY id",
              noFallBack = false) {
              df =>
                checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
                val executedPlan = df.queryExecution.executedPlan.collect { case p => p }
                assert(
                  !executedPlan.exists(_.isInstanceOf[IcebergScanTransformer]),
                  "Expected the scan to fall back to vanilla Spark for an IBM COS " +
                    "Stocator-style path, but found a native IcebergScanTransformer in " +
                    "the plan.")
            }
          }
        }
    }
  }

  testWithMinSparkVersion("iceberg v3 initial default for an added column", "3.4") {
    withTable("iceberg_v3_initial_default") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_v3_initial_default (id INT)
                    |USING iceberg
                    |TBLPROPERTIES ('format-version' = '3')
                    |""".stripMargin)
        spark.sql("INSERT INTO iceberg_v3_initial_default VALUES (1), (2)")

        val catalog = spark.sessionState.catalogManager
          .catalog("spark_catalog")
          .asInstanceOf[TableCatalog]
        val updateSchema = catalog
          .loadTable(Identifier.of(Array("default"), "iceberg_v3_initial_default"))
          .asInstanceOf[SparkTable]
          .table()
          .updateSchema()
        classOf[UpdateSchema]
          .getMethod(
            "addColumn",
            classOf[String],
            classOf[Type],
            classOf[Literal[_]])
          .invoke(updateSchema, "country", Types.StringType.get(), Literal.of("IN"))
        updateSchema.commit()
        spark.catalog.refreshTable("iceberg_v3_initial_default")
      }

      runQueryAndCompare(
        "SELECT id, country FROM iceberg_v3_initial_default ORDER BY id") {
        df =>
          checkAnswer(df, Seq(Row(1, "IN"), Row(2, "IN")))
          checkGlutenPlan[IcebergScanTransformer](df)
      }
    }
  }
}
