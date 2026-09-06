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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.utils.VeloxFileSystemValidationJniWrapper

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.iceberg.UpdateSchema
import org.apache.iceberg.expressions.Literal
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.{Type, Types}

class VeloxIcebergSuite extends IcebergSuite {
  test("iceberg parquet split uses name mapping for projected columns") {
    withTable("iceberg_parquet_name_mapping") {
      withSQLConf(VeloxConfig.PARQUET_USE_COLUMN_NAMES.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_parquet_name_mapping (
                    |  id BIGINT,
                    |  amount DECIMAL(12, 2),
                    |  note STRING
                    |)
                    |USING iceberg
                    |TBLPROPERTIES ('write.format.default' = 'parquet')
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_parquet_name_mapping
                    |VALUES (CAST(1 AS BIGINT), CAST(10.50 AS DECIMAL(12, 2)), 'a')
                    |""".stripMargin)

        runQueryAndCompare("SELECT amount FROM iceberg_parquet_name_mapping") {
          df =>
            checkAnswer(df, Seq(Row(BigDecimal("10.50"))))
            checkGlutenPlan[IcebergScanTransformer](df)
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

  test("iceberg root paths on an unsupported scheme fail native scheme validation") {
    // See https://github.com/apache/gluten/issues/12712: IcebergScanTransformer used to always
    // report Seq.empty root paths, so VeloxBackend.validateScanExec's scheme check was silently
    // skipped for every Iceberg scan, regardless of the scan's actual filesystem.
    withTable("iceberg_root_paths_scheme_tb") {
      spark.sql("""
                  |CREATE TABLE iceberg_root_paths_scheme_tb (id INT)
                  |USING iceberg
                  |""".stripMargin)
      spark.sql("INSERT INTO iceberg_root_paths_scheme_tb VALUES (1), (2)")

      runQueryAndCompare("SELECT * FROM iceberg_root_paths_scheme_tb") {
        df =>
          val scans = getExecutedPlan(df).collect { case i: IcebergScanTransformer => i }
          assert(scans.size == 1)
          val rootPaths = scans.head.getRootPathsInternal
          assert(rootPaths.nonEmpty)
          // Confirm these are real per-file scan paths (registered as `file` scheme), which
          // VeloxBackendSettings.distinctRootPaths always excludes from scheme validation (the
          // local filesystem is always registered) -- so a real root path from this suite
          // passes validation, as expected for a supported local table.
          assert(
            VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
              rootPaths.toArray))

          // Since this suite only exercises the local `file` scheme, which is always
          // considered supported, separately confirm -- with a clean, synthetic URI, not
          // derived from the real paths above -- that Velox's own native filesystem check (the
          // same one VeloxBackend.validateScanExec relies on to decide whether to fall back)
          // correctly rejects a genuinely unsupported scheme.
          assert(
            !VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
              Array("unsupported-test-scheme://bucket/path/file.parquet")),
            "expected an unsupported scheme to fail native filesystem validation")
      }
    }
  }
}
