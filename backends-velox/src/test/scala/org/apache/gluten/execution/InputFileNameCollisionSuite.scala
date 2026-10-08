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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.util.Arrays

/**
 * Regression tests for the collision between a mixed-case user column named "Input_File_Name" and
 * the Gluten-injected "input_file_name" metadata attribute.
 *
 * Under caseSensitive=false, ConverterUtils.normalizeColName lowercases all column names before
 * sending them to Velox. A user column "Input_File_Name" and the injected metadata attr both
 * lowercase to "input_file_name", which would cause Velox to throw: "Cannot map from same table
 * column to different outputs in table scan".
 *
 * The fix in PushDownInputFileExpression.PreOffload:
 *   - detects the normalised-name collision before rewriting expressions.
 *   - gives colliding injected aliases a private mangled schema name
 *     ("__gluten_input_file_col__input_file_name__") so no duplicate name appears in the Velox
 *     NamedStruct schema.
 *   - stores the canonical prettyName in the attr's Metadata so that BasicScanExecTransformer can
 *     populate the Velox split infoColumns map under the right key.
 *   - The injected aliases carry INPUT_FILE_COL_METADATA so that makeColumnTypeNode classifies them
 *     as METADATA_COL (kSynthesized) rather than NORMAL_COL (kRegular).
 *   - Under caseSensitive=true, normalizeColName preserves case so no collision occurs and no
 *     mangling is needed.
 *   - Both modes execute the scan natively -- no "fallback input file expression" fallback.
 *
 * These tests have NO beforeAll table setup and write test data through the JVM Parquet writer
 * (Gluten disabled during writes) so they run cleanly in any environment.
 */
// scalastyle:off caselocale
class InputFileNameCollisionSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "1g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Write rows with columns (id INT, Input_File_Name STRING) to a temp dir using the JVM Parquet
   * writer (Gluten disabled for the write so we avoid the VeloxColumnarWriteFilesRDD path).
   */
  private def writeCollisionTable(dir: java.io.File, values: Seq[(Int, String)]): Unit = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("Input_File_Name", StringType)
      ))
    val rows = Arrays.asList(values.map { case (i, s) => Row(i, s) }: _*)
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      spark
        .createDataFrame(rows, schema)
        .write
        .mode("overwrite")
        .format("parquet")
        .save(dir.getAbsolutePath)
    }
  }

  /**
   * Write rows with columns (id INT, input_file_name STRING) -- exact lowercase match -- to a temp
   * dir using the JVM Parquet writer.
   */
  private def writeExactCollisionTable(dir: java.io.File, values: Seq[(Int, String)]): Unit = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("input_file_name", StringType)
      ))
    val rows = Arrays.asList(values.map { case (i, s) => Row(i, s) }: _*)
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      spark
        .createDataFrame(rows, schema)
        .write
        .mode("overwrite")
        .format("parquet")
        .save(dir.getAbsolutePath)
    }
  }

  /**
   * Write simple (id INT, val STRING) rows to a temp dir using the JVM Parquet writer.
   */
  private def writePlainTable(dir: java.io.File): Unit = {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      spark
        .range(3)
        .selectExpr("cast(id as int) as id", "cast(id as string) as val")
        .write
        .mode("overwrite")
        .format("parquet")
        .save(dir.getAbsolutePath)
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 1: combined query -- caseSensitive=false (no collision -> native scan)
  // ---------------------------------------------------------------------------

  test(
    "input_file_name() with Input_File_Name user column -- no collision, native scan " +
      "(caseSensitive=false)") {
    // Under caseSensitive=false, PreOffload detects that "Input_File_Name" would normalise to
    // "input_file_name" in the Velox NamedStruct schema, colliding with the injected metadata
    // attribute.  The fix gives the injected alias a private mangled schema name
    // ("__gluten_input_file_col__input_file_name__") so there is no duplicate name in the Velox
    // schema.  The scan is offloaded natively and the infoColumns map is keyed by the mangled
    // name so Velox populates the synthesized column with the correct file path.
    // noFallBack=true: scan and project must execute natively -- no fallback.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "user-val-1"), (2, "user-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_collision_ci")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name`, input_file_name() AS fname " +
                "FROM ifn_collision_ci ORDER BY `Input_File_Name`",
              noFallBack = true
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  s"Expected native FileSourceScanExecTransformer under caseSensitive=false" +
                    s" but plan was:\n$plan"
                )
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val dataVals = rows.map(_.getString(0)).toSet
                assert(
                  dataVals == Set("user-val-1", "user-val-2"),
                  s"User column returned wrong values: $dataVals")
                val fileNames = rows.map(_.getString(1))
                assert(
                  fileNames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() returned empty/null: ${fileNames.mkString(", ")}")
                rows.foreach {
                  r =>
                    assert(
                      r.getString(0) != r.getString(1),
                      s"User value and file-path must differ, got same: ${r.getString(0)}")
                }
            }
          } finally {
            spark.catalog.dropTempView("ifn_collision_ci")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 1: combined query -- caseSensitive=true (no collision -> native scan)
  // ---------------------------------------------------------------------------

  test(
    "input_file_name() with Input_File_Name user column -- no collision, native scan " +
      "(caseSensitive=true)") {
    // Under caseSensitive=true, normalizeColName preserves case: "Input_File_Name" and
    // "input_file_name" remain distinct names in the Velox schema -- no collision, no fallback.
    // The injected attr carries INPUT_FILE_COL_METADATA so makeColumnTypeNode classifies it as
    // METADATA_COL (kSynthesized).  The scan is native; the outer ProjectExec (which passes
    // the injected attr through to the query result) stays on the JVM.
    // noFallBack=false: outer ProjectExec is JVM; but the scan itself MUST be native.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "cs-val-1"), (2, "cs-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_noncollision_cs")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name`, input_file_name() AS fname " +
                "FROM ifn_noncollision_cs ORDER BY `Input_File_Name`",
              noFallBack = false
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                // The scan itself must be native -- no unnecessary scan-level fallback.
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  s"Expected native FileSourceScanExecTransformer under caseSensitive=true" +
                    s" but plan was:\n$plan"
                )
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val dataVals = rows.map(_.getString(0)).toSet
                assert(
                  dataVals == Set("cs-val-1", "cs-val-2"),
                  s"User column returned wrong values: $dataVals")
                val fileNames = rows.map(_.getString(1))
                assert(
                  fileNames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() returned empty/null: ${fileNames.mkString(", ")}")
                rows.foreach {
                  r =>
                    assert(
                      r.getString(0) != r.getString(1),
                      s"User value and file-path must differ, got same: ${r.getString(0)}")
                }
            }
          } finally {
            spark.catalog.dropTempView("ifn_noncollision_cs")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 2: reverse projection order (input_file_name() first)
  // ---------------------------------------------------------------------------

  test(
    "reverse projection: input_file_name() first, then Input_File_Name (caseSensitive=false)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "rev-val-1"), (2, "rev-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_rev_ci")
          try {
            runQueryAndCompare(
              "SELECT input_file_name() AS fname, `Input_File_Name` " +
                "FROM ifn_rev_ci ORDER BY `Input_File_Name`",
              noFallBack = true
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=false")
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val fileNames = rows.map(_.getString(0))
                assert(
                  fileNames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() must be non-empty: ${fileNames.mkString(", ")}")
                val dataVals = rows.map(_.getString(1)).toSet
                assert(
                  dataVals == Set("rev-val-1", "rev-val-2"),
                  s"User column wrong: $dataVals")
            }
          } finally {
            spark.catalog.dropTempView("ifn_rev_ci")
          }
      }
    }
  }

  test(
    "reverse projection: input_file_name() first, then Input_File_Name (caseSensitive=true)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "rev-cs-1"), (2, "rev-cs-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_rev_cs")
          try {
            runQueryAndCompare(
              "SELECT input_file_name() AS fname, `Input_File_Name` " +
                "FROM ifn_rev_cs ORDER BY `Input_File_Name`",
              noFallBack = false
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=true")
                val rows = df.collect()
                assert(rows.length == 2)
                assert(rows.forall(r => r.getString(0) != null && r.getString(0).nonEmpty))
                val dataVals = rows.map(_.getString(1)).toSet
                assert(dataVals == Set("rev-cs-1", "rev-cs-2"), s"User data wrong: $dataVals")
            }
          } finally {
            spark.catalog.dropTempView("ifn_rev_cs")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 3: aliased user column + input_file_name()
  // ---------------------------------------------------------------------------

  test("Input_File_Name AS alias + input_file_name() (caseSensitive=false)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "alias-val-1"), (2, "alias-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_alias_ci")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name` AS user_col, input_file_name() AS fname " +
                "FROM ifn_alias_ci ORDER BY `Input_File_Name`",
              noFallBack = true
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=false")
                val rows = df.collect()
                assert(rows.length == 2)
                val dataVals = rows.map(_.getString(0)).toSet
                assert(
                  dataVals == Set("alias-val-1", "alias-val-2"),
                  s"Aliased user column wrong: $dataVals")
                assert(rows.forall(r => r.getString(1) != null && r.getString(1).nonEmpty))
            }
          } finally {
            spark.catalog.dropTempView("ifn_alias_ci")
          }
      }
    }
  }

  test("Input_File_Name AS alias + input_file_name() (caseSensitive=true)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "alias-cs-1"), (2, "alias-cs-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_alias_cs")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name` AS user_col, input_file_name() AS fname " +
                "FROM ifn_alias_cs ORDER BY `Input_File_Name`",
              noFallBack = false
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=true")
                val rows = df.collect()
                assert(rows.length == 2)
                val dataVals = rows.map(_.getString(0)).toSet
                assert(
                  dataVals == Set("alias-cs-1", "alias-cs-2"),
                  s"Aliased user column wrong: $dataVals")
                assert(rows.forall(r => r.getString(1) != null && r.getString(1).nonEmpty))
            }
          } finally {
            spark.catalog.dropTempView("ifn_alias_cs")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 4: multiple references to input_file_name()
  // ---------------------------------------------------------------------------

  test("multiple input_file_name() calls in same query (caseSensitive=false)") {
    // replacedExprs dedup ensures only one alias is created for input_file_name.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "multi-val-1"), (2, "multi-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_multi_ci")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name`, input_file_name() AS f1, input_file_name() AS f2 " +
                "FROM ifn_multi_ci ORDER BY `Input_File_Name`",
              noFallBack = true
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=false")
                val rows = df.collect()
                assert(rows.length == 2)
                val dataVals = rows.map(_.getString(0)).toSet
                assert(dataVals == Set("multi-val-1", "multi-val-2"))
                rows.foreach {
                  r =>
                    assert(r.getString(1) != null && r.getString(1).nonEmpty)
                    assert(r.getString(1) == r.getString(2), "f1 and f2 must be equal")
                }
            }
          } finally {
            spark.catalog.dropTempView("ifn_multi_ci")
          }
      }
    }
  }

  test("multiple input_file_name() calls in same query (caseSensitive=true)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "multi-cs-1"), (2, "multi-cs-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_multi_cs")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name`, input_file_name() AS f1, input_file_name() AS f2 " +
                "FROM ifn_multi_cs ORDER BY `Input_File_Name`",
              noFallBack = false
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=true")
                val rows = df.collect()
                assert(rows.length == 2)
                val dataVals = rows.map(_.getString(0)).toSet
                assert(dataVals == Set("multi-cs-1", "multi-cs-2"))
                rows.foreach {
                  r =>
                    assert(r.getString(1) != null && r.getString(1).nonEmpty)
                    assert(r.getString(1) == r.getString(2))
                }
            }
          } finally {
            spark.catalog.dropTempView("ifn_multi_cs")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 5: filter + input_file_name()
  // ---------------------------------------------------------------------------

  test("filter + input_file_name() with Input_File_Name user column (caseSensitive=false)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "filter-val-1"), (2, "filter-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_filter_ci")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name`, input_file_name() AS fname " +
                "FROM ifn_filter_ci WHERE id = 1",
              noFallBack = true
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=false")
                val rows = df.collect()
                assert(rows.length == 1, s"Expected 1 row after filter, got ${rows.length}")
                assert(rows(0).getString(0) == "filter-val-1")
                assert(rows(0).getString(1) != null && rows(0).getString(1).nonEmpty)
            }
          } finally {
            spark.catalog.dropTempView("ifn_filter_ci")
          }
      }
    }
  }

  test("filter + input_file_name() with Input_File_Name user column (caseSensitive=true)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "filter-cs-1"), (2, "filter-cs-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_filter_cs")
          try {
            runQueryAndCompare(
              "SELECT `Input_File_Name`, input_file_name() AS fname " +
                "FROM ifn_filter_cs WHERE id = 1",
              noFallBack = false
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  "Expected native scan under caseSensitive=true")
                val rows = df.collect()
                assert(rows.length == 1)
                assert(rows(0).getString(0) == "filter-cs-1")
                assert(rows(0).getString(1) != null && rows(0).getString(1).nonEmpty)
            }
          } finally {
            spark.catalog.dropTempView("ifn_filter_cs")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 6 (standalone): input_file_name() with mixed-case table, both modes
  // ---------------------------------------------------------------------------

  test(
    "input_file_name() standalone with Input_File_Name user column " +
      "returns file path (caseSensitive=true)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "cs-val-1"), (2, "cs-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_standalone_cs")
          try {
            runQueryAndCompare(
              "SELECT input_file_name() AS fname FROM ifn_standalone_cs",
              noFallBack = false
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val fileNames = rows.map(_.getString(0))
                assert(
                  fileNames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() returned empty/null: ${fileNames.mkString(", ")}")
            }
          } finally {
            spark.catalog.dropTempView("ifn_standalone_cs")
          }
      }
    }
  }

  test(
    "input_file_name() standalone with Input_File_Name user column " +
      "returns file path (caseSensitive=false)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "ci-val-1"), (2, "ci-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_standalone_ci")
          try {
            runQueryAndCompare(
              "SELECT input_file_name() AS fname FROM ifn_standalone_ci",
              noFallBack = false
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val fileNames = rows.map(_.getString(0))
                assert(
                  fileNames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() returned empty/null: ${fileNames.mkString(", ")}")
            }
          } finally {
            spark.catalog.dropTempView("ifn_standalone_ci")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 7: normal (non-colliding) column + input_file_name() as control
  // ---------------------------------------------------------------------------

  test("control: normal column + input_file_name() runs with fallback (caseSensitive=false)") {
    // Table has columns id (INT) and val (STRING) -- neither collides with "input_file_name".
    // input_file_name() always requires a JVM project to evaluate the metadata expression.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writePlainTable(dir)
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_ctrl_ci")
          try {
            runQueryAndCompare(
              "SELECT val, input_file_name() AS fname FROM ifn_ctrl_ci ORDER BY val",
              noFallBack = false
            ) {
              df =>
                val rows = df.collect()
                assert(rows.nonEmpty)
                assert(rows.forall(r => r.getString(1) != null && r.getString(1).nonEmpty))
                rows.foreach(r => assert(r.getString(0) != r.getString(1)))
            }
          } finally {
            spark.catalog.dropTempView("ifn_ctrl_ci")
          }
      }
    }
  }

  test("control: normal column + input_file_name() runs with fallback (caseSensitive=true)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writePlainTable(dir)
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_ctrl_cs")
          try {
            runQueryAndCompare(
              "SELECT val, input_file_name() AS fname FROM ifn_ctrl_cs ORDER BY val",
              noFallBack = false
            ) {
              df =>
                val rows = df.collect()
                assert(rows.nonEmpty)
                assert(rows.forall(r => r.getString(1) != null && r.getString(1).nonEmpty))
                rows.foreach(r => assert(r.getString(0) != r.getString(1)))
            }
          } finally {
            spark.catalog.dropTempView("ifn_ctrl_cs")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Regression: user column must NOT be filled with file path (PR #12726 core bug)
  // ---------------------------------------------------------------------------

  test(
    "Input_File_Name user column returns data values, not file paths (caseSensitive=false)") {
    // With caseSensitive=false and a user column named "Input_File_Name", a naive
    // normalizeColName() check in neededInputFileRelatedMetadataKeys would match
    // "input_file_name" and put the column into infoColumns, causing Velox to fill it with the
    // file path instead of the actual data values.
    // The fix uses exact (case-sensitive) name comparison: a.name == k.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "user-val-1"), (2, "user-val-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_data_ci")
          try {
            // Plain column read -- no input_file_name() call.  Must return user data.
            // noFallBack=true: PreOffload does NOT fire (no InputFileName expr in projectList),
            // so no FallbackTag is added and the scan stays fully native.
            runQueryAndCompare(
              "SELECT `Input_File_Name` FROM ifn_data_ci ORDER BY `Input_File_Name`",
              noFallBack = true
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val dataVals = rows.map(_.getString(0)).toSet
                assert(
                  dataVals == Set("user-val-1", "user-val-2"),
                  s"User column must return data values, not file paths. Got: $dataVals")
            }
          } finally {
            spark.catalog.dropTempView("ifn_data_ci")
          }
      }
    }
  }

  test(
    "Input_File_Name user column returns data values, not file paths (caseSensitive=true)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "cs-data-1"), (2, "cs-data-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_data_cs")
          try {
            // noFallBack=true: same reasoning -- no InputFileName expr -> no fallback.
            runQueryAndCompare(
              "SELECT `Input_File_Name` FROM ifn_data_cs ORDER BY `Input_File_Name`",
              noFallBack = true
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val dataVals = rows.map(_.getString(0)).toSet
                assert(
                  dataVals == Set("cs-data-1", "cs-data-2"),
                  s"User column must return data values, not file paths. Got: $dataVals")
            }
          } finally {
            spark.catalog.dropTempView("ifn_data_cs")
          }
      }
    }
  }
  // ---------------------------------------------------------------------------
  // Scenario 9: input_file_name() + UNION ALL regression (PR #12726)
  // ---------------------------------------------------------------------------

  test(
    "input_file_name() through UNION ALL returns file path for every branch " +
      "(caseSensitive=false, collision table)") {
    // Regression for PR #12726: addMetadataCol for the tail UNION branches used
    //   Alias(a.child, a.name)()
    // which constructs the alias with Metadata.empty, silently dropping
    // INPUT_FILE_COL_METADATA (and GLUTEN_INPUT_FILE_CANON_KEY when name-mangling is needed).
    // isInjectedInputFileAttr then returned false for those attrs, so injectedInputFileCols was
    // empty for every branch after the first; Velox got no infoColumns entry and returned the
    // user-column value (or null) instead of the file path.
    //
    // Fix: pass explicitMetadata = Some(a.metadata) when copying aliases for tail branches.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          // Table schema has Input_File_Name to exercise the collision / mangling code path.
          writeCollisionTable(dir, Seq((1, "user-a"), (2, "user-b")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_union_ci")
          try {
            // The outer project uses input_file_name(); the subquery is a UNION ALL of the same
            // table.  All four rows must carry the actual file path in the fname column.
            runQueryAndCompare(
              """
                |SELECT id, input_file_name() AS fname
                |FROM (
                |  SELECT * FROM ifn_union_ci
                |  UNION ALL
                |  SELECT * FROM ifn_union_ci
                |) u
                |ORDER BY id, fname
                |""".stripMargin,
              noFallBack = false // UNION prevents full native execution
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 4, s"Expected 4 rows from UNION ALL, got ${rows.length}")
                // Every row must have a non-empty file path in fname
                val fnames = rows.map(_.getString(1))
                assert(
                  fnames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() returned empty/null for some UNION branch: " +
                    s"${fnames.mkString(", ")}")
                // fname must be a file path, not a user-column value
                val userVals = Set("user-a", "user-b")
                fnames.foreach {
                  n =>
                    assert(
                      !userVals.contains(n),
                      s"input_file_name() returned user-column value '$n' instead of file path; " +
                        s"metadata was stripped from tail UNION branch alias"
                    )
                }
            }
          } finally {
            spark.catalog.dropTempView("ifn_union_ci")
          }
      }
    }
  }

  test(
    "input_file_name() through UNION ALL returns file path for every branch " +
      "(caseSensitive=true, collision table)") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeCollisionTable(dir, Seq((1, "user-a"), (2, "user-b")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_union_cs")
          try {
            runQueryAndCompare(
              """
                |SELECT id, input_file_name() AS fname
                |FROM (
                |  SELECT * FROM ifn_union_cs
                |  UNION ALL
                |  SELECT * FROM ifn_union_cs
                |) u
                |ORDER BY id, fname
                |""".stripMargin,
              noFallBack = false
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 4, s"Expected 4 rows from UNION ALL, got ${rows.length}")
                val fnames = rows.map(_.getString(1))
                assert(
                  fnames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() returned empty/null for some UNION branch: " +
                    s"${fnames.mkString(", ")}")
                val userVals = Set("user-a", "user-b")
                fnames.foreach {
                  n =>
                    assert(
                      !userVals.contains(n),
                      s"input_file_name() returned user-column value '$n' for caseSensitive=true")
                }
            }
          } finally {
            spark.catalog.dropTempView("ifn_union_cs")
          }
      }
    }
  }

  test(
    "input_file_name() through UNION ALL returns file path for every branch " +
      "(caseSensitive=false, plain table)") {
    // Simpler variant: plain table without a colliding user column.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writePlainTable(dir)
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_union_plain_ci")
          try {
            runQueryAndCompare(
              """
                |SELECT id, input_file_name() AS fname
                |FROM (
                |  SELECT * FROM ifn_union_plain_ci
                |  UNION ALL
                |  SELECT * FROM ifn_union_plain_ci
                |) u
                |ORDER BY id, fname
                |""".stripMargin,
              noFallBack = false
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 6, s"Expected 6 rows from UNION ALL, got ${rows.length}")
                val fnames = rows.map(_.getString(1))
                assert(
                  fnames.forall(n => n != null && n.nonEmpty),
                  s"input_file_name() returned empty/null for some UNION branch: " +
                    s"${fnames.mkString(", ")}")
            }
          } finally {
            spark.catalog.dropTempView("ifn_union_plain_ci")
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 10: EXACT lowercase collision -- `input_file_name` user column + input_file_name()
  //
  // This is the case reported in the IcebergSuite CI failure:
  //   VeloxRuntimeError: Inconsistent column handle type: input_file_name,
  //   expected Regular, got Synthesized
  //
  // Under caseSensitive=true, when the physical table column is EXACTLY named "input_file_name"
  // (same string as InputFileName().prettyName), PushDownInputFileExpression.PostOffload was
  // pushing a Synthesized "input_file_name" attribute into the scan output which already had a
  // Regular "input_file_name" column, producing the Velox conflict.
  //
  // The fix: PostOffload must guard against fallback-tagged ProjectExec nodes that were already
  // marked by PreOffload (so that the JVM evaluates InputFileName() via the thread-local).
  // ---------------------------------------------------------------------------

  test(
    "case-sensitive mode: lowercase input_file_name as data column -- platform compatibility") {
    // Under caseSensitive=true, a column named `input_file_name` (exact lowercase match) can be
    // queried alongside input_file_name().  For FileSourceScanExecTransformer (Parquet), PreOffload
    // detects the exact collision (normalizeColName preserves the name under caseSensitive=true,
    // so "input_file_name" == "input_file_name") and mangles the injected alias's schema name to
    // "__gluten_input_file_col__input_file_name__".  The scan is offloaded natively.
    // The physical data column must return the user value; input_file_name() must return a path.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeExactCollisionTable(dir, Seq((1, "exact-user-value-not-a-path")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_exact_cs")
          try {
            runQueryAndCompare(
              "SELECT id, `input_file_name`, input_file_name() AS fname " +
                "FROM ifn_exact_cs ORDER BY id",
              noFallBack = false // outer ProjectExec may be JVM; scan must be native
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                // The FileSourceScan must be native (not fallen back)
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  s"Expected native FileSourceScanExecTransformer under caseSensitive=true" +
                    s" but plan was:\n$plan"
                )
                val rows = df.collect()
                assert(rows.length == 1, s"Expected 1 row, got ${rows.length}")
                // Physical data column must contain the user-inserted value.
                assert(
                  rows(0).getString(1) == "exact-user-value-not-a-path",
                  s"Physical 'input_file_name' column should be user data, " +
                    s"got: '${rows(0).getString(1)}'")
                // input_file_name() function must return a non-empty file path.
                val fname = rows(0).getString(2)
                assert(
                  fname != null && fname.nonEmpty,
                  s"input_file_name() must return a non-empty path, got: '$fname'")
                // The two must not be equal -- if they are, the conflation bug is present.
                assert(
                  rows(0).getString(1) != rows(0).getString(2),
                  s"Physical column and function result must differ: " +
                    s"col='${rows(0).getString(1)}', fn='${rows(0).getString(2)}'"
                )
            }
          } finally {
            spark.catalog.dropTempView("ifn_exact_cs")
          }
      }
    }
  }

  test(
    "case-sensitive mode: lowercase input_file_name as data column -- caseSensitive=false") {
    // Same table but with caseSensitive=false.  normalizeColName lowercases both the user column
    // ("input_file_name") and the injected alias prettyName ("input_file_name") → collision
    // detected → PreOffload mangles the injected alias schema name.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir {
        dir =>
          writeExactCollisionTable(dir, Seq((1, "exact-ci-value")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_exact_ci")
          try {
            runQueryAndCompare(
              "SELECT id, `input_file_name`, input_file_name() AS fname " +
                "FROM ifn_exact_ci ORDER BY id",
              noFallBack = false
            ) {
              df =>
                val plan = df.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty,
                  s"Expected native FileSourceScanExecTransformer under caseSensitive=false" +
                    s" but plan was:\n$plan"
                )
                val rows = df.collect()
                assert(rows.length == 1, s"Expected 1 row, got ${rows.length}")
                assert(
                  rows(0).getString(1) == "exact-ci-value",
                  s"Physical column should be user data, got: '${rows(0).getString(1)}'")
                val fname = rows(0).getString(2)
                assert(
                  fname != null && fname.nonEmpty,
                  s"input_file_name() must return a non-empty path, got: '$fname'")
                assert(
                  rows(0).getString(1) != rows(0).getString(2),
                  s"Physical column and function result must differ: " +
                    s"col='${rows(0).getString(1)}', fn='${rows(0).getString(2)}'"
                )
            }
          } finally {
            spark.catalog.dropTempView("ifn_exact_ci")
          }
      }
    }
  }

  test(
    "exact lowercase input_file_name: only user column selected, no function call " +
      "(caseSensitive=true)") {
    // Verify that a plain SELECT of the physical column (no input_file_name() call) still returns
    // user data values when the column is named exactly "input_file_name".
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir {
        dir =>
          writeExactCollisionTable(dir, Seq((1, "data-only-cs-1"), (2, "data-only-cs-2")))
          spark.read
            .format("parquet")
            .load(dir.getAbsolutePath)
            .createOrReplaceTempView("ifn_exact_data_cs")
          try {
            runQueryAndCompare(
              "SELECT `input_file_name` FROM ifn_exact_data_cs ORDER BY `input_file_name`",
              noFallBack = true
            ) {
              df =>
                val rows = df.collect()
                assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
                val dataVals = rows.map(_.getString(0)).toSet
                assert(
                  dataVals == Set("data-only-cs-1", "data-only-cs-2"),
                  s"User column must return data values, not file paths. Got: $dataVals")
            }
          } finally {
            spark.catalog.dropTempView("ifn_exact_data_cs")
          }
      }
    }
  }
}
// scalastyle:on caselocale
