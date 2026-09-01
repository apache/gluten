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
package org.apache.spark.sql.delta

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Velox has no Arrow representation for VariantType, so a Delta write that Gluten offloads to the
 * native columnar writer (DataFrameWriter.save, UPDATE, ...) used to throw
 * `UnsupportedOperationException: Unsupported data type: variant` at runtime.
 * GlutenOptimisticTransaction now detects a schema Velox cannot write (such as one containing a
 * variant) and delegates such writes to the vanilla Delta write path.
 *
 * These tests use write commands that Gluten targets for native offload (DataFrameWriter.save and
 * UPDATE): without the fix they reach the native path and fail; with the fix they fall back to
 * vanilla Delta and succeed. Plain INSERT INTO is never offloaded, so it would pass regardless of
 * the fix and is intentionally not used here.
 */
class DeltaVariantWriteSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("write and read a top-level variant column") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        spark
          .range(3)
          .selectExpr("'foo' AS s", "parse_json(cast(id + 99 AS string)) AS v")
          .write
          .format("delta")
          .mode("overwrite")
          .save(path)
        checkAnswer(
          spark.read.format("delta").load(path).selectExpr("s", "to_json(v)"),
          Seq(Row("foo", "99"), Row("foo", "100"), Row("foo", "101")))
    }
  }

  test("write and read a variant nested in a struct") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        spark
          .range(2)
          .selectExpr(
            "'foo' AS s",
            "named_struct('inner', parse_json(cast(id + 99 AS string))) AS v")
          .write
          .format("delta")
          .mode("overwrite")
          .save(path)
        checkAnswer(
          spark.read.format("delta").load(path).selectExpr("s", "to_json(v.inner)"),
          Seq(Row("foo", "99"), Row("foo", "100")))
    }
  }

  test("update a table with a variant column") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        spark
          .range(3)
          .selectExpr("cast(id AS int) AS id", "parse_json(cast(id AS string)) AS v")
          .write
          .format("delta")
          .mode("overwrite")
          .save(path)
        sql(s"UPDATE delta.`$path` SET v = parse_json('123') WHERE id = 1")
        checkAnswer(
          spark.read.format("delta").load(path).selectExpr("id", "to_json(v)"),
          Seq(Row(0, "0"), Row(1, "123"), Row(2, "2")))
    }
  }
}
