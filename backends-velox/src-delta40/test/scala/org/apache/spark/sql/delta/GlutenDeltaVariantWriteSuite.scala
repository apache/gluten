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
 * Velox has no Arrow representation for VariantType, so writing a variant column through the native
 * columnar writer used to throw `UnsupportedOperationException: Unsupported data type: variant` at
 * runtime (apache/gluten#9003-adjacent). GlutenOptimisticTransaction now delegates such writes to
 * the vanilla Delta write path. These tests assert that writing and reading variant data works.
 */
class GlutenDeltaVariantWriteSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("write and read a top-level variant column") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(3)")
      checkAnswer(
        sql("SELECT s, to_json(v) FROM tbl ORDER BY to_json(v)"),
        Seq(Row("foo", "100"), Row("foo", "101"), Row("foo", "99")))
    }
  }

  test("write and read a variant nested in a struct") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v STRUCT<inner: VARIANT>) USING DELTA")
      sql(
        "INSERT INTO tbl SELECT 'foo', struct(parse_json(cast(id + 99 as string))) FROM range(2)")
      checkAnswer(
        sql("SELECT s, to_json(v.inner) FROM tbl ORDER BY to_json(v.inner)"),
        Seq(Row("foo", "100"), Row("foo", "99")))
    }
  }

  test("update a table with a variant column") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(id INT, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl SELECT id, parse_json(cast(id as string)) FROM range(3)")
      sql("UPDATE tbl SET v = parse_json('123') WHERE id = 1")
      checkAnswer(
        sql("SELECT id, to_json(v) FROM tbl ORDER BY id"),
        Seq(Row(0, "0"), Row(1, "123"), Row(2, "2")))
    }
  }
}
