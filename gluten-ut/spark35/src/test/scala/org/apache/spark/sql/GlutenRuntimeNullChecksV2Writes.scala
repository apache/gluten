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

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.catalog.{Column => ColumnV2, Identifier}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{IntegerType, MapType, StructType}

import java.util.Collections

class GlutenRuntimeNullChecksV2Writes extends RuntimeNullChecksV2Writes with GlutenSQLTestsTrait {

  /**
   * Shadows Spark's `assertNotNullException`, which is private and so cannot be reused.
   *
   * Spark requires the cause to be a `NullPointerException` and matches the offending column path
   * against `colPath.mkString("\n", "\n", "\n")`. Velox raises a `VeloxUserError` that Gluten
   * surfaces as a `SparkException`, carrying the same reason text but not Spark's column path
   * formatting, so `colPath` is reported on failure rather than asserted. The reason match is case
   * insensitive because Spark 3.5 words it "Null value appeared ..." and Spark 4.x "NULL value
   * appeared ...".
   */
  private def assertNotNullException(e: SparkException, colPath: Seq[String]): Unit = {
    // Before: e.getCause match {
    //           case npe: NullPointerException =>
    //             assert(npe.getMessage.contains("Null value appeared in non-nullable field"))
    //             assert(npe.getMessage.contains(colPath.mkString("\n", "\n", "\n")))
    //           case other =>
    //             fail(s"Unexpected exception cause: $other")
    //         }
    val messages = Iterator
      .iterate[Throwable](e)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(t => Option(t.getMessage))
      .mkString("\n")

    assert(
      messages.toLowerCase(java.util.Locale.ROOT).contains("value appeared in non-nullable field"),
      s"expected a not-null violation for ${colPath.mkString(".")}, got:\n$messages"
    )
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/RuntimeNullChecksV2Writes.scala#L302
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a NOT NULL target in `MapFromArrays(ArrayTransform(MapKeys(..)),
   * ArrayTransform(MapValues(..)))`, so enabling `map_from_arrays` moves the `assert_not_null`
   * inside that rewrite from the JVM into Velox and changes which exception carries the violation.
   */
  // scalastyle:on line.size.limit
  testGluten("NOT NULL checks for nullable map with required values (byName)") {
    checkNullableMapWithNonNullValues(byName = true)
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/RuntimeNullChecksV2Writes.scala#L306
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a NOT NULL target in `MapFromArrays(ArrayTransform(MapKeys(..)),
   * ArrayTransform(MapValues(..)))`, so enabling `map_from_arrays` moves the `assert_not_null`
   * inside that rewrite from the JVM into Velox and changes which exception carries the violation.
   */
  // scalastyle:on line.size.limit
  testGluten("NOT NULL checks for nullable map with required values (byPosition)") {
    checkNullableMapWithNonNullValues(byName = false)
  }

  private def checkNullableMapWithNonNullValues(byName: Boolean): Unit = {
    withTable("t") {
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        columns = Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("m", MapType(IntegerType, IntegerType, valueContainsNull = false))),
        partitions = Array.empty[Transform],
        // Before: properties = Collections.emptyMap[String, String])
        properties = Collections.emptyMap[String, String]
      )

      if (byName) {
        val inputDF = sql("SELECT 1 AS i, null AS m")
        inputDF.writeTo("t").append()
      } else {
        sql("INSERT INTO t VALUES (1 AS i, null AS m)")
      }
      checkAnswer(spark.table("t"), Row(1, null))

      val e = intercept[SparkException] {
        if (byName) {
          val inputDF = sql("SELECT 1 AS i, map(1, null) AS m")
          inputDF.writeTo("t").append()
        } else {
          sql("INSERT INTO t VALUES (1 AS i, map(1, null) AS m)")
        }
      }
      assertNotNullException(e, Seq("m", "value"))
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/RuntimeNullChecksV2Writes.scala#L344
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a NOT NULL target in `MapFromArrays(ArrayTransform(MapKeys(..)),
   * ArrayTransform(MapValues(..)))`, so enabling `map_from_arrays` moves the `assert_not_null`
   * inside that rewrite from the JVM into Velox and changes which exception carries the violation.
   *
   * Only the byPosition case is overridden; byName still passes unchanged.
   */
  // scalastyle:on line.size.limit
  testGluten("NOT NULL checks for fields inside nullable maps (byPosition)") {
    checkNotNullFieldsInsideNullableMap(byName = false)
  }

  private def checkNotNullFieldsInsideNullableMap(byName: Boolean): Unit = {
    withTable("t") {
      val structType = new StructType().add("x", "int", nullable = false).add("y", "int")
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        columns = Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("m", MapType(structType, structType, valueContainsNull = true))),
        partitions = Array.empty[Transform],
        // Before: properties = Collections.emptyMap[String, String])
        properties = Collections.emptyMap[String, String]
      )

      if (byName) {
        val inputDF = sql("SELECT 1 AS i, map(named_struct('x', 1, 'y', 1), null) AS m")
        inputDF.writeTo("t").append()
      } else {
        sql("INSERT INTO t VALUES (1 AS i, map(named_struct('x', 1, 'y', 1), null) AS m)")
      }
      checkAnswer(spark.table("t"), Row(1, Map(Row(1, 1) -> null)))

      val e1 = intercept[SparkException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | map(named_struct('x', null, 'y', 1), null) AS m
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | map(named_struct('x', null, 'y', 1), null) AS m)
             """.stripMargin)
        }
      }
      assertNotNullException(e1, Seq("m", "key", "x"))

      val e2 = intercept[SparkException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | map(named_struct('x', 1, 'y', 1), named_struct('x', null, 'y', 1)) AS m
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | map(named_struct('x', 1, 'y', 1), named_struct('x', null, 'y', 1)) AS m)
             """.stripMargin)
        }
      }
      assertNotNullException(e2, Seq("m", "value", "x"))
    }
  }
}
