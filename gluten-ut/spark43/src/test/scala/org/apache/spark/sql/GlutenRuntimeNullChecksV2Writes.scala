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

import org.apache.gluten.exception.GlutenException

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.catalog.{Column => ColumnV2, Identifier, TableInfo}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StructType}

class GlutenRuntimeNullChecksV2Writes extends RuntimeNullChecksV2Writes with GlutenSQLTestsTrait {

  /**
   * Shadows Spark's `assertNotNullException`, which is private and so cannot be reused.
   *
   * Spark expects a `SparkRuntimeException` carrying `NOT_NULL_ASSERT_VIOLATION` and never reads
   * `colPath`. When the projection is offloaded, Velox `assert_not_null` raises a `VeloxUserError`
   * instead, which reaches the test as a `SparkException` whose cause is a `GlutenException`
   * carrying the reason text but not the error condition.
   */
  private def assertNotNullException(e: SparkException, colPath: Seq[String]): Unit = {
    e.getCause match {
      case ge: GlutenException =>
        assert(
          ge.getMessage.contains("Null value appeared in non-nullable field"),
          s"expected a not-null violation for ${colPath.mkString(".")}, got:\n${ge.getMessage}"
        )
      case other =>
        fail(s"Unexpected exception cause: $other")
    }
  }

  testGluten("NOT NULL checks for nullable array with required element (byName)") {
    checkNullableArrayWithNotNullElement(byName = true)
  }

  testGluten("NOT NULL checks for nullable array with required element (byPosition)") {
    checkNullableArrayWithNotNullElement(byName = false)
  }

  private def checkNullableArrayWithNotNullElement(byName: Boolean): Unit = {
    withTable("t") {
      val structType = new StructType().add("x", "int").add("y", "int")
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("arr", ArrayType(structType, containsNull = false))))
        .build()
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

      if (byName) {
        val inputDF = sql("SELECT 1 AS i, null AS arr")
        inputDF.writeTo("t").append()
      } else {
        sql("INSERT INTO t VALUES (1 AS i, null AS arr)")
      }
      checkAnswer(spark.table("t"), Row(1, null))

      val e = intercept[SparkException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | array(null, named_struct('x', 1, 'y', 1)) AS arr
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | array(null, named_struct('x', 1, 'y', 1)) AS arr)
             """.stripMargin)
        }
      }
      assertNotNullException(e, Seq("arr", "element"))
    }
  }

  testGluten("NOT NULL checks for fields inside nullable array (byName)") {
    checkNotNullFieldsInsideNullableArray(byName = true)
  }

  testGluten("not null checks for fields inside nullable array (byPosition)") {
    checkNotNullFieldsInsideNullableArray(byName = false)
  }

  private def checkNotNullFieldsInsideNullableArray(byName: Boolean): Unit = {
    withTable("t") {
      val structType = new StructType().add("x", "int", nullable = false).add("y", "int")
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("arr", ArrayType(structType, containsNull = true))))
        .build()
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

      if (byName) {
        val inputDF = sql(
          s"""SELECT
             | 1 AS i,
             | array(null, named_struct('x', 1, 'y', 1)) AS arr
           """.stripMargin)
        inputDF.writeTo("t").append()
      } else {
        sql(
          s"""INSERT INTO t VALUES (
             | 1 AS i,
             | array(null, named_struct('x', 1, 'y', 1)) AS arr)
           """.stripMargin)
      }
      checkAnswer(spark.table("t"), Row(1, List(null, Row(1, 1))))

      val e = intercept[SparkException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | array(null, named_struct('x', null, 'y', 1)) AS arr
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | array(null, named_struct('x', null, 'y', 1)) AS arr)
             """.stripMargin)
        }
      }
      assertNotNullException(e, Seq("arr", "element", "x"))
    }
  }

  testGluten("NOT NULL checks for nullable map with required values (byName)") {
    checkNullableMapWithNonNullValues(byName = true)
  }

  testGluten("NOT NULL checks for nullable map with required values (byPosition)") {
    checkNullableMapWithNonNullValues(byName = false)
  }

  private def checkNullableMapWithNonNullValues(byName: Boolean): Unit = {
    withTable("t") {
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("m", MapType(IntegerType, IntegerType, valueContainsNull = false))))
        .build()
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

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

  testGluten("NOT NULL checks for fields inside nullable maps (byName)") {
    checkNotNullFieldsInsideNullableMap(byName = true)
  }

  testGluten("NOT NULL checks for fields inside nullable maps (byPosition)") {
    checkNotNullFieldsInsideNullableMap(byName = false)
  }

  private def checkNotNullFieldsInsideNullableMap(byName: Boolean): Unit = {
    withTable("t") {
      val structType = new StructType().add("x", "int", nullable = false).add("y", "int")
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("m", MapType(structType, structType, valueContainsNull = true))))
        .build()
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

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
