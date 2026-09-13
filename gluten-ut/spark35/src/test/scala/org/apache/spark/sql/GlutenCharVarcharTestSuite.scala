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
import org.apache.spark.sql.internal.SQLConf

class GlutenFileSourceCharVarcharTestSuite
  extends FileSourceCharVarcharTestSuite
  with GlutenSQLTestsTrait {
  private def testTableWrite(f: String => Unit): Unit = {
    withTable("t")(f("char"))
    withTable("t")(f("varchar"))
  }

  private val ERROR_MESSAGE =
    "Exceeds char/varchar type length limitation: 5"
  private val VELOX_ERROR_MESSAGE =
    "Exceeds allowed length limitation: 5"

  testGluten("length check for input string values: nested in struct") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: $typeName(5)>) USING $format")
        sql("INSERT INTO t SELECT struct(null)")
        checkAnswer(spark.table("t"), Row(Row(null)))
        val e = intercept[RuntimeException] {
          sql("INSERT INTO t SELECT struct('123456')")
        }
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<$typeName(5)>) USING $format")
        sql("INSERT INTO t VALUES (array(null))")
        checkAnswer(spark.table("t"), Row(Seq(null)))
        val e = intercept[SparkException] {
          sql("INSERT INTO t VALUES (array('a', '123456'))")
        }
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L385
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("length check for input string values: nested in map key") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<$typeName(5), STRING>) USING $format")
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
        // Before: assert(e.getMessage.contains(ERROR_MESSAGE))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L400
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("length check for input string values: nested in map value") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<STRING, $typeName(5)>) USING $format")
        sql("INSERT INTO t VALUES (map('a', null))")
        checkAnswer(spark.table("t"), Row(Map("a" -> null)))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
        // Before: assert(e.getMessage.contains(ERROR_MESSAGE))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L417
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("length check for input string values: nested in both map key and value") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<$typeName(5), $typeName(5)>) USING $format")
        val e1 = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
        // Before: assert(e1.getMessage.contains(ERROR_MESSAGE))
        assert(e1.getMessage.contains(VELOX_ERROR_MESSAGE))
        val e2 = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
        // Before: assert(e2.getMessage.contains(ERROR_MESSAGE))
        assert(e2.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in struct of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t SELECT struct(array(null))")
        checkAnswer(spark.table("t"), Row(Row(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t SELECT struct(array('123456'))"))
        assert(e.getCause.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array of struct") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<STRUCT<c: $typeName(5)>>) USING $format")
        sql("INSERT INTO t VALUES (array(struct(null)))")
        checkAnswer(spark.table("t"), Row(Seq(Row(null))))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(struct('123456')))"))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t VALUES (array(array(null)))")
        checkAnswer(spark.table("t"), Row(Seq(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(array('123456')))"))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: top-level columns") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c $typeName(5)) USING $format")
        sql("INSERT INTO t VALUES (null)")
        checkAnswer(spark.table("t"), Row(null))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES ('123456')"))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: partitioned columns") {
    // DS V2 doesn't support partitioned table.
    if (!conf.contains(SQLConf.DEFAULT_CATALOG.key)) {
      val tableName = "t"
      testTableWrite {
        typeName =>
          sql(s"CREATE TABLE $tableName(i INT, c $typeName(5)) USING $format PARTITIONED BY (c)")
          sql(s"INSERT INTO $tableName VALUES (1, null)")
          checkAnswer(spark.table(tableName), Row(1, null))
          val e = intercept[SparkException](sql(s"INSERT INTO $tableName VALUES (1, '123456')"))
          assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
      }
    }
  }

  testGluten("length check for input string values: with implicit cast") {
    withTable("t") {
      sql(s"CREATE TABLE t(c1 CHAR(5), c2 VARCHAR(5)) USING $format")
      sql("INSERT INTO t VALUES (1234, 1234)")
      checkAnswer(spark.table("t"), Row("1234 ", "1234"))
      val e1 = intercept[SparkException](sql("INSERT INTO t VALUES (123456, 1)"))
      assert(e1.getMessage.contains(VELOX_ERROR_MESSAGE))
      val e2 = intercept[SparkException](sql("INSERT INTO t VALUES (1, 123456)"))
      assert(e2.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("char/varchar type values length check: partitioned columns of other types") {
    val tableName = "t"
    Seq("CHAR(5)", "VARCHAR(5)").foreach {
      typ =>
        withTable(tableName) {
          sql(s"CREATE TABLE $tableName(i STRING, c $typ) USING $format PARTITIONED BY (c)")
          Seq(1, 10, 100, 1000, 10000).foreach {
            v =>
              sql(s"INSERT OVERWRITE $tableName VALUES ('1', $v)")
              checkPlainResult(spark.table(tableName), typ, v.toString)
              sql(s"ALTER TABLE $tableName DROP PARTITION(c=$v)")
              checkAnswer(spark.table(tableName), Nil)
          }

          val e1 =
            intercept[SparkException](sql(s"INSERT OVERWRITE $tableName VALUES ('1', 100000)"))
          assert(e1.getMessage.contains(VELOX_ERROR_MESSAGE))

          val e2 = intercept[RuntimeException](sql("ALTER TABLE t DROP PARTITION(c=100000)"))
          assert(e2.getMessage.contains(ERROR_MESSAGE))
        }
    }
  }
}

class GlutenDSV2CharVarcharTestSuite extends DSV2CharVarcharTestSuite with GlutenSQLTestsTrait {
  private val ERROR_MESSAGE =
    "Exceeds char/varchar type length limitation: 5"
  private val VELOX_ERROR_MESSAGE =
    "Exceeds allowed length limitation: 5"

  private def testTableWrite(f: String => Unit): Unit = {
    withTable("t")(f("char"))
    withTable("t")(f("varchar"))
  }

  testGluten("length check for input string values: top-level columns") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c $typeName(5)) USING $format")
        sql("INSERT INTO t VALUES (null)")
        checkAnswer(spark.table("t"), Row(null))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES ('123456')"))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<$typeName(5)>) USING $format")
        sql("INSERT INTO t VALUES (array(null))")
        checkAnswer(spark.table("t"), Row(Seq(null)))
        val e = intercept[SparkException] {
          sql("INSERT INTO t VALUES (array('a', '123456'))")
        }
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in struct of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t SELECT struct(array(null))")
        checkAnswer(spark.table("t"), Row(Row(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t SELECT struct(array('123456'))"))
        assert(e.getCause.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array of struct") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<STRUCT<c: $typeName(5)>>) USING $format")
        sql("INSERT INTO t VALUES (array(struct(null)))")
        checkAnswer(spark.table("t"), Row(Seq(Row(null))))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(struct('123456')))"))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t VALUES (array(array(null)))")
        checkAnswer(spark.table("t"), Row(Seq(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(array('123456')))"))
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: with implicit cast") {
    withTable("t") {
      sql(s"CREATE TABLE t(c1 CHAR(5), c2 VARCHAR(5)) USING $format")
      sql("INSERT INTO t VALUES (1234, 1234)")
      checkAnswer(spark.table("t"), Row("1234 ", "1234"))
      val e1 = intercept[SparkException](sql("INSERT INTO t VALUES (123456, 1)"))
      assert(e1.getMessage.contains(VELOX_ERROR_MESSAGE))
      val e2 = intercept[SparkException](sql("INSERT INTO t VALUES (1, 123456)"))
      assert(e2.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("char/varchar type values length check: partitioned columns of other types") {
    val tableName = "t"
    Seq("CHAR(5)", "VARCHAR(5)").foreach {
      typ =>
        withTable(tableName) {
          sql(s"CREATE TABLE $tableName(i STRING, c $typ) USING $format PARTITIONED BY (c)")
          Seq(1, 10, 100, 1000, 10000).foreach {
            v =>
              sql(s"INSERT OVERWRITE $tableName VALUES ('1', $v)")
              checkPlainResult(spark.table(tableName), typ, v.toString)
              sql(s"ALTER TABLE $tableName DROP PARTITION(c=$v)")
              checkAnswer(spark.table(tableName), Nil)
          }

          val e1 =
            intercept[SparkException](sql(s"INSERT OVERWRITE $tableName VALUES ('1', 100000)"))
          assert(e1.getMessage.contains(VELOX_ERROR_MESSAGE))

          val e2 = intercept[RuntimeException](sql("ALTER TABLE t DROP PARTITION(c=100000)"))
          assert(e2.getMessage.contains(ERROR_MESSAGE))
        }
    }
  }

  testGluten("SPARK-42611: check char/varchar length in reordered structs within arrays") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach {
      typ =>
        withTable("t") {
          sql(s"CREATE TABLE t(a ARRAY<STRUCT<n_c: $typ, n_i: INT>>) USING $format")
          val inputDF = sql("SELECT array(named_struct('n_i', 1, 'n_c', '123456')) AS a")
          val e = intercept[SparkException](inputDF.writeTo("t").append())
          assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
        }
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L385
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("length check for input string values: nested in map key") {
    // Before: testTableWrite { typeName =>
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<$typeName(5), STRING>) USING $format")
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
        // Before: checkError(
        //           exception = e.getCause match {
        //             case c: SparkRuntimeException => c
        //             case c: SparkException => c.getCause.asInstanceOf[SparkRuntimeException]
        //           },
        //           errorClass = "EXCEED_LIMIT_LENGTH",
        //           parameters = Map("limit" -> "5")
        //         )
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L400
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("length check for input string values: nested in map value") {
    // Before: testTableWrite { typeName =>
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<STRING, $typeName(5)>) USING $format")
        sql("INSERT INTO t VALUES (map('a', null))")
        checkAnswer(spark.table("t"), Row(Map("a" -> null)))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
        // Before: checkError(
        //           exception = e.getCause match {
        //             case c: SparkRuntimeException => c
        //             case c: SparkException => c.getCause.asInstanceOf[SparkRuntimeException]
        //           },
        //           errorClass = "EXCEED_LIMIT_LENGTH",
        //           parameters = Map("limit" -> "5")
        //         )
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L417
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("length check for input string values: nested in both map key and value") {
    // Before: testTableWrite { typeName =>
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<$typeName(5), $typeName(5)>) USING $format")
        val e1 = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
        // Before: checkError(
        //           exception = e1.getCause match {
        //             case c: SparkRuntimeException => c
        //             case c: SparkException => c.getCause.asInstanceOf[SparkRuntimeException]
        //           },
        //           errorClass = "EXCEED_LIMIT_LENGTH",
        //           parameters = Map("limit" -> "5")
        //         )
        assert(e1.getMessage.contains(VELOX_ERROR_MESSAGE))
        val e2 = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
        // Before: checkError(
        //           exception = e2.getCause match {
        //             case c: SparkRuntimeException => c
        //             case c: SparkException => c.getCause.asInstanceOf[SparkRuntimeException]
        //           },
        //           errorClass = "EXCEED_LIMIT_LENGTH",
        //           parameters = Map("limit" -> "5")
        //         )
        assert(e2.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L1196
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("SPARK-42611: check char/varchar length in reordered structs within map keys") {
    // Before: Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
    Seq("CHAR(5)", "VARCHAR(5)").foreach {
      typ =>
        withTable("t") {
          sql(s"CREATE TABLE t(m MAP<STRUCT<n_c: $typ, n_i: INT>, INT>) USING $format")

          val inputDF = sql("SELECT map(named_struct('n_i', 1, 'n_c', '123456'), 1) AS m")

          val e = intercept[SparkException](inputDF.writeTo("t").append())
          // Before: checkError(
          //           exception = e.getCause match {
          //             case c: SparkRuntimeException => c
          //             case c: SparkException => c.getCause.asInstanceOf[SparkRuntimeException]
          //           },
          //           errorClass = "EXCEED_LIMIT_LENGTH",
          //           parameters = Map("limit" -> "5")
          //         )
          assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
        }
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Source:
   * https://github.com/apache/spark/blob/v3.5.5/sql/core/src/test/scala/org/apache/spark/sql/CharVarcharTestSuite.scala#L1216
   *
   * Why:
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala#L425
   * wraps a map column written to a char/varchar target in `MapFromArrays(..)`, so enabling
   * `map_from_arrays` moves the length check into Velox, which words it differently.
   */
  // scalastyle:on line.size.limit
  testGluten("SPARK-42611: check char/varchar length in reordered structs within map values") {
    // Before: Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
    Seq("CHAR(5)", "VARCHAR(5)").foreach {
      typ =>
        withTable("t") {
          sql(s"CREATE TABLE t(m MAP<INT, STRUCT<n_c: $typ, n_i: INT>>) USING $format")

          val inputDF = sql("SELECT map(1, named_struct('n_i', 1, 'n_c', '123456')) AS m")

          val e = intercept[SparkException](inputDF.writeTo("t").append())
          // Before: checkError(
          //           exception = e.getCause match {
          //             case c: SparkRuntimeException => c
          //             case c: SparkException => c.getCause.asInstanceOf[SparkRuntimeException]
          //           },
          //           errorClass = "EXCEED_LIMIT_LENGTH",
          //           parameters = Map("limit" -> "5")
          //         )
          assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
        }
    }
  }
}
