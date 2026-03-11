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
package org.apache.gluten.functions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BatchScanExecTransformer, FilterExecTransformer, ProjectExecTransformer}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.NullPropagation
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types._

class ScalarFunctionsValidateSuiteRasOff extends ScalarFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.RAS_ENABLED.key, "false")
  }
}

class ScalarFunctionsValidateSuiteRasOn extends ScalarFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.RAS_ENABLED.key, "true")
  }
}

abstract class ScalarFunctionsValidateSuite extends FunctionsValidateSuite {
  disableFallbackCheck

  import testImplicits._

  // Test "SELECT ..." without a from clause.
  test("isnull") {
    runQueryAndCompare("SELECT isnull(1)")(checkGlutenPlan[ProjectExecTransformer])
  }

  test("bit_count") {
    runQueryAndCompare("SELECT bit_count(l_partkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("bit_get and getbit") {
    runQueryAndCompare("SELECT bit_get(l_partkey, 0) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT getbit(l_partkey, 0) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("array_append - INT", "3.4") {
    withTempPath {
      path =>
        Seq[(Array[Int], Int)](
          (Array(2, 1), 0),
          (Array(1), 1),
          (Array(), 0),
          (Array(1, 2, null.asInstanceOf[Int]), 1),
          (Array(null.asInstanceOf[Int]), 1),
          (Array(null.asInstanceOf[Int]), null.asInstanceOf[Int]),
          (Array(), null.asInstanceOf[Int]),
          (null.asInstanceOf[Array[Int]], 1)
        )
          .toDF("arr", "num")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, num, array_append(arr, num) from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("array_append - STRING", "3.4") {
    withTempPath {
      path =>
        Seq[(Array[String], String)](
          (Array("a", "b"), "c"),
          (Array("a"), "b"),
          (Array(), "a"),
          (Array("a", "b", null.asInstanceOf[String]), "c"),
          (Array(null.asInstanceOf[String]), "a"),
          (Array(null.asInstanceOf[String]), null.asInstanceOf[String]),
          (Array(), null.asInstanceOf[String])
        )
          .toDF("arr", "txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, txt, array_append(arr, txt) from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("array_prepend", "3.5") {
    withTempPath {
      path =>
        Seq[(Array[String], String)](
          (Array("a", "b"), "c"),
          (Array("a"), "b"),
          (Array(), "a"),
          (Array("a", "b", null.asInstanceOf[String]), "c"),
          (Array(null.asInstanceOf[String]), "a"),
          (Array(null.asInstanceOf[String]), null.asInstanceOf[String]),
          (Array(), null.asInstanceOf[String])
        )
          .toDF("arr", "txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, txt, array_prepend(arr, txt) from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("array_compact", "3.4") {
    withTempPath {
      path =>
        Seq[Array[String]](
          Array("a", "b"),
          Array(),
          Array("a", "b", null.asInstanceOf[String]),
          Array(null.asInstanceOf[String])
        )
          .toDF("arr")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, array_compact(arr) from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("null input for array_size") {
    withTempPath {
      path =>
        Seq[Array[Int]](
          null.asInstanceOf[Array[Int]]
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select array_size(txt) from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("chr") {
    val df = runQueryAndCompare("SELECT chr(l_orderkey + 64) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("hash") {
    runQueryAndCompare("SELECT hash(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("shiftright") {
    runQueryAndCompare("SELECT shiftright(int_field1, 1) from datatab limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("array_aggregate") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](
          Seq(1, 9, 8, 7),
          Seq(5, null, 8, 9, 7, 2),
          Seq.empty,
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare(
          "select aggregate(i, 0, (acc, x) -> acc + x," +
            " acc -> acc * 3) as v from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
    withTempPath {
      path =>
        Seq(
          (1, Array[Int](1, 2, 3)),
          (5, Array[Int](4, 5, 6))
        )
          .toDF("x", "ys")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select aggregate(ys, 0, (y, a) -> y + a + x) as v from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("HOUR") {
    withTable("t1") {
      sql("create table t1 (c1 int, c2 timestamp) USING PARQUET")
      sql("INSERT INTO t1 VALUES(1, NOW())")
      runQueryAndCompare("SELECT c1, HOUR(c2) FROM t1 LIMIT 1")(df => checkFallbackOperators(df, 0))
    }
  }

  test("map extract - getmapvalue") {
    withTempPath {
      path =>
        Seq(
          Map[Int, Int](1 -> 100, 2 -> 200),
          Map[Int, Int](),
          Map[Int, Int](1 -> 100, 2 -> 200, 3 -> 300),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select i[\"1\"] from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("map_entries") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_entries(i) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("map_keys") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_keys(i) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("map_contains_key") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_contains_key(i, 1) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("map_values") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_values(i) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("map_zip_with") {
    withTempPath {
      path =>
        Seq((Map("a" -> 1, "b" -> 2), Map("a" -> 2, "b" -> 3)))
          .toDF("m1", "m2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare(
          "select map_zip_with(m1, m2, (k, v1, v2) -> k == v1 + v2) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("map_concat") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")
        runQueryAndCompare("select map_concat(m, map('c', 4)) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("map_filter") {
    withTempPath {
      path =>
        Seq(Map("a" -> 1, "b" -> 2, "c" -> 3))
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_filter(m, (k, v) -> k != 'b') from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("transform_keys") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select transform_keys(m, (k, v) -> upper(k)) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("transform_values") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select transform_values(m, (k, v) -> v + 1) from map_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("zip_with") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])](
          (Seq(9001, 9002, 9003), Seq(4, 5, 6)),
          (Seq(1, 2), Seq(3, 4)),
          (Seq.empty, Seq.empty),
          (null, null)
        ).toDF("val1", "val2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select zip_with(val1, val2, (x, y) -> x + y) from array_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("isnan") {
    runQueryAndCompare(
      "SELECT isnan(l_orderkey), isnan(cast('NaN' as double)), isnan(0.0F/0.0F)" +
        " from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("conditional") {
    Seq("nanvl", "nullif", "nvl").foreach {
      func =>
        runQueryAndCompare(s"""
                              |SELECT
                              | $func(cast('nan' as float), 1f),
                              | $func(l_orderkey, cast('null' as double)),
                              | $func(cast('null' as double), l_orderkey),
                              | $func(l_orderkey, l_orderkey / 0.0d),
                              | $func(cast('nan' as float), l_orderkey)
                              | from lineitem limit 1
                              |""".stripMargin) {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("nvl2") {
    Seq("null", "l_orderkey").foreach {
      expr =>
        runQueryAndCompare(s"""
                              |SELECT
                              | nvl2($expr, cast('nan' as float), 1f),
                              | nvl2($expr, l_orderkey, cast('null' as double)),
                              | nvl2($expr, cast('null' as double), l_orderkey),
                              | nvl2($expr, l_orderkey, l_orderkey / 0.0d),
                              | nvl2($expr, cast('nan' as float), l_orderkey)
                              | from lineitem limit 1
                              |""".stripMargin) {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  // FIXME: Ignored: https://github.com/apache/incubator-gluten/issues/7600.
  ignore("monotonically_increasintestg_id") {
    runQueryAndCompare("""SELECT monotonically_increasing_id(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("sequence function optimized by Spark constant folding") {
    withSQLConf(("spark.sql.optimizer.excludedRules", NullPropagation.ruleName)) {
      runQueryAndCompare("""SELECT sequence(1, 5), l_orderkey
                           | from lineitem limit 100""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("map_from_arrays optimized by Spark constant folding") {
    withSQLConf(("spark.sql.optimizer.excludedRules", "")) {
      runQueryAndCompare("""SELECT map_from_arrays(sequence(1, 5),sequence(1, 5)), l_orderkey
                           | from lineitem limit 10""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("raise_error, assert_true") {
    runQueryAndCompare("""SELECT assert_true(l_orderkey >= 1), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    val e = intercept[SparkException] {
      sql("""SELECT assert_true(l_orderkey >= 100), l_orderkey from
            | lineitem limit 100""".stripMargin).collect()
    }
    assert(e.getCause.isInstanceOf[RuntimeException])
    assert(e.getMessage.contains("l_orderkey"))
  }

  test("EulerNumber") {
    runQueryAndCompare("""SELECT E() from lineitem limit 10""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT E(), l_orderkey
                         | from lineitem limit 10""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("Pi") {
    runQueryAndCompare("""SELECT Pi() from lineitem limit 10""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT Pi(), l_orderkey
                         | from lineitem limit 10""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testWithMaxSparkVersion("version", "3.5") {
    runQueryAndCompare("""SELECT version() from lineitem limit 10""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT version(), l_orderkey
                         | from lineitem limit 10""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("sum/count") {
    withTempPath {
      path =>
        Seq[(Integer, Integer)](
          (2, 2)
        )
          .toDF("val1", "val2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("SELECT sum(val1),count(val2) from tbl") {
          checkGlutenPlan[BatchScanExecTransformer]
        }
    }
  }

  test("spark_partition_id") {
    runQueryAndCompare("""SELECT spark_partition_id(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT spark_partition_id()
                         |from lineitem limit 100""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("url_decode", "3.4") {
    withTempPath {
      path =>
        Seq("https%3A%2F%2Fspark.apache.org")
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("url_tbl")
        runQueryAndCompare("select url_decode(a) from url_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("url_encode", "3.4") {
    withTempPath {
      path =>
        Seq("https://spark.apache.org")
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("url_tbl")
        runQueryAndCompare("select url_encode(a) from url_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  // Add test suite for CharVarcharCodegenUtils functions.
  // A ProjectExecTransformer is expected to be constructed after expr support.
  // We currently test below functions with Spark v3.4
  testWithMinSparkVersion("charTypeWriteSideCheck", "3.4") {
    withTable("src", "dest") {

      sql("create table src(id string) USING PARQUET")
      sql("insert into src values('s')")
      sql("create table dest(id char(3)) USING PARQUET")
      // check whether the executed plan of a dataframe contains the expected plan.
      runQueryAndCompare("insert into dest select id from src") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("varcharTypeWriteSideCheck", "3.4") {
    withTable("src", "dest") {

      sql("create table src(id string) USING PARQUET")
      sql("insert into src values('abc')")
      sql("create table dest(id varchar(10)) USING PARQUET")
      // check whether the executed plan of a dataframe contains the expected plan.
      runQueryAndCompare("insert into dest select id from src") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("readSidePadding", "3.4") {
    withTable("src", "dest") {

      sql("create table tgt(id char(3)) USING PARQUET")
      sql("insert into tgt values('p')")
      // check whether the executed plan of a dataframe contains the expected plan.
      runQueryAndCompare("select id from tgt") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("soundex") {
    runQueryAndCompare("select soundex(c_comment) from customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("uuid") {
    runQueryAndCompare("""SELECT uuid() from lineitem limit 100""".stripMargin, false) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("regexp_replace") {
    runQueryAndCompare(
      "SELECT regexp_replace(c_comment, '\\w', 'something') FROM customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare(
      "SELECT regexp_replace(c_comment, '\\w', 'something', 3) FROM customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("mask", "3.4") {
    runQueryAndCompare("SELECT mask(c_comment) FROM customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y') FROM customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y') FROM customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y', 'o') FROM customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y', 'o', '*') FROM customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("bit_length") {
    runQueryAndCompare(
      "select bit_length(c_comment), bit_length(cast(c_comment as binary))" +
        " from customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("bitwise_and") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) & cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) & cast(l_partkey as int), l_orderkey & l_partkey" +
        " from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("bitwise_not") {
    runQueryAndCompare(
      "select ~(cast(l_orderkey as tinyint)), ~(cast(l_orderkey as int)), ~l_orderkey" +
        " from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("bitwise_or") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) | cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) | cast(l_partkey as int), l_orderkey | l_partkey" +
        " from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("bitwise_xor") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) ^ cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) ^ cast(l_partkey as int), l_orderkey ^ l_partkey" +
        " from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("filter") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select filter(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }

        runQueryAndCompare("select filter(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("concat") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select concat(value, array(1)) from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }

    }
  }

  test("transform") {
    withTable("t") {
      sql("create table t (arr ARRAY<INT>) using parquet")
      sql("insert into t values(array(1, 2, 3, null))")
      runQueryAndCompare("select transform(arr, x -> x + 1) from t") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("forall") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select forall(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }

        runQueryAndCompare("select forall(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("exists") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select exists(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }

        runQueryAndCompare("select exists(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("arrays_zip") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])](
          (Seq(1, 2, 3), Seq(3, 4)),
          (Seq(5, null), Seq(null, 1, 2)))
          .toDF("v1", "v2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select arrays_zip(v1, v2) from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("shuffle") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select shuffle(value) from array_tbl;", false) {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("flatten nested array") {
    withTempPath {
      path =>
        Seq[Seq[Seq[Integer]]](
          Seq(Seq(1, 2), Seq(4, 5)),
          null,
          Seq(null, Seq(1, 2)),
          Seq(null, null),
          Seq(Seq(1, 2, null), Seq(null, null), Seq(3, 4), Seq.empty))
          .toDF("arrays")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select flatten(arrays) as res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("get", "3.4") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare(
          "select get(value, 0), get(value, 1), get(value, 2), get(value, 3) from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("length") {
    runQueryAndCompare(
      "select length(c_comment), length(cast(c_comment as binary))" +
        " from customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("arrays_overlap") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])]((Seq(1, 2, 3), Seq(3, 4)), (Seq(5, null), Seq()))
          .toDF("v1", "v2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select arrays_overlap(v1, v2) from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("levenshtein") {
    runQueryAndCompare("select levenshtein(c_comment, c_address) from customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("levenshtein with limit", "3.5") {
    runQueryAndCompare("select levenshtein(c_comment, c_address, 3) from customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("substring_index") {
    withTempView("substring_index_table") {
      withTempPath {
        path =>
          Seq[(String, String, Int)](
            ("www.apache.org", ".", 3),
            ("www.apache.org", ".", 2),
            ("www.apache.org", ".", 1),
            ("www.apache.org", ".", 0),
            ("www.apache.org", ".", -1),
            ("www.apache.org", ".", -2),
            ("www.apache.org", ".", -3),
            ("www.apache.org", "", 1),
            ("www.apache.org", "#", 1),
            ("www||apache||org", "||", 2),
            ("www||apache||org", "||", -2),
            ("", ".", 1),
            ("||||||", "|||", 3),
            ("||||||", "|||", -4)
          )
            .toDF("str", "delim", "count")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("substring_index_table")
          runQueryAndCompare(
            """
              |select substring_index(str, delim, count) from substring_index_table
              |""".stripMargin
          ) {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }
  }

  test("repeat") {
    runQueryAndCompare("select repeat(c_comment, 5) from customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("concat_ws") {
    runQueryAndCompare("SELECT concat_ws('~~', c_comment, c_address) FROM customer LIMIT 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq[Seq[String]](Seq("ab", null, "cd", "", "ef"), Seq(null, "x", "", "y"), Seq.empty, null)
          .toDF("col")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("SELECT concat_ws('~~', col, 'end') AS res from array_tbl;") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("input_file_name") {
    runQueryAndCompare("""SELECT input_file_name(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }

    runQueryAndCompare("""SELECT input_file_name(), l_orderkey
                         | from
                         | (select l_orderkey from lineitem
                         | union all
                         | select o_orderkey as l_orderkey from orders)
                         | limit 100""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq(1, 2, 3).toDF("a").write.json(path.getCanonicalPath)
        spark.read.json(path.getCanonicalPath).createOrReplaceTempView("json_table")
        val sql =
          """
            |SELECT input_file_name(), a
            |FROM
            |(SELECT a FROM json_table
            |UNION ALL
            |SELECT l_orderkey as a FROM lineitem)
            |LIMIT 100
            |""".stripMargin
        compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }

    // Collapse project if scan is fallback and the outer project is cheap or fallback.
    Seq("true", "false").foreach {
      flag =>
        withSQLConf(
          GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> flag,
          GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false") {
          runQueryAndCompare("SELECT l_orderkey, input_file_name() as name FROM lineitem") {
            df =>
              val plan = df.queryExecution.executedPlan
              assert(collect(plan) { case f: ProjectExecTransformer => f }.isEmpty)
              assert(collect(plan) { case f: ProjectExec => f }.size == 1)
          }
        }
    }
  }

  testWithMinSparkVersion("array insert", "3.4") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        Seq("true", "false").foreach {
          legacyNegativeIndex =>
            withSQLConf("spark.sql.legacy.negativeIndexInArrayInsert" -> legacyNegativeIndex) {
              runQueryAndCompare("""
                                   |select
                                   |  array_insert(value, 1, 0), array_insert(value, 10, 0),
                                   |  array_insert(value, -1, 0), array_insert(value, -10, 0)
                                   |from array_tbl
                                   |""".stripMargin) {
                checkGlutenPlan[ProjectExecTransformer]
              }
            }
        }
    }
  }

  test("internal function: AtLeastNNonNulls") {
    // AtLeastNNonNulls is called by drop DataFrameNafunction
    withTempPath {
      path =>
        val input = Seq[(String, java.lang.Integer, java.lang.Double)](
          ("Bob", 16, 176.5),
          ("Alice", null, 164.3),
          ("David", 60, null),
          ("Nina", 25, Double.NaN),
          ("Amy", null, null),
          (null, null, null)
        ).toDF("name", "age", "height")
        val rows = input.collect()
        input.write.parquet(path.getCanonicalPath)

        val df = spark.read.parquet(path.getCanonicalPath).na.drop(2, Seq("age", "height"))
        checkAnswer(df, rows(0) :: Nil)
        checkGlutenPlan[FilterExecTransformer](df)
    }
  }

  testWithMinSparkVersion("try_cast", "3.4") {
    withTempView("try_cast_table") {
      withTempPath {
        path =>
          Seq[String]("123456", "000A1234")
            .toDF("str")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("try_cast_table")
          runQueryAndCompare("select try_cast(str as bigint) from try_cast_table") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select try_cast(str as double) from try_cast_table") {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }
    runQueryAndCompare("select try_cast(' 123 ' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast('2147483648' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast('12a34' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast('2023-08-21 ' AS date)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast(' true' AS boolean)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast('null' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast('on' AS BOOLEAN)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast(128 AS DECIMAL(2, 0))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast(128 AS TINYINT)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast(9223372036854775807 AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select try_cast('123.0' AS INT)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("cast") {
    withTempView("cast_table") {
      withTempPath {
        path =>
          Seq[String]("123456", "000A1234")
            .toDF("str")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("cast_table")
          runQueryAndCompare("select cast(str as bigint) from cast_table") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(str as double) from cast_table") {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }
    runQueryAndCompare("select cast(' 123 ' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast('2147483648' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast('12a34' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast('2023-08-21 ' AS date)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(' true' AS boolean)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast('null' AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast('on' AS BOOLEAN)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(128 AS DECIMAL(2, 0))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(128 AS TINYINT)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(9223372036854775807 AS int)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast('123.0' AS INT)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Cast Array as Array[String]
    runQueryAndCompare("select cast(array(1, null) AS array<string>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(1L, null) AS array<string>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(1.1d, null) AS array<string>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(false, null) AS array<string>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(date'2024-01-01') AS array<string>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(timestamp'2024-01-01 12:00:00') AS array<string>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Cast Array[String] to Array
    runQueryAndCompare("select cast(array('123', '-98', 'abc', null) AS array<tinyint>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array('123', '-98', 'abc', null) AS array<smallint>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array('123', '-98', 'abc', null) AS array<int>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array('123', '-98', 'abc', null) AS array<bigint>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array('123e-2', '-234.548', 'xyz', null) AS array<float>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array('123e-2', '-234.548', 'xyz', null) AS array<double>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("""
                         |select
                         |  cast(
                         |    array('2023-01-01 12:00:00', '2023-01-02 12:00:00', 'def', null)
                         |      AS array<timestamp>)
                         |""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare(
      "select cast(array('2024-01-01', '2024-01-02', 'uvw', null) AS array<date>)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Cast Array as String
    withTempView("cast_table") {
      withTempPath {
        path =>
          Seq[Array[String]](Array("a", null), Array(), null)
            .toDF("c1")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("cast_table")
          runQueryAndCompare("select cast(c1 as string) from cast_table") {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }
    runQueryAndCompare("select cast(array(1, 2) AS string)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(1L, null) AS string)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(1.1d, null) AS string)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(false, null) AS string)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(date'2024-01-01') AS string)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("select cast(array(timestamp'2024-01-01 12:00:00') AS string)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Cast Map
    withTempView("byte_map_tbl") {
      withTempPath {
        path =>
          Seq[Map[Byte, Byte]](
            Map(0.toByte -> 1.toByte, 2.toByte -> 3.toByte, 4.toByte -> 0.toByte))
            .toDF("c1")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("byte_map_tbl")

          runQueryAndCompare("select cast(c1 as map<tinyint, double>) from byte_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<double, tinyint>) from byte_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<tinyint, string>) from byte_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<string, tinyint>) from byte_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<tinyint, boolean>) from byte_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }

    withTempView("small_int_map_tbl") {
      withTempPath {
        path =>
          Seq[Map[Short, Short]](
            Map(
              1000.toShort -> 1001.toShort,
              1002.toShort -> 1003.toShort,
              1004.toShort -> 0.toShort))
            .toDF("c1")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("small_int_map_tbl")

          runQueryAndCompare("select cast(c1 as map<smallint, double>) from small_int_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<double, smallint>) from small_int_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<smallint, string>) from small_int_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<string, smallint>) from small_int_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<smallint, boolean>) from small_int_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }

    withTempView("int_map_tbl") {
      withTempPath {
        path =>
          Seq[Map[Int, String]](Map(100 -> "101", 102 -> "103", 104 -> "xyz"))
            .toDF("c1")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("int_map_tbl")

          runQueryAndCompare("select cast(c1 as map<double, int>) from int_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<string, string>) from int_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }

    withTempView("float_map_tbl") {
      withTempPath {
        path =>
          Seq[Map[Float, String]](Map(1.0f -> "2.0", -3.0f -> "40e-1", 5.0f -> "xyz"))
            .toDF("c1")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("float_map_tbl")

          runQueryAndCompare("select cast(c1 as map<float, float>) from float_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(c1 as map<string, string>) from float_map_tbl") {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }

    runQueryAndCompare("""
                         |select
                         |  cast(
                         |    map(
                         |      timestamp '2023-01-01 12:00:00', '2023-01-01 13:00:00',
                         |      timestamp '2023-01-02 12:00:00', 'xyz')
                         |    as map<string, timestamp>)
                         |""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("""
                         |select
                         |  cast(
                         |    map(date '2024-01-01', '2024-01-02', date '2024-02-01', 'xyz')
                         |      as map<string, date>)
                         |""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Cast struct
    withTempView("struct_tbl") {
      val structData = Seq(
        Row(
          Row(
            Seq("123", "456.7", "2023-01-01 12:00:00", "2024-01-01"),
            Map(1.toByte -> 2.toShort, 3.toByte -> 4.toShort),
            Row(
              123.0,
              456.1f,
              0
            )
          )))

      val structSchema = new StructType().add(
        "c1",
        new StructType()
          .add("a", ArrayType(StringType))
          .add("b", MapType(ByteType, ShortType))
          .add("c", new StructType().add("x", DoubleType).add("y", FloatType).add("z", IntegerType))
      )
      withTempPath {
        path =>
          spark
            .createDataFrame(spark.sparkContext.parallelize(structData), structSchema)
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("struct_tbl")

          runQueryAndCompare("""
                               |select
                               |  cast(
                               |    c1 as
                               |      struct<
                               |        a: array<bigint>,
                               |        b: map<smallint, int>,
                               |        c: struct<x: string, y: string, z:boolean>>)
                               |from struct_tbl
                               |""".stripMargin) {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("""
                               |select
                               |  cast(
                               |    c1 as
                               |      struct<
                               |        a: array<double>,
                               |        b: map<int, bigint>,
                               |        c: struct<x: int, y: boolean, z:string>>)
                               |from struct_tbl
                               |""".stripMargin) {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("""
                               |select
                               |  cast(
                               |    c1 as
                               |      struct<
                               |        a: array<timestamp>,
                               |        b: map<bigint, boolean>,
                               |        c: struct<x: tinyint, y: smallint, z:double>>)
                               |from struct_tbl
                               |""".stripMargin) {
            checkGlutenPlan[ProjectExecTransformer]
          }
          runQueryAndCompare("""
                               |select
                               |  cast(
                               |    c1 as
                               |      struct<
                               |        a: array<date>,
                               |        b: map<string, double>,
                               |        c: struct<x: int, y: bigint, z:float>>)
                               |from struct_tbl
                               |""".stripMargin) {
            checkGlutenPlan[ProjectExecTransformer]
          }
      }
    }
  }

  testWithMinSparkVersion("equal_null", "3.4") {
    Seq[(Integer, Integer)]().toDF("a", "b")
    withTempPath {
      path =>
        Seq[(Integer, Integer)](
          (null, 8),
          (8, 8)
        )
          .toDF("val1", "val2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select equal_null(val1, val2) from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("get_array_struct_fields") {
    withTempPath {
      path =>
        val df = Seq(
          Seq((100, "foo"), (200, "bar"), (300, null)),
          Seq((400, "baz"), (500, "qux"))
        ).toDF("items")
        df.write.mode("overwrite").parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("""
                             |SELECT
                             |  items._1 AS item_ids,
                             |  items._2 AS item_values
                             |FROM view
        """.stripMargin) {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }
}
