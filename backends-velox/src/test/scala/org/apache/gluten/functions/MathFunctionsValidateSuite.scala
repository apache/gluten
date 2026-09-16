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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{BatchScanExecTransformer, ProjectExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

class MathFunctionsValidateSuiteAnsiOn extends FunctionsValidateSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.ANSI_ENABLED.key, "true")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
  }

  disableFallbackCheck

  test("try_add") {
    runQueryAndCompare(
      "select try_add(cast(l_orderkey as int), 1), try_add(cast(l_orderkey as int), 2147483647)" +
        " from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("try_divide") {
    runQueryAndCompare(
      "select try_divide(cast(l_orderkey as int), 0) from lineitem",
      noFallBack = false) {
      _ => // Spark would always cast inputs to double for this function.
    }
  }

  test("try_multiply") {
    runQueryAndCompare(
      "select try_multiply(2147483647, cast(l_orderkey as int)), " +
        "try_multiply(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("try_subtract") {
    runQueryAndCompare(
      "select try_subtract(2147483647, cast(l_orderkey as int)), " +
        "try_subtract(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }
}

class MathFunctionsValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck
  import testImplicits._

  test("abs") {
    val df = runQueryAndCompare("SELECT abs(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("acos") {
    runQueryAndCompare("SELECT acos(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("asin") {
    runQueryAndCompare("SELECT asin(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("atan") {
    runQueryAndCompare("SELECT atan(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("atan2") {
    runQueryAndCompare("SELECT atan2(double_field1, 0) from datatab limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("bin") {
    val df = runQueryAndCompare("SELECT bin(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("ceil") {
    val df = runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("ceiling") {
    runQueryAndCompare("SELECT ceiling(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("2-arg ceiling / floor on decimals (RoundCeil / RoundFloor)") {
    // The 2-arg forms produce Spark RoundCeil / RoundFloor and dispatch to the Velox
    // decimal_ceil / decimal_floor special forms. The projection is native only when the
    // expression offloads, so checkGlutenPlan[ProjectExecTransformer] doubles as an offload
    // assertion; runQueryAndCompare additionally validates results against vanilla Spark.
    runQueryAndCompare(
      "SELECT ceiling(cast(l_quantity as decimal(12, 2)), 1) FROM lineitem limit 10") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare(
      "SELECT floor(cast(l_quantity as decimal(12, 2)), 1) FROM lineitem limit 10") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Negative scale rounds to the left of the decimal point.
    runQueryAndCompare(
      "SELECT ceiling(cast(l_extendedprice as decimal(20, 4)), -2) FROM lineitem limit 10") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare(
      "SELECT floor(cast(l_extendedprice as decimal(20, 4)), -2) FROM lineitem limit 10") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("cos") {
    runQueryAndCompare("SELECT cos(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("cosh") {
    runQueryAndCompare("SELECT cosh(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("degrees") {
    runQueryAndCompare("SELECT degrees(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("exp") {
    val df = runQueryAndCompare("SELECT exp(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("factorial") {
    withTable("factorial_input") {
      sql("CREATE TABLE factorial_input(id INT) USING parquet")
      sql("""
            |INSERT INTO factorial_input VALUES
            |(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
            |""".stripMargin)

      val query =
        """
          |SELECT
          |  id,
          |  factorial(id)
          |FROM factorial_input
          |""".stripMargin

      val expectedResults = Seq(
        Row(0, 1L),
        Row(1, 1L),
        Row(2, 2L),
        Row(3, 6L),
        Row(4, 24L),
        Row(5, 120L),
        Row(6, 720L),
        Row(7, 5040L),
        Row(8, 40320L),
        Row(9, 362880L),
        Row(10, 3628800L)
      )

      runSql(query) {
        df =>
          checkGlutenPlan[ProjectExecTransformer](df)
          val result = df.collect()
          assert(result.length == expectedResults.length)
          assert(result === expectedResults)
      }
    }
  }

  test("floor") {
    val df = runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("greatest") {
    runQueryAndCompare(
      "SELECT greatest(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        spark
          .sql("""SELECT *
                FROM VALUES (CAST(5.345 AS DECIMAL(6, 2)), CAST(5.35 AS DECIMAL(5, 4))),
                (CAST(5.315 AS DECIMAL(6, 2)), CAST(5.355 AS DECIMAL(5, 4))),
                (CAST(3.345 AS DECIMAL(6, 2)), CAST(4.35 AS DECIMAL(5, 4))) AS data(a, b);""")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT greatest(a, b) from view") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("hex") {
    runQueryAndCompare("SELECT hex(l_partkey), hex(l_shipmode) FROM lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("least") {
    runQueryAndCompare(
      "SELECT least(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        spark
          .sql("""SELECT *
                FROM VALUES (CAST(5.345 AS DECIMAL(6, 2)), CAST(5.35 AS DECIMAL(5, 4))),
                (CAST(5.315 AS DECIMAL(6, 2)), CAST(5.355 AS DECIMAL(5, 4))),
                (CAST(3.345 AS DECIMAL(6, 2)), CAST(4.35 AS DECIMAL(5, 4))) AS data(a, b);""")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT least(a, b) from view") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("ln") {
    runQueryAndCompare("SELECT ln(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Verify null semantics: ln(0) and ln(-1) must return null, not -Infinity/NaN
    compareResultsAgainstVanillaSpark(
      "SELECT ln(0), ln(-1), ln(cast(null as double))",
      true,
      { _ => })
  }

  test("log") {
    runQueryAndCompare("SELECT log(10, l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("log10") {
    runQueryAndCompare("SELECT log10(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("negative") {
    runQueryAndCompare("select negative(l_orderkey) from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("pmod") {
    val df = runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("power") {
    val df = runQueryAndCompare("SELECT power(l_orderkey, 2) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("rand") {
    runQueryAndCompare(
      """SELECT rand() from lineitem limit 100""".stripMargin,
      compareResult = false) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("randn") {
    // randn draws from the standard normal distribution, so only verify native execution.
    runQueryAndCompare("SELECT randn() from lineitem limit 100", compareResult = false) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT randn(0) from lineitem limit 100", compareResult = false) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("randstr", "4.0") {
    // randstr generates random strings, so we only verify native execution, not result equality.
    runQueryAndCompare("SELECT randstr(5, 0) from lineitem limit 100", compareResult = false) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("radians") {
    runQueryAndCompare("SELECT radians(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("rint") {
    withTempPath {
      path =>
        Seq(1.2, 1.5, 1.9).toDF("d").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("double")
        runQueryAndCompare("select rint(d) from double") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("round") {
    runQueryAndCompare(
      "SELECT round(cast(l_orderkey as int), 2)" +
        "from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    runQueryAndCompare("""
                         |select round(l_quantity, 2) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // Scale > 0 should return same value as input on integral values
    compareResultsAgainstVanillaSpark("select round(78, 1)", true, { _ => })
    // Scale < 0 should round down even on integral values
    compareResultsAgainstVanillaSpark("select round(44, -1)", true, { _ => })
  }

  test("shiftleft") {
    runQueryAndCompare("SELECT shiftleft(int_field1, 1) from datatab limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("sin") {
    runQueryAndCompare("SELECT sin(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("tan") {
    runQueryAndCompare("SELECT tan(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("tanh") {
    runQueryAndCompare("SELECT tanh(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("try_add") {
    runQueryAndCompare(
      "select try_add(cast(l_orderkey as int), 1), try_add(cast(l_orderkey as int), 2147483647)" +
        " from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("try_divide") {
    runQueryAndCompare(
      "select try_divide(cast(l_orderkey as int), 0) from lineitem",
      noFallBack = false) {
      _ => // Spark would always cast inputs to double for this function.
    }
  }

  test("try_multiply") {
    runQueryAndCompare(
      "select try_multiply(2147483647, cast(l_orderkey as int)), " +
        "try_multiply(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("try_subtract") {
    runQueryAndCompare(
      "select try_subtract(2147483647, cast(l_orderkey as int)), " +
        "try_subtract(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("unhex") {
    runQueryAndCompare("SELECT unhex(hex(l_shipmode)) FROM lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("width_bucket", "3.4") {
    withTempPath {
      path =>
        Seq[(Integer, Integer, Integer, Integer)](
          (2, 0, 4, 3)
        )
          .toDF("val1", "val2", "val3", "val4")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("SELECT width_bucket(val1, val2, val3, val4) from tbl") {
          checkGlutenPlan[BatchScanExecTransformer]
        }
    }
  }

  test("sqrt") {
    val df = runQueryAndCompare("SELECT sqrt(l_orderkey) from lineitem limit 1") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("decimal arithmetic") {
    withTempView("t") {
      sql("""
            |SELECT
            |CAST('1234567890123456789012345.12345678901' AS DECIMAL(38,11)) AS a,
            |CAST('1234567890123456789012345.02345678901' AS DECIMAL(38,11)) AS b;""".stripMargin)
        .createOrReplaceTempView("t")

      Seq("true", "false").foreach {
        enabled =>
          withSQLConf("spark.sql.decimalOperations.allowPrecisionLoss" -> enabled) {
            runQueryAndCompare("SELECT a - b, a + b, a * b, a / b FROM t") {
              checkGlutenPlan[ProjectExecTransformer]
            }
          }
      }
    }
  }

  // Gluten's checkAnswer accepts any two doubles within 1e-5 of each other, which is far
  // wider than the precision loss under test here: 214.4 and 214.39999999999998 compare
  // equal under it. Comparing the rendered values admits no tolerance at all, because
  // Double.toString emits the shortest decimal that round-trips to the same double.
  private def assertDoublesMatchVanillaExactly(sqlText: String, glutenRows: Array[Row]): Unit = {
    var vanillaRows: Array[Row] = Array.empty
    withSQLConf(vanillaSparkConfs(): _*) {
      vanillaRows = spark.sql(sqlText).collect()
    }
    def render(rows: Array[Row]): Seq[String] = rows.toSeq.map(_.toSeq.mkString("|")).sorted
    assert(render(glutenRows) === render(vanillaRows))
  }

  test("decimal to double preserves precision after decimal division") {
    withSQLConf(
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding",
      "spark.sql.decimalOperations.allowPrecisionLoss" -> "false",
      VeloxConfig.DECIMAL_TO_FLOAT_HIGH_PRECISION_CAST_ENABLED.key -> "true"
    ) {
      val sqlText = "SELECT CAST(2.8 / CAST(0.0130597014925373134 AS DECIMAL(38,19)) AS DOUBLE)"
      runQueryAndCompare(sqlText) {
        df =>
          checkGlutenPlan[ProjectExecTransformer](df)
          assertDoublesMatchVanillaExactly(sqlText, df.collect())
      }
    }
  }

  test("GLUTEN-12356: high-precision decimal to double matches vanilla Spark") {
    withTempView("decimal_double_cast") {
      withTempPath {
        path =>
          // Cover short decimal, long decimal at both precision bounds,
          // zero/negative/NULL values, values around 2^53, and near-max 38-digit values.
          // The d38_27 and d38_37 columns are the ones whose direct conversion actually
          // differs from Spark: divergence depends on the unscaled value and the scale,
          // and the other columns here agree even without the high-precision cast.
          spark
            .sql("""
                   |SELECT
                   |  CAST(v18 AS DECIMAL(18,6)) AS d18_6,
                   |  CAST(v19 AS DECIMAL(19,0)) AS d19_0,
                   |  CAST(v38_0 AS DECIMAL(38,0)) AS d38_0,
                   |  CAST(v38_19 AS DECIMAL(38,19)) AS d38_19,
                   |  CAST(v38_38 AS DECIMAL(38,38)) AS d38_38,
                   |  CAST(v38_27 AS DECIMAL(38,27)) AS d38_27,
                   |  CAST(v38_37 AS DECIMAL(38,37)) AS d38_37
                   |FROM VALUES
                   |  ('123456789012.345678', '9007199254740991',
                   |   '12345678901234567890123456789012345678',
                   |   '214.4000000000000006143270301075587414',
                   |   '0.99999999999999999999999999999999999999',
                   |   '214.400000000000000539062857143',
                   |   '1.2345678901234567890123456789012345678'),
                   |  ('999999999999.999999', '9007199254740992',
                   |   '9007199254740993',
                   |   '9007199254740993.0000000000000000001',
                   |   '0.00000000000000000000000000000000000001',
                   |   '1.000000000000000000000000001',
                   |   '0.0000000000000000000000000000000000001'),
                   |  ('-123456789012.345678', '-9007199254740993',
                   |   '-99999999999999999999999999999999999999',
                   |   '-0.0130597014925373134',
                   |   '-0.5',
                   |   '-214.400000000000000539062857143',
                   |   '-1.2345678901234567890123456789012345678'),
                   |  ('0', '0', '0', '0', '0', '0', '0'),
                   |  (NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                   |  ('999999999999.999999', '9999999999999999999',
                   |   '99999999999999999999999999999999999999',
                   |   '9999999999999999999.9999999999999999999',
                   |   '0.12345678901234567890123456789012345678',
                   |   '99999999999.999999999999999999999999999',
                   |   '9.9999999999999999999999999999999999999')
                   |AS t(v18, v19, v38_0, v38_19, v38_38, v38_27, v38_37)
                   |""".stripMargin)
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("decimal_double_cast")

          Seq("true", "false").foreach {
            ansi =>
              withSQLConf(
                SQLConf.ANSI_ENABLED.key -> ansi,
                GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
                VeloxConfig.DECIMAL_TO_FLOAT_HIGH_PRECISION_CAST_ENABLED.key -> "true"
              ) {
                val sqlText = """
                                |SELECT
                                |  CAST(d18_6 AS DOUBLE),
                                |  CAST(d19_0 AS DOUBLE),
                                |  CAST(d38_0 AS DOUBLE),
                                |  CAST(d38_19 AS DOUBLE),
                                |  CAST(d38_38 AS DOUBLE),
                                |  CAST(d38_27 AS DOUBLE),
                                |  CAST(d38_37 AS DOUBLE)
                                |FROM decimal_double_cast
                                |""".stripMargin
                runQueryAndCompare(sqlText) {
                  df =>
                    checkGlutenPlan[ProjectExecTransformer](df)
                    assertDoublesMatchVanillaExactly(sqlText, df.collect())
                }
              }
          }
      }
    }
  }

  testWithMinSparkVersion(
    "decimal arithmetic respects allowPrecisionLoss captured at view analysis time",
    "4.1") {
    // Regression test for GLUTEN-11917: in Spark 4.1, arithmetic expressions embed
    // allowPrecisionLoss in their evalContext at analysis time. Gluten must read from
    // the expression rather than SQLConf.get, which can differ when querying a view
    // analyzed under a different session config.
    withTempView("t", "v") {
      sql("""
            |SELECT
            |CAST('1234567890123456789012345.12345678901' AS DECIMAL(38,11)) AS a,
            |CAST('1234567890123456789012345.02345678901' AS DECIMAL(38,11)) AS b""".stripMargin)
        .createOrReplaceTempView("t")

      // Analyze arithmetic with allowPrecisionLoss=false and cache it in the view's plan.
      withSQLConf("spark.sql.decimalOperations.allowPrecisionLoss" -> "false") {
        sql("CREATE OR REPLACE TEMP VIEW v AS SELECT a - b, a + b, a * b, a / b FROM t")
      }

      // Query under the opposite setting -- Gluten must use the captured context, not SQLConf.
      withSQLConf("spark.sql.decimalOperations.allowPrecisionLoss" -> "true") {
        runQueryAndCompare("SELECT * FROM v") {
          checkGlutenPlan[ProjectExecTransformer]
        }
      }
    }
  }
}
