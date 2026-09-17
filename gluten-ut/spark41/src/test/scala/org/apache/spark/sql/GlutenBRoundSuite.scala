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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql.catalyst.expressions.BRound
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._
import scala.util.Properties

class GlutenBRoundSuite extends GlutenSQLTestsTrait {
  private def withInput(schema: StructType, rows: Seq[Row])(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withTempPath {
        path =>
          withTempView("bround_input") {
            spark
              .createDataFrame(rows.asJava, schema)
              .coalesce(1)
              .write
              .parquet(path.getCanonicalPath)
            spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("bround_input")
            f
          }
      }
    }
  }

  private def isBroundFullyNative(df: DataFrame): Boolean = {
    val plan = df.queryExecution.executedPlan
    val hasNativeCall = plan.exists {
      case project: ProjectExecTransformer =>
        project.projectList.exists(_.exists(_.isInstanceOf[BRound]))
      case _ => false
    }
    val hasSparkCall = plan.exists {
      case _: ProjectExecTransformer => false
      case node => node.expressions.exists(_.exists(_.isInstanceOf[BRound]))
    }
    hasNativeCall && !hasSparkCall
  }

  private def assertNativeBround(df: DataFrame): Unit = {
    assert(
      isBroundFullyNative(df),
      s"BROUND was not executed natively:\n${df.queryExecution.executedPlan}")
  }

  private def checkBround(query: String, expectNative: Boolean = true): Unit = {
    val (expectedType, expected) = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      val df = sql(query)
      (df.schema, df.collect().toSeq)
    }
    val df = sql(query)
    assert(
      isBroundFullyNative(df) == expectNative,
      s"Unexpected BROUND execution path:\n${df.queryExecution.executedPlan}")
    assert(df.schema == expectedType)
    val actual = df.collect().toSeq
    def exactRows(rows: Seq[Row]): Map[Seq[Any], Int] = {
      rows
        .map(_.toSeq.map {
          case value: Double => java.lang.Double.doubleToRawLongBits(value)
          case value: Float => java.lang.Float.floatToRawIntBits(value)
          case value => value
        })
        .groupMapReduce(identity)(_ => 1)(_ + _)
    }
    assert(exactRows(actual) == exactRows(expected))
  }

  testGluten("bround primitive types and positive and negative midpoint ties") {
    val schema = new StructType()
      .add("b", ByteType)
      .add("s", ShortType)
      .add("i", IntegerType)
      .add("l", LongType)
      .add("f", FloatType)
      .add("d", DoubleType)
    val rows = Seq(25, 35, -25, -35, 0).map {
      value => Row(value.toByte, value.toShort, value, value.toLong, value / 10.0f, value / 10.0d)
    } :+ Row(null, null, null, null, null, null)
    withInput(schema, rows) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            checkBround(
              "SELECT bround(b, -1), bround(s, -1), bround(i, -1), " +
                "bround(l, -1), bround(f), bround(d, 0) FROM bround_input")
          }
      }
    }
  }

  testGluten("bround floating results match Spark bit for bit") {
    val schema = new StructType().add("d", DoubleType).add("f", FloatType)
    val values = Seq(
      0.575d,
      2.55d,
      -2.55d,
      -0.0d,
      Double.MinPositiveValue,
      Double.MaxValue,
      java.lang.Double.longBitsToDouble(0x43abc16d674ec804L),
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      Double.NaN
    )
    withInput(schema, values.map(value => Row(value, value.toFloat)) :+ Row(null, null)) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            checkBround(
              "SELECT bround(d, 2), bround(d, -3), bround(f, 2), " +
                "bround(f, -3) FROM bround_input",
              expectNative = Properties.isJavaAtLeast("21"))
          }
      }
    }
  }

  testGluten("bround short and long decimals and resolved precision") {
    val schema = new StructType()
      .add("short_value", DecimalType(3, 2))
      .add("boundary_value", DecimalType(18, 0))
      .add("long_value", DecimalType(38, 38))
    val rows = Seq(
      Row(
        new java.math.BigDecimal("2.55"),
        new java.math.BigDecimal("999999999999999999"),
        new java.math.BigDecimal("0.6")),
      Row(
        new java.math.BigDecimal("-2.55"),
        new java.math.BigDecimal("-999999999999999999"),
        new java.math.BigDecimal("-0.6")),
      Row(null, null, null)
    )
    withInput(schema, rows) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            checkBround(
              "SELECT bround(short_value, 1), bround(short_value, 0), " +
                "bround(boundary_value, -1), bround(long_value, -1), " +
                "bround(long_value, -39) FROM bround_input")
          }
      }
    }
  }

  testGluten("bround integral overflow wraps in legacy mode and throws in ANSI mode") {
    val schema = new StructType().add("value", LongType)
    withInput(schema, Seq(Row(Long.MaxValue), Row(Long.MinValue), Row(null))) {
      val query = "SELECT bround(value, -19) FROM bround_input"
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkBround(query)
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val df = sql(query)
        assertNativeBround(df)
        val error = intercept[Exception](df.collect())
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
        assert(causes.exists {
          cause =>
            Option(
              cause.getMessage).exists(_.toLowerCase(java.util.Locale.ROOT).contains("overflow"))
        })
      }
    }
  }

  testGluten("bround decimal precision overflow throws in both modes") {
    val schema = new StructType().add("value", DecimalType(38, 0))
    withInput(schema, Seq(Row(new java.math.BigDecimal("9" * 38)))) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            val query = "SELECT bround(value, -1) FROM bround_input"
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              intercept[Exception](sql(query).collect())
            }
            val df = sql(query)
            assertNativeBround(df)
            val error = intercept[Exception](df.collect())
            val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
            assert(causes.exists {
              cause =>
                Option(cause.getMessage).exists {
                  message =>
                    val normalized = message.toLowerCase(java.util.Locale.ROOT)
                    normalized.contains("overflow") || normalized.contains("out of range")
                }
            })
          }
      }
    }
  }

  testGluten("bround accepts foldable and null scales") {
    val schema = new StructType()
      .add("value", DecimalType(19, 2))
      .add("f", FloatType)
      .add("d", DoubleType)
    withInput(
      schema,
      Seq(Row(new java.math.BigDecimal("2.55"), 2.55f, 2.55d), Row(null, null, null))) {
      checkBround("SELECT bround(value, 1 + 0) FROM bround_input")
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            Seq("value", "f", "d").foreach {
              column =>
                val query = s"SELECT bround($column, CAST(NULL AS INT)) FROM bround_input"
                withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> NullPropagation.ruleName) {
                  checkBround(query)
                }
                withSQLConf(
                  SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
                    s"${ConstantFolding.ruleName},${NullPropagation.ruleName}") {
                  checkBround(query, expectNative = false)
                }
            }
          }
      }
    }
  }

  testGluten("bround outside the supported native scale interval falls back") {
    val schema = new StructType()
      .add("integral_value", LongType)
      .add("floating_value", DoubleType)
      .add("decimal_value", DecimalType(3, 2))
    withInput(
      schema,
      Seq(Row(25L, 2.55d, new java.math.BigDecimal("2.55")), Row(null, null, null))) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            Seq(-401, 401).foreach {
              scale =>
                Seq("integral_value", "floating_value", "decimal_value").foreach {
                  column =>
                    checkBround(
                      s"SELECT bround($column, $scale) FROM bround_input",
                      expectNative = false)
                }
            }
          }
      }
    }
  }

  testGluten("bround preserves analyzed ANSI mode after the session mode changes") {
    val schema = new StructType().add("value", ByteType)
    withInput(schema, Seq(Row(127.toByte))) {
      Seq(false, true).foreach {
        capturedAnsi =>
          val df = withSQLConf(SQLConf.ANSI_ENABLED.key -> capturedAnsi.toString) {
            val result = sql("SELECT bround(value, -1) FROM bround_input")
            result.queryExecution.analyzed
            result
          }
          withSQLConf(SQLConf.ANSI_ENABLED.key -> (!capturedAnsi).toString) {
            assertNativeBround(df)
            if (capturedAnsi) {
              intercept[Exception](df.collect())
            } else {
              checkAnswer(df, Seq(Row((-126).toByte)))
            }
          }
      }
    }
  }

  testGluten("bround preserves ANSI mode after native physical planning") {
    val schema = new StructType().add("value", ByteType)
    withInput(schema, Seq(Row(127.toByte))) {
      Seq(false, true).foreach {
        capturedAnsi =>
          val df = withSQLConf(SQLConf.ANSI_ENABLED.key -> capturedAnsi.toString) {
            val result = sql("SELECT bround(value, -1) FROM bround_input")
            assertNativeBround(result)
            result
          }
          withSQLConf(SQLConf.ANSI_ENABLED.key -> (!capturedAnsi).toString) {
            if (capturedAnsi) {
              intercept[Exception](df.collect())
            } else {
              checkAnswer(df, Seq(Row((-126).toByte)))
            }
          }
      }
    }
  }
}
