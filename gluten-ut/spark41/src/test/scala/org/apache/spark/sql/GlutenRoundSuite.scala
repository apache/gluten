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

import org.apache.spark.sql.catalyst.expressions.Round
import org.apache.spark.sql.catalyst.optimizer.NullPropagation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._
import scala.util.Properties

class GlutenRoundSuite extends GlutenSQLTestsTrait {
  private def withInput(schema: StructType, rows: Seq[Row])(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withTempPath {
        path =>
          withTempView("round_input") {
            spark.createDataFrame(rows.asJava, schema).coalesce(1).write.parquet(path.toString)
            spark.read.parquet(path.toString).createOrReplaceTempView("round_input")
            f
          }
      }
    }
  }

  private def assertExecution(df: DataFrame, native: Boolean): Unit = {
    val plan = df.queryExecution.executedPlan
    val nativeCalls = plan.collect {
      case project: ProjectExecTransformer =>
        project.projectList.flatMap(_.collect { case round: Round => round })
    }.flatten
    val sparkCalls = plan.collect {
      case node if !node.isInstanceOf[ProjectExecTransformer] =>
        node.expressions.flatMap(_.collect { case round: Round => round })
    }.flatten
    if (native) {
      assert(nativeCalls.nonEmpty && sparkCalls.isEmpty, s"ROUND was not fully native:\n$plan")
    } else {
      assert(nativeCalls.isEmpty && sparkCalls.nonEmpty, s"ROUND did not fall back:\n$plan")
    }
  }

  private def exactRows(rows: Seq[Row]): Map[Seq[Any], Int] = {
    rows.map(_.toSeq.map {
      case value: Double => java.lang.Double.doubleToRawLongBits(value)
      case value: Float => java.lang.Float.floatToRawIntBits(value)
      case value => value
    }).groupMapReduce(identity)(_ => 1)(_ + _)
  }

  private def checkRound(query: String, native: Boolean = true): Unit = {
    val (expectedSchema, expected) = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      val df = sql(query)
      (df.schema, df.collect().toSeq)
    }
    val df = sql(query)
    assertExecution(df, native)
    assert(df.schema == expectedSchema)
    assert(exactRows(df.collect().toSeq) == exactRows(expected))
  }

  testGluten("round negative scales and HALF_UP ties for every integral width") {
    val schema = new StructType()
      .add("b", ByteType).add("s", ShortType).add("i", IntegerType).add("l", LongType)
    val rows = Seq(25, 35, -25, -35, 0).map {
      value => Row(value.toByte, value.toShort, value, value.toLong)
    } :+ Row(null, null, null, null)
    withInput(schema, rows) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            Seq(-2, -1, 0, 2, 400).foreach {
              scale =>
                checkRound(
                  s"SELECT round(b, $scale), round(s, $scale), " +
                    s"round(i, $scale), round(l, $scale) FROM round_input")
            }
          }
      }
    }
  }

  testGluten("round floating precision regressions and special values match Spark bits") {
    val schema = new StructType().add("d", DoubleType).add("f", FloatType)
    val values = Seq(
      0.575d,
      0.5549999999999999d,
      0.499999999999994d,
      2.5d,
      -2.5d,
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
            checkRound("SELECT round(d), round(f, 0) FROM round_input")
            Seq(-400, -3, 2, 400).foreach {
              scale =>
                checkRound(
                  s"SELECT round(d, $scale), round(f, $scale) FROM round_input",
                  native = Properties.isJavaAtLeast("21"))
            }
          }
      }
    }
  }

  testGluten("round decimal precision and storage transitions at negative scales") {
    Seq((1, 0), (3, 2), (18, 0), (18, 6), (19, 0), (20, 7), (38, 0), (38, 38)).foreach {
      case (precision, scale) =>
        val schema = new StructType().add("value", DecimalType(precision, scale))
        val magnitude = if (scale == precision) "0.5" else "5"
        val rows = Seq(
          Row(new java.math.BigDecimal(magnitude)),
          Row(new java.math.BigDecimal("-" + magnitude)),
          Row(null))
        withInput(schema, rows) {
          Seq("false", "true").foreach {
            ansi =>
              withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
                Seq(-400, -39, -2, -1, 0, 2, 38, 400).foreach {
                  requested => checkRound(s"SELECT round(value, $requested) FROM round_input")
                }
              }
          }
        }
    }
  }

  testGluten("round null scales remain native for integral floating and decimal inputs") {
    val schema = new StructType().add("i", IntegerType).add("f", FloatType)
      .add("d", DoubleType).add("n", DecimalType(19, 2))
    withInput(
      schema,
      Seq(Row(25, 2.5f, 2.5d, new java.math.BigDecimal("2.55")), Row(null, null, null, null))) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(
            SQLConf.ANSI_ENABLED.key -> ansi,
            SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> NullPropagation.ruleName) {
            Seq("i", "f", "d", "n").foreach {
              column => checkRound(s"SELECT round($column, CAST(NULL AS INT)) FROM round_input")
            }
          }
      }
    }
  }

  testGluten("round overflow follows captured ANSI before and after native planning") {
    val schema = new StructType().add("value", ByteType)
    withInput(schema, Seq(Row(127.toByte))) {
      for (planFirst <- Seq(false, true); capturedAnsi <- Seq(false, true)) {
        val df = withSQLConf(SQLConf.ANSI_ENABLED.key -> capturedAnsi.toString) {
          val result = sql("SELECT round(value, -1) FROM round_input")
          result.queryExecution.analyzed
          if (planFirst) {
            assertExecution(result, native = true)
          }
          result
        }
        withSQLConf(SQLConf.ANSI_ENABLED.key -> (!capturedAnsi).toString) {
          assertExecution(df, native = true)
          if (capturedAnsi) {
            val error = intercept[Exception](df.collect())
            assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists {
              cause =>
                Option(cause.getMessage).exists(
                  _.toLowerCase(java.util.Locale.ROOT).contains("overflow"))
            })
          } else {
            checkAnswer(df, Seq(Row((-126).toByte)))
          }
        }
      }
    }
  }

  testGluten("round decimal overflow raises an error in both modes") {
    val schema = new StructType().add("value", DecimalType(38, 0))
    withInput(schema, Seq(Row(new java.math.BigDecimal("9" * 38)))) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            val query = "SELECT round(value, -1) FROM round_input"
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              intercept[Exception](sql(query).collect())
            }
            val df = sql(query)
            assertExecution(df, native = true)
            val error = intercept[Exception](df.collect())
            assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists {
              cause =>
                Option(cause.getMessage).exists {
                  message =>
                    val lower = message.toLowerCase(java.util.Locale.ROOT)
                    lower.contains("overflow") || lower.contains("out of range")
                }
            })
          }
      }
    }
  }

  testGluten("round unsupported native scales retain Spark execution") {
    val schema = new StructType().add("i", IntegerType).add("d", DoubleType)
      .add("n", DecimalType(3, 2))
    val rows = Seq(Row(25, 2.55d, new java.math.BigDecimal("2.55")), Row(null, null, null))
    withInput(schema, rows) {
      for (ansi <- Seq("false", "true"); scale <- Seq(-401, 401); column <- Seq("i", "d", "n")) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          checkRound(s"SELECT round($column, $scale) FROM round_input", native = false)
        }
      }
    }
  }

  testGluten("round capability remains mandatory when general native validation is disabled") {
    val schema = new StructType().add("i", IntegerType).add("d", DoubleType)
      .add("n", DecimalType(3, 2))
    val rows = Seq(Row(25, 2.5d, new java.math.BigDecimal("2.55")), Row(null, null, null))
    val nativeExpected = sys.props.get("gluten.test.round.nativeCapability").forall(_.toBoolean)
    withInput(schema, rows) {
      withSQLConf(GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false") {
        checkRound("SELECT round(i, -1) FROM round_input", native = nativeExpected)
        checkRound("SELECT round(d, 0) FROM round_input", native = nativeExpected)
        checkRound("SELECT round(n, 1) FROM round_input", native = nativeExpected)
      }
    }
  }
}
