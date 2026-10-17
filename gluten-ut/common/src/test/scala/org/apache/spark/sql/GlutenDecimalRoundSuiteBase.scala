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

import org.apache.spark.sql.catalyst.expressions.{Literal, Round}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

abstract class GlutenDecimalRoundSuiteBase extends GlutenSQLTestsTrait {
  private def withInput(dataType: DecimalType, values: Seq[String])(f: => Unit): Unit = {
    val schema = new StructType().add("value", dataType)
    val rows = values.map(value => Row(new java.math.BigDecimal(value))) :+ Row(null)
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withTempPath {
        path =>
          spark.createDataFrame(rows.asJava, schema).coalesce(1).write.parquet(path.toString)
          withTempView("decimal_round_input") {
            spark.read.parquet(path.toString).createOrReplaceTempView("decimal_round_input")
            f
          }
      }
    }
  }

  private def assertExecution(df: DataFrame, native: Boolean): Unit = {
    val plan = df.queryExecution.executedPlan
    val locations = plan.collect {
      case node if node.expressions.exists(_.exists(_.isInstanceOf[Round])) =>
        node.isInstanceOf[ProjectExecTransformer]
    }
    assert(
      locations.nonEmpty && locations.forall(_ == native),
      s"Unexpected ROUND execution:\n$plan")
  }

  private def checkRound(scale: String, native: Boolean = true): Unit = {
    val query = s"SELECT round(value, $scale) FROM decimal_round_input"
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      val vanilla = sql(query)
      val expectedSchema = vanilla.schema
      val expected = vanilla.collect().toSeq
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
        val df = sql(query)
        assertExecution(df, native)
        assert(df.schema == expectedSchema)
        checkAnswer(df, expected)
      }
    }
  }

  testGluten("decimal ROUND preserves schemas and HALF_UP results across storage boundaries") {
    val cases = Seq(
      (DecimalType(3, 1), Seq("2.5", "-2.5", "0")),
      (DecimalType(18, 0), Seq("999999999999999999", "-999999999999999999")),
      (DecimalType(19, 0), Seq("9999999999999999999", "-9999999999999999999")),
      (DecimalType(38, 1), Seq("9" * 37 + ".9", "-" + "9" * 37 + ".9")),
      (DecimalType(38, 38), Seq("0.6", "-0.6", "0.5", "-0.5"))
    )
    for ((dataType, values) <- cases) {
      withInput(dataType, values) {
        for (ansi <- Seq("false", "true")) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            Seq(-400, -39, -1, 0, 1, 38, 400).foreach(scale => checkRound(scale.toString))
            checkRound("-1 + 0")
          }
        }
      }
    }
  }

  testGluten("decimal ROUND handles the 38-digit HALF_UP tie and precision overflow") {
    withInput(DecimalType(38, 0), Seq("4" + "9" * 37, "-4" + "9" * 37)) {
      checkRound("-38")
    }
    for ((value, scale) <- Seq(("9" * 38, -1), ("5" + "0" * 37, -38), ("-5" + "0" * 37, -38))) {
      withInput(DecimalType(38, 0), Seq(value)) {
        for (ansi <- Seq("false", "true")) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            val query = s"SELECT round(value, $scale) FROM decimal_round_input"
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val error = intercept[Exception](sql(query).collect())
              assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists {
                cause =>
                  Option(cause.getMessage).exists {
                    message =>
                      message.contains("NUMERIC_VALUE_OUT_OF_RANGE") ||
                      message.contains("DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION")
                  }
              })
            }
            val df = sql(query)
            assertExecution(df, native = true)
            val error = intercept[Exception](df.collect())
            assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists {
              cause => Option(cause.getMessage).exists(_.contains("Decimal overflow"))
            })
          }
        }
      }
    }
  }

  testGluten(
    "decimal ROUND retains NULL scales and guards extreme scales without native validation") {
    withInput(DecimalType(3, 1), Seq("2.5", "-2.5", "0")) {
      withSQLConf(
        GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> NullPropagation.ruleName) {
        checkRound("CAST(NULL AS INT)")
        checkRound("-401", native = false)
        checkRound("401", native = false)
      }
    }
  }

  testGluten("decimal ROUND accepts foldable scales with constant folding disabled") {
    withInput(DecimalType(3, 1), Seq("2.5", "-2.5", "0")) {
      for (nativeValidation <- Seq("false", "true")) {
        withSQLConf(
          GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> nativeValidation,
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
            s"${ConstantFolding.ruleName},${NullPropagation.ruleName}"
        ) {
          val scales = sql("SELECT round(value, 1 - 2) FROM decimal_round_input")
            .queryExecution.optimizedPlan.collect {
              case node => node.expressions.flatMap(_.collect { case round: Round => round.scale })
            }.flatten
          assert(scales.nonEmpty)
          assert(scales.forall(scale => scale.foldable && !scale.isInstanceOf[Literal]))
          checkRound("1 - 2")
          checkRound("-200 - 200")
          checkRound("200 + 200")
          checkRound("CAST(NULL AS INT)")
          checkRound("400 + 1", native = false)
          checkRound("-400 - 1", native = false)
        }
      }
    }
  }
}
