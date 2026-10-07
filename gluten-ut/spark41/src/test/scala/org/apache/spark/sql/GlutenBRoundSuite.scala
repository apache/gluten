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
import org.apache.spark.sql.catalyst.optimizer.NullPropagation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util.Locale

import scala.jdk.CollectionConverters._

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

  private def assertNativeBround(df: DataFrame): Unit = {
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
    assert(hasNativeCall && !hasSparkCall, s"BROUND was not fully native:\n$plan")
  }

  private def checkNativeAnswer(query: String, expected: Seq[Row]): Unit = {
    val df = sql(query)
    assertNativeBround(df)
    checkAnswer(df, expected)
  }

  testGluten("bround integral and decimal half-even semantics") {
    val integralSchema = new StructType()
      .add("b", ByteType)
      .add("s", ShortType)
      .add("i", IntegerType)
      .add("l", LongType)
    val integralRows = Seq(25, 35, -25, -35).map {
      value => Row(value.toByte, value.toShort, value, value.toLong)
    } :+ Row(null, null, null, null)
    val integralExpected = Seq(20, 40, -20, -40).map {
      value => Row(value.toByte, value.toShort, value, value.toLong)
    } :+ Row(null, null, null, null)

    withInput(integralSchema, integralRows) {
      checkNativeAnswer(
        "SELECT bround(b, -1), bround(s, -1), bround(i, -1), bround(l, -1) " +
          "FROM bround_input",
        integralExpected)
    }

    val decimalSchema = new StructType().add("value", DecimalType(3, 2))
    val decimalRows = Seq("2.45", "2.55", "-2.45", "-2.55").map {
      value => Row(new java.math.BigDecimal(value))
    } :+ Row(null)
    val decimalExpected = Seq("2.4", "2.6", "-2.4", "-2.6").map {
      value => Row(new java.math.BigDecimal(value))
    } :+ Row(null)

    withInput(decimalSchema, decimalRows) {
      checkNativeAnswer("SELECT bround(value, 1) FROM bround_input", decimalExpected)
    }
  }

  testGluten("bround floating-point uses native binary rounding") {
    val schema = new StructType().add("value", DoubleType)
    val rows = Seq(0.575d, -0.575d, 2.5d, 3.5d).map(value => Row(value)) :+ Row(null)

    withInput(schema, rows) {
      checkNativeAnswer(
        "SELECT bround(value, 2), bround(value, 0) FROM bround_input",
        Seq(
          Row(0.57d, 1.0d),
          Row(-0.57d, -1.0d),
          Row(2.5d, 2.0d),
          Row(3.5d, 4.0d),
          Row(null, null)))
    }
  }

  testGluten("bround integral overflow follows ANSI mode") {
    val schema = new StructType().add("value", ByteType)
    withInput(schema, Seq(Row(127.toByte))) {
      val query = "SELECT bround(value, -1) FROM bround_input"
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkNativeAnswer(query, Seq(Row((-126).toByte)))
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val df = sql(query)
        assertNativeBround(df)
        val error = intercept[Exception](df.collect())
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
        assert(causes.exists {
          cause => Option(cause.getMessage).exists(_.toLowerCase(Locale.ROOT).contains("overflow"))
        })
      }
    }
  }

  testGluten("bround supports null and extreme constant scales") {
    val schema = new StructType()
      .add("integral_value", LongType)
      .add("floating_value", DoubleType)
      .add("decimal_value", DecimalType(3, 2))
    val rows = Seq(Row(1L, Double.MinPositiveValue, new java.math.BigDecimal("0.60")))

    withInput(schema, rows) {
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> NullPropagation.ruleName) {
        checkNativeAnswer(
          "SELECT bround(integral_value, CAST(NULL AS INT)), " +
            "bround(floating_value, CAST(NULL AS INT)), " +
            "bround(decimal_value, CAST(NULL AS INT)) FROM bround_input",
          Seq(Row(null, null, null))
        )
      }

      checkNativeAnswer(
        "SELECT bround(integral_value, CAST(-2147483648 AS INT)), " +
          "bround(floating_value, CAST(-2147483648 AS INT)), " +
          "bround(decimal_value, CAST(-2147483648 AS INT)) FROM bround_input",
        Seq(Row(0L, 0.0d, new java.math.BigDecimal("0")))
      )

      checkNativeAnswer(
        "SELECT bround(floating_value, 2147483647), " +
          "bround(decimal_value, 2147483647) FROM bround_input",
        Seq(Row(Double.MinPositiveValue, new java.math.BigDecimal("0.60")))
      )
    }
  }
}
