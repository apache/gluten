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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.Round
import org.apache.spark.sql.catalyst.optimizer.NullPropagation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

class GlutenIntegralRoundSuite extends GlutenSQLTestsTrait {
  private def withInput(f: => Unit): Unit = {
    val schema = new StructType()
      .add("b", ByteType).add("s", ShortType).add("i", IntegerType).add("l", LongType)
    val rows = Seq(127, -128, 25, -25, 0).map {
      value => Row(value.toByte, value.toShort, value, value.toLong)
    } :+ Row(null, null, null, null)
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withTempPath {
        path =>
          spark.createDataFrame(rows.asJava, schema).coalesce(1).write.parquet(path.toString)
          withTempView("integral_round_input") {
            spark.read.parquet(path.toString).createOrReplaceTempView("integral_round_input")
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

  private def assertOverflow(df: DataFrame): Unit = {
    val error = intercept[Exception](df.collect())
    assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists {
      case cause: SparkThrowable => cause.getCondition == "ARITHMETIC_OVERFLOW"
      case _ => false
    })
  }

  testGluten("integral ROUND uses the native identity path for every signed width") {
    withInput {
      for (ansi <- Seq("false", "true")) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          for (scale <- Seq(0, 2, 400)) {
            val columns = Seq("b", "s", "i", "l")
            val query = columns.map(c => s"round($c, $scale)").mkString(
              "SELECT ",
              ", ",
              " FROM integral_round_input")
            val expected = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              sql(query).collect().toSeq
            }
            val df = sql(query)
            assertExecution(df, native = true)
            checkAnswer(df, expected)
          }
        }
      }
    }
  }

  testGluten("negative integral ROUND preserves Spark ANSI overflow and legacy wrapping") {
    withInput {
      for (ansi <- Seq(false, true)) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val query = "SELECT round(b, -1) FROM integral_round_input"
          val df = sql(query)
          assertExecution(df, native = false)
          if (ansi) {
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              assertOverflow(sql(query))
            }
            assertOverflow(df)
          } else {
            checkAnswer(
              df,
              Seq(
                Row((-126).toByte),
                Row(126.toByte),
                Row(30.toByte),
                Row((-30).toByte),
                Row(0.toByte),
                Row(null)))
          }
        }
      }
    }
  }

  testGluten("integral ROUND keeps captured overflow behavior after the session changes") {
    withInput {
      for (captured <- Seq(false, true); planFirst <- Seq(false, true)) {
        val df = withSQLConf(SQLConf.ANSI_ENABLED.key -> captured.toString) {
          val query = sql("SELECT round(b, -1) FROM integral_round_input")
          query.queryExecution.analyzed
          if (planFirst) {
            query.queryExecution.executedPlan
          }
          query
        }
        withSQLConf(SQLConf.ANSI_ENABLED.key -> (!captured).toString) {
          if (captured) {
            assertOverflow(df)
          } else {
            checkAnswer(
              df,
              Seq(
                Row((-126).toByte),
                Row(126.toByte),
                Row(30.toByte),
                Row((-30).toByte),
                Row(0.toByte),
                Row(null)))
          }
        }
      }
    }
  }

  testGluten("integral ROUND NULL scales short-circuit the child natively") {
    withInput {
      for (nativeValidation <- Seq("false", "true")) {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> NullPropagation.ruleName,
          GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> nativeValidation
        ) {
          val query = "SELECT round(b, CAST(NULL AS INT)), round(s, CAST(NULL AS INT)), " +
            "round(i, CAST(NULL AS INT)), round(l, CAST(NULL AS INT)), " +
            "round(CAST(l + 1000 AS TINYINT), CAST(NULL AS INT)) FROM integral_round_input"
          val (expectedSchema, expected) =
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val vanilla = sql(query)
              (vanilla.schema, vanilla.collect().toSeq)
            }
          val df = sql(query)
          withClue(s"native validation=$nativeValidation: ") {
            assertExecution(df, native = true)
            assert(df.schema == expectedSchema)
            checkAnswer(df, expected)
          }
        }
      }
    }
  }

  testGluten("integral ROUND extreme scales retain Spark execution without native validation") {
    withInput {
      withSQLConf(GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false") {
        for (scale <- Seq(-401, 401)) {
          val query = s"SELECT round(l, $scale) FROM integral_round_input"
          val expected = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
            sql(query).collect().toSeq
          }
          val df = sql(query)
          assertExecution(df, native = false)
          checkAnswer(df, expected)
        }
      }
    }
  }
}
