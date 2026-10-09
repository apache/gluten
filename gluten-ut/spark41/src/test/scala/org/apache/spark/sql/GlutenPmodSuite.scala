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

import org.apache.gluten.backendsapi.velox.VeloxValidatorApi
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Pmod}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

class GlutenPmodSuite extends GlutenSQLTestsTrait {
  private def withInput(schema: StructType, rows: Seq[Row])(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withTempPath {
        path =>
          withTempView("pmod_input") {
            spark.createDataFrame(rows.asJava, schema).coalesce(1).write.parquet(path.toString)
            spark.read.parquet(path.toString).createOrReplaceTempView("pmod_input")
            f
          }
      }
    }
  }

  private def assertExecution(df: DataFrame, native: Boolean): Unit = {
    val plan = df.queryExecution.executedPlan
    val nativeCalls = plan.collect {
      case p: ProjectExecTransformer =>
        p.projectList.flatMap(_.collect { case expression: Pmod => expression })
    }.flatten
    val sparkCalls = plan.collect {
      case p if !p.isInstanceOf[ProjectExecTransformer] =>
        p.expressions.flatMap(_.collect { case expression: Pmod => expression })
    }.flatten
    if (native) {
      assert(nativeCalls.nonEmpty && sparkCalls.isEmpty, s"PMOD is not fully native:\n$plan")
    } else {
      assert(nativeCalls.isEmpty && sparkCalls.nonEmpty, s"PMOD did not fall back:\n$plan")
    }
  }

  private def comparable(rows: Seq[Row]): Map[Seq[Any], Int] = {
    rows.map(_.toSeq.map {
      case value: Double =>
        if (value.isNaN) "double-NaN" else java.lang.Double.doubleToRawLongBits(value)
      case value: Float =>
        if (value.isNaN) "float-NaN" else java.lang.Float.floatToRawIntBits(value)
      case value => value
    }).groupMapReduce(identity)(_ => 1)(_ + _)
  }

  private def checkPmod(query: String, native: Boolean = true): Unit = {
    val (schema, expected) = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      val df = sql(query)
      (df.schema, comparable(df.collect().toSeq))
    }
    val df = sql(query)
    assertExecution(df, native)
    assert(df.schema == schema)
    assert(comparable(df.collect().toSeq) == expected)
  }

  testGluten("pmod matches Spark for every primitive width and operand signs") {
    val schema = new StructType().add("left_value", LongType).add("right_value", LongType)
    val rows = Seq(
      Row(-10L, 3L),
      Row(10L, -3L),
      Row(-10L, -3L),
      Row(10L, 3L),
      Row(0L, -3L),
      Row(-9L, 3L),
      Row(null, 3L),
      Row(10L, null))
    withInput(schema, rows) {
      for (
        ansi <- Seq("false", "true");
        dataType <- Seq(
          "TINYINT",
          "SMALLINT",
          "INT",
          "BIGINT",
          "FLOAT",
          "DOUBLE")
      ) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          checkPmod(
            s"SELECT pmod(CAST(left_value AS $dataType), " +
              s"CAST(right_value AS $dataType)) FROM pmod_input")
        }
      }
    }
  }

  testGluten("pmod floating special values preserve Spark semantics and signed zero") {
    val schema = new StructType().add("left_value", DoubleType).add("right_value", DoubleType)
    val rows = Seq(
      Row(-0.0d, 3.0d),
      Row(0.0d, -3.0d),
      Row(-9.0d, 3.0d),
      Row(1.0d, Double.PositiveInfinity),
      Row(-1.0d, Double.PositiveInfinity),
      Row(0.0d, Double.NegativeInfinity),
      Row(Double.PositiveInfinity, 3.0d),
      Row(Double.NaN, 3.0d),
      Row(3.0d, Double.NaN),
      Row(Double.MinPositiveValue, 3.0d),
      Row(-Double.MinPositiveValue, 3.0d),
      Row(Double.MaxValue, -3.0d),
      Row(null, 3.0d),
      Row(3.0d, null)
    )
    withInput(schema, rows) {
      for (ansi <- Seq("false", "true"); dataType <- Seq("FLOAT", "DOUBLE")) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          checkPmod(
            s"SELECT pmod(CAST(left_value AS $dataType), " +
              s"CAST(right_value AS $dataType)) FROM pmod_input")
        }
      }
    }
  }

  testGluten("pmod integral boundaries match JVM wrapping and division guards") {
    Seq(
      ("TINYINT", Byte.MinValue.toLong, Byte.MaxValue.toLong),
      ("SMALLINT", Short.MinValue.toLong, Short.MaxValue.toLong),
      ("INT", Int.MinValue.toLong, Int.MaxValue.toLong),
      ("BIGINT", Long.MinValue, Long.MaxValue)
    ).foreach {
      case (dataType, minimum, maximum) =>
        val schema = new StructType().add("left_value", LongType).add("right_value", LongType)
        val rows = Seq(
          Row(minimum, -1L),
          Row(minimum + 1, minimum),
          Row(-1L, minimum),
          Row(maximum, minimum),
          Row(minimum, maximum))
        withInput(schema, rows) {
          Seq("false", "true").foreach {
            ansi =>
              withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
                checkPmod(
                  s"SELECT pmod(CAST(left_value AS $dataType), " +
                    s"CAST(right_value AS $dataType)) FROM pmod_input")
              }
          }
        }
    }
  }

  testGluten("pmod zero divisors return null in legacy and raise errors in ANSI") {
    val schema = new StructType().add("value", IntegerType)
    withInput(schema, Seq(Row(10), Row(null))) {
      Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE").foreach {
        dataType =>
          val query = s"SELECT pmod(CAST(value AS $dataType), CAST(0 AS $dataType)) " +
            "FROM pmod_input"
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
            checkPmod(query)
          }
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              intercept[Exception](sql(query).collect())
            }
            val df = sql(query)
            assertExecution(df, native = true)
            val error = intercept[Exception](df.collect())
            assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists {
              cause =>
                Option(cause.getMessage).exists {
                  text =>
                    val lower = text.toLowerCase(java.util.Locale.ROOT)
                    lower.contains("zero") || lower.contains("divide_by_zero")
                }
            })
          }
      }
    }
  }

  testGluten("pmod respects Spark numeric coercion") {
    val schema = new StructType().add("value", IntegerType).add("divisor", IntegerType)
    withInput(schema, Seq(Row(-10, 3), Row(10, -3), Row(null, 3))) {
      Seq("false", "true").foreach {
        ansi =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            checkPmod(
              "SELECT pmod(CAST(value AS TINYINT), CAST(divisor AS BIGINT)), " +
                "pmod(CAST(value AS FLOAT), CAST(divisor AS DOUBLE)), " +
                "pmod(CAST(value AS SMALLINT), divisor) FROM pmod_input")
          }
      }
    }
  }

  testGluten("pmod keeps its captured mode after analysis and physical planning") {
    val schema = new StructType().add("value", IntegerType)
    withInput(schema, Seq(Row(10))) {
      for (planFirst <- Seq(false, true); ansi <- Seq(false, true)) {
        val df = withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val result = sql("SELECT pmod(value, 0) FROM pmod_input")
          result.queryExecution.analyzed
          if (planFirst) assertExecution(result, native = true)
          result
        }
        withSQLConf(SQLConf.ANSI_ENABLED.key -> (!ansi).toString) {
          assertExecution(df, native = true)
          if (ansi) intercept[Exception](df.collect())
          else checkAnswer(df, Seq(Row(null)))
        }
      }
    }
  }

  testGluten("pmod evaluates a null or legacy zero divisor before a failing dividend") {
    val schema = new StructType().add("divisor", IntegerType)
    withInput(schema, Seq(Row(0), Row(null))) {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          s"${ConstantFolding.ruleName},${NullPropagation.ruleName}") {
        checkPmod(
          "SELECT pmod(CAST(raise_error('left operand evaluated') AS INT), divisor) " +
            "FROM pmod_input")
      }
    }
  }

  testGluten("pmod legacy nonnullable zero skips a failing dividend") {
    val schema = new StructType().add("value", IntegerType)
    withInput(schema, Seq(Row(1))) {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          s"${ConstantFolding.ruleName},${NullPropagation.ruleName}") {
        for (dataType <- Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE")) {
          val query = s"SELECT pmod(coalesce(CAST(raise_error(concat('left ', " +
            s"CAST(value AS STRING))) AS $dataType), CAST(0 AS $dataType)), " +
            s"CAST(0 AS $dataType)) FROM pmod_input"
          val expressions = sql(query).queryExecution.optimizedPlan.expressions.flatMap(
            _.collect { case expression: Pmod => expression })
          assert(expressions.nonEmpty)
          assert(expressions.forall(pmod => !pmod.left.nullable && !pmod.right.nullable))
          checkPmod(query)
        }
      }
    }
  }

  testGluten("pmod null dividend suppresses ANSI zero-divisor errors") {
    val schema = new StructType().add("value", IntegerType).add("divisor", IntegerType)
    withInput(schema, Seq(Row(null, 0), Row(null, null))) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkPmod("SELECT pmod(value, divisor) FROM pmod_input")
      }
    }
  }

  testGluten("pmod ambiguous nonnullable ANSI composition retains Spark error ordering") {
    val schema = new StructType().add("value", IntegerType)
    withInput(schema, Seq(Row(1))) {
      for (factoryMode <- Seq("CODEGEN_ONLY", "NO_CODEGEN")) {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.CODEGEN_FACTORY_MODE.key -> factoryMode,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val query = "SELECT pmod(coalesce(CAST(raise_error(concat('left ', " +
            "CAST(value AS STRING))) AS INT), 0), 0) FROM pmod_input"
          val expected = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
            intercept[Exception](sql(query).collect())
          }
          val df = sql(query)
          assertExecution(df, native = false)
          val actual = intercept[Exception](df.collect())
          def condition(error: Throwable): String =
            Iterator.iterate(error)(_.getCause).takeWhile(_ != null).flatMap {
              case sparkError: org.apache.spark.SparkThrowable => Option(sparkError.getCondition)
              case _ => None
            }.toSeq.lastOption.getOrElse(fail(s"No Spark error condition in $error"))
          val expectedCondition = if (factoryMode == "NO_CODEGEN") {
            "USER_RAISED_EXCEPTION"
          } else {
            "REMAINDER_BY_ZERO"
          }
          assert(condition(expected) == expectedCondition)
          assert(condition(actual) == condition(expected))
        }
      }
    }
  }

  testGluten("pmod decimal inputs remain outside this native contribution") {
    val schema = new StructType().add("value", DecimalType(19, 2))
    withInput(schema, Seq(Row(new java.math.BigDecimal("-10.25")), Row(null))) {
      checkPmod(
        "SELECT pmod(value, CAST(3 AS DECIMAL(19,2))) FROM pmod_input",
        native = false)
    }
  }

  testGluten("pmod capability remains mandatory with general native validation disabled") {
    val schema = new StructType().add("value", DoubleType).add("divisor", DoubleType)
    withInput(schema, Seq(Row(-0.0d, 3.0d), Row(0.0d, Double.PositiveInfinity), Row(null, 0.0d))) {
      for (ansi <- Seq("false", "true")) {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansi,
          GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false") {
          val expression = new Pmod(
            BoundReference(0, DoubleType, nullable = true),
            BoundReference(1, DoubleType, nullable = true))
          val available = new VeloxValidatorApi().doExprValidate("pmod", expression)
          checkPmod("SELECT pmod(value, divisor) FROM pmod_input", native = available)
        }
      }
    }
  }
}
