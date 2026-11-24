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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale

class GlutenFailOnFallbackSuite extends WholeStageTransformerSuite {

  protected val resourcePath: String = null
  protected val fileFormat: String = null

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf.set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    }
    conf
  }

  private def withScalarSubqueryTable(f: => Unit): Unit = {
    import testImplicits._
    withTable("scalar_subquery_t") {
      Seq(
        ("a", 1L, "1"),
        ("a", 2L, "2"),
        ("b", 3L, "2")
      ).toDF("group_key", "value", "epoch")
        .write
        .format("parquet")
        .saveAsTable("scalar_subquery_t")
      f
    }
  }

  private def findGlutenException(t: Throwable): Option[GlutenException] = t match {
    case e: GlutenException => Some(e)
    case _ if t.getCause != null => findGlutenException(t.getCause)
    case _ => None
  }

  test("failOnFallback throws only when a fallback is present") {
    withTable("t") {
      spark.range(10).write.format("parquet").saveAsTable("t")

      // failOnFallback explicitly disabled: a fallback must NOT throw.
      withSQLConf(
        GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "false",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        sql("SELECT * FROM t").collect()
      }

      // failOnFallback enabled, fully native query (no fallback): must NOT throw.
      withSQLConf(GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true") {
        sql("SELECT * FROM t").collect()
      }

      // failOnFallback enabled, fallback present: must throw with the config key
      // and the offending operator/reason in the message.
      withSQLConf(
        GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        val ex = intercept[GlutenException] {
          sql("SELECT * FROM t").collect()
        }
        assert(ex.getMessage.contains(GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key))
        assert(ex.getMessage.contains("fell back to Spark"))
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("scan"))
      }
    }
  }

  test("failOnFallback allows supported AQE scalar subqueries") {
    withScalarSubqueryTable {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true") {
        val filtered = sql(
          """
            |SELECT group_key, value
            |FROM scalar_subquery_t
            |WHERE epoch = (SELECT MAX(epoch) FROM scalar_subquery_t)
            |ORDER BY group_key, value
            |""".stripMargin)
        assert(filtered.collect().toSeq == Seq(Row("a", 2L), Row("b", 3L)))

        val grouped = sql(
          """
            |WITH max_value AS (
            |  SELECT COALESCE(MAX(value), 0) AS value FROM scalar_subquery_t
            |)
            |SELECT group_key, scalar_value, AVG(value)
            |FROM (
            |  SELECT group_key, value, (SELECT value FROM max_value) AS scalar_value
            |  FROM scalar_subquery_t
            |)
            |GROUP BY group_key, scalar_value
            |ORDER BY group_key
            |""".stripMargin)
        assert(grouped.collect().toSeq == Seq(Row("a", 3L, 1.5), Row("b", 3L, 3.0)))

        Seq(filtered, grouped).foreach {
          df =>
            val subqueries = subqueriesAll(df.queryExecution.executedPlan)
            assert(subqueries.nonEmpty, df.queryExecution)
            assert(
              subqueries.exists(
                find(_)(_.isInstanceOf[BasicScanExecTransformer]).isDefined),
              df.queryExecution)
            assert(
              subqueries.exists(
                find(_)(_.isInstanceOf[HashAggregateExecBaseTransformer]).isDefined),
              df.queryExecution)
        }
      }
    }
  }

  test("failOnFallback still rejects fallbacks inside AQE scalar subqueries") {
    withScalarSubqueryTable {
      Seq(
        ("abs", "SELECT MAX(ABS(value)) FROM scalar_subquery_t"),
        ("coalesce", "SELECT COALESCE(MAX(value), 0) FROM scalar_subquery_t")
      ).foreach {
        case (blacklistedExpression, subquery) =>
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true",
            GlutenConfig.ENABLE_COLUMNAR_PARTIAL_PROJECT.key -> "false",
            GlutenConfig.EXPRESSION_BLACK_LIST.key -> blacklistedExpression
          ) {
            val thrown = intercept[Throwable] {
              sql(s"SELECT value FROM scalar_subquery_t WHERE value = ($subquery)").collect()
            }
            // Spark may wrap GlutenException when an asynchronous subquery fails.
            val glutenException = findGlutenException(thrown).getOrElse {
              fail(s"Expected GlutenException in cause chain, got: $thrown")
            }
            assert(glutenException.getMessage.contains(GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key))
            // Structural subquery wrappers must not be reported as fallback operators.
            assert(!glutenException.getMessage.split("\n").exists(_.trim.startsWith("Subquery:")))
            assert(
              glutenException.getMessage.toLowerCase(Locale.ROOT).contains(blacklistedExpression))
          }
      }
    }
  }
}
