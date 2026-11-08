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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.SortExecTransformer

import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait}
import org.apache.spark.sql.internal.SQLConf

class GlutenPushDownLocalSortSuite extends PushDownLocalSortSuite with GlutenSQLTestsTrait {

  // Gluten offloads local sorts to SortExecTransformer, so count both sort operators.
  private def checkNumSorts(df: DataFrame, count: Int): Unit = {
    df.collect()
    val sorts = collectWithSubqueries(df.queryExecution.executedPlan) {
      case s: SortExec => s
      case s: SortExecTransformer => s
    }
    assert(sorts.length == count, df.queryExecution.executedPlan)
  }

  private def checkSorts(query: String, enabledCount: Int, disabledCount: Int): Unit = {
    withSQLConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true") {
      val df = sql(query)
      checkNumSorts(df, enabledCount)
      val result = df.collect()
      withSQLConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "false") {
        val df = sql(query)
        checkNumSorts(df, disabledCount)
        checkAnswer(df, result)
      }
    }
  }

  private def withTable3(numRows: Int, cMod: Option[Int])(f: => Unit): Unit = {
    withTempView("t") {
      val c = cMod.map(m => s"id % $m as c").getOrElse("id as c")
      spark.range(numRows).selectExpr("id % 10 as a", "id % 7 as b", c).createOrReplaceTempView("t")
      f
    }
  }

  testGluten(
    "Push a wider local sort down across stacked windows with prefix-compatible order specs") {
    withTable3(100, None) {
      checkSorts(
        """
          |SELECT a, b, c,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS rn
          |FROM t
          |""".stripMargin,
        1,
        2
      )
    }
  }

  testGluten("No-op when the wider sort is already below the narrower one") {
    withTable3(100, None) {
      checkSorts(
        """
          |SELECT a, b, c,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS rn,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk
          |FROM t
          |""".stripMargin,
        1,
        1
      )
    }
  }

  testGluten("Push-down still applies and stays correct with a filter on a window output") {
    withTable3(200, None) {
      checkSorts(
        """
          |SELECT * FROM (
          |  SELECT a, b, c,
          |    RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS rn
          |  FROM t
          |) WHERE rn > 1
          |""".stripMargin,
        1,
        2
      )
    }
  }

  testGluten("Push a wider sort down through a renaming project by rewriting the ordering") {
    withTable3(200, Some(5)) {
      checkSorts(
        """
          |SELECT a, bb, c, rn1,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY bb, c) AS rn2
          |FROM (
          |  SELECT a, b AS bb, c, rn1 FROM (
          |    SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS rn1 FROM t
          |  )
          |)
          |""".stripMargin,
        1,
        2
      )
    }
  }

  testGluten("Negative: no push-down when the two orderings are disjoint") {
    withTable3(300, Some(5)) {
      checkSorts(
        """
          |SELECT a, b, c,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  RANK() OVER (PARTITION BY a ORDER BY c) AS rc
          |FROM t
          |""".stripMargin,
        2,
        2
      )
    }
  }

  testGluten("Negative: no push-down when a shuffle separates the two sorts") {
    withTable3(300, Some(5)) {
      checkSorts(
        """
          |SELECT a, b,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  RANK() OVER (PARTITION BY b ORDER BY a) AS rb
          |FROM t
          |""".stripMargin,
        2,
        2
      )
    }
  }

  testGluten("Negative: no push-down when the sort directions differ") {
    withTable3(300, Some(5)) {
      checkSorts(
        """
          |SELECT a, b, c,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC, c) AS rn
          |FROM t
          |""".stripMargin,
        2,
        2
      )
    }
  }

  testGluten("Negative: no push-down when an intermediate project computes the ordering column") {
    withTable3(300, Some(5)) {
      checkSorts(
        """
          |SELECT a, bx, c, rn1,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY bx, c) AS rn2
          |FROM (
          |  SELECT a, b + 1 AS bx, c, rn1 FROM (
          |    SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS rn1 FROM t
          |  )
          |)
          |""".stripMargin,
        2,
        2
      )
    }
  }
}
