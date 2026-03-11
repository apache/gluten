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
package org.apache.spark.sql.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan

class GlutenCollapseProjectExecTransformerSuite extends GlutenSQLTestsTrait {

  import testImplicits._

  testGluten("Support ProjectExecTransformer collapse") {
    val query =
      """
        |SELECT
        |  o_orderpriority
        |FROM
        |  orders
        |WHERE
        |  o_shippriority >= 0
        |  AND EXISTS (
        |    SELECT
        |      *
        |    FROM
        |      lineitem
        |    WHERE
        |      l_orderkey = o_orderkey
        |      AND l_linenumber < 10
        |  )
        |ORDER BY
        | o_orderpriority
        |LIMIT
        | 100;
        |""".stripMargin

    val ordersData = Seq[(Int, Int, String)](
      (30340, 1, "3-MEDIUM"),
      (31140, 1, "1-URGENT"),
      (31940, 1, "2-HIGH"),
      (32740, 1, "3-MEDIUM"),
      (33540, 1, "5-LOW"),
      (34340, 1, "2-HIGH"),
      (35140, 1, "3-MEDIUM"),
      (35940, 1, "1-URGENT"),
      (36740, 1, "3-MEDIUM"),
      (37540, 1, "4-NOT SPECIFIED")
    )
    val lineitemData = Seq[(Int, Int, String)](
      (30340, 1, "F"),
      (31140, 4, "F"),
      (31940, 7, "O"),
      (32740, 6, "O"),
      (33540, 2, "F"),
      (34340, 3, "F"),
      (35140, 1, "O"),
      (35940, 2, "F"),
      (36740, 3, "F"),
      (37540, 5, "O")
    )
    withTable("orders", "lineitem") {
      ordersData
        .toDF("o_orderkey", "o_shippriority", "o_orderpriority")
        .write
        .format("parquet")
        .saveAsTable("orders")
      lineitemData
        .toDF("l_orderkey", "l_linenumber", "l_linestatus")
        .write
        .format("parquet")
        .saveAsTable("lineitem")
      Seq(true, false).foreach {
        collapsed =>
          withSQLConf(
            GlutenConfig.ENABLE_COLUMNAR_PROJECT_COLLAPSE.key -> collapsed.toString,
            "spark.sql.autoBroadcastJoinThreshold" -> "-1") {
            val df = sql(query)
            checkAnswer(
              df,
              Seq(
                Row("1-URGENT"),
                Row("1-URGENT"),
                Row("2-HIGH"),
                Row("2-HIGH"),
                Row("3-MEDIUM"),
                Row("3-MEDIUM"),
                Row("3-MEDIUM"),
                Row("3-MEDIUM"),
                Row("4-NOT SPECIFIED"),
                Row("5-LOW")
              )
            )
            assert(
              getExecutedPlan(df).exists {
                case _ @ProjectExecTransformer(_, _: ProjectExecTransformer) => true
                case _ => false
              } == !collapsed
            )
            if (collapsed) {
              val projectPlan = getExecutedPlan(df).collect {
                case plan: ProjectExecTransformer => plan
              }.head
              assert(projectPlan.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isDefined)
            }
          }
      }
    }
  }
}
