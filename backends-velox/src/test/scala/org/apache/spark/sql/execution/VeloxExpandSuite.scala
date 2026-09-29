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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.events.GlutenPlanFallbackEvent
import org.apache.gluten.execution.{ExpandExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.Date

import scala.collection.mutable.ArrayBuffer

class VeloxExpandSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "true")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      // The gluten ui event test suite expects the spark ui to be enable
      .set(UI_ENABLED, true)
  }

  private def assertContainsNativeExpand(df: DataFrame): Unit = {
    assert(
      getExecutedPlan(df).exists(_.isInstanceOf[ExpandExecTransformer]),
      s"Expected ExpandExecTransformer in plan, got:\n${df.queryExecution.executedPlan}"
    )
  }

  test("Expand with duplicated group keys") {
    withTable("t1") {
      val events = new ArrayBuffer[GlutenPlanFallbackEvent]
      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: GlutenPlanFallbackEvent => events.append(e)
            case _ =>
          }
        }
      }
      spark.sparkContext.addSparkListener(listener)

      spark.sql("""
                  |create table t1 (
                  |id int,
                  |status int,
                  |a int,
                  |b int,
                  |dt string)
                  |using parquet
                  |""".stripMargin)
      try {
        val df = spark.sql("""
                             |select dt as d1, id, dt as d2,
                             |count(distinct case when status = 1 then a end) as a_cnt,
                             |count(distinct case when status = 1 then b end) as b_cnt
                             |from t1
                             |group by dt, id, dt
                             |""".stripMargin)
        df.collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
        assert(
          !events.exists(
            _.fallbackNodeToReason.values.toSet.exists(_.contains("Failed to bind reference for"))))
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }
  }

  test("Expand with round(avg(decimal)) and multiple distinct aggregates") {
    withTempPath {
      pendingPath =>
        withTempPath {
          verifiedPath =>
            withTempView("pending_events", "verified_events") {
              spark
                .sql("""
                       |SELECT * FROM VALUES
                       |  (1L, DATE'2026-04-22', 'A24', 0L),
                       |  (2L, DATE'2026-04-22', 'A24', 0L)
                       |AS pending_events(order_id, pending_date, pending_reason, pending_timestamp)
                       |""".stripMargin)
                .write
                .mode("overwrite")
                .parquet(pendingPath.getCanonicalPath)

              spark
                .sql("""
                       |SELECT * FROM VALUES
                       |  (1L, 90000L),
                       |  (2L, 180000L)
                       |AS verified_events(order_id, verified_timestamp)
                       |""".stripMargin)
                .write
                .mode("overwrite")
                .parquet(verifiedPath.getCanonicalPath)

              spark.read
                .parquet(pendingPath.getCanonicalPath)
                .createOrReplaceTempView("pending_events")
              spark.read
                .parquet(verifiedPath.getCanonicalPath)
                .createOrReplaceTempView("verified_events")

              val df = spark.sql(
                """
                  |WITH sla_calc AS (
                  |  SELECT
                  |    p.pending_date,
                  |    p.pending_reason,
                  |    p.order_id,
                  |    round(
                  |      cast((v.verified_timestamp - p.pending_timestamp) as decimal(38, 18)) /
                  |        3600.000000000000000000,
                  |      1) AS sla_hours
                  |  FROM pending_events p
                  |  JOIN verified_events v
                  |    ON p.order_id = v.order_id
                  |)
                  |SELECT
                  |  pending_date,
                  |  pending_reason,
                  |  COUNT(DISTINCT order_id) AS total_order,
                  |  round(AVG(sla_hours), 1) AS avg_sla_hours,
                  |  COUNT(DISTINCT CASE WHEN sla_hours > 24 THEN order_id END) AS backlog_24,
                  |  COUNT(DISTINCT CASE WHEN sla_hours > 48 THEN order_id END) AS backlog_48
                  |FROM sla_calc
                  |GROUP BY pending_date, pending_reason
                  |""".stripMargin)

              checkAnswer(
                df,
                Row(Date.valueOf("2026-04-22"), "A24", 2L, BigDecimal("37.5"), 2L, 1L))
              assertContainsNativeExpand(df)
            }
        }
    }
  }

  test("Expand with decimal case-when sum and multiple distinct aggregates") {
    withTempPath {
      eventsPath =>
        withTempView("smart_events") {
          spark
            .sql("""
                   |SELECT * FROM VALUES
                   |  (1, 101L, 1001L, 1, 0, 1,
                   |    CAST(1.1000000000 AS DECIMAL(25, 10)),
                   |    CAST(2.2000000000 AS DECIMAL(25, 10)),
                   |    CAST(3.3000000000 AS DECIMAL(25, 10))),
                   |  (1, 102L, 1002L, 0, 1, 0,
                   |    CAST(4.4000000000 AS DECIMAL(25, 10)),
                   |    CAST(5.5000000000 AS DECIMAL(25, 10)),
                   |    CAST(6.6000000000 AS DECIMAL(25, 10)))
                   |AS smart_events(
                   |  campaign_id,
                   |  order_id,
                   |  checkout_id,
                   |  has_dd,
                   |  has_ccb,
                   |  has_fsv,
                   |  dd_cost_usd,
                   |  ccb_cost_usd,
                   |  fsv_cost_usd)
                   |""".stripMargin)
            .write
            .mode("overwrite")
            .parquet(eventsPath.getCanonicalPath)

          spark.read.parquet(eventsPath.getCanonicalPath).createOrReplaceTempView("smart_events")

          val df =
            spark.sql("""
                        |SELECT
                        |  campaign_id,
                        |  COUNT(DISTINCT order_id) AS total_order,
                        |  COUNT(DISTINCT CASE WHEN has_dd = 1 THEN order_id END) AS dd_order,
                        |  COUNT(DISTINCT checkout_id) AS checkout_count,
                        |  SUM(
                        |    CASE WHEN has_dd = 1 THEN dd_cost_usd ELSE 0 END +
                        |    CASE WHEN has_ccb = 1 THEN ccb_cost_usd ELSE 0 END +
                        |    CASE WHEN has_fsv = 1 THEN fsv_cost_usd ELSE 0 END
                        |  ) AS smart_voucher_cost_usd
                        |FROM smart_events
                        |GROUP BY campaign_id
                        |""".stripMargin)

          checkAnswer(df, Row(1, 2L, 1L, 2L, BigDecimal("9.9000000000")))
          assertContainsNativeExpand(df)
        }
    }
  }
}
