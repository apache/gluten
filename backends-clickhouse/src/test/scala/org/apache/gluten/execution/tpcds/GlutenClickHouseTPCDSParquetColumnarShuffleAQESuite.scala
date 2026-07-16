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
package org.apache.gluten.execution.tpcds

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.execution.{ColumnarSubqueryBroadcastExec, ReusedSubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

class GlutenClickHouseTPCDSParquetColumnarShuffleAQESuite
  extends GlutenClickHouseTPCDSAbstractSuite
  with AdaptiveSparkPlanHelper {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
  }

  executeTPCDSTest(true)

  test("test reading from partitioned table") {
    val result = runSql("""
                          |select count(*)
                          |  from store_sales
                          |  where ss_quantity between 1 and 20
                          |""".stripMargin) { _ => }
    assertResult(550458L)(result.head.getLong(0))
  }

  test("test reading from partitioned table with partition column filter") {
    compareResultsAgainstVanillaSpark(
      """
        |select avg(ss_net_paid_inc_tax)
        |  from store_sales
        |  where ss_quantity between 1 and 20
        |  and ss_sold_date_sk = 2452635
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("test select avg(int), avg(long)") {
    val result = runSql("""
                          |select avg(cs_item_sk), avg(cs_order_number)
                          |  from catalog_sales
                          |""".stripMargin) { _ => }
    assertResult(8998.463336886734)(result.head.getDouble(0))
    assertResult(80037.12727449503)(result.head.getDouble(1))
  }

  test("Gluten-1235: Fix missing reading from the broadcasted value when executing DPP") {
    val testSql =
      """
        |select dt.d_year
        |       ,sum(ss_ext_sales_price) sum_agg
        | from  date_dim dt
        |      ,store_sales
        | where dt.d_date_sk = store_sales.ss_sold_date_sk
        |   and dt.d_moy=12
        | group by dt.d_year
        | order by dt.d_year
        |         ,sum_agg desc
        |  LIMIT 100 ;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      testSql,
      compareResult = true,
      df => {
        val foundDynamicPruningExpr = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(foundDynamicPruningExpr.size == 2)
        assert(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .partitionFilters
            .exists(_.isInstanceOf[DynamicPruningExpression]))
        assertResult(1823)(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .selectedPartitions
            .length)
      }
    )
  }

  test("TPCDS Q9 - ReusedSubquery check") {
    runTPCDSQuery("q9") {
      df =>
        val subqueryAdaptiveSparkPlan = collectWithSubqueries(df.queryExecution.executedPlan) {
          case a: AdaptiveSparkPlanExec if a.isSubquery => true
          case r: ReusedSubqueryExec => true
          case _ => false
        }
        // 5 AdaptiveSparkPlanExec and 10 ReusedSubqueryExec on Spark 3.3+.
        assertResult(15)(subqueryAdaptiveSparkPlan.count(_ == true))
    }
  }

  test("GLUTEN-1848: Fix execute subquery repeatedly issue with ReusedSubquery") {
    runTPCDSQuery("q5") {
      df =>
        val subqueriesId = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: ColumnarSubqueryBroadcastExec => s.id
          case r: ReusedSubqueryExec => r.child.id
        }
        assert(subqueriesId.distinct.size == 1)
    }
  }

  test("TPCDS Q21 - DPP check") {
    runTPCDSQuery("q21") {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val foundDynamicPruningExpr = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer if f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              } =>
            f
        }
        assert(foundDynamicPruningExpr.nonEmpty)

        val reusedExchangeExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }
        assert(reusedExchangeExec.nonEmpty)
    }
  }

  test("TPCDS Q21 with DPP + SHJ") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")) {
      runTPCDSQuery("q21") {
        df =>
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val foundDynamicPruningExpr = collect(df.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer if f.partitionFilters.exists {
                  case _: DynamicPruningExpression => true
                  case _ => false
                } =>
              f
          }
          assert(foundDynamicPruningExpr.nonEmpty)

          val reusedExchangeExec = collectWithSubqueries(df.queryExecution.executedPlan) {
            case r: ReusedExchangeExec => r
          }
          assert(reusedExchangeExec.isEmpty)
      }
    }
  }

  test("Gluten-1234: Fix error when executing hash agg after union all") {
    val testSql =
      """
        |select channel, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        |  SELECT 'store' as channel, ss_ext_sales_price ext_sales_price
        |  FROM store_sales, item, date_dim
        |  WHERE ss_addr_sk IS NULL
        |    AND ss_sold_date_sk=d_date_sk
        |    AND ss_item_sk=i_item_sk
        |  UNION ALL
        |  SELECT 'web' as channel, ws_ext_sales_price ext_sales_price
        |  FROM web_sales, item, date_dim
        |  WHERE ws_web_page_sk IS NULL
        |    AND ws_sold_date_sk=d_date_sk
        |    AND ws_item_sk=i_item_sk
        |  ) foo
        |GROUP BY channel
        |ORDER BY channel
        | LIMIT 100 ;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(testSql, compareResult = true, df => {})
  }

  test("GLUTEN-1620: fix 'attribute binding failed.' when executing hash agg with aqe") {
    val sql =
      """
        |select
        |cs_ship_mode_sk,
        |   count(distinct cs_order_number) as `order count`
        |  ,avg(cs_ext_ship_cost) as `avg shipping cost`
        |  ,sum(cs_ext_ship_cost) as `total shipping cost`
        |  ,sum(cs_net_profit) as `total net profit`
        |from
        |   catalog_sales cs1
        |  ,date_dim
        |  ,customer_address
        |  ,call_center
        |where
        |    d_date between '1999-5-01' and
        |           (cast('1999-5-01' as date) + interval '60' day)
        |and cs1.cs_ship_date_sk = d_date_sk
        |and cs1.cs_ship_addr_sk = ca_address_sk
        |and ca_state = 'OH'
        |and cs1.cs_call_center_sk = cc_call_center_sk
        |and cc_county in ('Ziebach County','Williamson County','Walker County','Williamson County',
        |                  'Ziebach County'
        |)
        |and exists (select *
        |            from catalog_sales cs2
        |            where cs1.cs_order_number = cs2.cs_order_number
        |              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
        |and not exists(select *
        |               from catalog_returns cr1
        |               where cs1.cs_order_number = cr1.cr_order_number)
        |group by cs_ship_mode_sk
        |order by cs_ship_mode_sk, count(distinct cs_order_number)
        | LIMIT 100 ;
        |""".stripMargin
    // There are some BroadcastHashJoin with NOT condition
    compareResultsAgainstVanillaSpark(sql, true, { df => })
  }

  test("GLUTEN-7358: Optimize the strategy of the partition split according to the files count") {
    Seq(("-1", 8), ("100", 8), ("2000", 1)).foreach(
      conf => {
        withSQLConf(
          (CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD -> conf._1)) {
          val sql =
            s"""
               |select count(1) from store_sales
               |""".stripMargin
          compareResultsAgainstVanillaSpark(
            sql,
            true,
            {
              df =>
                val scanExec = collect(df.queryExecution.executedPlan) {
                  case f: FileSourceScanExecTransformer => f
                }
                assert(scanExec(0).getPartitions.size == conf._2)
            }
          )
        }
      })
  }

  test("GLUTEN-7971: Support using left side as the build table for the left anti/semi join") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      (CHBackendSettings.GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT, "true")) {
      val sql1 =
        s"""
           |select
           |  cd_gender,
           |  cd_marital_status,
           |  cd_education_status,
           |  count(*) cnt1
           | from
           |  customer c,customer_address ca,customer_demographics
           | where
           |  c.c_current_addr_sk = ca.ca_address_sk and
           |  ca_county in ('Walker County','Richland County','Gaines County','Douglas County')
           |  and cd_demo_sk = c.c_current_cdemo_sk and
           |  exists (select *
           |          from store_sales
           |          where c.c_customer_sk = ss_customer_sk)
           | group by cd_gender,
           |          cd_marital_status,
           |          cd_education_status
           | order by cd_gender,
           |          cd_marital_status,
           |          cd_education_status
           | LIMIT 100 ;
           |""".stripMargin
      runQueryAndCompare(sql1)(
        df => {
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val shuffledHashJoinExecs = collect(df.queryExecution.executedPlan) {
            case h: CHShuffledHashJoinExecTransformer if h.joinType == LeftSemi => h
          }
          assertResult(1)(shuffledHashJoinExecs.size)
          assertResult(BuildLeft)(shuffledHashJoinExecs(0).buildSide)
        })

      val sql2 =
        s"""
           |select
           |  cd_gender,
           |  cd_marital_status,
           |  cd_education_status,
           |  count(*) cnt1
           | from
           |  customer c,customer_address ca,customer_demographics
           | where
           |  c.c_current_addr_sk = ca.ca_address_sk and
           |  ca_county in ('Walker County','Richland County','Gaines County','Douglas County')
           |  and cd_demo_sk = c.c_current_cdemo_sk and
           |  not exists (select *
           |          from store_sales
           |          where c.c_customer_sk = ss_customer_sk)
           | group by cd_gender,
           |          cd_marital_status,
           |          cd_education_status
           | order by cd_gender,
           |          cd_marital_status,
           |          cd_education_status
           | LIMIT 100 ;
           |""".stripMargin
      runQueryAndCompare(sql2)(
        df => {
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val shuffledHashJoinExecs = collect(df.queryExecution.executedPlan) {
            case h: CHShuffledHashJoinExecTransformer if h.joinType == LeftAnti => h
          }
          assertResult(1)(shuffledHashJoinExecs.size)
          assertResult(BuildLeft)(shuffledHashJoinExecs(0).buildSide)
        })
    }
  }
}
