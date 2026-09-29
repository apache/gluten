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

import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution.ReusedSubqueryExec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

class GlutenClickHouseTPCDSParquetAQESuite
  extends GlutenClickHouseTPCDSAbstractSuite
  with AdaptiveSparkPlanHelper {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
  }

  executeTPCDSTest(true)

  test("test 'select count(*)'") {
    val result = runSql("""
                          |select count(c_customer_sk) from customer
                          |""".stripMargin) { _ => }
    assertResult(100000L)(result.head.getLong(0))
  }

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
}
