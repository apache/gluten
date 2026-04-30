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
import org.apache.gluten.extension.joinagg.{ImplementJoinAggregate, PushAggregateThroughJoinBatch}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class VanillaJoinAggregateLogicalOnlyExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(PushAggregateThroughJoinBatch.apply)
  }
}

class VanillaStarSchemaJoinAggregateLogicalOnlySuite extends StarSchemaJoinAggregateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ENABLED.key, "false")
      .set("spark.shuffle.manager", "sort")
      .set(
        "spark.sql.extensions",
        classOf[VanillaJoinAggregateLogicalOnlyExtensions].getCanonicalName)
  }

  override protected def checkPlan(df: DataFrame): Unit = {}
}

class VanillaJoinAggregateExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(PushAggregateThroughJoinBatch.apply)
    extensions.injectPlannerStrategy(ImplementJoinAggregate.apply)
  }
}

class VanillaStarSchemaJoinAggregateSuite extends StarSchemaJoinAggregateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ENABLED.key, "false")
      .set("spark.shuffle.manager", "sort")
      .set("spark.sql.extensions", classOf[VanillaJoinAggregateExtensions].getCanonicalName)
  }

  override protected def checkPlan(df: DataFrame): Unit = {}
}

class StarSchemaJoinAggregateSingleDepthSuite extends StarSchemaJoinAggregateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key, "1")
  }
}

class StarSchemaJoinAggregateSuite extends VeloxTPCHTableSupport with AdaptiveSparkPlanHelper {
  private val factMeasureColumnNames = Set(
    "sales_price",
    "return_amt",
    "profit",
    "net_loss",
    "ss_sales_price",
    "ss_net_profit",
    "sr_return_amt",
    "sr_net_loss",
    "cs_ext_sales_price",
    "cs_net_profit",
    "cr_return_amount",
    "cr_net_loss",
    "ws_ext_sales_price",
    "ws_net_profit",
    "wr_return_amt",
    "wr_net_loss"
  )

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true")
      .set(GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key, "true")
      .set(GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key, s"${Int.MaxValue}")
      .set("spark.sql.adaptive.enabled", "false")
  }

  private val tpcdsTempTables = Seq(
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
    "date_dim",
    "store",
    "catalog_page",
    "customer_address",
    "call_center",
    "web_site",
    "warehouse",
    "ship_mode",
    "item",
    "promotion",
    "customer_demographics"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCDSMiniTables()
  }

  override def afterAll(): Unit = {
    tpcdsTempTables.foreach {
      t =>
        if (spark.catalog.tableExists(t)) {
          spark.catalog.dropTempView(t)
        }
    }
    super.afterAll()
  }

  private def createTPCDSMiniTables(): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMP VIEW date_dim AS
        |SELECT CAST(d_date_sk AS BIGINT) AS d_date_sk,
        |       CAST(d_date AS DATE) AS d_date,
        |       CAST(d_year AS INT) AS d_year,
        |       CAST(d_moy AS INT) AS d_moy,
        |       CAST(d_month_seq AS INT) AS d_month_seq,
        |       CAST(d_week_seq AS INT) AS d_week_seq,
        |       CAST(d_quarter_name AS STRING) AS d_quarter_name,
        |       CAST(d_day_name AS STRING) AS d_day_name
        |FROM VALUES
        |  (1, DATE'1998-08-05', 1998, 8, 1100, 10, '1998Q1', 'Wednesday'),
        |  (2, DATE'1998-08-06', 1998, 8, 1100, 10, '1998Q1', 'Thursday'),
        |  (3, DATE'1998-08-07', 1998, 8, 1100, 10, '1998Q1', 'Friday'),
        |  (10, DATE'1999-02-10', 1999, 2, 1212, 40, '1999Q1', 'Wednesday'),
        |  (11, DATE'1999-03-01', 1999, 3, 1213, 43, '1999Q1', 'Monday'),
        |  (12, DATE'1999-04-01', 1999, 4, 1214, 48, '1999Q2', 'Thursday'),
        |  (13, DATE'1999-07-01', 1999, 7, 1217, 61, '1999Q3', 'Thursday'),
        |  (200, DATE'1999-12-15', 1999, 12, 1300, 70, '1999Q4', 'Wednesday'),
        |  (201, DATE'2000-01-15', 2000, 1, 1301, 71, '2000Q1', 'Saturday'),
        |  (202, DATE'2001-01-15', 2001, 1, 1452, 72, '2001Q1', 'Monday'),
        |  (1001, DATE'2001-01-07', 2001, 1, 1452, 1, '2001Q1', 'Sunday'),
        |  (1002, DATE'2001-01-08', 2001, 1, 1452, 1, '2001Q1', 'Monday'),
        |  (1054, DATE'2002-01-13', 2002, 1, 1464, 54, '2002Q1', 'Sunday'),
        |  (1055, DATE'2002-01-14', 2002, 1, 1464, 54, '2002Q1', 'Monday')
        |AS t(d_date_sk, d_date, d_year, d_moy, d_month_seq, d_week_seq, d_quarter_name, d_day_name)
        |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW store AS
                |SELECT CAST(s_store_sk AS BIGINT) AS s_store_sk,
                |       CAST(s_store_id AS STRING) AS s_store_id,
                |       CAST(s_store_name AS STRING) AS s_store_name,
                |       CAST(s_company_name AS STRING) AS s_company_name,
                |       CAST(s_state AS STRING) AS s_state
                |FROM VALUES
                |  (10, 'S10', 'Store 10', 'StoreCo', 'CA'),
                |  (20, 'S20', 'Store 20', 'StoreCo', 'WA')
                |AS t(s_store_sk, s_store_id, s_store_name, s_company_name, s_state)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW catalog_page AS
                |SELECT CAST(cp_catalog_page_sk AS BIGINT) AS cp_catalog_page_sk,
                |       CAST(cp_catalog_page_id AS STRING) AS cp_catalog_page_id
                |FROM VALUES
                |  (100, 'CP100'),
                |  (200, 'CP200')
                |AS t(cp_catalog_page_sk, cp_catalog_page_id)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW web_site AS
                |SELECT CAST(web_site_sk AS BIGINT) AS web_site_sk,
                |       CAST(web_site_id AS STRING) AS web_site_id,
                |       CAST(web_name AS STRING) AS web_name
                |FROM VALUES
                |  (1000, 'W1000', 'Web 1000'),
                |  (2000, 'W2000', 'Web 2000')
                |AS t(web_site_sk, web_site_id, web_name)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW warehouse AS
                |SELECT CAST(w_warehouse_sk AS BIGINT) AS w_warehouse_sk,
                |       CAST(w_warehouse_name AS STRING) AS w_warehouse_name
                |FROM VALUES
                |  (1, 'Warehouse 1'),
                |  (2, 'Warehouse 2')
                |AS t(w_warehouse_sk, w_warehouse_name)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW ship_mode AS
                |SELECT CAST(sm_ship_mode_sk AS BIGINT) AS sm_ship_mode_sk,
                |       CAST(sm_type AS STRING) AS sm_type
                |FROM VALUES
                |  (11, 'AIR'),
                |  (22, 'GROUND')
                |AS t(sm_ship_mode_sk, sm_type)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW store_sales AS
                |SELECT CAST(ss_store_sk AS BIGINT) AS ss_store_sk,
                |       CAST(ss_sold_date_sk AS BIGINT) AS ss_sold_date_sk,
                |       CAST(ss_ext_sales_price AS DECIMAL(7,2)) AS ss_ext_sales_price,
                |       CAST(ss_net_profit AS DECIMAL(7,2)) AS ss_net_profit,
                |       CAST(ss_item_sk AS BIGINT) AS ss_item_sk,
                |       CAST(ss_customer_sk AS BIGINT) AS ss_customer_sk,
                |       CAST(ss_ticket_number AS BIGINT) AS ss_ticket_number,
                |       CAST(ss_cdemo_sk AS BIGINT) AS ss_cdemo_sk,
                |       CAST(ss_promo_sk AS BIGINT) AS ss_promo_sk,
                |       CAST(ss_quantity AS DECIMAL(7,2)) AS ss_quantity,
                |       CAST(ss_list_price AS DECIMAL(7,2)) AS ss_list_price,
                |       CAST(ss_coupon_amt AS DECIMAL(7,2)) AS ss_coupon_amt,
                |       CAST(ss_sales_price AS DECIMAL(7,2)) AS ss_sales_price
                |FROM VALUES
                |  (10, 1, 11.00, 3.00, 1001, 5001, 7001, 2001, 3001, 1.00, 10.00, 0.50, 9.50),
                |  (20, 2, 12.00, 4.00, 1002, 5002, 7002, 2002, 3002, 2.00, 20.00, 1.00, 19.00),
                |  (10, 3, 13.00, 5.00, 1001, 5001, 7003, 2001, 3001, 3.00, 30.00, 1.50, 28.50),
                |  (10, 200, 14.00, 2.00, 1001, 5001, 7101, 2001, 3001, 1.00, 11.00, 0.30, 10.70),
                |  (10, 201, 20.00, 4.00, 1001, 5001, 7102, 2001, 3001, 2.00, 12.00, 0.40, 11.60),
                |  (10, 202, 10.00, 1.00, 1001, 5001, 7103, 2001, 3001, 3.00, 13.00, 0.20, 12.80)
                |AS t(
                |  ss_store_sk, ss_sold_date_sk, ss_ext_sales_price, ss_net_profit,
                |  ss_item_sk, ss_customer_sk, ss_ticket_number, ss_cdemo_sk, ss_promo_sk,
                |  ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW item AS
                |SELECT CAST(i_item_sk AS BIGINT) AS i_item_sk,
                |       CAST(i_item_id AS STRING) AS i_item_id,
                |       CAST(i_category AS STRING) AS i_category,
                |       CAST(i_brand AS STRING) AS i_brand,
                |       CAST(i_item_desc AS STRING) AS i_item_desc
                |FROM VALUES
                |  (1001, 'I1001', 'Category 1', 'Brand 1', 'Item 1001 desc'),
                |  (1002, 'I1002', 'Category 2', 'Brand 2', 'Item 1002 desc')
                |AS t(i_item_sk, i_item_id, i_category, i_brand, i_item_desc)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW promotion AS
                |SELECT CAST(p_promo_sk AS BIGINT) AS p_promo_sk,
                |       CAST(p_channel_email AS STRING) AS p_channel_email,
                |       CAST(p_channel_event AS STRING) AS p_channel_event
                |FROM VALUES
                |  (3001, 'N', 'Y'),
                |  (3002, 'Y', 'N')
                |AS t(p_promo_sk, p_channel_email, p_channel_event)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW customer_demographics AS
                |SELECT CAST(cd_demo_sk AS BIGINT) AS cd_demo_sk,
                |       CAST(cd_gender AS STRING) AS cd_gender,
                |       CAST(cd_marital_status AS STRING) AS cd_marital_status,
                |       CAST(cd_education_status AS STRING) AS cd_education_status
                |FROM VALUES
                |  (2001, 'F', 'W', 'Primary'),
                |  (2002, 'F', 'W', 'Primary')
                |AS t(cd_demo_sk, cd_gender, cd_marital_status, cd_education_status)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW store_returns AS
                |SELECT CAST(sr_store_sk AS BIGINT) AS sr_store_sk,
                |       CAST(sr_returned_date_sk AS BIGINT) AS sr_returned_date_sk,
                |       CAST(sr_customer_sk AS BIGINT) AS sr_customer_sk,
                |       CAST(sr_item_sk AS BIGINT) AS sr_item_sk,
                |       CAST(sr_ticket_number AS BIGINT) AS sr_ticket_number,
                |       CAST(sr_return_quantity AS DECIMAL(7,2)) AS sr_return_quantity,
                |       CAST(sr_return_amt AS DECIMAL(7,2)) AS sr_return_amt,
                |       CAST(sr_net_loss AS DECIMAL(7,2)) AS sr_net_loss
                |FROM VALUES
                |  (10, 1, 5001, 1001, 7001, 1.00, 1.00, 0.50),
                |  (20, 2, 5002, 1002, 7002, 1.00, 2.00, 0.25),
                |  (10, 3, 5001, 1001, 7003, 2.00, 0.50, 0.10)
                |AS t(
                |  sr_store_sk, sr_returned_date_sk, sr_customer_sk, sr_item_sk, sr_ticket_number,
                |  sr_return_quantity, sr_return_amt, sr_net_loss)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW catalog_sales AS
                |SELECT CAST(cs_catalog_page_sk AS BIGINT) AS cs_catalog_page_sk,
                |       CAST(cs_sold_date_sk AS BIGINT) AS cs_sold_date_sk,
                |       CAST(cs_ext_sales_price AS DECIMAL(7,2)) AS cs_ext_sales_price,
                |       CAST(cs_net_profit AS DECIMAL(7,2)) AS cs_net_profit,
                |       CAST(cs_order_number AS BIGINT) AS cs_order_number,
                |       CAST(cs_bill_customer_sk AS BIGINT) AS cs_bill_customer_sk,
                |       CAST(cs_item_sk AS BIGINT) AS cs_item_sk,
                |       CAST(cs_quantity AS DECIMAL(7,2)) AS cs_quantity,
                |       CAST(cs_ext_ship_cost AS DECIMAL(7,2)) AS cs_ext_ship_cost,
                |       CAST(cs_ship_date_sk AS BIGINT) AS cs_ship_date_sk,
                |       CAST(cs_ship_addr_sk AS BIGINT) AS cs_ship_addr_sk,
                |       CAST(cs_call_center_sk AS BIGINT) AS cs_call_center_sk,
                |       CAST(cs_warehouse_sk AS BIGINT) AS cs_warehouse_sk
                |FROM VALUES
                |  (100, 1, 21.00, 5.00, 4001, 5001, 1001, 2.00, 2.00, 1, 9001, 7001, 1),
                |  (200, 2, 22.00, 6.00, 4002, 5002, 1002, 1.00, 3.00, 2, 9002, 7002, 1),
                |  (100, 10, 30.00, 2.00, 5001, 5001, 1001, 1.00, 5.00, 10, 9001, 7001, 1),
                |  (200, 10, 35.00, 1.50, 5001, 5001, 1001, 1.00, 3.00, 10, 9001, 7001, 2),
                |  (100, 10, 40.00, 1.00, 5002, 5002, 1002, 2.00, 4.00, 10, 9001, 7001, 1),
                |  (100, 1001, 50.00, 3.00, 6001, 5001, 1001, 3.00, 2.00, 1001, 9001, 7001, 1),
                |  (100, 1054, 70.00, 4.00, 6002, 5001, 1001, 4.00, 2.00, 1054, 9001, 7001, 1)
                |AS t(
                |  cs_catalog_page_sk, cs_sold_date_sk, cs_ext_sales_price, cs_net_profit,
                |  cs_order_number, cs_bill_customer_sk, cs_item_sk, cs_quantity, cs_ext_ship_cost,
                |  cs_ship_date_sk, cs_ship_addr_sk, cs_call_center_sk, cs_warehouse_sk)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW catalog_returns AS
                |SELECT CAST(cr_catalog_page_sk AS BIGINT) AS cr_catalog_page_sk,
                |       CAST(cr_returned_date_sk AS BIGINT) AS cr_returned_date_sk,
                |       CAST(cr_return_amount AS DECIMAL(7,2)) AS cr_return_amount,
                |       CAST(cr_net_loss AS DECIMAL(7,2)) AS cr_net_loss,
                |       CAST(cr_order_number AS BIGINT) AS cr_order_number
                |FROM VALUES
                |  (100, 1, 1.50, 0.30, 3001),
                |  (200, 2, 0.50, 0.10, 3002),
                |  (100, 10, 1.00, 0.10, 5002)
                |AS t(
                |  cr_catalog_page_sk, cr_returned_date_sk, cr_return_amount, cr_net_loss,
                |  cr_order_number)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW customer_address AS
                |SELECT CAST(ca_address_sk AS BIGINT) AS ca_address_sk,
                |       CAST(ca_state AS STRING) AS ca_state
                |FROM VALUES
                |  (9001, 'IL'),
                |  (9002, 'CA')
                |AS t(ca_address_sk, ca_state)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW call_center AS
                |SELECT CAST(cc_call_center_sk AS BIGINT) AS cc_call_center_sk,
                |       CAST(cc_county AS STRING) AS cc_county
                |FROM VALUES
                |  (7001, 'Williamson County'),
                |  (7002, 'Other County')
                |AS t(cc_call_center_sk, cc_county)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW web_sales AS
                |SELECT CAST(ws_web_site_sk AS BIGINT) AS ws_web_site_sk,
                |       CAST(ws_sold_date_sk AS BIGINT) AS ws_sold_date_sk,
                |       CAST(ws_ship_date_sk AS BIGINT) AS ws_ship_date_sk,
                |       CAST(ws_warehouse_sk AS BIGINT) AS ws_warehouse_sk,
                |       CAST(ws_ship_mode_sk AS BIGINT) AS ws_ship_mode_sk,
                |       CAST(ws_ext_sales_price AS DECIMAL(7,2)) AS ws_ext_sales_price,
                |       CAST(ws_net_profit AS DECIMAL(7,2)) AS ws_net_profit,
                |       CAST(ws_item_sk AS BIGINT) AS ws_item_sk,
                |       CAST(ws_order_number AS BIGINT) AS ws_order_number
                |FROM VALUES
                |  (1000, 1, 10, 1, 11, 31.00, 7.00, 500, 9000),
                |  (2000, 2, 11, 2, 22, 32.00, 8.00, 600, 9001),
                |  (1000, 1002, 1054, 1, 11, 40.00, 5.00, 700, 9100),
                |  (1000, 1055, 1055, 1, 22, 80.00, 6.00, 701, 9101)
                |AS t(
                |  ws_web_site_sk, ws_sold_date_sk, ws_ship_date_sk, ws_warehouse_sk,
                |  ws_ship_mode_sk, ws_ext_sales_price, ws_net_profit, ws_item_sk, ws_order_number)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW web_returns AS
                |SELECT CAST(wr_returned_date_sk AS BIGINT) AS wr_returned_date_sk,
                |       CAST(wr_return_amt AS DECIMAL(7,2)) AS wr_return_amt,
                |       CAST(wr_net_loss AS DECIMAL(7,2)) AS wr_net_loss,
                |       CAST(wr_item_sk AS BIGINT) AS wr_item_sk,
                |       CAST(wr_order_number AS BIGINT) AS wr_order_number
                |FROM VALUES
                |  (1, 2.00, 0.60, 500, 9000),
                |  (2, 3.00, 0.40, 600, 9001)
                |AS t(wr_returned_date_sk, wr_return_amt, wr_net_loss, wr_item_sk, wr_order_number)
                |""".stripMargin)
  }

  private def checkDf(df: DataFrame): Unit = {
    assert(df.queryExecution.optimizedPlan.toString().contains("join_agg_wrapper_"))
    val invalidPushedAggregates = df.queryExecution.optimizedPlan.collect {
      case agg: Aggregate
          if agg.aggregateExpressions.exists(_.exists {
            case AggregateExpression(_, _, _, _, _) => true
            case _ => false
          }) &&
            agg.aggregateExpressions.exists(_.toString.contains("join_agg_wrapper_partial")) =>
        val offendingGroupingExprs = agg.groupingExpressions.filter {
          groupingExpr =>
            groupingExpr.references.exists(attr => factMeasureColumnNames.contains(attr.name))
        }
        if (offendingGroupingExprs.nonEmpty) {
          Some((agg, offendingGroupingExprs))
        } else {
          None
        }
    }.flatten
    assert(
      invalidPushedAggregates.isEmpty,
      invalidPushedAggregates
        .map {
          case (agg, offendingGroupingExprs) =>
            s"Unexpected fact measure in pushed aggregate grouping: " +
              s"${offendingGroupingExprs.mkString(", ")}\n${agg.treeString}"
        }
        .mkString("\n---\n")
    )
    checkPlan(df)
  }

  private def checkDfNoPush(df: DataFrame): Unit = {
    assert(!df.queryExecution.optimizedPlan.toString().contains("join_agg_wrapper_"))
    checkPlan(df)
  }

  protected def checkPlan(df: DataFrame): Unit = {
    checkGlutenPlan[HashAggregateExecTransformer](df)
  }

  test("Join-aggregate wrapper aggregate") {
    val query =
      """
        |SELECT
        |  SUM(l_extendedprice * (1 - l_discount)) AS revenue
        |FROM lineitem
        |JOIN part
        |  ON l_partkey = p_partkey
        |WHERE p_size > 10 AND l_shipmode IN ('AIR', 'RAIL')
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Join-aggregate wrapper aggregate 2") {
    val query =
      """
        |SELECT
        |  l_discount, p_partkey, AVG(l_extendedprice)
        |FROM lineitem
        |JOIN part
        |  ON l_partkey = p_partkey
        |GROUP BY l_discount, p_partkey
        |ORDER BY l_discount, p_partkey
        |LIMIT 100
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Support join-aggregate wrapper aggregate for simplified TPC-H q12") {
    val query =
      """
        |SELECT
        |  l_shipmode,
        |  SUM(CASE
        |    WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
        |    THEN 1
        |    ELSE 0
        |  END) AS high_line_count,
        |  SUM(CASE
        |    WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
        |    THEN 1
        |    ELSE 0
        |  END) AS low_line_count
        |FROM orders
        |JOIN lineitem
        |  ON o_orderkey = l_orderkey
        |WHERE l_shipmode IN ('MAIL', 'SHIP')
        |  AND l_commitdate < l_receiptdate
        |  AND l_shipdate < l_commitdate
        |  AND l_receiptdate >= DATE '1994-01-01'
        |  AND l_receiptdate < DATE '1995-01-01'
        |GROUP BY l_shipmode
        |ORDER BY l_shipmode
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Support join-aggregate wrapper aggregate for multi-field agg buffer (avg)") {
    val query =
      """
        |SELECT
        |  c_nationkey,
        |  AVG(CAST(o_shippriority AS DOUBLE)) AS avg_shippriority
        |FROM customer
        |JOIN orders
        |  ON c_custkey = o_custkey
        |WHERE c_mktsegment = 'BUILDING'
        |GROUP BY c_nationkey
        |ORDER BY c_nationkey
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Support join-aggregate wrapper aggregate with duplicate decimal sum buffers") {
    val query =
      """
        |SELECT
        |  c_nationkey,
        |  SUM(o_totalprice) AS sum_totalprice_a,
        |  SUM(o_totalprice + CAST(1 AS DECIMAL(12, 2))) AS sum_totalprice_b,
        |  SUM(CAST(o_shippriority AS BIGINT)) AS sum_shippriority,
        |  SUM(CAST(o_orderkey AS BIGINT)) AS sum_orderkey
        |FROM customer
        |JOIN orders
        |  ON c_custkey = o_custkey
        |WHERE c_mktsegment = 'BUILDING'
        |GROUP BY c_nationkey
        |ORDER BY c_nationkey
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Minimal pushdown for two different decimal sums") {
    withTempView("ss_fact", "ss_dim") {
      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_fact AS
                  |SELECT
                  |  CAST(item_sk AS INT) AS item_sk,
                  |  CAST(cp_catalog_page_id AS INT) AS grp_id,
                  |  CAST(sales_price AS DECIMAL(12, 2)) AS dec_a,
                  |  CAST(return_amt AS DECIMAL(12, 2)) AS dec_b
                  |FROM VALUES
                  |  (1, 100, 10.00, 5.00),
                  |  (1, 100, 20.00, 8.00),
                  |  (2, 200, 30.00, 9.00)
                  |AS t(item_sk, cp_catalog_page_id, sales_price, return_amt)
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_dim AS
                  |SELECT CAST(item_sk AS INT) AS item_sk
                  |FROM VALUES
                  |  (1), (1), (2), (2), (2)
                  |AS t(item_sk)
                  |""".stripMargin)

      val query =
        """
          |SELECT
          |  f.grp_id,
          |  SUM(f.dec_a) AS sum_dec_a,
          |  SUM(f.dec_b) AS sum_dec_b
          |FROM ss_fact f
          |JOIN ss_dim d
          |  ON f.item_sk = d.item_sk
          |GROUP BY f.grp_id
          |ORDER BY f.grp_id
          |""".stripMargin

      withSQLConf(
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.shuffle.partitions" -> "100",
        "spark.sql.autoBroadcastJoinThreshold" -> "10m") {
        runQueryAndCompare(query)(df => checkDf(df))
      }
    }
  }

  test("Minimal pushdown for decimal avg") {
    withTempView("ss_fact_avg", "ss_dim_avg") {
      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_fact_avg AS
                  |SELECT
                  |  CAST(item_sk AS INT) AS item_sk,
                  |  CAST(grp_id AS INT) AS grp_id,
                  |  CAST(metric AS DECIMAL(7, 2)) AS metric
                  |FROM VALUES
                  |  (1, 100, 10.00),
                  |  (1, 100, 20.00),
                  |  (2, 200, 30.00)
                  |AS t(item_sk, grp_id, metric)
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_dim_avg AS
                  |SELECT CAST(item_sk AS INT) AS item_sk
                  |FROM VALUES
                  |  (1), (1), (2), (2), (2)
                  |AS t(item_sk)
                  |""".stripMargin)

      val query =
        """
          |SELECT
          |  f.grp_id,
          |  AVG(f.metric) AS avg_metric
          |FROM ss_fact_avg f
          |JOIN ss_dim_avg d
          |  ON f.item_sk = d.item_sk
          |GROUP BY f.grp_id
          |ORDER BY f.grp_id
          |""".stripMargin

      withSQLConf(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.shuffle.partitions" -> "100",
        "spark.sql.autoBroadcastJoinThreshold" -> "10m") {
        runQueryAndCompare(query)(df => checkDf(df))
      }
    }
  }

  test("Support TPC-DS q5 shape") {
    val query =
      """
        |with ssr as
        | (select s_store_id,
        |        sum(sales_price) as sales,
        |        sum(profit) as profit,
        |        sum(return_amt) as returns,
        |        sum(net_loss) as profit_loss
        | from
        |  ( select  ss_store_sk as store_sk,
        |            ss_sold_date_sk  as date_sk,
        |            ss_ext_sales_price as sales_price,
        |            ss_net_profit as profit,
        |            cast(0 as decimal(7,2)) as return_amt,
        |            cast(0 as decimal(7,2)) as net_loss
        |    from store_sales
        |    union all
        |    select sr_store_sk as store_sk,
        |           sr_returned_date_sk as date_sk,
        |           cast(0 as decimal(7,2)) as sales_price,
        |           cast(0 as decimal(7,2)) as profit,
        |           sr_return_amt as return_amt,
        |           sr_net_loss as net_loss
        |    from store_returns
        |   ) salesreturns,
        |     date_dim,
        |     store
        | where date_sk = d_date_sk
        |       and d_date between cast('1998-08-04' as date)
        |                  and (cast('1998-08-04' as date) + interval '14' day)
        |       and store_sk = s_store_sk
        | group by s_store_id)
        | ,
        | csr as
        | (select cp_catalog_page_id,
        |        sum(sales_price) as sales,
        |        sum(profit) as profit,
        |        sum(return_amt) as returns,
        |        sum(net_loss) as profit_loss
        | from
        |  ( select  cs_catalog_page_sk as page_sk,
        |            cs_sold_date_sk  as date_sk,
        |            cs_ext_sales_price as sales_price,
        |            cs_net_profit as profit,
        |            cast(0 as decimal(7,2)) as return_amt,
        |            cast(0 as decimal(7,2)) as net_loss
        |    from catalog_sales
        |    union all
        |    select cr_catalog_page_sk as page_sk,
        |           cr_returned_date_sk as date_sk,
        |           cast(0 as decimal(7,2)) as sales_price,
        |           cast(0 as decimal(7,2)) as profit,
        |           cr_return_amount as return_amt,
        |           cr_net_loss as net_loss
        |    from catalog_returns
        |   ) salesreturns,
        |     date_dim,
        |     catalog_page
        | where date_sk = d_date_sk
        |       and d_date between cast('1998-08-04' as date)
        |                  and (cast('1998-08-04' as date) + interval '14' day)
        |       and page_sk = cp_catalog_page_sk
        | group by cp_catalog_page_id)
        | ,
        | wsr as
        | (select web_site_id,
        |        sum(sales_price) as sales,
        |        sum(profit) as profit,
        |        sum(return_amt) as returns,
        |        sum(net_loss) as profit_loss
        | from
        |  ( select  ws_web_site_sk as wsr_web_site_sk,
        |            ws_sold_date_sk  as date_sk,
        |            ws_ext_sales_price as sales_price,
        |            ws_net_profit as profit,
        |            cast(0 as decimal(7,2)) as return_amt,
        |            cast(0 as decimal(7,2)) as net_loss
        |    from web_sales
        |    union all
        |    select ws_web_site_sk as wsr_web_site_sk,
        |           wr_returned_date_sk as date_sk,
        |           cast(0 as decimal(7,2)) as sales_price,
        |           cast(0 as decimal(7,2)) as profit,
        |           wr_return_amt as return_amt,
        |           wr_net_loss as net_loss
        |    from web_returns left outer join web_sales on
        |         ( wr_item_sk = ws_item_sk
        |           and wr_order_number = ws_order_number)
        |   ) salesreturns,
        |     date_dim,
        |     web_site
        | where date_sk = d_date_sk
        |       and d_date between cast('1998-08-04' as date)
        |                  and (cast('1998-08-04' as date) + interval '14' day)
        |       and wsr_web_site_sk = web_site_sk
        | group by web_site_id)
        |  select  channel
        |        , id
        |        , sum(sales) as sales
        |        , sum(returns) as returns
        |        , sum(profit) as profit
        | from
        | (select 'store channel' as channel
        |        , 'store' || s_store_id as id
        |        , sales
        |        , returns
        |        , (profit - profit_loss) as profit
        | from   ssr
        | union all
        | select 'catalog channel' as channel
        |        , 'catalog_page' || cp_catalog_page_id as id
        |        , sales
        |        , returns
        |        , (profit - profit_loss) as profit
        | from  csr
        | union all
        | select 'web channel' as channel
        |        , 'web_site' || web_site_id as id
        |        , sales
        |        , returns
        |        , (profit - profit_loss) as profit
        | from   wsr
        | ) x
        | group by rollup (channel, id)
        | order by channel
        |         ,id
        | LIMIT 100
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Support simplified TPC-DS q7 shape") {
    val query =
      """
        |select  i_item_id,
        |        avg(ss_quantity) agg1
        |from store_sales, item
        |where ss_item_sk = i_item_sk
        |group by i_item_id
        |order by i_item_id
        |limit 100
        |""".stripMargin

    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "100",
      "spark.sql.autoBroadcastJoinThreshold" -> "10m") {
      runQueryAndCompare(query)(df => checkDf(df))
    }
  }

  test("Support simplified TPC-DS q16 shape") {
    val query =
      """
        |select
        |   count(distinct cs_order_number) as `order count`
        |  ,sum(cs_ext_ship_cost) as `total shipping cost`
        |  ,sum(cs_net_profit) as `total net profit`
        |from
        |   catalog_sales cs1
        |  ,date_dim
        |  ,customer_address
        |  ,call_center
        |where
        |    d_date between '1999-2-01' and
        |           (cast('1999-2-01' as date) + interval '60' day)
        |and cs1.cs_ship_date_sk = d_date_sk
        |and cs1.cs_ship_addr_sk = ca_address_sk
        |and ca_state = 'IL'
        |and cs1.cs_call_center_sk = cc_call_center_sk
        |and cc_county in ('Williamson County','Williamson County','Williamson County',
        |                  'Williamson County','Williamson County')
        |and exists (select *
        |            from catalog_sales cs2
        |            where cs1.cs_order_number = cs2.cs_order_number
        |              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
        |and not exists(select *
        |               from catalog_returns cr1
        |               where cs1.cs_order_number = cr1.cr_order_number)
        |order by count(distinct cs_order_number)
        |limit 100
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        // Mixed distinct + non-distinct aggregate shape is currently not pushed by the
        // Join-aggregate pre-aggregate rule.
        checkDfNoPush(df)
    }
  }

  test("Support TPC-DS q17 shape") {
    val query =
      """
        |select  i_item_id
        |       ,i_item_desc
        |       ,s_state
        |       ,count(ss_quantity) as store_sales_quantitycount
        |       ,avg(ss_quantity) as store_sales_quantityave
        |       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
        |       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
        |       ,count(sr_return_quantity) as store_returns_quantitycount
        |       ,avg(sr_return_quantity) as store_returns_quantityave
        |       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
        |       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity)
        |           as store_returns_quantitycov
        |       ,count(cs_quantity) as catalog_sales_quantitycount
        |       ,avg(cs_quantity) as catalog_sales_quantityave
        |       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
        |       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
        |from store_sales
        |    ,store_returns
        |    ,catalog_sales
        |    ,date_dim d1
        |    ,date_dim d2
        |    ,date_dim d3
        |    ,store
        |    ,item
        |where d1.d_quarter_name = '1998Q1'
        |  and d1.d_date_sk = ss_sold_date_sk
        |  and i_item_sk = ss_item_sk
        |  and s_store_sk = ss_store_sk
        |  and ss_customer_sk = sr_customer_sk
        |  and ss_item_sk = sr_item_sk
        |  and ss_ticket_number = sr_ticket_number
        |  and sr_returned_date_sk = d2.d_date_sk
        |  and d2.d_quarter_name in ('1998Q1','1998Q2','1998Q3')
        |  and sr_customer_sk = cs_bill_customer_sk
        |  and sr_item_sk = cs_item_sk
        |  and cs_sold_date_sk = d3.d_date_sk
        |  and d3.d_quarter_name in ('1998Q1','1998Q2','1998Q3')
        |group by i_item_id
        |        ,i_item_desc
        |        ,s_state
        |order by i_item_id
        |        ,i_item_desc
        |        ,s_state
        |limit 100
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Support SUM + COUNT DISTINCT shape") {
    withTempView("ss_fact_distinct", "ss_dim_distinct") {
      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_fact_distinct AS
                  |SELECT
                  |  CAST(item_sk AS INT) AS item_sk,
                  |  CAST(grp_id AS INT) AS grp_id,
                  |  CAST(metric AS DECIMAL(7, 2)) AS metric,
                  |  CAST(order_no AS BIGINT) AS order_no
                  |FROM VALUES
                  |  (1, 100, 10.00, 9001),
                  |  (1, 100, 20.00, 9002),
                  |  (2, 200, 30.00, 9002),
                  |  (2, 200, 15.00, 9003)
                  |AS t(item_sk, grp_id, metric, order_no)
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_dim_distinct AS
                  |SELECT CAST(item_sk AS INT) AS item_sk
                  |FROM VALUES
                  |  (1), (2)
                  |AS t(item_sk)
                  |""".stripMargin)

      val query =
        """
          |SELECT
          |  f.grp_id,
          |  SUM(f.metric) AS sum_metric,
          |  COUNT(DISTINCT f.order_no) AS distinct_orders
          |FROM ss_fact_distinct f
          |JOIN ss_dim_distinct d
          |  ON f.item_sk = d.item_sk
          |GROUP BY f.grp_id
          |ORDER BY f.grp_id
          |""".stripMargin

      runQueryAndCompare(query)(df => checkDfNoPush(df))
    }
  }

  test("Support TPC-DS q2 shape") {
    val query =
      """
        |with wscs as
        | (select sold_date_sk
        |        ,sales_price
        |  from (select ws_sold_date_sk sold_date_sk
        |              ,ws_ext_sales_price sales_price
        |        from web_sales
        |        union all
        |        select cs_sold_date_sk sold_date_sk
        |              ,cs_ext_sales_price sales_price
        |        from catalog_sales)),
        | wswscs as
        | (select d_week_seq,
        |        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        |        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        |        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        |        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        |        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        |        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        |        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
        | from wscs
        |     ,date_dim
        | where d_date_sk = sold_date_sk
        | group by d_week_seq)
        | select d_week_seq1
        |       ,round(sun_sales1/sun_sales2,2)
        |       ,round(mon_sales1/mon_sales2,2)
        |       ,round(tue_sales1/tue_sales2,2)
        |       ,round(wed_sales1/wed_sales2,2)
        |       ,round(thu_sales1/thu_sales2,2)
        |       ,round(fri_sales1/fri_sales2,2)
        |       ,round(sat_sales1/sat_sales2,2)
        | from
        | (select wswscs.d_week_seq d_week_seq1
        |        ,sun_sales sun_sales1
        |        ,mon_sales mon_sales1
        |        ,tue_sales tue_sales1
        |        ,wed_sales wed_sales1
        |        ,thu_sales thu_sales1
        |        ,fri_sales fri_sales1
        |        ,sat_sales sat_sales1
        |  from wswscs,date_dim
        |  where date_dim.d_week_seq = wswscs.d_week_seq and
        |        d_year = 2001) y,
        | (select wswscs.d_week_seq d_week_seq2
        |        ,sun_sales sun_sales2
        |        ,mon_sales mon_sales2
        |        ,tue_sales tue_sales2
        |        ,wed_sales wed_sales2
        |        ,thu_sales thu_sales2
        |        ,fri_sales fri_sales2
        |        ,sat_sales sat_sales2
        |  from wswscs
        |      ,date_dim
        |  where date_dim.d_week_seq = wswscs.d_week_seq and
        |        d_year = 2001+1) z
        | where d_week_seq1=d_week_seq2-53
        | order by d_week_seq1
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Support TPC-DS q62 shape") {
    val query =
      """
        |select
        |   substr(w_warehouse_name,1,20)
        |  ,sm_type
        |  ,web_name
        |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)
        |       as `30 days`
        |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and
        |                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end)
        |       as `31-60 days`
        |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and
        |                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)
        |       as `61-90 days`
        |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
        |                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)
        |       as `91-120 days`
        |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1 else 0 end)
        |       as `>120 days`
        |from
        |   web_sales
        |  ,warehouse
        |  ,ship_mode
        |  ,web_site
        |  ,date_dim
        |where
        |    d_month_seq between 1212 and 1212 + 11
        |and ws_ship_date_sk   = d_date_sk
        |and ws_warehouse_sk   = w_warehouse_sk
        |and ws_ship_mode_sk   = sm_ship_mode_sk
        |and ws_web_site_sk    = web_site_sk
        |group by
        |   substr(w_warehouse_name,1,20)
        |  ,sm_type
        |  ,web_name
        |order by substr(w_warehouse_name,1,20)
        |        ,sm_type
        |        ,web_name
        |limit 100
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }

  test("Support join-aggregate wrapper aggregate for simplified TPC-H q18") {
    val query =
      """
        |SELECT
        |  c_name,
        |  c_custkey,
        |  o_orderkey,
        |  o_orderdate,
        |  o_totalprice,
        |  SUM(l_quantity)
        |FROM customer, orders, lineitem
        |WHERE o_orderkey IN (
        |  SELECT
        |    l_orderkey
        |  FROM lineitem
        |  GROUP BY l_orderkey
        |  HAVING SUM(l_quantity) > 300
        |)
        |  AND c_custkey = o_custkey
        |  AND o_orderkey = l_orderkey
        |GROUP BY
        |  c_name,
        |  c_custkey,
        |  o_orderkey,
        |  o_orderdate,
        |  o_totalprice
        |ORDER BY
        |  o_totalprice DESC,
        |  o_orderdate
        |LIMIT 100
        |""".stripMargin

    runQueryAndCompare(query)(df => checkDf(df))
  }
}
