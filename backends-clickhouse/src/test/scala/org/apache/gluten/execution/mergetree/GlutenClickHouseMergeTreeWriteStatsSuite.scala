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
package org.apache.gluten.execution.mergetree

import org.apache.gluten.backendsapi.clickhouse.{CHConfig, RuntimeSettings}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseTPCDSAbstractSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.input_file_name

import org.scalatest.DoNotDiscover

@DoNotDiscover
class GlutenClickHouseMergeTreeWriteStatsSuite extends GlutenClickHouseTPCDSAbstractSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.driver.memory", "2G")
      .set("spark.memory.offHeap.size", "4G")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.databricks.delta.stats.enabled", "true")
      .set("spark.databricks.delta.optimizeWrite.enabled", "true")
      .set("spark.sql.storeAssignmentPolicy", "LEGACY")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
      .set(RuntimeSettings.MERGE_AFTER_INSERT.key, "false")
  }

  private val remotePath: String = hdfsHelper.independentHdfsURL("stats")
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    hdfsHelper.deleteDir(remotePath)
    hdfsHelper.resetMeta()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    hdfsHelper.resetMeta()
  }

  def tpcdsMergetreeTables: Map[String, String] = {
    Map(
      "store_sales" ->
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS store_sales
           |(
           |     ss_sold_time_sk INT,
           |     ss_item_sk INT,
           |     ss_customer_sk INT,
           |     ss_cdemo_sk INT,
           |     ss_hdemo_sk INT,
           |     ss_addr_sk INT,
           |     ss_store_sk INT,
           |     ss_promo_sk INT,
           |     ss_ticket_number bigint,
           |     ss_quantity INT,
           |     ss_wholesale_cost DECIMAL(7,2),
           |     ss_list_price DECIMAL(7,2),
           |     ss_sales_price DECIMAL(7,2),
           |     ss_ext_discount_amt DECIMAL(7,2),
           |     ss_ext_sales_price DECIMAL(7,2),
           |     ss_ext_wholesale_cost DECIMAL(7,2),
           |     ss_ext_list_price DECIMAL(7,2),
           |     ss_ext_tax DECIMAL(7,2),
           |     ss_coupon_amt DECIMAL(7,2),
           |     ss_net_paid DECIMAL(7,2),
           |     ss_net_paid_inc_tax DECIMAL(7,2),
           |     ss_net_profit DECIMAL(7,2),
           |     ss_sold_date_sk INT
           |)
           |USING clickhouse
           |LOCATION '$remotePath/store_sales'
           |TBLPROPERTIES (storage_policy='__hdfs_main')
           |""".stripMargin,
      "store_returns" ->
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS store_returns
           |(
           |     sr_return_time_sk INT,
           |     sr_item_sk INT,
           |     sr_customer_sk INT,
           |     sr_cdemo_sk INT,
           |     sr_hdemo_sk INT,
           |     sr_addr_sk INT,
           |     sr_store_sk INT,
           |     sr_reason_sk INT,
           |     sr_ticket_number INT,
           |     sr_return_quantity INT,
           |     sr_return_amt DECIMAL(7, 2),
           |     sr_return_tax DECIMAL(7, 2),
           |     sr_return_amt_inc_tax DECIMAL(7, 2),
           |     sr_fee DECIMAL(7, 2),
           |     sr_return_ship_cost DECIMAL(7, 2),
           |     sr_refunded_cash DECIMAL(7, 2),
           |     sr_reversed_charge DECIMAL(7, 2),
           |     sr_store_credit DECIMAL(7, 2),
           |     sr_net_loss DECIMAL(7, 2),
           |     sr_returned_date_sk INT
           |)
           |USING clickhouse
           |LOCATION '$remotePath/store_returns'
           |TBLPROPERTIES (storage_policy='__hdfs_main')
           |""".stripMargin,
      "catalog_sales" ->
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS catalog_sales
           |(
           |     cs_sold_time_sk INT,
           |     cs_ship_date_sk INT,
           |     cs_bill_customer_sk INT,
           |     cs_bill_cdemo_sk INT,
           |     cs_bill_hdemo_sk INT,
           |     cs_bill_addr_sk INT,
           |     cs_ship_customer_sk INT,
           |     cs_ship_cdemo_sk INT,
           |     cs_ship_hdemo_sk INT,
           |     cs_ship_addr_sk INT,
           |     cs_call_center_sk INT,
           |     cs_catalog_page_sk INT,
           |     cs_ship_mode_sk INT,
           |     cs_warehouse_sk INT,
           |     cs_item_sk INT,
           |     cs_promo_sk INT,
           |     cs_order_number INT,
           |     cs_quantity INT,
           |     cs_wholesale_cost DECIMAL(7, 2),
           |     cs_list_price DECIMAL(7, 2),
           |     cs_sales_price DECIMAL(7, 2),
           |     cs_ext_discount_amt DECIMAL(7, 2),
           |     cs_ext_sales_price DECIMAL(7, 2),
           |     cs_ext_wholesale_cost DECIMAL(7, 2),
           |     cs_ext_list_price DECIMAL(7, 2),
           |     cs_ext_tax DECIMAL(7, 2),
           |     cs_coupon_amt DECIMAL(7, 2),
           |     cs_ext_ship_cost DECIMAL(7, 2),
           |     cs_net_paid DECIMAL(7, 2),
           |     cs_net_paid_inc_tax DECIMAL(7, 2),
           |     cs_net_paid_inc_ship DECIMAL(7, 2),
           |     cs_net_paid_inc_ship_tax DECIMAL(7, 2),
           |     cs_net_profit DECIMAL(7, 2),
           |     cs_sold_date_sk INT
           |)
           |USING clickhouse
           |LOCATION '$remotePath/catalog_sales'
           |TBLPROPERTIES (storage_policy='__hdfs_main')
           |""".stripMargin,
      "catalog_returns" ->
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS catalog_returns
           |(
           |     cr_returned_time_sk INT,
           |     cr_item_sk INT,
           |     cr_refunded_customer_sk INT,
           |     cr_refunded_cdemo_sk INT,
           |     cr_refunded_hdemo_sk INT,
           |     cr_refunded_addr_sk INT,
           |     cr_returning_customer_sk INT,
           |     cr_returning_cdemo_sk INT,
           |     cr_returning_hdemo_sk INT,
           |     cr_returning_addr_sk INT,
           |     cr_call_center_sk INT,
           |     cr_catalog_page_sk INT,
           |     cr_ship_mode_sk INT,
           |     cr_warehouse_sk INT,
           |     cr_reason_sk INT,
           |     cr_order_number INT,
           |     cr_return_quantity INT,
           |     cr_return_amount DECIMAL(7, 2),
           |     cr_return_tax DECIMAL(7, 2),
           |     cr_return_amt_inc_tax DECIMAL(7, 2),
           |     cr_fee DECIMAL(7, 2),
           |     cr_return_ship_cost DECIMAL(7, 2),
           |     cr_refunded_cash DECIMAL(7, 2),
           |     cr_reversed_charge DECIMAL(7, 2),
           |     cr_store_credit DECIMAL(7, 2),
           |     cr_net_loss DECIMAL(7, 2),
           |     cr_returned_date_sk INT
           |)
           |USING clickhouse
           |LOCATION '$remotePath/catalog_returns'
           |TBLPROPERTIES (storage_policy='__hdfs_main')
           |""".stripMargin,
      "web_sales" ->
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS web_sales
           |(
           |     ws_sold_time_sk INT,
           |     ws_ship_date_sk INT,
           |     ws_item_sk INT,
           |     ws_bill_customer_sk INT,
           |     ws_bill_cdemo_sk INT,
           |     ws_bill_hdemo_sk INT,
           |     ws_bill_addr_sk INT,
           |     ws_ship_customer_sk INT,
           |     ws_ship_cdemo_sk INT,
           |     ws_ship_hdemo_sk INT,
           |     ws_ship_addr_sk INT,
           |     ws_web_page_sk INT,
           |     ws_web_site_sk INT,
           |     ws_ship_mode_sk INT,
           |     ws_warehouse_sk INT,
           |     ws_promo_sk INT,
           |     ws_order_number INT,
           |     ws_quantity INT,
           |     ws_wholesale_cost DECIMAL(7, 2),
           |     ws_list_price DECIMAL(7, 2),
           |     ws_sales_price DECIMAL(7, 2),
           |     ws_ext_discount_amt DECIMAL(7, 2),
           |     ws_ext_sales_price DECIMAL(7, 2),
           |     ws_ext_wholesale_cost DECIMAL(7, 2),
           |     ws_ext_list_price DECIMAL(7, 2),
           |     ws_ext_tax DECIMAL(7, 2),
           |     ws_coupon_amt DECIMAL(7, 2),
           |     ws_ext_ship_cost DECIMAL(7, 2),
           |     ws_net_paid DECIMAL(7, 2),
           |     ws_net_paid_inc_tax DECIMAL(7, 2),
           |     ws_net_paid_inc_ship DECIMAL(7, 2),
           |     ws_net_paid_inc_ship_tax DECIMAL(7, 2),
           |     ws_net_profit DECIMAL(7, 2),
           |     ws_sold_date_sk INT
           |)
           |USING clickhouse
           |LOCATION '$remotePath/web_sales'
           |TBLPROPERTIES (storage_policy='__hdfs_main')
           |""".stripMargin,
      "web_returns" ->
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS web_returns
           |(
           |     wr_returned_time_sk INT,
           |     wr_item_sk INT,
           |     wr_refunded_customer_sk INT,
           |     wr_refunded_cdemo_sk INT,
           |     wr_refunded_hdemo_sk INT,
           |     wr_refunded_addr_sk INT,
           |     wr_returning_customer_sk INT,
           |     wr_returning_cdemo_sk INT,
           |     wr_returning_hdemo_sk INT,
           |     wr_returning_addr_sk INT,
           |     wr_web_page_sk INT,
           |     wr_reason_sk INT,
           |     wr_order_number INT,
           |     wr_return_quantity INT,
           |     wr_return_amt DECIMAL(7, 2),
           |     wr_return_tax DECIMAL(7, 2),
           |     wr_return_amt_inc_tax DECIMAL(7, 2),
           |     wr_fee DECIMAL(7, 2),
           |     wr_return_ship_cost DECIMAL(7, 2),
           |     wr_refunded_cash DECIMAL(7, 2),
           |     wr_reversed_charge DECIMAL(7, 2),
           |     wr_account_credit DECIMAL(7, 2),
           |     wr_net_loss DECIMAL(7, 2),
           |     wr_returned_date_sk INT
           |)
           |USING clickhouse
           |LOCATION '$remotePath/web_returns'
           |TBLPROPERTIES (storage_policy='__hdfs_main')
           |""".stripMargin,
      "inventory" ->
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS inventory
           |(
           |     inv_item_sk INT,
           |     inv_warehouse_sk INT,
           |     inv_quantity_on_hand INT,
           |     inv_date_sk INT
           |)
           |USING clickhouse
           |LOCATION '$remotePath/inventory'
           |TBLPROPERTIES (storage_policy='__hdfs_main')
           |""".stripMargin
    )

  }

  test("test mergetree virtual columns") {
    spark.sql("create database if not exists mergetree")
    spark.sql("use mergetree")
    spark.sql("drop table if exists store_sales")
    spark.sql(tpcdsMergetreeTables("store_sales"))
    // scalastyle:off line.size.limit
    spark.sql(
      "insert into mergetree.store_sales select /*+ REPARTITION(3) */ * from tpcdsdb.store_sales")
    val df = spark.sql(
      "select input_file_name(), count(*) from mergetree.store_sales group by input_file_name()")
    val inputFiles = df.collect().map(_.getString(0))
    val snapshot = getDeltaSnapshot(df)
    val deltaLogFiles = snapshot.allFiles
      .collect()
      .map(addFile => snapshot.path.getParent.toString + "/" + addFile.path)
    assertResult(inputFiles.toSet)(deltaLogFiles.toSet)
    // scalastyle:on line.size.limit
  }

  test("verify mergetree delta stats") {
    if (isSparkVersionGE("3.5")) {
      spark.sql("create database if not exists mergetree")
      spark.sql("use mergetree")
      val tables = Seq(
        "store_sales",
        "store_returns",
        "web_sales",
        "web_returns",
        "catalog_sales",
        "catalog_returns",
        "inventory")
      tables.foreach(writeAndCompareDeltaStats(_))
    }
  }

  def getDeltaSnapshot(df: DataFrame): Snapshot = {
    val scanExec = collect(df.queryExecution.sparkPlan) {
      case nf: FileSourceScanExecTransformer => nf
      case f: FileSourceScanExec => f
    }
    assertResult(1)(scanExec.size)
    val mergetreeScan = scanExec.head
    val snapshot: Snapshot = mergetreeScan.relation.location match {
      case pdf: PreparedDeltaFileIndex => pdf.preparedScan.scannedSnapshot
      case tlf: TahoeLogFileIndex => tlf.getSnapshot
    }
    assert(snapshot != null)
    snapshot
  }

  def writeAndCompareDeltaStats(table: String, partNum: Int = 3): Unit = {
    spark.sql(s"drop table if exists $table")
    spark.sql(tpcdsMergetreeTables(table))
    spark.sql(s"insert into $table select /*+ REPARTITION($partNum) */ * from tpcdsdb.$table")
    val tableDf = spark.table(table)
    val snapshot = getDeltaSnapshot(tableDf)
    val tableStats = tableDf
      .groupBy(input_file_name().as("path"))
      .agg(snapshot.statsCollector)
      .orderBy("path")
    val deltaLogStats = snapshot.withStats
      .selectExpr(s"concat('${snapshot.path.getParent}/', path)", "stats")
      .orderBy("path")
    checkAnswer(tableStats, deltaLogStats)
  }

}
