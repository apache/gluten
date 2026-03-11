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
package org.apache.gluten.integration.ds

import org.apache.gluten.integration.{DataGen, TypeModifier}
import org.apache.gluten.integration.shim.Shim

import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import io.trino.tpcds._

import java.io.File

import scala.collection.JavaConverters._

class TpcdsDataGen(
    scale: Double,
    partitions: Int,
    source: String,
    dir: String,
    genPartitionedData: Boolean,
    featureNames: Seq[String],
    typeModifiers: Seq[TypeModifier]
) extends Serializable
  with DataGen {

  private val featureRegistry = new DataGen.FeatureRegistry

  featureRegistry.register(TpcdsDataGenFeatures.EnableDeltaDeletionVector)
  featureRegistry.register(TpcdsDataGenFeatures.DeleteTenPercentData)

  private val features = featureNames.map(featureRegistry.getFeature)

  def writeParquetTable(spark: SparkSession, t: Table): Unit = {
    val name = t.getName
    if (name.equals("dbgen_version")) {
      return
    }
    val schema = name match {
      case "catalog_sales" => TpcdsDataGen.catalogSalesSchema
      case "catalog_returns" => TpcdsDataGen.catalogReturnsSchema
      case "inventory" => TpcdsDataGen.inventorySchema
      case "store_sales" => TpcdsDataGen.storeSalesSchema
      case "store_returns" => TpcdsDataGen.storeReturnsSchema
      case "web_sales" => TpcdsDataGen.webSalesSchema
      case "web_returns" => TpcdsDataGen.webReturnsSchema
      case "call_center" => TpcdsDataGen.callCenterSchema
      case "catalog_page" => TpcdsDataGen.catalogPageSchema
      case "customer" => TpcdsDataGen.customerSchema
      case "customer_address" => TpcdsDataGen.customerAddressSchema
      case "customer_demographics" => TpcdsDataGen.customerDemographicsSchema
      case "date_dim" => TpcdsDataGen.dateDimSchema
      case "household_demographics" => TpcdsDataGen.householdDemographicsSchema
      case "income_band" => TpcdsDataGen.incomeBandSchema
      case "item" => TpcdsDataGen.itemSchema
      case "promotion" => TpcdsDataGen.promotionSchema
      case "reason" => TpcdsDataGen.reasonSchema
      case "ship_mode" => TpcdsDataGen.shipModeSchema
      case "store" => TpcdsDataGen.storeSchema
      case "time_dim" => TpcdsDataGen.timeDimSchema
      case "warehouse" => TpcdsDataGen.warehouseSchema
      case "web_page" => TpcdsDataGen.webPageSchema
      case "web_site" => TpcdsDataGen.webSiteSchema
    }
    val partitionBy: List[String] = if (!genPartitionedData) {
      List[String]()
    } else {
      name match {
        case "store_sales" => List("ss_sold_date_sk")
        case "catalog_sales" => List("cs_sold_date_sk")
        case "web_sales" => List("ws_sold_date_sk")
        case _ => List[String]()
      }
    }

    writeParquetTable(spark, name, t, schema, partitionBy)
  }

  private def writeParquetTable(
      spark: SparkSession,
      tableName: String,
      t: Table,
      schema: StructType,
      partitionBy: List[String]): Unit = {
    println(s"Generating table $tableName...")
    val rowModifier = DataGen.getRowModifier(schema, typeModifiers)
    val modifiedSchema = DataGen.modifySchema(schema, rowModifier)

    val stringSchema = StructType(modifiedSchema.fields.map(f => StructField(f.name, StringType)))

    val columns = modifiedSchema.fields.map(f => new Column(f.name).cast(f.dataType).as(f.name))
    spark
      .range(0, partitions, 1L, partitions)
      .mapPartitions {
        itr =>
          val id = itr.toArray
          if (id.length != 1) {
            throw new IllegalStateException()
          }
          val options = new Options()
          options.scale = scale
          options.parallelism = partitions
          val session = options.toSession
          val chunkSession = session.withChunkNumber(id(0).toInt + 1)
          val results = Results.constructResults(t, chunkSession).asScala.toIterator
          results.map {
            parentAndChildRow =>
              // Skip child table when generating parent table,
              // we generate every table individually no matter it is parent or child.
              val array: Array[String] = parentAndChildRow.get(0).asScala.toArray
              Row(array: _*)
          }
      }(Shim.getExpressionEncoder(stringSchema))
      .select(columns: _*)
      .write
      .format(source)
      .partitionBy(partitionBy.toArray: _*)
      .mode(SaveMode.Overwrite)
      .option("path", dir + File.separator + tableName) // storage location
      .saveAsTable(tableName)
  }

  override def gen(spark: SparkSession): Unit = {
    Table.getBaseTables.forEach(t => writeParquetTable(spark, t))

    features.foreach(feature => DataGen.Feature.run(spark, source, feature))
  }
}

object TpcdsDataGen {

  // generated by script
  private def catalogSalesSchema = {
    StructType(
      Seq(
        StructField("cs_sold_date_sk", LongType),
        StructField("cs_sold_time_sk", LongType),
        StructField("cs_ship_date_sk", LongType),
        StructField("cs_bill_customer_sk", LongType),
        StructField("cs_bill_cdemo_sk", LongType),
        StructField("cs_bill_hdemo_sk", LongType),
        StructField("cs_bill_addr_sk", LongType),
        StructField("cs_ship_customer_sk", LongType),
        StructField("cs_ship_cdemo_sk", LongType),
        StructField("cs_ship_hdemo_sk", LongType),
        StructField("cs_ship_addr_sk", LongType),
        StructField("cs_call_center_sk", LongType),
        StructField("cs_catalog_page_sk", LongType),
        StructField("cs_ship_mode_sk", LongType),
        StructField("cs_warehouse_sk", LongType),
        StructField("cs_item_sk", LongType),
        StructField("cs_promo_sk", LongType),
        StructField("cs_order_number", LongType),
        StructField("cs_quantity", LongType),
        StructField("cs_wholesale_cost", DecimalType(7, 2)),
        StructField("cs_list_price", DecimalType(7, 2)),
        StructField("cs_sales_price", DecimalType(7, 2)),
        StructField("cs_ext_discount_amt", DecimalType(7, 2)),
        StructField("cs_ext_sales_price", DecimalType(7, 2)),
        StructField("cs_ext_wholesale_cost", DecimalType(7, 2)),
        StructField("cs_ext_list_price", DecimalType(7, 2)),
        StructField("cs_ext_tax", DecimalType(7, 2)),
        StructField("cs_coupon_amt", DecimalType(7, 2)),
        StructField("cs_ext_ship_cost", DecimalType(7, 2)),
        StructField("cs_net_paid", DecimalType(7, 2)),
        StructField("cs_net_paid_inc_tax", DecimalType(7, 2)),
        StructField("cs_net_paid_inc_ship", DecimalType(7, 2)),
        StructField("cs_net_paid_inc_ship_tax", DecimalType(7, 2)),
        StructField("cs_net_profit", DecimalType(7, 2))
      ))
  }

  private def catalogReturnsSchema = {
    StructType(
      Seq(
        StructField("cr_returned_date_sk", LongType),
        StructField("cr_returned_time_sk", LongType),
        StructField("cr_item_sk", LongType),
        StructField("cr_refunded_customer_sk", LongType),
        StructField("cr_refunded_cdemo_sk", LongType),
        StructField("cr_refunded_hdemo_sk", LongType),
        StructField("cr_refunded_addr_sk", LongType),
        StructField("cr_returning_customer_sk", LongType),
        StructField("cr_returning_cdemo_sk", LongType),
        StructField("cr_returning_hdemo_sk", LongType),
        StructField("cr_returning_addr_sk", LongType),
        StructField("cr_call_center_sk", LongType),
        StructField("cr_catalog_page_sk", LongType),
        StructField("cr_ship_mode_sk", LongType),
        StructField("cr_warehouse_sk", LongType),
        StructField("cr_reason_sk", LongType),
        StructField("cr_order_number", LongType),
        StructField("cr_return_quantity", LongType),
        StructField("cr_return_amount", DecimalType(7, 2)),
        StructField("cr_return_tax", DecimalType(7, 2)),
        StructField("cr_return_amt_inc_tax", DecimalType(7, 2)),
        StructField("cr_fee", DecimalType(7, 2)),
        StructField("cr_return_ship_cost", DecimalType(7, 2)),
        StructField("cr_refunded_cash", DecimalType(7, 2)),
        StructField("cr_reversed_charge", DecimalType(7, 2)),
        StructField("cr_store_credit", DecimalType(7, 2)),
        StructField("cr_net_loss", DecimalType(7, 2))
      ))
  }

  private def inventorySchema = {
    StructType(
      Seq(
        StructField("inv_date_sk", LongType),
        StructField("inv_item_sk", LongType),
        StructField("inv_warehouse_sk", LongType),
        StructField("inv_quantity_on_hand", LongType)
      ))
  }

  private def storeSalesSchema = {
    StructType(
      Seq(
        StructField("ss_sold_date_sk", LongType),
        StructField("ss_sold_time_sk", LongType),
        StructField("ss_item_sk", LongType),
        StructField("ss_customer_sk", LongType),
        StructField("ss_cdemo_sk", LongType),
        StructField("ss_hdemo_sk", LongType),
        StructField("ss_addr_sk", LongType),
        StructField("ss_store_sk", LongType),
        StructField("ss_promo_sk", LongType),
        StructField("ss_ticket_number", LongType),
        StructField("ss_quantity", LongType),
        StructField("ss_wholesale_cost", DecimalType(7, 2)),
        StructField("ss_list_price", DecimalType(7, 2)),
        StructField("ss_sales_price", DecimalType(7, 2)),
        StructField("ss_ext_discount_amt", DecimalType(7, 2)),
        StructField("ss_ext_sales_price", DecimalType(7, 2)),
        StructField("ss_ext_wholesale_cost", DecimalType(7, 2)),
        StructField("ss_ext_list_price", DecimalType(7, 2)),
        StructField("ss_ext_tax", DecimalType(7, 2)),
        StructField("ss_coupon_amt", DecimalType(7, 2)),
        StructField("ss_net_paid", DecimalType(7, 2)),
        StructField("ss_net_paid_inc_tax", DecimalType(7, 2)),
        StructField("ss_net_profit", DecimalType(7, 2))
      ))
  }

  private def storeReturnsSchema = {
    StructType(
      Seq(
        StructField("sr_returned_date_sk", LongType),
        StructField("sr_return_time_sk", LongType),
        StructField("sr_item_sk", LongType),
        StructField("sr_customer_sk", LongType),
        StructField("sr_cdemo_sk", LongType),
        StructField("sr_hdemo_sk", LongType),
        StructField("sr_addr_sk", LongType),
        StructField("sr_store_sk", LongType),
        StructField("sr_reason_sk", LongType),
        StructField("sr_ticket_number", LongType),
        StructField("sr_return_quantity", LongType),
        StructField("sr_return_amt", DecimalType(7, 2)),
        StructField("sr_return_tax", DecimalType(7, 2)),
        StructField("sr_return_amt_inc_tax", DecimalType(7, 2)),
        StructField("sr_fee", DecimalType(7, 2)),
        StructField("sr_return_ship_cost", DecimalType(7, 2)),
        StructField("sr_refunded_cash", DecimalType(7, 2)),
        StructField("sr_reversed_charge", DecimalType(7, 2)),
        StructField("sr_store_credit", DecimalType(7, 2)),
        StructField("sr_net_loss", DecimalType(7, 2))
      ))
  }

  private def webSalesSchema = {
    StructType(
      Seq(
        StructField("ws_sold_date_sk", LongType),
        StructField("ws_sold_time_sk", LongType),
        StructField("ws_ship_date_sk", LongType),
        StructField("ws_item_sk", LongType),
        StructField("ws_bill_customer_sk", LongType),
        StructField("ws_bill_cdemo_sk", LongType),
        StructField("ws_bill_hdemo_sk", LongType),
        StructField("ws_bill_addr_sk", LongType),
        StructField("ws_ship_customer_sk", LongType),
        StructField("ws_ship_cdemo_sk", LongType),
        StructField("ws_ship_hdemo_sk", LongType),
        StructField("ws_ship_addr_sk", LongType),
        StructField("ws_web_page_sk", LongType),
        StructField("ws_web_site_sk", LongType),
        StructField("ws_ship_mode_sk", LongType),
        StructField("ws_warehouse_sk", LongType),
        StructField("ws_promo_sk", LongType),
        StructField("ws_order_number", LongType),
        StructField("ws_quantity", LongType),
        StructField("ws_wholesale_cost", DecimalType(7, 2)),
        StructField("ws_list_price", DecimalType(7, 2)),
        StructField("ws_sales_price", DecimalType(7, 2)),
        StructField("ws_ext_discount_amt", DecimalType(7, 2)),
        StructField("ws_ext_sales_price", DecimalType(7, 2)),
        StructField("ws_ext_wholesale_cost", DecimalType(7, 2)),
        StructField("ws_ext_list_price", DecimalType(7, 2)),
        StructField("ws_ext_tax", DecimalType(7, 2)),
        StructField("ws_coupon_amt", DecimalType(7, 2)),
        StructField("ws_ext_ship_cost", DecimalType(7, 2)),
        StructField("ws_net_paid", DecimalType(7, 2)),
        StructField("ws_net_paid_inc_tax", DecimalType(7, 2)),
        StructField("ws_net_paid_inc_ship", DecimalType(7, 2)),
        StructField("ws_net_paid_inc_ship_tax", DecimalType(7, 2)),
        StructField("ws_net_profit", DecimalType(7, 2))
      ))
  }

  private def webReturnsSchema = {
    StructType(
      Seq(
        StructField("wr_returned_date_sk", LongType),
        StructField("wr_returned_time_sk", LongType),
        StructField("wr_item_sk", LongType),
        StructField("wr_refunded_customer_sk", LongType),
        StructField("wr_refunded_cdemo_sk", LongType),
        StructField("wr_refunded_hdemo_sk", LongType),
        StructField("wr_refunded_addr_sk", LongType),
        StructField("wr_returning_customer_sk", LongType),
        StructField("wr_returning_cdemo_sk", LongType),
        StructField("wr_returning_hdemo_sk", LongType),
        StructField("wr_returning_addr_sk", LongType),
        StructField("wr_web_page_sk", LongType),
        StructField("wr_reason_sk", LongType),
        StructField("wr_order_number", LongType),
        StructField("wr_return_quantity", LongType),
        StructField("wr_return_amt", DecimalType(7, 2)),
        StructField("wr_return_tax", DecimalType(7, 2)),
        StructField("wr_return_amt_inc_tax", DecimalType(7, 2)),
        StructField("wr_fee", DecimalType(7, 2)),
        StructField("wr_return_ship_cost", DecimalType(7, 2)),
        StructField("wr_refunded_cash", DecimalType(7, 2)),
        StructField("wr_reversed_charge", DecimalType(7, 2)),
        StructField("wr_account_credit", DecimalType(7, 2)),
        StructField("wr_net_loss", DecimalType(7, 2))
      ))
  }

  private def callCenterSchema = {
    StructType(
      Seq(
        StructField("cc_call_center_sk", LongType),
        StructField("cc_call_center_id", StringType),
        StructField("cc_rec_start_date", DateType),
        StructField("cc_rec_end_date", DateType),
        StructField("cc_closed_date_sk", LongType),
        StructField("cc_open_date_sk", LongType),
        StructField("cc_name", StringType),
        StructField("cc_class", StringType),
        StructField("cc_employees", LongType),
        StructField("cc_sq_ft", LongType),
        StructField("cc_hours", StringType),
        StructField("cc_manager", StringType),
        StructField("cc_mkt_id", LongType),
        StructField("cc_mkt_class", StringType),
        StructField("cc_mkt_desc", StringType),
        StructField("cc_market_manager", StringType),
        StructField("cc_division", LongType),
        StructField("cc_division_name", StringType),
        StructField("cc_company", LongType),
        StructField("cc_company_name", StringType),
        StructField("cc_street_number", StringType),
        StructField("cc_street_name", StringType),
        StructField("cc_street_type", StringType),
        StructField("cc_suite_number", StringType),
        StructField("cc_city", StringType),
        StructField("cc_county", StringType),
        StructField("cc_state", StringType),
        StructField("cc_zip", StringType),
        StructField("cc_country", StringType),
        StructField("cc_gmt_offset", DecimalType(5, 2)),
        StructField("cc_tax_percentage", DecimalType(5, 2))
      ))
  }

  private def catalogPageSchema = {
    StructType(
      Seq(
        StructField("cp_catalog_page_sk", LongType),
        StructField("cp_catalog_page_id", StringType),
        StructField("cp_start_date_sk", LongType),
        StructField("cp_end_date_sk", LongType),
        StructField("cp_department", StringType),
        StructField("cp_catalog_number", LongType),
        StructField("cp_catalog_page_number", LongType),
        StructField("cp_description", StringType),
        StructField("cp_type", StringType)
      ))
  }

  private def customerSchema = {
    StructType(
      Seq(
        StructField("c_customer_sk", LongType),
        StructField("c_customer_id", StringType),
        StructField("c_current_cdemo_sk", LongType),
        StructField("c_current_hdemo_sk", LongType),
        StructField("c_current_addr_sk", LongType),
        StructField("c_first_shipto_date_sk", LongType),
        StructField("c_first_sales_date_sk", LongType),
        StructField("c_salutation", StringType),
        StructField("c_first_name", StringType),
        StructField("c_last_name", StringType),
        StructField("c_preferred_cust_flag", StringType),
        StructField("c_birth_day", LongType),
        StructField("c_birth_month", LongType),
        StructField("c_birth_year", LongType),
        StructField("c_birth_country", StringType),
        StructField("c_login", StringType),
        StructField("c_email_address", StringType),
        StructField("c_last_review_date", StringType)
      ))
  }

  private def customerAddressSchema = {
    StructType(
      Seq(
        StructField("ca_address_sk", LongType),
        StructField("ca_address_id", StringType),
        StructField("ca_street_number", StringType),
        StructField("ca_street_name", StringType),
        StructField("ca_street_type", StringType),
        StructField("ca_suite_number", StringType),
        StructField("ca_city", StringType),
        StructField("ca_county", StringType),
        StructField("ca_state", StringType),
        StructField("ca_zip", StringType),
        StructField("ca_country", StringType),
        StructField("ca_gmt_offset", DecimalType(5, 2)),
        StructField("ca_location_type", StringType)
      ))
  }

  private def customerDemographicsSchema = {
    StructType(
      Seq(
        StructField("cd_demo_sk", LongType),
        StructField("cd_gender", StringType),
        StructField("cd_marital_status", StringType),
        StructField("cd_education_status", StringType),
        StructField("cd_purchase_estimate", LongType),
        StructField("cd_credit_rating", StringType),
        StructField("cd_dep_count", LongType),
        StructField("cd_dep_employed_count", LongType),
        StructField("cd_dep_college_count", LongType)
      ))
  }

  private def dateDimSchema = {
    StructType(
      Seq(
        StructField("d_date_sk", LongType),
        StructField("d_date_id", StringType),
        StructField("d_date", DateType),
        StructField("d_month_seq", LongType),
        StructField("d_week_seq", LongType),
        StructField("d_quarter_seq", LongType),
        StructField("d_year", LongType),
        StructField("d_dow", LongType),
        StructField("d_moy", LongType),
        StructField("d_dom", LongType),
        StructField("d_qoy", LongType),
        StructField("d_fy_year", LongType),
        StructField("d_fy_quarter_seq", LongType),
        StructField("d_fy_week_seq", LongType),
        StructField("d_day_name", StringType),
        StructField("d_quarter_name", StringType),
        StructField("d_holiday", StringType),
        StructField("d_weekend", StringType),
        StructField("d_following_holiday", StringType),
        StructField("d_first_dom", LongType),
        StructField("d_last_dom", LongType),
        StructField("d_same_day_ly", LongType),
        StructField("d_same_day_lq", LongType),
        StructField("d_current_day", StringType),
        StructField("d_current_week", StringType),
        StructField("d_current_month", StringType),
        StructField("d_current_quarter", StringType),
        StructField("d_current_year", StringType)
      ))
  }

  private def householdDemographicsSchema = {
    StructType(
      Seq(
        StructField("hd_demo_sk", LongType),
        StructField("hd_income_band_sk", LongType),
        StructField("hd_buy_potential", StringType),
        StructField("hd_dep_count", LongType),
        StructField("hd_vehicle_count", LongType)
      ))
  }

  private def incomeBandSchema = {
    StructType(
      Seq(
        StructField("ib_income_band_sk", LongType),
        StructField("ib_lower_bound", LongType),
        StructField("ib_upper_bound", LongType)))
  }

  private def itemSchema = {
    StructType(
      Seq(
        StructField("i_item_sk", LongType),
        StructField("i_item_id", StringType),
        StructField("i_rec_start_date", DateType),
        StructField("i_rec_end_date", DateType),
        StructField("i_item_desc", StringType),
        StructField("i_current_price", DecimalType(7, 2)),
        StructField("i_wholesale_cost", DecimalType(7, 2)),
        StructField("i_brand_id", LongType),
        StructField("i_brand", StringType),
        StructField("i_class_id", LongType),
        StructField("i_class", StringType),
        StructField("i_category_id", LongType),
        StructField("i_category", StringType),
        StructField("i_manufact_id", LongType),
        StructField("i_manufact", StringType),
        StructField("i_size", StringType),
        StructField("i_formulation", StringType),
        StructField("i_color", StringType),
        StructField("i_units", StringType),
        StructField("i_container", StringType),
        StructField("i_manager_id", LongType),
        StructField("i_product_name", StringType)
      ))
  }

  private def promotionSchema = {
    StructType(
      Seq(
        StructField("p_promo_sk", LongType),
        StructField("p_promo_id", StringType),
        StructField("p_start_date_sk", LongType),
        StructField("p_end_date_sk", LongType),
        StructField("p_item_sk", LongType),
        StructField("p_cost", DecimalType(15, 2)),
        StructField("p_response_target", LongType),
        StructField("p_promo_name", StringType),
        StructField("p_channel_dmail", StringType),
        StructField("p_channel_email", StringType),
        StructField("p_channel_catalog", StringType),
        StructField("p_channel_tv", StringType),
        StructField("p_channel_radio", StringType),
        StructField("p_channel_press", StringType),
        StructField("p_channel_event", StringType),
        StructField("p_channel_demo", StringType),
        StructField("p_channel_details", StringType),
        StructField("p_purpose", StringType),
        StructField("p_discount_active", StringType)
      ))
  }

  private def reasonSchema = {
    StructType(
      Seq(
        StructField("r_reason_sk", LongType),
        StructField("r_reason_id", StringType),
        StructField("r_reason_desc", StringType)))
  }

  private def shipModeSchema = {
    StructType(
      Seq(
        StructField("sm_ship_mode_sk", LongType),
        StructField("sm_ship_mode_id", StringType),
        StructField("sm_type", StringType),
        StructField("sm_code", StringType),
        StructField("sm_carrier", StringType),
        StructField("sm_contract", StringType)
      ))
  }

  private def storeSchema = {
    StructType(
      Seq(
        StructField("s_store_sk", LongType),
        StructField("s_store_id", StringType),
        StructField("s_rec_start_date", DateType),
        StructField("s_rec_end_date", DateType),
        StructField("s_closed_date_sk", LongType),
        StructField("s_store_name", StringType),
        StructField("s_number_employees", LongType),
        StructField("s_floor_space", LongType),
        StructField("s_hours", StringType),
        StructField("s_manager", StringType),
        StructField("s_market_id", LongType),
        StructField("s_geography_class", StringType),
        StructField("s_market_desc", StringType),
        StructField("s_market_manager", StringType),
        StructField("s_division_id", LongType),
        StructField("s_division_name", StringType),
        StructField("s_company_id", LongType),
        StructField("s_company_name", StringType),
        StructField("s_street_number", StringType),
        StructField("s_street_name", StringType),
        StructField("s_street_type", StringType),
        StructField("s_suite_number", StringType),
        StructField("s_city", StringType),
        StructField("s_county", StringType),
        StructField("s_state", StringType),
        StructField("s_zip", StringType),
        StructField("s_country", StringType),
        StructField("s_gmt_offset", DecimalType(5, 2)),
        StructField("s_tax_precentage", DecimalType(5, 2))
      ))
  }

  private def timeDimSchema = {
    StructType(
      Seq(
        StructField("t_time_sk", LongType),
        StructField("t_time_id", StringType),
        StructField("t_time", LongType),
        StructField("t_hour", LongType),
        StructField("t_minute", LongType),
        StructField("t_second", LongType),
        StructField("t_am_pm", StringType),
        StructField("t_shift", StringType),
        StructField("t_sub_shift", StringType),
        StructField("t_meal_time", StringType)
      ))
  }

  private def warehouseSchema = {
    StructType(
      Seq(
        StructField("w_warehouse_sk", LongType),
        StructField("w_warehouse_id", StringType),
        StructField("w_warehouse_name", StringType),
        StructField("w_warehouse_sq_ft", LongType),
        StructField("w_street_number", StringType),
        StructField("w_street_name", StringType),
        StructField("w_street_type", StringType),
        StructField("w_suite_number", StringType),
        StructField("w_city", StringType),
        StructField("w_county", StringType),
        StructField("w_state", StringType),
        StructField("w_zip", StringType),
        StructField("w_country", StringType),
        StructField("w_gmt_offset", DecimalType(5, 2))
      ))
  }

  private def webPageSchema = {
    StructType(
      Seq(
        StructField("wp_web_page_sk", LongType),
        StructField("wp_web_page_id", StringType),
        StructField("wp_rec_start_date", DateType),
        StructField("wp_rec_end_date", DateType),
        StructField("wp_creation_date_sk", LongType),
        StructField("wp_access_date_sk", LongType),
        StructField("wp_autogen_flag", StringType),
        StructField("wp_customer_sk", LongType),
        StructField("wp_url", StringType),
        StructField("wp_type", StringType),
        StructField("wp_char_count", LongType),
        StructField("wp_link_count", LongType),
        StructField("wp_image_count", LongType),
        StructField("wp_max_ad_count", LongType)
      ))
  }

  private def webSiteSchema = {
    StructType(
      Seq(
        StructField("web_site_sk", LongType),
        StructField("web_site_id", StringType),
        StructField("web_rec_start_date", DateType),
        StructField("web_rec_end_date", DateType),
        StructField("web_name", StringType),
        StructField("web_open_date_sk", LongType),
        StructField("web_close_date_sk", LongType),
        StructField("web_class", StringType),
        StructField("web_manager", StringType),
        StructField("web_mkt_id", LongType),
        StructField("web_mkt_class", StringType),
        StructField("web_mkt_desc", StringType),
        StructField("web_market_manager", StringType),
        StructField("web_company_id", LongType),
        StructField("web_company_name", StringType),
        StructField("web_street_number", StringType),
        StructField("web_street_name", StringType),
        StructField("web_street_type", StringType),
        StructField("web_suite_number", StringType),
        StructField("web_city", StringType),
        StructField("web_county", StringType),
        StructField("web_state", StringType),
        StructField("web_zip", StringType),
        StructField("web_country", StringType),
        StructField("web_gmt_offset", StringType),
        StructField("web_tax_percentage", DecimalType(5, 2))
      ))
  }
}
