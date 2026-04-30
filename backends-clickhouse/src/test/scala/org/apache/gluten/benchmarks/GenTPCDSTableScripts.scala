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
package org.apache.gluten.benchmarks

import org.apache.spark.sql.TPCDSSchema

import scala.collection.mutable.ArrayBuffer

class TPCDSSchemaProvider extends TPCDSSchema {
  def getTableSchema: Map[String, String] = tableColumns
}
object GenTPCDSTableScripts {
  val providerInSpark = new TPCDSSchemaProvider()

  // version 0: tpcds schema in vanilla spark TPCDSSchema,
  //            using char(n)/varchar(n) for strings, and Decimal for floats
  // version 1: using String for strings, and double for floats
  //            (because CH backed has not fully supported Decimal)
  // version 2: using char(n)/varchar(n) for strings, and double for floats.
  def getTableSchema(schemaVersion: Int): Map[String, String] = {
    schemaVersion match {
      case 0 => providerInSpark.getTableSchema
      case 1 => customizedTableColumnsV1(false)
      case 2 => customizedTableColumnsV2
      case 3 => customizedTableColumnsV1(true)
      case _ => throw new IllegalArgumentException("Unsupported schema version: " + schemaVersion)
    }
  }

  private def customizedTableColumnsV1(decimal: Boolean): Map[String, String] = {
    val common_float_type = if (decimal) {
      "decimal(7,2)"
    } else {
      "DOUBLE"
    }

    val common_float_type2 = if (decimal) {
      "decimal(5,2)"
    } else {
      "DOUBLE"
    }

    val common_float_type3 = if (decimal) {
      "decimal(15,2)"
    } else {
      "DOUBLE"
    }

    Map(
      "catalog_sales" ->
        s"""
           |  cs_sold_date_sk          INT,
           |  cs_sold_time_sk          INT,
           |  cs_ship_date_sk          INT,
           |  cs_bill_customer_sk      INT,
           |  cs_bill_cdemo_sk         INT,
           |  cs_bill_hdemo_sk         INT,
           |  cs_bill_addr_sk          INT,
           |  cs_ship_customer_sk      INT,
           |  cs_ship_cdemo_sk         INT,
           |  cs_ship_hdemo_sk         INT,
           |  cs_ship_addr_sk          INT,
           |  cs_call_center_sk        INT,
           |  cs_catalog_page_sk       INT,
           |  cs_ship_mode_sk          INT,
           |  cs_warehouse_sk          INT,
           |  cs_item_sk               INT,
           |  cs_promo_sk              INT,
           |  cs_order_number          LONG,
           |  cs_quantity              INT,
           |  cs_wholesale_cost        $common_float_type,
           |  cs_list_price            $common_float_type,
           |  cs_sales_price           $common_float_type,
           |  cs_ext_discount_amt      $common_float_type,
           |  cs_ext_sales_price       $common_float_type,
           |  cs_ext_wholesale_cost    $common_float_type,
           |  cs_ext_list_price        $common_float_type,
           |  cs_ext_tax               $common_float_type,
           |  cs_coupon_amt            $common_float_type,
           |  cs_ext_ship_cost         $common_float_type,
           |  cs_net_paid              $common_float_type,
           |  cs_net_paid_inc_tax      $common_float_type,
           |  cs_net_paid_inc_ship     $common_float_type,
           |  cs_net_paid_inc_ship_tax $common_float_type,
           |  cs_net_profit            $common_float_type
           |""".stripMargin,
      "catalog_returns" ->
        s"""
           |  cr_returned_date_sk      INT,
           |  cr_returned_time_sk      INT,
           |  cr_item_sk               INT,
           |  cr_refunded_customer_sk  INT,
           |  cr_refunded_cdemo_sk     INT,
           |  cr_refunded_hdemo_sk     INT,
           |  cr_refunded_addr_sk      INT,
           |  cr_returning_customer_sk INT,
           |  cr_returning_cdemo_sk    INT,
           |  cr_returning_hdemo_sk    INT,
           |  cr_returning_addr_sk     INT,
           |  cr_call_center_sk        INT,
           |  cr_catalog_page_sk       INT,
           |  cr_ship_mode_sk          INT,
           |  cr_warehouse_sk          INT,
           |  cr_reason_sk             INT,
           |  cr_order_number          LONG,
           |  cr_return_quantity       INT,
           |  cr_return_amount         $common_float_type,
           |  cr_return_tax            $common_float_type,
           |  cr_return_amt_inc_tax    $common_float_type,
           |  cr_fee                   $common_float_type,
           |  cr_return_ship_cost      $common_float_type,
           |  cr_refunded_cash         $common_float_type,
           |  cr_reversed_charge       $common_float_type,
           |  cr_store_credit          $common_float_type,
           |  cr_net_loss              $common_float_type
           |""".stripMargin,
      "inventory" ->
        s"""
           |  inv_date_sk          INT,
           |  inv_item_sk          INT,
           |  inv_warehouse_sk     INT,
           |  inv_quantity_on_hand INT
           |""".stripMargin,
      "store_sales" ->
        s"""
           |  ss_sold_date_sk      INT,
           |  ss_sold_time_sk      INT,
           |  ss_item_sk           INT,
           |  ss_customer_sk       INT,
           |  ss_cdemo_sk          INT,
           |  ss_hdemo_sk          INT,
           |  ss_addr_sk           INT,
           |  ss_store_sk          INT,
           |  ss_promo_sk          INT,
           |  ss_ticket_number     LONG,
           |  ss_quantity          INT,
           |  ss_wholesale_cost     $common_float_type,
           |  ss_list_price         $common_float_type,
           |  ss_sales_price        $common_float_type,
           |  ss_ext_discount_amt   $common_float_type,
           |  ss_ext_sales_price    $common_float_type,
           |  ss_ext_wholesale_cost $common_float_type,
           |  ss_ext_list_price     $common_float_type,
           |  ss_ext_tax            $common_float_type,
           |  ss_coupon_amt         $common_float_type,
           |  ss_net_paid           $common_float_type,
           |  ss_net_paid_inc_tax   $common_float_type,
           |  ss_net_profit         $common_float_type
           |""".stripMargin,
      "store_returns" ->
        s"""
           |  sr_returned_date_sk  INT,
           |  sr_return_time_sk    INT,
           |  sr_item_sk           INT,
           |  sr_customer_sk       INT,
           |  sr_cdemo_sk          INT,
           |  sr_hdemo_sk          INT,
           |  sr_addr_sk           INT,
           |  sr_store_sk          INT,
           |  sr_reason_sk         INT,
           |  sr_ticket_number     LONG,
           |  sr_return_quantity   INT,
           |  sr_return_amt         $common_float_type,
           |  sr_return_tax         $common_float_type,
           |  sr_return_amt_inc_tax $common_float_type,
           |  sr_fee                $common_float_type,
           |  sr_return_ship_cost   $common_float_type,
           |  sr_refunded_cash      $common_float_type,
           |  sr_reversed_charge    $common_float_type,
           |  sr_store_credit       $common_float_type,
           |  sr_net_loss           $common_float_type
           |""".stripMargin,
      "web_sales" ->
        s"""
           |  ws_sold_date_sk          INT,
           |  ws_sold_time_sk          INT,
           |  ws_ship_date_sk          INT,
           |  ws_item_sk               INT,
           |  ws_bill_customer_sk      INT,
           |  ws_bill_cdemo_sk         INT,
           |  ws_bill_hdemo_sk         INT,
           |  ws_bill_addr_sk          INT,
           |  ws_ship_customer_sk      INT,
           |  ws_ship_cdemo_sk         INT,
           |  ws_ship_hdemo_sk         INT,
           |  ws_ship_addr_sk          INT,
           |  ws_web_page_sk           INT,
           |  ws_web_site_sk           INT,
           |  ws_ship_mode_sk          INT,
           |  ws_warehouse_sk          INT,
           |  ws_promo_sk              INT,
           |  ws_order_number          LONG,
           |  ws_quantity              INT,
           |  ws_wholesale_cost        $common_float_type,
           |  ws_list_price            $common_float_type,
           |  ws_sales_price           $common_float_type,
           |  ws_ext_discount_amt      $common_float_type,
           |  ws_ext_sales_price       $common_float_type,
           |  ws_ext_wholesale_cost    $common_float_type,
           |  ws_ext_list_price        $common_float_type,
           |  ws_ext_tax               $common_float_type,
           |  ws_coupon_amt            $common_float_type,
           |  ws_ext_ship_cost         $common_float_type,
           |  ws_net_paid              $common_float_type,
           |  ws_net_paid_inc_tax      $common_float_type,
           |  ws_net_paid_inc_ship     $common_float_type,
           |  ws_net_paid_inc_ship_tax $common_float_type,
           |  ws_net_profit            $common_float_type
           |""".stripMargin,
      "web_returns" ->
        s"""
           |  wr_returned_date_sk       INT,
           |  wr_returned_time_sk       INT,
           |  wr_item_sk                INT,
           |  wr_refunded_customer_sk   INT,
           |  wr_refunded_cdemo_sk      INT,
           |  wr_refunded_hdemo_sk      INT,
           |  wr_refunded_addr_sk       INT,
           |  wr_returning_customer_sk  INT,
           |  wr_returning_cdemo_sk     INT,
           |  wr_returning_hdemo_sk     INT,
           |  wr_returning_addr_sk      INT,
           |  wr_web_page_sk            INT,
           |  wr_reason_sk              INT,
           |  wr_order_number           LONG,
           |  wr_return_quantity        INT,
           |  wr_return_amt             $common_float_type,
           |  wr_return_tax             $common_float_type,
           |  wr_return_amt_inc_tax     $common_float_type,
           |  wr_fee                    $common_float_type,
           |  wr_return_ship_cost       $common_float_type,
           |  wr_refunded_cash          $common_float_type,
           |  wr_reversed_charge        $common_float_type,
           |  wr_account_credit         $common_float_type,
           |  wr_net_loss               $common_float_type
           |""".stripMargin,
      "call_center" ->
        s"""
           |  cc_call_center_sk        INT,
           |  cc_call_center_id        STRING,
           |  cc_rec_start_date        DATE,
           |  cc_rec_end_date          DATE,
           |  cc_closed_date_sk        INT,
           |  cc_open_date_sk          INT,
           |  cc_name                  STRING,
           |  cc_class                 STRING,
           |  cc_employees             INT,
           |  cc_sq_ft                 INT,
           |  cc_hours                 STRING,
           |  cc_manager               STRING,
           |  cc_mkt_id                INT,
           |  cc_mkt_class             STRING,
           |  cc_mkt_desc              STRING,
           |  cc_market_manager        STRING,
           |  cc_division              INT,
           |  cc_division_name         STRING,
           |  cc_company               INT,
           |  cc_company_name          STRING,
           |  cc_street_number         STRING,
           |  cc_street_name           STRING,
           |  cc_street_type           STRING,
           |  cc_suite_number          STRING,
           |  cc_city                  STRING,
           |  cc_county                STRING,
           |  cc_state                 STRING,
           |  cc_zip                   STRING,
           |  cc_country               STRING,
           |  cc_gmt_offset            $common_float_type2,
           |  cc_tax_percentage        $common_float_type2
           |""".stripMargin,
      "catalog_page" ->
        s"""
           |  cp_catalog_page_sk       int,
           |  cp_catalog_page_id       string,
           |  cp_start_date_sk         int,
           |  cp_end_date_sk           int,
           |  cp_department            string,
           |  cp_catalog_number        int,
           |  cp_catalog_page_number   int,
           |  cp_description           string,
           |  cp_type                  string
           |""".stripMargin,
      "customer" ->
        s"""
           |  c_customer_sk             int,
           |  c_customer_id             string,
           |  c_current_cdemo_sk        int,
           |  c_current_hdemo_sk        int,
           |  c_current_addr_sk         int,
           |  c_first_shipto_date_sk    int,
           |  c_first_sales_date_sk     int,
           |  c_salutation              string,
           |  c_first_name              string,
           |  c_last_name               string,
           |  c_preferred_cust_flag     string,
           |  c_birth_day               int,
           |  c_birth_month             int,
           |  c_birth_year              int,
           |  c_birth_country           string,
           |  c_login                   string,
           |  c_email_address           string,
           |  c_last_review_date        string
           |""".stripMargin,
      "customer_address" ->
        s"""
           |  ca_address_sk             int,
           |  ca_address_id             string,
           |  ca_street_number          string,
           |  ca_street_name            string,
           |  ca_street_type            string,
           |  ca_suite_number           string,
           |  ca_city                   string,
           |  ca_county                 string,
           |  ca_state                  string,
           |  ca_zip                    string,
           |  ca_country                string,
           |  ca_gmt_offset             $common_float_type2,
           |  ca_location_type          string
           |""".stripMargin,
      "customer_demographics" ->
        s"""
           |  cd_demo_sk                int,
           |  cd_gender                 string,
           |  cd_marital_status         string,
           |  cd_education_status       string,
           |  cd_purchase_estimate      int,
           |  cd_credit_rating          string,
           |  cd_dep_count              int,
           |  cd_dep_employed_count     int,
           |  cd_dep_college_count      int
           |""".stripMargin,
      "date_dim" ->
        s"""
           |  d_date_sk                 int,
           |  d_date_id                 string,
           |  d_date                    date,
           |  d_month_seq               int,
           |  d_week_seq                int,
           |  d_quarter_seq             int,
           |  d_year                    int,
           |  d_dow                     int,
           |  d_moy                     int,
           |  d_dom                     int,
           |  d_qoy                     int,
           |  d_fy_year                 int,
           |  d_fy_quarter_seq          int,
           |  d_fy_week_seq             int,
           |  d_day_name                string,
           |  d_quarter_name            string,
           |  d_holiday                 string,
           |  d_weekend                 string,
           |  d_following_holiday       string,
           |  d_first_dom               int,
           |  d_last_dom                int,
           |  d_same_day_ly             int,
           |  d_same_day_lq             int,
           |  d_current_day             string,
           |  d_current_week            string,
           |  d_current_month           string,
           |  d_current_quarter         string,
           |  d_current_year            string
           |""".stripMargin,
      "household_demographics" ->
        s"""
           |  hd_demo_sk                int,
           |  hd_income_band_sk         int,
           |  hd_buy_potential          string,
           |  hd_dep_count              int,
           |  hd_vehicle_count          int
           |""".stripMargin,
      "income_band" ->
        s"""
           |  ib_income_band_sk         int,
           |  ib_lower_bound            int,
           |  ib_upper_bound            int
           |""".stripMargin,
      "item" ->
        s"""
           |  i_item_sk                 int,
           |  i_item_id                 string,
           |  i_rec_start_date          date,
           |  i_rec_end_date            date,
           |  i_item_desc               string,
           |  i_current_price           $common_float_type,
           |  i_wholesale_cost          $common_float_type,
           |  i_brand_id                int,
           |  i_brand                   string,
           |  i_class_id                int,
           |  i_class                   string,
           |  i_category_id             int,
           |  i_category                string,
           |  i_manufact_id             int,
           |  i_manufact                string,
           |  i_size                    string,
           |  i_formulation             string,
           |  i_color                   string,
           |  i_units                   string,
           |  i_container               string,
           |  i_manager_id              int,
           |  i_product_name            string
           |""".stripMargin,
      "promotion" ->
        s"""
           |  p_promo_sk                int,
           |  p_promo_id                string,
           |  p_start_date_sk           int,
           |  p_end_date_sk             int,
           |  p_item_sk                 int,
           |  p_cost                    $common_float_type3,
           |  p_response_target         int,
           |  p_promo_name              string,
           |  p_channel_dmail           string,
           |  p_channel_email           string,
           |  p_channel_catalog         string,
           |  p_channel_tv              string,
           |  p_channel_radio           string,
           |  p_channel_press           string,
           |  p_channel_event           string,
           |  p_channel_demo            string,
           |  p_channel_details         string,
           |  p_purpose                 string,
           |  p_discount_active         string
           |""".stripMargin,
      "reason" ->
        s"""
           |  r_reason_sk               int,
           |  r_reason_id               string,
           |  r_reason_desc             string
           |""".stripMargin,
      "ship_mode" ->
        s"""
           |  sm_ship_mode_sk           int,
           |  sm_ship_mode_id           string,
           |  sm_type                   string,
           |  sm_code                   string,
           |  sm_carrier                string,
           |  sm_contract               string
           |""".stripMargin,
      "store" ->
        s"""
           |  s_store_sk                int,
           |  s_store_id                string,
           |  s_rec_start_date          date,
           |  s_rec_end_date            date,
           |  s_closed_date_sk          int,
           |  s_store_name              string,
           |  s_number_employees        int,
           |  s_floor_space             int,
           |  s_hours                   string,
           |  s_manager                 string,
           |  s_market_id               int,
           |  s_geography_class         string,
           |  s_market_desc             string,
           |  s_market_manager          string,
           |  s_division_id             int,
           |  s_division_name           string,
           |  s_company_id              int,
           |  s_company_name            string,
           |  s_street_number           string,
           |  s_street_name             string,
           |  s_street_type             string,
           |  s_suite_number            string,
           |  s_city                    string,
           |  s_county                  string,
           |  s_state                   string,
           |  s_zip                     string,
           |  s_country                 string,
           |  s_gmt_offset              $common_float_type2,
           |  s_tax_precentage          $common_float_type2
           |""".stripMargin,
      "time_dim" ->
        s"""
           |  t_time_sk                 int,
           |  t_time_id                 string,
           |  t_time                    int,
           |  t_hour                    int,
           |  t_minute                  int,
           |  t_second                  int,
           |  t_am_pm                   string,
           |  t_shift                   string,
           |  t_sub_shift               string,
           |  t_meal_time               string
           |""".stripMargin,
      "warehouse" ->
        s"""
           |  w_warehouse_sk           int,
           |  w_warehouse_id           string,
           |  w_warehouse_name         string,
           |  w_warehouse_sq_ft        int,
           |  w_street_number          string,
           |  w_street_name            string,
           |  w_street_type            string,
           |  w_suite_number           string,
           |  w_city                   string,
           |  w_county                 string,
           |  w_state                  string,
           |  w_zip                    string,
           |  w_country                string,
           |  w_gmt_offset             $common_float_type2
           |""".stripMargin,
      "web_page" ->
        s"""
           |  wp_web_page_sk           int,
           |  wp_web_page_id           string,
           |  wp_rec_start_date        date,
           |  wp_rec_end_date          date,
           |  wp_creation_date_sk      int,
           |  wp_access_date_sk        int,
           |  wp_autogen_flag          string,
           |  wp_customer_sk           int,
           |  wp_url                   string,
           |  wp_type                  string,
           |  wp_char_count            int,
           |  wp_link_count            int,
           |  wp_image_count           int,
           |  wp_max_ad_count          int
           |""".stripMargin,
      "web_site" ->
        s"""
           |  web_site_sk              int,
           |  web_site_id              string,
           |  web_rec_start_date       date,
           |  web_rec_end_date         date,
           |  web_name                 string,
           |  web_open_date_sk         int,
           |  web_close_date_sk        int,
           |  web_class                string,
           |  web_manager              string,
           |  web_mkt_id               int,
           |  web_mkt_class            string,
           |  web_mkt_desc             string,
           |  web_market_manager       string,
           |  web_company_id           int,
           |  web_company_name         string,
           |  web_street_number        string,
           |  web_street_name          string,
           |  web_street_type          string,
           |  web_suite_number         string,
           |  web_city                 string,
           |  web_county               string,
           |  web_state                string,
           |  web_zip                  string,
           |  web_country              string,
           |  web_gmt_offset           $common_float_type2,
           |  web_tax_percentage       $common_float_type2
           |""".stripMargin
    )
  }

  private val customizedTableColumnsV2: Map[String, String] = Map(
    "store_sales" ->
      """
        |`ss_sold_date_sk` INT,
        |`ss_sold_time_sk` INT,
        |`ss_item_sk` INT,
        |`ss_customer_sk` INT,
        |`ss_cdemo_sk` INT,
        |`ss_hdemo_sk` INT,
        |`ss_addr_sk` INT,
        |`ss_store_sk` INT,
        |`ss_promo_sk` INT,
        |`ss_ticket_number` INT,
        |`ss_quantity` INT,
        |`ss_wholesale_cost` DOUBLE,
        |`ss_list_price` DOUBLE,
        |`ss_sales_price` DOUBLE,
        |`ss_ext_discount_amt` DOUBLE,
        |`ss_ext_sales_price` DOUBLE,
        |`ss_ext_wholesale_cost` DOUBLE,
        |`ss_ext_list_price` DOUBLE,
        |`ss_ext_tax` DOUBLE,
        |`ss_coupon_amt` DOUBLE,
        |`ss_net_paid` DOUBLE,
        |`ss_net_paid_inc_tax` DOUBLE,
        |`ss_net_profit` DOUBLE
        """.stripMargin,
    "store_returns" ->
      """
        |`sr_returned_date_sk` INT,
        |`sr_return_time_sk` INT,
        |`sr_item_sk` INT,
        |`sr_customer_sk` INT,
        |`sr_cdemo_sk` INT,
        |`sr_hdemo_sk` INT,
        |`sr_addr_sk` INT,
        |`sr_store_sk` INT,
        |`sr_reason_sk` INT,
        |`sr_ticket_number` INT,
        |`sr_return_quantity` INT,
        |`sr_return_amt` DOUBLE,
        |`sr_return_tax` DOUBLE,
        |`sr_return_amt_inc_tax` DOUBLE,
        |`sr_fee` DOUBLE,
        |`sr_return_ship_cost` DOUBLE,
        |`sr_refunded_cash` DOUBLE,
        |`sr_reversed_charge` DOUBLE,
        |`sr_store_credit` DOUBLE,
        |`sr_net_loss` DOUBLE
        """.stripMargin,
    "catalog_sales" ->
      """
        |`cs_sold_date_sk` INT,
        |`cs_sold_time_sk` INT,
        |`cs_ship_date_sk` INT,
        |`cs_bill_customer_sk` INT,
        |`cs_bill_cdemo_sk` INT,
        |`cs_bill_hdemo_sk` INT,
        |`cs_bill_addr_sk` INT,
        |`cs_ship_customer_sk` INT,
        |`cs_ship_cdemo_sk` INT,
        |`cs_ship_hdemo_sk` INT,
        |`cs_ship_addr_sk` INT,
        |`cs_call_center_sk` INT,
        |`cs_catalog_page_sk` INT,
        |`cs_ship_mode_sk` INT,
        |`cs_warehouse_sk` INT,
        |`cs_item_sk` INT,
        |`cs_promo_sk` INT,
        |`cs_order_number` INT,
        |`cs_quantity` INT,
        |`cs_wholesale_cost` DOUBLE,
        |`cs_list_price` DOUBLE,
        |`cs_sales_price` DOUBLE,
        |`cs_ext_discount_amt` DOUBLE,
        |`cs_ext_sales_price` DOUBLE,
        |`cs_ext_wholesale_cost` DOUBLE,
        |`cs_ext_list_price` DOUBLE,
        |`cs_ext_tax` DOUBLE,
        |`cs_coupon_amt` DOUBLE,
        |`cs_ext_ship_cost` DOUBLE,
        |`cs_net_paid` DOUBLE,
        |`cs_net_paid_inc_tax` DOUBLE,
        |`cs_net_paid_inc_ship` DOUBLE,
        |`cs_net_paid_inc_ship_tax` DOUBLE,
        |`cs_net_profit` DOUBLE
        """.stripMargin,
    "catalog_returns" ->
      """
        |`cr_returned_date_sk` INT,
        |`cr_returned_time_sk` INT,
        |`cr_item_sk` INT,
        |`cr_refunded_customer_sk` INT,
        |`cr_refunded_cdemo_sk` INT,
        |`cr_refunded_hdemo_sk` INT,
        |`cr_refunded_addr_sk` INT,
        |`cr_returning_customer_sk` INT,
        |`cr_returning_cdemo_sk` INT,
        |`cr_returning_hdemo_sk` INT,
        |`cr_returning_addr_sk` INT,
        |`cr_call_center_sk` INT,
        |`cr_catalog_page_sk` INT,
        |`cr_ship_mode_sk` INT,
        |`cr_warehouse_sk` INT,
        |`cr_reason_sk` INT,`cr_order_number` INT,
        |`cr_return_quantity` INT,
        |`cr_return_amount` DOUBLE,
        |`cr_return_tax` DOUBLE,
        |`cr_return_amt_inc_tax` DOUBLE,
        |`cr_fee` DOUBLE,
        |`cr_return_ship_cost` DOUBLE,
        |`cr_refunded_cash` DOUBLE,
        |`cr_reversed_charge` DOUBLE,
        |`cr_store_credit` DOUBLE,
        |`cr_net_loss` DOUBLE
        """.stripMargin,
    "web_sales" ->
      """
        |`ws_sold_date_sk` INT,
        |`ws_sold_time_sk` INT,
        |`ws_ship_date_sk` INT,
        |`ws_item_sk` INT,
        |`ws_bill_customer_sk` INT,
        |`ws_bill_cdemo_sk` INT,
        |`ws_bill_hdemo_sk` INT,
        |`ws_bill_addr_sk` INT,
        |`ws_ship_customer_sk` INT,
        |`ws_ship_cdemo_sk` INT,
        |`ws_ship_hdemo_sk` INT,
        |`ws_ship_addr_sk` INT,
        |`ws_web_page_sk` INT,
        |`ws_web_site_sk` INT,
        |`ws_ship_mode_sk` INT,
        |`ws_warehouse_sk` INT,
        |`ws_promo_sk` INT,
        |`ws_order_number` INT,
        |`ws_quantity` INT,
        |`ws_wholesale_cost` DOUBLE,
        |`ws_list_price` DOUBLE,
        |`ws_sales_price` DOUBLE,
        |`ws_ext_discount_amt` DOUBLE,
        |`ws_ext_sales_price` DOUBLE,
        |`ws_ext_wholesale_cost` DOUBLE,
        |`ws_ext_list_price` DOUBLE,
        |`ws_ext_tax` DOUBLE,
        |`ws_coupon_amt` DOUBLE,
        |`ws_ext_ship_cost` DOUBLE,
        |`ws_net_paid` DOUBLE,
        |`ws_net_paid_inc_tax` DOUBLE,
        |`ws_net_paid_inc_ship` DOUBLE,
        |`ws_net_paid_inc_ship_tax` DOUBLE,
        |`ws_net_profit` DOUBLE
        """.stripMargin,
    "web_returns" ->
      """
        |`wr_returned_date_sk` INT,
        |`wr_returned_time_sk` INT,
        |`wr_item_sk` INT,
        |`wr_refunded_customer_sk` INT,
        |`wr_refunded_cdemo_sk` INT,
        |`wr_refunded_hdemo_sk` INT,
        |`wr_refunded_addr_sk` INT,
        |`wr_returning_customer_sk` INT,
        |`wr_returning_cdemo_sk` INT,
        |`wr_returning_hdemo_sk` INT,
        |`wr_returning_addr_sk` INT,
        |`wr_web_page_sk` INT,
        |`wr_reason_sk` INT,
        |`wr_order_number` INT,
        |`wr_return_quantity` INT,
        |`wr_return_amt` DOUBLE,
        |`wr_return_tax` DOUBLE,
        |`wr_return_amt_inc_tax` DOUBLE,
        |`wr_fee` DOUBLE,
        |`wr_return_ship_cost` DOUBLE,
        |`wr_refunded_cash` DOUBLE,
        |`wr_reversed_charge` DOUBLE,
        |`wr_account_credit` DOUBLE,
        |`wr_net_loss` DOUBLE
        """.stripMargin,
    "inventory" ->
      """
        |`inv_date_sk` INT,
        |`inv_item_sk` INT,
        |`inv_warehouse_sk` INT,
        |`inv_quantity_on_hand` INT
        """.stripMargin,
    "store" ->
      """
        |`s_store_sk` INT,
        |`s_store_id` CHAR(16),
        |`s_rec_start_date` DATE,
        |`s_rec_end_date` DATE,
        |`s_closed_date_sk` INT,
        |`s_store_name` VARCHAR(50),
        |`s_number_employees` INT,
        |`s_floor_space` INT,
        |`s_hours` CHAR(20),
        |`s_manager` VARCHAR(40),
        |`s_market_id` INT,
        |`s_geography_class` VARCHAR(100),
        |`s_market_desc` VARCHAR(100),
        |`s_market_manager` VARCHAR(40),
        |`s_division_id` INT,
        |`s_division_name` VARCHAR(50),
        |`s_company_id` INT,
        |`s_company_name` VARCHAR(50),
        |`s_street_number` VARCHAR(10),
        |`s_street_name` VARCHAR(60),
        |`s_street_type` CHAR(15),
        |`s_suite_number` CHAR(10),
        |`s_city` VARCHAR(60),
        |`s_county` VARCHAR(30),
        |`s_state` CHAR(2),
        |`s_zip` CHAR(10),
        |`s_country` VARCHAR(20),
        |`s_gmt_offset` DOUBLE,
        |`s_tax_percentage` DOUBLE
        """.stripMargin,
    "call_center" ->
      """
        |`cc_call_center_sk` INT,
        |`cc_call_center_id` CHAR(16),
        |`cc_rec_start_date` DATE,
        |`cc_rec_end_date` DATE,
        |`cc_closed_date_sk` INT,
        |`cc_open_date_sk` INT,
        |`cc_name` VARCHAR(50),
        |`cc_class` VARCHAR(50),
        |`cc_employees` INT,
        |`cc_sq_ft` INT,
        |`cc_hours` CHAR(20),
        |`cc_manager` VARCHAR(40),
        |`cc_mkt_id` INT,
        |`cc_mkt_class` CHAR(50),
        |`cc_mkt_desc` VARCHAR(100),
        |`cc_market_manager` VARCHAR(40),
        |`cc_division` INT,
        |`cc_division_name` VARCHAR(50),
        |`cc_company` INT,
        |`cc_company_name` CHAR(50),
        |`cc_street_number` CHAR(10),
        |`cc_street_name` VARCHAR(60),
        |`cc_street_type` CHAR(15),
        |`cc_suite_number` CHAR(10),
        |`cc_city` VARCHAR(60),
        |`cc_county` VARCHAR(30),
        |`cc_state` CHAR(2),
        |`cc_zip` CHAR(10),
        |`cc_country` VARCHAR(20),
        |`cc_gmt_offset` DOUBLE,
        |`cc_tax_percentage` DOUBLE
        """.stripMargin,
    "catalog_page" ->
      """
        |`cp_catalog_page_sk` INT,
        |`cp_catalog_page_id` CHAR(16),
        |`cp_start_date_sk` INT,
        |`cp_end_date_sk` INT,
        |`cp_department` VARCHAR(50),
        |`cp_catalog_number` INT,
        |`cp_catalog_page_number` INT,
        |`cp_description` VARCHAR(100),
        |`cp_type` VARCHAR(100)
        """.stripMargin,
    "web_site" ->
      """
        |`web_site_sk` INT,
        |`web_site_id` CHAR(16),
        |`web_rec_start_date` DATE,
        |`web_rec_end_date` DATE,
        |`web_name` VARCHAR(50),
        |`web_open_date_sk` INT,
        |`web_close_date_sk` INT,
        |`web_class` VARCHAR(50),
        |`web_manager` VARCHAR(40),
        |`web_mkt_id` INT,
        |`web_mkt_class` VARCHAR(50),
        |`web_mkt_desc` VARCHAR(100),
        |`web_market_manager` VARCHAR(40),
        |`web_company_id` INT,
        |`web_company_name` CHAR(50),
        |`web_street_number` CHAR(10),
        |`web_street_name` VARCHAR(60),
        |`web_street_type` CHAR(15),
        |`web_suite_number` CHAR(10),
        |`web_city` VARCHAR(60),
        |`web_county` VARCHAR(30),
        |`web_state` CHAR(2),
        |`web_zip` CHAR(10),
        |`web_country` VARCHAR(20),
        |`web_gmt_offset` DOUBLE,
        |`web_tax_percentage` DOUBLE
        """.stripMargin,
    "web_page" ->
      """
        |`wp_web_page_sk` INT,
        |`wp_web_page_id` CHAR(16),
        |`wp_rec_start_date` DATE,
        |`wp_rec_end_date` DATE,
        |`wp_creation_date_sk` INT,
        |`wp_access_date_sk` INT,
        |`wp_autogen_flag` CHAR(1),
        |`wp_customer_sk` INT,
        |`wp_url` VARCHAR(100),
        |`wp_type` CHAR(50),
        |`wp_char_count` INT,
        |`wp_link_count` INT,
        |`wp_image_count` INT,
        |`wp_max_ad_count` INT
        """.stripMargin,
    "warehouse" ->
      """
        |`w_warehouse_sk` INT,
        |`w_warehouse_id` CHAR(16),
        |`w_warehouse_name` VARCHAR(20),
        |`w_warehouse_sq_ft` INT,
        |`w_street_number` CHAR(10),
        |`w_street_name` VARCHAR(20),
        |`w_street_type` CHAR(15),
        |`w_suite_number` CHAR(10),
        |`w_city` VARCHAR(60),
        |`w_county` VARCHAR(30),
        |`w_state` CHAR(2),
        |`w_zip` CHAR(10),
        |`w_country` VARCHAR(20),
        |`w_gmt_offset` DOUBLE
        """.stripMargin,
    "customer" ->
      """
        |`c_customer_sk` INT,
        |`c_customer_id` CHAR(16),
        |`c_current_cdemo_sk` INT,
        |`c_current_hdemo_sk` INT,
        |`c_current_addr_sk` INT,
        |`c_first_shipto_date_sk` INT,
        |`c_first_sales_date_sk` INT,
        |`c_salutation` CHAR(10),
        |`c_first_name` CHAR(20),
        |`c_last_name` CHAR(30),
        |`c_preferred_cust_flag` CHAR(1),
        |`c_birth_day` INT,
        |`c_birth_month` INT,
        |`c_birth_year` INT,
        |`c_birth_country` VARCHAR(20),
        |`c_login` CHAR(13),
        |`c_email_address` CHAR(50),
        |`c_last_review_date` INT
        """.stripMargin,
    "customer_address" ->
      """
        |`ca_address_sk` INT,
        |`ca_address_id` CHAR(16),
        |`ca_street_number` CHAR(10),
        |`ca_street_name` VARCHAR(60),
        |`ca_street_type` CHAR(15),
        |`ca_suite_number` CHAR(10),
        |`ca_city` VARCHAR(60),
        |`ca_county` VARCHAR(30),
        |`ca_state` CHAR(2),
        |`ca_zip` CHAR(10),
        |`ca_country` VARCHAR(20),
        |`ca_gmt_offset` DOUBLE,
        |`ca_location_type` CHAR(20)
        """.stripMargin,
    "customer_demographics" ->
      """
        |`cd_demo_sk` INT,
        |`cd_gender` CHAR(1),
        |`cd_marital_status` CHAR(1),
        |`cd_education_status` CHAR(20),
        |`cd_purchase_estimate` INT,
        |`cd_credit_rating` CHAR(10),
        |`cd_dep_count` INT,
        |`cd_dep_employed_count` INT,
        |`cd_dep_college_count` INT
        """.stripMargin,
    "date_dim" ->
      """
        |`d_date_sk` INT,
        |`d_date_id` CHAR(16),
        |`d_date` DATE,
        |`d_month_seq` INT,
        |`d_week_seq` INT,
        |`d_quarter_seq` INT,
        |`d_year` INT,
        |`d_dow` INT,
        |`d_moy` INT,
        |`d_dom` INT,
        |`d_qoy` INT,
        |`d_fy_year` INT,
        |`d_fy_quarter_seq` INT,
        |`d_fy_week_seq` INT,
        |`d_day_name` CHAR(9),
        |`d_quarter_name` CHAR(6),
        |`d_holiday` CHAR(1),
        |`d_weekend` CHAR(1),
        |`d_following_holiday` CHAR(1),
        |`d_first_dom` INT,
        |`d_last_dom` INT,
        |`d_same_day_ly` INT,
        |`d_same_day_lq` INT,
        |`d_current_day` CHAR(1),
        |`d_current_week` CHAR(1),
        |`d_current_month` CHAR(1),
        |`d_current_quarter` CHAR(1),
        |`d_current_year` CHAR(1)
        """.stripMargin,
    "household_demographics" ->
      """
        |`hd_demo_sk` INT,
        |`hd_income_band_sk` INT,
        |`hd_buy_potential` CHAR(15),
        |`hd_dep_count` INT,
        |`hd_vehicle_count` INT
        """.stripMargin,
    "item" ->
      """
        |`i_item_sk` INT,
        |`i_item_id` CHAR(16),
        |`i_rec_start_date` DATE,
        |`i_rec_end_date` DATE,
        |`i_item_desc` VARCHAR(200),
        |`i_current_price` DOUBLE,
        |`i_wholesale_cost` DOUBLE,
        |`i_brand_id` INT,
        |`i_brand` CHAR(50),
        |`i_class_id` INT,
        |`i_class` CHAR(50),
        |`i_category_id` INT,
        |`i_category` CHAR(50),
        |`i_manufact_id` INT,
        |`i_manufact` CHAR(50),
        |`i_size` CHAR(20),
        |`i_formulation` CHAR(20),
        |`i_color` CHAR(20),
        |`i_units` CHAR(10),
        |`i_container` CHAR(10),
        |`i_manager_id` INT,
        |`i_product_name` CHAR(50)
        """.stripMargin,
    "income_band" ->
      """
        |`ib_income_band_sk` INT,
        |`ib_lower_bound` INT,
        |`ib_upper_bound` INT
        """.stripMargin,
    "promotion" ->
      """
        |`p_promo_sk` INT,
        |`p_promo_id` CHAR(16),
        |`p_start_date_sk` INT,
        |`p_end_date_sk` INT,
        |`p_item_sk` INT,
        |`p_cost` DOUBLE,
        |`p_response_target` INT,
        |`p_promo_name` CHAR(50),
        |`p_channel_dmail` CHAR(1),
        |`p_channel_email` CHAR(1),
        |`p_channel_catalog` CHAR(1),
        |`p_channel_tv` CHAR(1),
        |`p_channel_radio` CHAR(1),
        |`p_channel_press` CHAR(1),
        |`p_channel_event` CHAR(1),
        |`p_channel_demo` CHAR(1),
        |`p_channel_details` VARCHAR(100),
        |`p_purpose` CHAR(15),
        |`p_discount_active` CHAR(1)
        """.stripMargin,
    "reason" ->
      """
        |`r_reason_sk` INT,
        |`r_reason_id` CHAR(16),
        |`r_reason_desc` CHAR(100)
        """.stripMargin,
    "ship_mode" ->
      """
        |`sm_ship_mode_sk` INT,
        |`sm_ship_mode_id` CHAR(16),
        |`sm_type` CHAR(30),
        |`sm_code` CHAR(10),
        |`sm_carrier` CHAR(20),
        |`sm_contract` CHAR(20)
        """.stripMargin,
    "time_dim" ->
      """
        |`t_time_sk` INT,
        |`t_time_id` CHAR(16),
        |`t_time` INT,
        |`t_hour` INT,
        |`t_minute` INT,
        |`t_second` INT,
        |`t_am_pm` CHAR(2),
        |`t_shift` CHAR(20),
        |`t_sub_shift` CHAR(20),
        |`t_meal_time` CHAR(20)
        """.stripMargin
  )
  def main(args: Array[String]): Unit = {}

  def genTPCDSMergeTreeTables(
      dbName: String,
      dataPathRoot: String,
      tablePrefix: String,
      tableSuffix: String): Unit = {}

  def genTPCDSParquetTables(
      dbName: String,
      dataPathRoot: String,
      tablePrefix: String,
      tableSuffix: String,
      schemaVersion: Int = 1): ArrayBuffer[String] = {

    val catalogSalesTbl = "catalog_sales"
    val catalogSalesPartitionCols = "PARTITIONED BY (cs_sold_date_sk)"

    val catalogReturnsTbl = "catalog_returns"
    val catalogReturnsPartitionCols = "PARTITIONED BY (cr_returned_date_sk)"

    val inventoryTbl = "inventory"
    val inventoryPartitionCols = "PARTITIONED BY (inv_date_sk)"

    val storeSalesTbl = "store_sales"
    val storeSalesPartitionCols = "PARTITIONED BY (ss_sold_date_sk)"

    val storeReturnsTbl = "store_returns"
    val storeReturnsPartitionCols = "PARTITIONED BY (sr_returned_date_sk)"

    val webSalesTbl = "web_sales"
    val webSalesPartitionCols = "PARTITIONED BY (ws_sold_date_sk)"

    val webReturnsTbl = "web_returns"
    val webReturnsPartitionCols = "PARTITIONED BY (wr_returned_date_sk)"

    val callCenterTbl = "call_center"
    val callCenterPartitionCols = ""

    val catalogPageTbl = "catalog_page"
    val catalogPagePartitionCols = ""

    val customerTbl = "customer"
    val customerPartitionCols = ""

    val customerAddressTbl = "customer_address"
    val customerAddressPartitionCols = ""

    val customerDemographicsTbl = "customer_demographics"
    val customerDemographicsPartitionCols = ""

    val dateDimTbl = "date_dim"
    val dateDimPartitionCols = ""

    val householdDemographicsTbl = "household_demographics"
    val householdDemographicsPartitionCols = ""

    val incomeBandTbl = "income_band"
    val incomeBandPartitionCols = ""

    val itemTbl = "item"
    val itemPartitionCols = ""

    val promotionTbl = "promotion"
    val promotionPartitionCols = ""

    val reasonTbl = "reason"
    val reasonPartitionCols = ""

    val shipModeTbl = "ship_mode"
    val shipModePartitionCols = ""

    val storeTbl = "store"
    val storePartitionCols = ""

    val timeDimTbl = "time_dim"
    val timeDimPartitionCols = ""

    val warehouseTbl = "warehouse"
    val warehousePartitionCols = ""

    val webPageTbl = "web_page"
    val webPagePartitionCols = ""

    val webSiteTbl = "web_site"
    val webSitePartitionCols = ""

    val res = new ArrayBuffer[String]()
    res +=
      s"""
         |CREATE DATABASE IF NOT EXISTS $dbName
         |WITH DBPROPERTIES (engine='Parquet');
         |""".stripMargin

    res += s"""use $dbName;"""

    // catalog_sales
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      catalogSalesTbl,
      getTableSchema(schemaVersion)(catalogSalesTbl),
      catalogSalesPartitionCols,
      tablePrefix,
      tableSuffix)

    // catalog_returns
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      catalogReturnsTbl,
      getTableSchema(schemaVersion)(catalogReturnsTbl),
      catalogReturnsPartitionCols,
      tablePrefix,
      tableSuffix)

    // inventory
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      inventoryTbl,
      getTableSchema(schemaVersion)(inventoryTbl),
      inventoryPartitionCols,
      tablePrefix,
      tableSuffix)

    // store_sales
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      storeSalesTbl,
      getTableSchema(schemaVersion)(storeSalesTbl),
      storeSalesPartitionCols,
      tablePrefix,
      tableSuffix)

    // store_returns
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      storeReturnsTbl,
      getTableSchema(schemaVersion)(storeReturnsTbl),
      storeReturnsPartitionCols,
      tablePrefix,
      tableSuffix)

    // web_sales
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webSalesTbl,
      getTableSchema(schemaVersion)(webSalesTbl),
      webSalesPartitionCols,
      tablePrefix,
      tableSuffix)

    // web_returns
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webReturnsTbl,
      getTableSchema(schemaVersion)(webReturnsTbl),
      webReturnsPartitionCols,
      tablePrefix,
      tableSuffix)

    // call_center
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      callCenterTbl,
      getTableSchema(schemaVersion)(callCenterTbl),
      callCenterPartitionCols,
      tablePrefix,
      tableSuffix)

    // catalog_page
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      catalogPageTbl,
      getTableSchema(schemaVersion)(catalogPageTbl),
      catalogPagePartitionCols,
      tablePrefix,
      tableSuffix)

    // customer
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      customerTbl,
      getTableSchema(schemaVersion)(customerTbl),
      customerPartitionCols,
      tablePrefix,
      tableSuffix)

    // customer_address
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      customerAddressTbl,
      getTableSchema(schemaVersion)(customerAddressTbl),
      customerAddressPartitionCols,
      tablePrefix,
      tableSuffix)

    // customer_demographics
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      customerDemographicsTbl,
      getTableSchema(schemaVersion)(customerDemographicsTbl),
      customerDemographicsPartitionCols,
      tablePrefix,
      tableSuffix
    )

    // date_dim
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      dateDimTbl,
      getTableSchema(schemaVersion)(dateDimTbl),
      dateDimPartitionCols,
      tablePrefix,
      tableSuffix)

    // household_demographics
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      householdDemographicsTbl,
      getTableSchema(schemaVersion)(householdDemographicsTbl),
      householdDemographicsPartitionCols,
      tablePrefix,
      tableSuffix
    )

    // income_band
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      incomeBandTbl,
      getTableSchema(schemaVersion)(incomeBandTbl),
      incomeBandPartitionCols,
      tablePrefix,
      tableSuffix)

    // item
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      itemTbl,
      getTableSchema(schemaVersion)(itemTbl),
      itemPartitionCols,
      tablePrefix,
      tableSuffix)

    // promotion
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      promotionTbl,
      getTableSchema(schemaVersion)(promotionTbl),
      promotionPartitionCols,
      tablePrefix,
      tableSuffix)

    // reason
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      reasonTbl,
      getTableSchema(schemaVersion)(reasonTbl),
      reasonPartitionCols,
      tablePrefix,
      tableSuffix)

    // ship_mode
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      shipModeTbl,
      getTableSchema(schemaVersion)(shipModeTbl),
      shipModePartitionCols,
      tablePrefix,
      tableSuffix)

    // store
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      storeTbl,
      getTableSchema(schemaVersion)(storeTbl),
      storePartitionCols,
      tablePrefix,
      tableSuffix)

    // time_dim
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      timeDimTbl,
      getTableSchema(schemaVersion)(timeDimTbl),
      timeDimPartitionCols,
      tablePrefix,
      tableSuffix)

    // warehouse
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      warehouseTbl,
      getTableSchema(schemaVersion)(warehouseTbl),
      warehousePartitionCols,
      tablePrefix,
      tableSuffix)

    // web_page
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webPageTbl,
      getTableSchema(schemaVersion)(webPageTbl),
      webPagePartitionCols,
      tablePrefix,
      tableSuffix)

    // web_site
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webSiteTbl,
      getTableSchema(schemaVersion)(webSiteTbl),
      webSitePartitionCols,
      tablePrefix,
      tableSuffix)

    // scalastyle:off println
    // println(res.mkString("\n\n"))
    // scalastyle:on println
    res
  }

  def genOneTPCDSParquetTableSQL(
      res: ArrayBuffer[String],
      dataPathRoot: String,
      tblName: String,
      tblFields: String,
      tblPartitionCols: String,
      tablePrefix: String,
      tableSuffix: String): Unit = {
    // scalastyle:off println
    println(s"start to generate sqls for table $tblName")
    // scalastyle:on println
    res += s"""DROP TABLE IF EXISTS $tablePrefix$tblName$tableSuffix;"""
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$tblName$tableSuffix (
         |$tblFields
         | )
         | USING PARQUET
         | $tblPartitionCols
         | LOCATION '${dataPathRoot + tblName}'
         | ;
         |""".stripMargin

    if (!tblPartitionCols.isEmpty) {
      res += s"""MSCK REPAIR TABLE $tablePrefix$tblName$tableSuffix;"""
    }
  }

  def genTPCDSCSV2ParquetSQL(
      dbName: String,
      parquetDbName: String,
      csvPathRoot: String,
      parquetPathRoot: String,
      tablePrefix: String,
      tableSuffix: String): ArrayBuffer[String] = {
    val schemaVersion = 1

    val catalogSalesTbl = "catalog_sales"
    val catalogSalesParts = "/*+ REPARTITION(1) */"
    val catalogSalesPartitionCols = "PARTITIONED BY (cs_sold_date_sk)"

    val catalogReturnsTbl = "catalog_returns"
    val catalogReturnsParts = ""
    val catalogReturnsPartitionCols = "PARTITIONED BY (cr_returned_date_sk)"

    val inventoryTbl = "inventory"
    val inventoryParts = "/*+ REPARTITION(1) */"
    val inventoryPartitionCols = "PARTITIONED BY (inv_date_sk)"

    val storeSalesTbl = "store_sales"
    val storeSalesParts = "/*+ REPARTITION(1) */"
    val storeSalesPartitionCols = "PARTITIONED BY (ss_sold_date_sk)"

    val storeReturnsTbl = "store_returns"
    val storeReturnsParts = ""
    val storeReturnsPartitionCols = "PARTITIONED BY (sr_returned_date_sk)"

    val webSalesTbl = "web_sales"
    val webSalesParts = ""
    val webSalesPartitionCols = "PARTITIONED BY (ws_sold_date_sk)"

    val webReturnsTbl = "web_returns"
    val webReturnsParts = ""
    val webReturnsPartitionCols = "PARTITIONED BY (wr_returned_date_sk)"

    val callCenterTbl = "call_center"
    val callCenterParts = ""
    val callCenterPartitionCols = ""

    val catalogPageTbl = "catalog_page"
    val catalogPageParts = ""
    val catalogPagePartitionCols = ""

    val customerTbl = "customer"
    val customerParts = ""
    val customerPartitionCols = ""

    val customerAddressTbl = "customer_address"
    val customerAddressParts = ""
    val customerAddressPartitionCols = ""

    val customerDemographicsTbl = "customer_demographics"
    val customerDemographicsParts = ""
    val customerDemographicsPartitionCols = ""

    val dateDimTbl = "date_dim"
    val dateDimParts = ""
    val dateDimPartitionCols = ""

    val householdDemographicsTbl = "household_demographics"
    val householdDemographicsParts = ""
    val householdDemographicsPartitionCols = ""

    val incomeBandTbl = "income_band"
    val incomeBandParts = ""
    val incomeBandPartitionCols = ""

    val itemTbl = "item"
    val itemParts = ""
    val itemPartitionCols = ""

    val promotionTbl = "promotion"
    val promotionParts = ""
    val promotionPartitionCols = ""

    val reasonTbl = "reason"
    val reasonParts = ""
    val reasonPartitionCols = ""

    val shipModeTbl = "ship_mode"
    val shipModeParts = ""
    val shipModePartitionCols = ""

    val storeTbl = "store"
    val storeParts = ""
    val storePartitionCols = ""

    val timeDimTbl = "time_dim"
    val timeDimParts = ""
    val timeDimPartitionCols = ""

    val warehouseTbl = "warehouse"
    val warehouseParts = ""
    val warehousePartitionCols = ""

    val webPageTbl = "web_page"
    val webPageParts = ""
    val webPagePartitionCols = ""

    val webSiteTbl = "web_site"
    val webSiteParts = ""
    val webSitePartitionCols = ""

    val res = new ArrayBuffer[String]()
    res +=
      s"""
         |CREATE DATABASE IF NOT EXISTS $parquetDbName
         |WITH DBPROPERTIES (engine='Parquet');
         |""".stripMargin

    res += s"""CREATE DATABASE IF NOT EXISTS $dbName;"""
    res += s"""use $dbName;"""

    // catalog_sales
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      catalogSalesTbl,
      getTableSchema(schemaVersion)(catalogSalesTbl),
      catalogSalesPartitionCols,
      catalogSalesParts,
      tablePrefix,
      tableSuffix
    )

    // catalog_returns
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      catalogReturnsTbl,
      getTableSchema(schemaVersion)(catalogReturnsTbl),
      catalogReturnsPartitionCols,
      catalogReturnsParts,
      tablePrefix,
      tableSuffix
    )

    // inventory
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      inventoryTbl,
      getTableSchema(schemaVersion)(inventoryTbl),
      inventoryPartitionCols,
      inventoryParts,
      tablePrefix,
      tableSuffix
    )

    // store_sales
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      storeSalesTbl,
      getTableSchema(schemaVersion)(storeSalesTbl),
      storeSalesPartitionCols,
      storeSalesParts,
      tablePrefix,
      tableSuffix
    )

    // store_returns
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      storeReturnsTbl,
      getTableSchema(schemaVersion)(storeReturnsTbl),
      storeReturnsPartitionCols,
      storeReturnsParts,
      tablePrefix,
      tableSuffix
    )

    // web_sales
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webSalesTbl,
      getTableSchema(schemaVersion)(webSalesTbl),
      webSalesPartitionCols,
      webSalesParts,
      tablePrefix,
      tableSuffix
    )

    // web_returns
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webReturnsTbl,
      getTableSchema(schemaVersion)(webReturnsTbl),
      webReturnsPartitionCols,
      webReturnsParts,
      tablePrefix,
      tableSuffix
    )

    // call_center
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      callCenterTbl,
      getTableSchema(schemaVersion)(callCenterTbl),
      callCenterPartitionCols,
      callCenterParts,
      tablePrefix,
      tableSuffix
    )

    // catalog_page
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      catalogPageTbl,
      getTableSchema(schemaVersion)(catalogPageTbl),
      catalogPagePartitionCols,
      catalogPageParts,
      tablePrefix,
      tableSuffix
    )

    // customer
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      customerTbl,
      getTableSchema(schemaVersion)(customerTbl),
      customerPartitionCols,
      customerParts,
      tablePrefix,
      tableSuffix
    )

    // customer_address
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      customerAddressTbl,
      getTableSchema(schemaVersion)(customerAddressTbl),
      customerAddressPartitionCols,
      customerAddressParts,
      tablePrefix,
      tableSuffix
    )

    // customer_demographics
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      customerDemographicsTbl,
      getTableSchema(schemaVersion)(customerDemographicsTbl),
      customerDemographicsPartitionCols,
      customerDemographicsParts,
      tablePrefix,
      tableSuffix
    )

    // date_dim
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      dateDimTbl,
      getTableSchema(schemaVersion)(dateDimTbl),
      dateDimPartitionCols,
      dateDimParts,
      tablePrefix,
      tableSuffix
    )

    // household_demographics
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      householdDemographicsTbl,
      getTableSchema(schemaVersion)(householdDemographicsTbl),
      householdDemographicsPartitionCols,
      householdDemographicsParts,
      tablePrefix,
      tableSuffix
    )

    // income_band
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      incomeBandTbl,
      getTableSchema(schemaVersion)(incomeBandTbl),
      incomeBandPartitionCols,
      incomeBandParts,
      tablePrefix,
      tableSuffix
    )

    // item
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      itemTbl,
      getTableSchema(schemaVersion)(itemTbl),
      itemPartitionCols,
      itemParts,
      tablePrefix,
      tableSuffix)

    // promotion
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      promotionTbl,
      getTableSchema(schemaVersion)(promotionTbl),
      promotionPartitionCols,
      promotionParts,
      tablePrefix,
      tableSuffix
    )

    // reason
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      reasonTbl,
      getTableSchema(schemaVersion)(reasonTbl),
      reasonPartitionCols,
      reasonParts,
      tablePrefix,
      tableSuffix)

    // ship_mode
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      shipModeTbl,
      getTableSchema(schemaVersion)(shipModeTbl),
      shipModePartitionCols,
      shipModeParts,
      tablePrefix,
      tableSuffix
    )

    // store
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      storeTbl,
      getTableSchema(schemaVersion)(storeTbl),
      storePartitionCols,
      storeParts,
      tablePrefix,
      tableSuffix)

    // time_dim
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      timeDimTbl,
      getTableSchema(schemaVersion)(timeDimTbl),
      timeDimPartitionCols,
      timeDimParts,
      tablePrefix,
      tableSuffix
    )

    // warehouse
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      warehouseTbl,
      getTableSchema(schemaVersion)(warehouseTbl),
      warehousePartitionCols,
      warehouseParts,
      tablePrefix,
      tableSuffix
    )

    // web_page
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webPageTbl,
      getTableSchema(schemaVersion)(webPageTbl),
      webPagePartitionCols,
      webPageParts,
      tablePrefix,
      tableSuffix
    )

    // web_site
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webSiteTbl,
      getTableSchema(schemaVersion)(webSiteTbl),
      webSitePartitionCols,
      webSiteParts,
      tablePrefix,
      tableSuffix
    )

    // scalastyle:off println
    // println(res.mkString("\n\n"))
    // scalastyle:on println
    res
  }

  def genOneTPCDSCSV2ParquetSQL(
      res: ArrayBuffer[String],
      csvPathRoot: String,
      parquetPathRoot: String,
      tblName: String,
      tblFields: String,
      tblPartitionCols: String,
      tblParts: String,
      tablePrefix: String,
      tableSuffix: String): Unit = {
    // scalastyle:off println
    println(s"start to generate sqls for table $tblName")
    // scalastyle:on println
    res += s"""DROP TABLE IF EXISTS ${tblName}_csv;"""
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS ${tblName}_csv (
         |$tblFields
         | )
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + tblName + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$tblName$tableSuffix;"""
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$tblName$tableSuffix
         | USING PARQUET
         | $tblPartitionCols
         | LOCATION '${parquetPathRoot + tblName}'
         | AS SELECT $tblParts * FROM ${tblName}_csv;
         |""".stripMargin
  }

}
