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
import org.apache.spark.sql.execution.debug._
import scala.io.Source
import java.io.File
import java.util.Arrays
import sys.process._

// Configurations:
var parquet_file_path = "/PATH/TO/TPCDS_PARQUET_PATH"
var gluten_root = "/PATH/TO/GLUTEN"

// File root path: file://, hdfs:// , s3 , ...
// e.g. hdfs://hostname:8020
var paq_file_root = "/ROOT_PATH"

var tpcds_queries_path = "/tools/gluten-it/common/src/main/resources/tpcds-queries/"

def time[R](fileName: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$fileName, elapsed time: " + (t1 - t0)/1000000000.0 + " seconds")
    result
}

// Read TPC-DS Table from parquet files.
val call_center = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/call_center")
val catalog_page = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/catalog_page")
val catalog_returns = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/catalog_returns")
val catalog_sales = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/catalog_sales")
val customer = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/customer")
val customer_address = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/customer_address")
val customer_demographics = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/customer_demographics")
val date_dim = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/date_dim")
val household_demographics = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/household_demographics")
val income_band = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/income_band")
val inventory = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/inventory")
val item = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/item")
val promotion = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/promotion")
val reason = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/reason")
val ship_mode = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/ship_mode")
val store = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/store")
val store_returns = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/store_returns")
val store_sales = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/store_sales")
val time_dim = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/time_dim")
val warehouse = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/warehouse")
val web_page = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_page")
val web_returns = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_returns")
val web_sales = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_sales")
val web_site = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_site")

// Create parquet based TPC-DS Table View.
call_center.createOrReplaceTempView("call_center")
catalog_page.createOrReplaceTempView("catalog_page")
catalog_returns.createOrReplaceTempView("catalog_returns")
catalog_sales.createOrReplaceTempView("catalog_sales")
customer.createOrReplaceTempView("customer")
customer_address.createOrReplaceTempView("customer_address")
customer_demographics.createOrReplaceTempView("customer_demographics")
household_demographics.createOrReplaceTempView("household_demographics")
income_band.createOrReplaceTempView("income_band")
inventory.createOrReplaceTempView("inventory")
item.createOrReplaceTempView("item")
promotion.createOrReplaceTempView("promotion")
reason.createOrReplaceTempView("reason")
ship_mode.createOrReplaceTempView("ship_mode")
store.createOrReplaceTempView("store")
store_returns.createOrReplaceTempView("store_returns")
store_sales.createOrReplaceTempView("store_sales")
time_dim.createOrReplaceTempView("time_dim")
warehouse.createOrReplaceTempView("warehouse")
web_page.createOrReplaceTempView("web_page")
web_returns.createOrReplaceTempView("web_returns")
web_sales.createOrReplaceTempView("web_sales")
web_site.createOrReplaceTempView("web_site")
date_dim.createOrReplaceTempView("date_dim")

def getListOfFiles(dir: String): List[File] = {
     val d = new File(dir)
     if (d.exists && d.isDirectory) {
         // You can run a specific query by using below line.
         // d.listFiles.filter(_.isFile).filter(_.getName().contains("17.sql")).toList
         d.listFiles.filter(_.isFile).toList
     } else {
         List[File]()
     }
}
val fileLists = getListOfFiles(gluten_root + tpcds_queries_path)
val sorted = fileLists.sortBy {
       f => f.getName match {
       case name =>
         var str = name
         str = str.replaceFirst("a", ".1")
         str = str.replaceFirst("b", ".2")
         str = str.replaceFirst(".sql", "")
         str = str.replaceFirst("q", "")
         str.toDouble
     }}

// Main program to run TPC-H testing
for (t <- sorted) {
  println(t)
  val fileContents = Source.fromFile(t).getLines.filter(!_.startsWith("--")).mkString(" ")
  println(fileContents)
  time(t.getName){spark.sql(fileContents).collectAsList()}
  // spark.sql(fileContents).explain
  Thread.sleep(2000)
}
