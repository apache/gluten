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
var delta_table_path = "/PATH/TO/TPCDS_DELTA_TABLE_PATH"
var gluten_root = "/PATH/TO/GLUTEN"

// File root path: file://, hdfs:// , s3 , ...
// e.g. hdfs://hostname:8020
var delta_file_root = "/ROOT_PATH"

var tpcds_queries_path = "/tools/gluten-it/common/src/main/resources/tpcds-queries/"

def time[R](fileName: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$fileName, elapsed time: " + (t1 - t0)/1000000000.0 + " seconds")
    result
}

// Create TPC-DS Delta Tables.
spark.catalog.createTable("call_center", delta_file_root + delta_table_path + "/call_center", "delta")
spark.catalog.createTable("catalog_page", delta_file_root + delta_table_path + "/catalog_page", "delta")
spark.catalog.createTable("catalog_returns", delta_file_root + delta_table_path + "/catalog_returns", "delta")
spark.catalog.createTable("catalog_sales", delta_file_root + delta_table_path + "/catalog_sales", "delta")
spark.catalog.createTable("customer", delta_file_root + delta_table_path + "/customer", "delta")
spark.catalog.createTable("customer_address", delta_file_root + delta_table_path + "/customer_address", "delta")
spark.catalog.createTable("customer_demographics", delta_file_root + delta_table_path + "/customer_demographics", "delta")
spark.catalog.createTable("date_dim", delta_file_root + delta_table_path + "/date_dim", "delta")
spark.catalog.createTable("household_demographics", delta_file_root + delta_table_path + "/household_demographics", "delta")
spark.catalog.createTable("income_band", delta_file_root + delta_table_path + "/income_band", "delta")
spark.catalog.createTable("inventory", delta_file_root + delta_table_path + "/inventory", "delta")
spark.catalog.createTable("item", delta_file_root + delta_table_path + "/item", "delta")
spark.catalog.createTable("promotion", delta_file_root + delta_table_path + "/promotion", "delta")
spark.catalog.createTable("reason", delta_file_root + delta_table_path + "/reason", "delta")
spark.catalog.createTable("ship_mode", delta_file_root + delta_table_path + "/ship_mode", "delta")
spark.catalog.createTable("store", delta_file_root + delta_table_path + "/store", "delta")
spark.catalog.createTable("store_returns", delta_file_root + delta_table_path + "/store_returns", "delta")
spark.catalog.createTable("store_sales", delta_file_root + delta_table_path + "/store_sales", "delta")
spark.catalog.createTable("time_dim", delta_file_root + delta_table_path + "/time_dim", "delta")
spark.catalog.createTable("warehouse", delta_file_root + delta_table_path + "/warehouse", "delta")
spark.catalog.createTable("web_page", delta_file_root + delta_table_path + "/web_page", "delta")
spark.catalog.createTable("web_returns", delta_file_root + delta_table_path + "/web_returns", "delta")
spark.catalog.createTable("web_sales", delta_file_root + delta_table_path + "/web_sales", "delta")
spark.catalog.createTable("web_site", delta_file_root + delta_table_path + "/web_site", "delta")

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

// Main program to run TPC-DS testing
for (t <- sorted) {
  println(t)
  val fileContents = Source.fromFile(t).getLines.filter(!_.startsWith("--")).mkString(" ")
  println(fileContents)
  time(t.getName){spark.sql(fileContents).collectAsList()}
  // spark.sql(fileContents).explain
  Thread.sleep(2000)
}
