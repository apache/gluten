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

//Configurations:
var parquet_file_path = "/PATH/TO/TPCH_PARQUET_PATH"
var gluten_root = "/PATH/TO/GLUTEN"

def time[R](fileName: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$fileName, elapsed time: " + (t1 - t0)/1000000000.0 + " seconds")
    result
}

//Read TPC-H Table from DWRF files
val lineitem = spark.read.format("parquet").load("file://" + parquet_file_path + "/lineitem")
val part = spark.read.format("parquet").load("file://" + parquet_file_path + "/part")
val orders = spark.read.format("parquet").load("file://" + parquet_file_path + "/orders")
val customer = spark.read.format("parquet").load("file://" + parquet_file_path + "/customer")
val supplier = spark.read.format("parquet").load("file://" + parquet_file_path + "/supplier")
val partsupp = spark.read.format("parquet").load("file://" + parquet_file_path + "/partsupp")
val region = spark.read.format("parquet").load("file://" + parquet_file_path + "/region")
val nation = spark.read.format("parquet").load("file://" + parquet_file_path + "/nation")

//Create DWRF based TPC-H Table View
lineitem.createOrReplaceTempView("lineitem")
orders.createOrReplaceTempView("orders")
customer.createOrReplaceTempView("customer")
part.createOrReplaceTempView("part")
supplier.createOrReplaceTempView("supplier")
partsupp.createOrReplaceTempView("partsupp")
nation.createOrReplaceTempView("nation")
region.createOrReplaceTempView("region")


def getListOfFiles(dir: String):List[File] = {
     val d = new File(dir)
     if (d.exists && d.isDirectory) {
         //You can run a specific query by using below line
         //d.listFiles.filter(_.isFile).filter(_.getName().contains("17.sql")).toList
         d.listFiles.filter(_.isFile).toList
     } else {
         List[File]()
     }
}
val fileLists =
  getListOfFiles(gluten_root + "/tools/gluten-it/common/src/main/resources/tpch-queries/")
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
  val fileContents = {
    val src = Source.fromFile(t)
    try {
      src.getLines().filter(!_.startsWith("--")).mkString(" ")
    } finally {
      src.close()
    }
  }
  println(t)
  println(fileContents)
  time(t.getName){spark.sql(fileContents).collectAsList()}
  Thread.sleep(2000)
}
