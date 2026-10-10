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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

import java.io.File
import java.util

import scala.io.Source

// just used to test TPCDS locally
// Usage: please export SPARK_TPCDS_DATA to your local TPCDS absolute path
// The query is original TPCDS query, you can also change it to your query path
// Then set the `ignore` to `test`
class BoltTPCDSSuite extends BoltWholeStageTransformerSuite {

  override protected val resourcePath: String =
    sys.env.getOrElse("SPARK_TPCDS_DATA", "/tmp/tpcds-generated")
  override protected val fileFormat: String = "parquet"

  private val queryPath = System.getProperty("user.dir") +
    "/tools/gluten-it/common/src/main/resources/tpcds-queries"

  protected var queryTables: Map[String, DataFrame] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    createQueryTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "4g")
      .set("spark.sql.shuffle.partitions", "256")
      .set("spark.memory.offHeap.size", "200g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "10M")
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.adaptive.enabled", "true")
      .set(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key, "4096")
      .set(GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE.key, "4096")
      .set("spark.executor.memory", "4g")
      .set("spark.executor.instances", "16")
      .set("spark.executor.cores", "8")
      .set("spark.driver.memory", "20g")
      .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
  }

  protected def createQueryTables(): Unit = {
    queryTables = Seq(
      "call_center",
      "catalog_page",
      "catalog_returns",
      "catalog_sales",
      "customer",
      "customer_address",
      "customer_demographics",
      "date_dim",
      "household_demographics",
      "income_band",
      "inventory",
      "item",
      "promotion",
      "reason",
      "ship_mode",
      "store",
      "store_returns",
      "store_sales",
      "time_dim",
      "warehouse",
      "web_page",
      "web_returns",
      "web_sales",
      "web_site"
    ).map {
      table =>
        val tablePath = new File(resourcePath, table).getAbsolutePath
        val tableDF = spark.read.format(fileFormat).load(tablePath)
        tableDF.createOrReplaceTempView(table)
        (table, tableDF)
    }.toMap
  }

  ignore("q7") {
    val source = Source.fromFile(queryPath + "q7.sql")
    val sql = source.mkString
    source.close()
    runQueryAndCompare(sql)(_ => {})
  }

  ignore("all query") {
    val s = new util.ArrayList[String]()
    new File(queryPath)
      .listFiles()
      .foreach(
        f => {
          val source = Source.fromFile(f.getAbsolutePath)
          val sql = source.mkString
          source.close()
          print("query " + f.getName + "\n")
          try {
            runQueryAndCompare(sql) { _ => }
          } catch {
            case e: Exception =>
              s.add(f.getName)
              print("query failed " + f.getName + " by " + e.getMessage + "\n")
          }
        })
    print("All failed queries \n" + s)
  }

}
