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

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col

import org.apache.iceberg.spark.SparkWriteOptions

import java.io.File

class VeloxTPCHIcebergSuite extends VeloxTPCHSuite {
  protected val tpchBasePath: String =
    getClass.getResource("/").getPath + "../../../src/test/resources"

  override protected val resourcePath: String =
    new File(tpchBasePath, "tpch-data-parquet").getCanonicalPath

  // FIXME: Unused.
  override protected val queriesResults: String =
    new File(tpchBasePath, "queries-output").getCanonicalPath

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.executor.instances", "1")
      .set("spark.executor.memory", "4g")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/tpch-data-iceberg-velox")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    TPCHTables
      .map(_.name)
      .map {
        table =>
          val tablePath = new File(resourcePath, table).getAbsolutePath
          val tableDF = spark.read.format(fileFormat).load(tablePath)
          tableDF.write.format("iceberg").mode("overwrite").saveAsTable(table)
          (table, tableDF)
      }
      .toMap
  }

  override protected def afterAll(): Unit = {
    TPCHTables.map(_.name).foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))
    super.afterAll()
  }

  test("iceberg transformer exists") {
    runQueryAndCompare("""
                         |SELECT
                         |  l_orderkey,
                         |  o_orderdate
                         |FROM
                         |  orders,
                         |  lineitem
                         |WHERE
                         |  l_orderkey = o_orderkey
                         |ORDER BY
                         |  l_orderkey,
                         |  o_orderdate
                         |LIMIT
                         |  10;
                         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[IcebergScanTransformer]
              }) == 2)
        }
    }
  }
}

class VeloxPartitionedTableTPCHIcebergSuite extends VeloxTPCHIcebergSuite {

  override protected def createTPCHNotNullTables(): Unit = {
    TPCHTables.map {
      table =>
        val tablePath = new File(resourcePath, table.name).getAbsolutePath
        val tableDF = spark.read
          .format(fileFormat)
          .load(tablePath)
          .repartition(table.partitionColumns.map(col): _*)
          .sortWithinPartitions(table.partitionColumns.map(col): _*)

        // Use ORC to disable native write in case of OOM, velox behavior likes FANOUT_ENABLED true
        // The number of writers too much.
        tableDF.write
          .format("iceberg")
          .option(SparkWriteOptions.WRITE_FORMAT, "orc")
          .partitionBy(table.partitionColumns: _*)
          .option(SparkWriteOptions.FANOUT_ENABLED, "false")
          .mode("overwrite")
          .saveAsTable(table.name)
        (table.name, tableDF)
    }.toMap
  }
}
