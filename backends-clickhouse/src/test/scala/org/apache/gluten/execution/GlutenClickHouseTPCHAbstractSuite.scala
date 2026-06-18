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
import org.apache.gluten.test.TPCHCHSchema

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.{ClickhouseSnapshot, DeltaLog}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.test.SharedSparkSession

import org.scalatest.DoNotDiscover
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.io.File

trait TPCHDatabase extends SharedSparkSession with TPCHCHSchema with TestDatabase {
  final protected def createTPCHTables(
      tablePath: String,
      format: String = "parquet",
      DB: String = "default",
      isNull: Boolean = true,
      props: Map[String, String] = Map.empty[String, String]): Unit = {
    if (DB != "default") {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $DB")
      spark.sql(s"use $DB")
    }
    tpchTables.foreach {
      table =>
        spark.sql(s"DROP TABLE IF EXISTS $table")
        val s = createTableBuilder(table, format, s"$tablePath/$table")
          .withProps(props)
          .withIsNull(isNull)
          .build()
        spark.sql(s)
    }
    assert(spark.sql("show tables").collect().length === 8)
  }

  final protected def insertIntoTPCHTables(dataSourceDB: String): Unit = {
    tpchTables
      .map(table => s"insert into $table select * from $dataSourceDB.$table")
      .foreach(spark.sql)
  }

  /** Parquet Source DB, Empty means no need Parquet Source DB */
  protected def parquetSourceDB: String = ""
  final def createParquetSource(): Unit = {
    if (parquetSourceDB.nonEmpty) {
      // create parquet data source table
      createTPCHTables(testParquetAbsolutePath, DB = parquetSourceDB)
    }
    spark.sql(s"use default")
  }
  final override protected def prepareTestTables(): Unit = {
    createParquetSource()
    createTestTables()
  }
  protected def createTestTables(): Unit
}

trait TPCHParquetSource extends TPCHDatabase {
  final override protected def parquetSourceDB: String = "default"
  override protected def createTestTables(): Unit = {}
}

trait TPCHNullableMergeTreeSource extends TPCHDatabase {
  override protected def parquetSourceDB: String = "parquet_source"
  override protected def createTestTables(): Unit = {
    createTPCHTables(
      s"$dataHome/tpch-data-ch",
      format = "clickhouse",
      props = Map("engine" -> "'MergeTree'")
    )
    insertIntoTPCHTables(parquetSourceDB)
  }
}

trait TPCHMergeTreeSource extends TPCHDatabase {
  override protected def parquetSourceDB: String = "parquet_source"
  override protected def createTestTables(): Unit = {
    createTPCHTables(
      s"$dataHome/tpch-data-ch",
      format = "clickhouse",
      isNull = false,
      props = Map("engine" -> "'MergeTree'")
    )
    insertIntoTPCHTables(parquetSourceDB)
  }
}

trait TPCHBucketTableSource extends TPCHDatabase {
  val hasSortByCol: Boolean
  val tableFormat: String
  override protected def parquetSourceDB: String = "parquet_source"
  override protected def createTestTables(): Unit = {
    createTPCHDefaultBucketTables(s"$dataHome/tpch-data-ch")
    insertIntoTPCHTables(parquetSourceDB)
  }

  final def createTPCHDefaultBucketTables(dataPath: String, DB: String = "default"): Unit = {
    if (DB != "default") {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $DB")
      spark.sql(s"use $DB")
    }
    val table2ClusterKey: Map[String, (String, Int)] = Map(
      "customer" -> ("c_custkey", 2),
      "lineitem" -> ("l_orderkey", 2),
      "nation" -> ("n_nationkey", 1),
      "region" -> ("r_regionkey", 1),
      "orders" -> ("o_orderkey", 2),
      "part" -> ("p_partkey", 2),
      "partsupp" -> ("ps_partkey", 2),
      "supplier" -> ("s_suppkey", 1)
    )
    val table2SortByCol: Map[String, Seq[String]] = Map(
      "customer" -> Seq("c_custkey"),
      "lineitem" -> Seq("l_shipdate", "l_orderkey"),
      "nation" -> Seq("n_nationkey"),
      "region" -> Seq("r_regionkey"),
      "orders" -> Seq("o_orderkey", "o_orderdate"),
      "part" -> Seq("p_partkey"),
      "partsupp" -> Seq("ps_partkey"),
      "supplier" -> Seq("s_suppkey")
    )
    def create(table: String): Unit = {
      spark.sql(s"DROP TABLE IF EXISTS $table")
      val s = createTableBuilder(table, tableFormat, s"$dataPath/$table")
        .withClusterKey(table2ClusterKey(table))
        .withSortByOfBuckets(if (hasSortByCol) table2SortByCol(table) else Seq.empty)
        .build()
      spark.sql(s)
    }
    tpchTables.foreach(create)
    assert(spark.sql("show tables").collect().length === 8)
  }
}

trait withTPCHQuery extends GlutenClickHouseWholeStageTransformerSuite {
  private val tpchQueryPath: String = new File(s"$queryPath/tpch-queries-ch").getCanonicalPath

  // Reusable empty function to avoid creating a new lambda for each call
  val NOOP: DataFrame => Unit = _ => {}

  def customCheck(num: Int, compare: Boolean = true, native: Boolean = true)(
      customCheck: DataFrame => Unit): Unit = {
    require(num >= 1 && num <= 22, s"Query number must be between 1 and 22, got $num")
    compareTPCHQueryAgainstVanillaSpark(
      num,
      tpchQueryPath,
      noFallBack = native,
      compareResult = compare,
      customCheck = customCheck)
  }

  def check(num: Int, compare: Boolean = true): Unit = {
    require(num >= 1 && num <= 22, s"Query number must be between 1 and 22, got $num")
    compareTPCHQueryAgainstVanillaSpark(
      num,
      tpchQueryPath,
      compareResult = compare,
      customCheck = NOOP)
  }

  def withDataFrame(queryNum: Int)(f: DataFrame => Unit): Unit =
    withDataFrame(tpchSQL(queryNum, tpchQueryPath))(f)

  def doRunTPCHQuery(num: Int, queriesResults: String, compare: Boolean, native: Boolean)(
      customCheck: DataFrame => Unit): Unit =
    super.runTPCHQuery(num, tpchQueryPath, queriesResults, compare, native)(customCheck)

  def q1(tableName: String): (String, Int) = {
    val sql =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    $tableName
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    (sql, 1)
  }

  def q6(tableName: String): (String, Int) = {
    val sql =
      s"""
         |SELECT
         |    sum(l_extendedprice * l_discount) AS revenue
         |FROM
         |    $tableName
         |WHERE
         |    l_shipdate >= date'1994-01-01'
         |    AND l_shipdate < date'1994-01-01' + interval 1 year
         |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
         |    AND l_quantity < 24
         |""".stripMargin
    (sql, 6)
  }
}

trait withCacheResult extends withTPCHQuery {
  val queriesResults: String

  final override def customCheck(num: Int, compare: Boolean = true, native: Boolean = true)(
      customCheck: DataFrame => Unit): Unit = {
    require(num >= 1 && num <= 22, s"Query number must be between 1 and 22, got $num")
    doRunTPCHQuery(num, queriesResults, compare, native)(customCheck)
  }

  final override def check(queryNum: Int, compare: Boolean = true): Unit =
    doRunTPCHQuery(queryNum, queriesResults, compare, native = true)(NOOP)

  final def customCheckQuery(query: (String, Int), compare: Boolean = true, native: Boolean = true)(
      customCheck: DataFrame => Unit): Unit = {
    require(
      query._2 >= 1 && query._2 <= 22,
      s"Query number must be between 1 and 22, got ${query._2}")
    require(query._1.nonEmpty, "SQL query string cannot be empty")
    withDataFrame(query._1) {
      df =>
        if (compare) {
          verifyTPCHResult(df, s"q${query._2}", queriesResults)
        } else {
          df.collect()
        }
        checkDataFrame(native, customCheck, df)
    }
  }

  final def checkQuery(query: (String, Int), compare: Boolean = true): Unit =
    customCheckQuery(query, compare)(NOOP)
}

trait TPCHParquetResult extends withCacheResult {
  final val queriesResults: String = s"${resPath}result/queries-output"
}

trait TPCHMergeTreeResult extends withCacheResult {
  final val queriesResults: String = s"${resPath}result/mergetree-queries-output"
}

abstract class GlutenClickHouseTPCHAbstractSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with withTPCHQuery {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set("spark.sql.warehouse.dir", warehouse)
  }

  val testCases: Seq[Int] = Seq.empty
  val testCasesWithConfig: Map[Int, Seq[(String, String)]] = Map.empty
  def setupTestCase(): Unit = {
    testCases.foreach {
      num =>
        test(s"TPCH Q$num") {
          check(num)
        }
    }

    testCasesWithConfig.foreach {
      case (num, configs) =>
        test(s"TPCH Q$num") {
          withSQLConf(configs: _*) {
            check(num)
          }
        }
    }
  }

  override protected def afterAll(): Unit = {

    // if SparkEnv.get returns null which means something wrong at beforeAll()
    if (SparkEnv.get != null) {
      // guava cache invalidate event trigger remove operation may in seconds delay, so wait a bit
      // normally this doesn't take more than 1s
      eventually(timeout(60.seconds), interval(1.seconds)) {
        // Spark listener message was not sent in time with ci env.
        // In tpch case, there are more than 10 hbj data has built.
        // Let's just verify it was cleaned ever.
        assert(CHBroadcastBuildSideCache.size() <= 10)
      }
      ClickhouseSnapshot.clearAllFileStatusCache()
    }
    DeltaLog.clearCache()
    super.afterAll()
  }
}

/**
 * This test suite is designed to validate the creation of MergeTree tables in ClickHouse. The
 * source data, based on the TPCH schema, is stored in Parquet format files referenced through
 * external tables in the default database.
 */
class CreateMergeTreeSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with TPCHMergeTreeResult
  with TPCHParquetSource {}

/**
 * `MergeTreeSuite` extends GlutenClickHouseTPCHAbstractSuite and integrates functionality provided
 * by the `MergeTreeResult` trait. It provides the structure necessary to test and validate merge
 * tree query executions against the ClickHouse backend using the TPCH schema.
 */
@DoNotDiscover
class MergeTreeSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with TPCHMergeTreeResult
  with TPCHMergeTreeSource {}

/**
 * A test suite designed for testing nullable support in the MergeTree table implementation in the
 * ClickHouse backend of Gluten's integration with Apache Spark.
 *
 * This suite extends the `GlutenClickHouseTPCHAbstractSuite` and incorporates additional traits
 * specific to MergeTree table behavior and nullable data handling.
 */
@DoNotDiscover
class NullableMergeTreeSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with TPCHMergeTreeResult
  with TPCHNullableMergeTreeSource {}

class ParquetTPCHSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with TPCHParquetResult
  with TPCHParquetSource

class ParquetSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with withTPCHQuery
  with TPCHParquetSource
