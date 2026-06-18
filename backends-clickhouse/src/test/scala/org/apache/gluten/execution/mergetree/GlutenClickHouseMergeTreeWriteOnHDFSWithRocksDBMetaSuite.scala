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
import org.apache.gluten.execution.{CreateMergeTreeSuite, FileSourceScanExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import org.scalatest.DoNotDiscover

import scala.concurrent.duration.DurationInt

@DoNotDiscover
class GlutenClickHouseMergeTreeWriteOnHDFSWithRocksDBMetaSuite extends CreateMergeTreeSuite {

  override protected def sparkConf: SparkConf = {

    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
      .set(RuntimeSettings.MERGE_AFTER_INSERT.key, "false")
  }

  private val remotePath: String = hdfsHelper.independentHdfsURL("test")
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    hdfsHelper.deleteDir(remotePath)
    hdfsHelper.resetMeta()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    hdfsHelper.resetMeta()
  }

  test("test mergetree table write") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_hdfs
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$remotePath/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main_rocksdb')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem
                 |""".stripMargin)
    hdfsHelper.resetMeta()

    customCheckQuery(q1("lineitem_mergetree_hdfs")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKey === StorageMeta.DEFAULT_ORDER_BY_KEY)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKey.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(1)(addFiles.size)
        assertResult(600572)(addFiles.head.rows)
    }
    spark.sql("drop table lineitem_mergetree_hdfs")
  }

  test("test mergetree write with orderby keys / primary keys") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey_hdfs
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |TBLPROPERTIES (storage_policy='__hdfs_main_rocksdb',
                 |               orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION '$remotePath/lineitem_mergetree_orderbykey_hdfs'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey_hdfs
                 | select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree_orderbykey_hdfs")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assertResult("l_shipdate,l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKey)
        assertResult("l_shipdate")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKey)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(1)(addFiles.size)
        assertResult(600572)(addFiles.head.rows)
    }
    spark.sql("drop table lineitem_mergetree_orderbykey_hdfs")
  }

  test("test mergetree write with partition") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_partition_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_partition_hdfs
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |TBLPROPERTIES (storage_policy='__hdfs_main_rocksdb',
                 |               orderByKey='l_orderkey',
                 |               primaryKey='l_orderkey')
                 |LOCATION '$remotePath/lineitem_mergetree_partition_hdfs'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_mergetree_partition_hdfs
                 | select * from lineitem
                 |""".stripMargin)

    // write with dataframe api
    val source = spark.sql(s"""
                              |select
                              | l_orderkey      ,
                              | l_partkey       ,
                              | l_suppkey       ,
                              | l_linenumber    ,
                              | l_quantity      ,
                              | l_extendedprice ,
                              | l_discount      ,
                              | l_tax           ,
                              | l_returnflag    ,
                              | l_linestatus    ,
                              | l_shipdate      ,
                              | l_commitdate    ,
                              | l_receiptdate   ,
                              | l_shipinstruct  ,
                              | l_shipmode      ,
                              | l_comment
                              | from lineitem
                              | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
                              |""".stripMargin)

    source.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .insertInto("lineitem_mergetree_partition_hdfs")

    // static partition
    spark.sql(s"""
                 | insert into lineitem_mergetree_partition_hdfs PARTITION (l_returnflag = 'A')
                 | (l_shipdate,
                 |  l_orderkey,
                 |  l_partkey,
                 |  l_suppkey,
                 |  l_linenumber,
                 |  l_quantity,
                 |  l_extendedprice,
                 |  l_discount,
                 |  l_tax,
                 |  l_linestatus,
                 |  l_commitdate,
                 |  l_receiptdate,
                 |  l_shipinstruct,
                 |  l_shipmode,
                 |  l_comment)
                 | select
                 |  l_shipdate,
                 |  l_orderkey,
                 |  l_partkey,
                 |  l_suppkey,
                 |  l_linenumber,
                 |  l_quantity,
                 |  l_extendedprice,
                 |  l_discount,
                 |  l_tax,
                 |  l_linestatus,
                 |  l_commitdate,
                 |  l_receiptdate,
                 |  l_shipinstruct,
                 |  l_shipmode,
                 |  l_comment from lineitem
                 |  where l_returnflag = 'A'
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree_partition_hdfs"), compare = false) {
      df =>
        val result = df.collect()
        assertResult(4)(result.length)
        assertResult("A")(result(0).getString(0))
        assertResult("F")(result(0).getString(1))
        assertResult(7578058.0)(result(0).getDouble(2))

        assertResult("N")(result(2).getString(0))
        assertResult("O")(result(2).getString(1))
        assertResult(7454519.0)(result(2).getDouble(2))

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))
        assertResult(6)(mergetreeScan.metrics("numFiles").value)

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assertResult("l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKey)
        assertResult("l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKey)
        assertResult(1)(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size)
        assertResult("l_returnflag")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns
            .head)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(6)(addFiles.size)
        assertResult(750735)(addFiles.map(_.rows).sum)
    }
    spark.sql("drop table lineitem_mergetree_partition_hdfs")
  }

  testSparkVersionLE33("test mergetree write with bucket table") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_bucket_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_bucket_hdfs
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |CLUSTERED BY (l_orderkey)
                 |${if (spark32) "" else "SORTED BY (l_partkey)"} INTO 4 BUCKETS
                 |LOCATION '$remotePath/lineitem_mergetree_bucket_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main_rocksdb')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_bucket_hdfs
                 | select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree_bucket_hdfs")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isDefined)
        if (spark32) {
          assert(
            ClickHouseTableV2
              .getTable(fileIndex.deltaLog)
              .orderByKey === StorageMeta.DEFAULT_ORDER_BY_KEY)
        } else {
          assertResult("l_partkey")(
            ClickHouseTableV2
              .getTable(fileIndex.deltaLog)
              .orderByKey)
        }
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKey.isEmpty)
        assertResult(1)(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size)
        assertResult("l_returnflag")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns
            .head)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(12)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)
    }
    spark.sql("drop table lineitem_mergetree_bucket_hdfs purge")
  }

  testSparkVersionLE33("test mergetree write with the path based bucket table") {
    val dataPath = s"$remotePath/lineitem_mergetree_bucket_hdfs"

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .partitionBy("l_returnflag")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .option("clickhouse.numBuckets", "4")
      .option("clickhouse.bucketColumnNames", "l_orderkey")
      .option("clickhouse.storage_policy", "__hdfs_main_rocksdb")
      .save(dataPath)

    customCheckQuery(q1(s"clickhouse.`$dataPath`")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isDefined)
        assertResult("l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKey)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKey.nonEmpty)
        assertResult(1)(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size)
        assertResult("l_returnflag")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns
            .head)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(12)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)
    }

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .count()
    assertResult(600572)(result)
  }

  test("test mergetree insert with optimize basic") {
    val tableName = "lineitem_mergetree_insert_optimize_basic_hdfs"
    val dataPath = s"$remotePath/$tableName"

    withSQLConf(
      "spark.databricks.delta.optimize.minFileSize" -> "200000000",
      RuntimeSettings.MERGE_AFTER_INSERT.key -> "true",
      RuntimeSettings.INSERT_WITHOUT_LOCAL_STORAGE.key -> "true",
      RuntimeSettings.MIN_INSERT_BLOCK_SIZE_ROWS.key -> "10000"
    ) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS $tableName;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS $tableName
                   |USING clickhouse
                   |LOCATION '$dataPath'
                   |TBLPROPERTIES (storage_policy='__hdfs_main_rocksdb')
                   | as select * from lineitem
                   |""".stripMargin)

      val ret = spark.sql(s"select count(*) from $tableName").collect()
      assertResult(600572)(ret.apply(0).get(0))

      eventually(timeout(60.seconds), interval(2.seconds)) {
        val files = hdfsHelper.countDir(dataPath)
        assertResult(4)(files)
      }
    }
  }
}
