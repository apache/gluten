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
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import org.scalatest.DoNotDiscover

import java.io.File

import scala.concurrent.duration.DurationInt

@DoNotDiscover
class GlutenClickHouseMergeTreeCacheDataSuite extends CreateMergeTreeSuite {

  override protected def sparkConf: SparkConf = {

    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.soft-affinity.enabled", "true")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
      .set(RuntimeSettings.MERGE_AFTER_INSERT.key, "false")
  }

  private val remotePath: String = hdfsHelper.independentHdfsURL("test")
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    hdfsHelper.deleteDir(remotePath)
    hdfsHelper.resetMeta()
    hdfsHelper.resetCache()
  }

  def countFiles(directory: File): Int = {
    if (directory.exists && directory.isDirectory) {
      val files = directory.listFiles
      val count = files
        .count(_.isFile) + files.filter(_.isDirectory).map(countFiles).sum
      count
    } else {
      0
    }
  }

  test("test cache mergetree data sync") {
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
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$remotePath/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main',
                 |               orderByKey='l_linenumber,l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    hdfsHelper.resetMeta()
    val dataPath = new File(hdfsHelper.HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val res = spark
      .sql(s"""
              |cache data
              |  select l_orderkey, l_partkey from lineitem_mergetree_hdfs
              |  after l_shipdate AS OF '1995-01-10'
              |  CACHEPROPERTIES(storage_policy='__hdfs_main',
              |                aaa='ccc')""".stripMargin)
      .collect()
    assertResult(true)(res(0).getBoolean(0))
    val metaPath = new File(hdfsHelper.metaPath(s"$SPARK_DIR_NAME/test/lineitem_mergetree_hdfs"))
    assertResult(true)(metaPath.exists() && metaPath.isDirectory)
    assertResult(22)(metaPath.list().length)
    assert(countFiles(dataPath) > initial_cache_files)

    val first_cache_files = countFiles(dataPath)
    val res1 = spark.sql(s"cache data select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    assertResult(31)(metaPath.list().length)
    assert(countFiles(dataPath) > first_cache_files)

    val sqlStr =
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }

  test("test cache mergetree data async") {
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
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$remotePath/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main',
                 |               orderByKey='l_linenumber,l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    hdfsHelper.resetMeta()
    val dataPath = new File(hdfsHelper.HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val res = spark
      .sql(s"""
              |cache data async
              |  select * from lineitem_mergetree_hdfs
              |  after l_shipdate AS OF '1995-01-10'
              |  CACHEPROPERTIES(storage_policy='__hdfs_main',
              |                aaa='ccc')""".stripMargin)
      .collect()
    assertResult(true)(res(0).getBoolean(0))
    val metaPath = new File(hdfsHelper.metaPath(s"$SPARK_DIR_NAME/test/lineitem_mergetree_hdfs"))
    assertResult(true)(metaPath.exists() && metaPath.isDirectory)
    eventually(timeout(60.seconds), interval(2.seconds)) {
      assertResult(22)(metaPath.list().length)
      assert(countFiles(dataPath) > initial_cache_files)
    }

    val first_cache_files = countFiles(dataPath)
    val res1 = spark.sql(s"cache data async select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    eventually(timeout(60.seconds), interval(2.seconds)) {
      assertResult(31)(metaPath.list().length)
      assert(countFiles(dataPath) > first_cache_files)
    }

    val sqlStr =
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }

  test("test cache mergetree data with the path") {
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
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$remotePath/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main',
                 |               orderByKey='l_linenumber,l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    hdfsHelper.resetMeta()
    val dataPath = new File(hdfsHelper.HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val res = spark
      .sql(s"""
              |cache data
              |  select * from '$remotePath/lineitem_mergetree_hdfs'
              |  after l_shipdate AS OF '1995-01-10'
              |  CACHEPROPERTIES(storage_policy='__hdfs_main',
              |                aaa='ccc')""".stripMargin)
      .collect()
    assertResult(true)(res(0).getBoolean(0))
    val metaPath = new File(hdfsHelper.metaPath(s"$SPARK_DIR_NAME/test/lineitem_mergetree_hdfs"))
    assertResult(true)(metaPath.exists() && metaPath.isDirectory)
    assertResult(22)(metaPath.list().length)
    assert(countFiles(dataPath) > initial_cache_files)
    val first_cache_files = countFiles(dataPath)
    val res1 = spark.sql(s"cache data select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    assertResult(31)(metaPath.list().length)
    assert(countFiles(dataPath) > first_cache_files)

    val sqlStr =
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }

  test("test cache mergetree data no partition columns") {

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_hdfs
                 |(
                 | L_ORDERKEY      bigint,
                 | L_PARTKEY       bigint,
                 | L_SUPPKEY       bigint,
                 | L_LINENUMBER    bigint,
                 | L_QUANTITY      double,
                 | L_EXTENDEDPRICE double,
                 | L_DISCOUNT      double,
                 | L_TAX           double,
                 | L_RETURNFLAG    string,
                 | L_LINESTATUS    string,
                 | L_SHIPDATE      date,
                 | L_COMMITDATE    date,
                 | L_RECEIPTDATE   date,
                 | L_SHIPINSTRUCT  string,
                 | L_SHIPMODE      string,
                 | L_COMMENT       string
                 |)
                 |USING clickhouse
                 |LOCATION '$remotePath/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    hdfsHelper.resetMeta()
    val dataPath = new File(hdfsHelper.HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val metaPath = new File(hdfsHelper.metaPath(s"$SPARK_DIR_NAME/test/lineitem_mergetree_hdfs"))
    val res1 = spark.sql(s"cache data select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    assertResult(1)(metaPath.list().length)
    assert(countFiles(dataPath) > initial_cache_files)

    val sqlStr =
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }

  test("test cache mergetree data with upper case column name") {

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_hdfs
                 |(
                 | L_ORDERKEY      bigint,
                 | L_PARTKEY       bigint,
                 | L_SUPPKEY       bigint,
                 | L_LINENUMBER    bigint,
                 | L_QUANTITY      double,
                 | L_EXTENDEDPRICE double,
                 | L_DISCOUNT      double,
                 | L_TAX           double,
                 | L_RETURNFLAG    string,
                 | L_LINESTATUS    string,
                 | L_SHIPDATE      date,
                 | L_COMMITDATE    date,
                 | L_RECEIPTDATE   date,
                 | L_SHIPINSTRUCT  string,
                 | L_SHIPMODE      string,
                 | L_COMMENT       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (L_SHIPDATE)
                 |LOCATION '$remotePath/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main',
                 |               orderByKey='L_LINENUMBER,L_ORDERKEY')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    hdfsHelper.resetMeta()
    val dataPath = new File(hdfsHelper.HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val res = spark
      .sql(s"""
              |cache data
              |  select * from '$remotePath/lineitem_mergetree_hdfs'
              |  after L_SHIPDATE AS OF '1995-01-10'
              |  CACHEPROPERTIES(storage_policy='__hdfs_main',
              |                aaa='ccc')""".stripMargin)
      .collect()
    assertResult(true)(res(0).getBoolean(0))
    val metaPath = new File(hdfsHelper.metaPath(s"$SPARK_DIR_NAME/test/lineitem_mergetree_hdfs"))
    assertResult(true)(metaPath.exists() && metaPath.isDirectory)
    assertResult(22)(metaPath.list().length)
    assert(countFiles(dataPath) > initial_cache_files)
    val first_cache_files = countFiles(dataPath)
    val res1 = spark.sql(s"cache data select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    assertResult(31)(metaPath.list().length)
    assert(countFiles(dataPath) > first_cache_files)

    val sqlStr =
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }

  test("test disable cache files return") {
    withSQLConf(CHConfig.ENABLE_GLUTEN_LOCAL_FILE_CACHE.key -> "false") {
      runSql(
        s"CACHE FILES select * from '${hdfsHelper.hdfsURL("tpch-data/lineitem")}'",
        noFallBack = false) {
        df =>
          val res = df.collect()
          assert(res.length == 1)
          assert(!res.apply(0).getBoolean(0))
      }
    }
  }
}
