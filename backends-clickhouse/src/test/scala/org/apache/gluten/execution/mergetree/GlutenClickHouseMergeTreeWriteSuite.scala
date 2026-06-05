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

import org.apache.gluten.backendsapi.clickhouse.{CHBackendSettings, CHConfig, RuntimeSettings}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.utils.Arm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import org.apache.commons.io.filefilter.WildcardFileFilter
import org.scalatest.DoNotDiscover

import java.io.File

import scala.io.Source

@DoNotDiscover
class GlutenClickHouseMergeTreeWriteSuite extends CreateMergeTreeSuite {

  import org.apache.gluten.backendsapi.clickhouse.CHConfig._

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
      .set(RuntimeSettings.MIN_INSERT_BLOCK_SIZE_ROWS.key, "100000")
      .set(RuntimeSettings.MERGE_AFTER_INSERT.key, "false")
      .setCHSettings("input_format_parquet_max_block_size", 8192)
  }

  test("test mergetree table write") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree;
                 |""".stripMargin)

    // write.format.default = mergetree
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree
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
                 |TBLPROPERTIES (write.format.default = 'mergetree')
                 |LOCATION '$dataHome/lineitem_mergetree'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree
                 | select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree")) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
          case w: WholeStageTransformer => w
        }
        assertResult(4)(plans.size)

        val mergetreeScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
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
        val addFiles =
          fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(6)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)

        // GLUTEN-5060: check the unnecessary FilterExec
        val wholeStageTransformer = plans(2).asInstanceOf[WholeStageTransformer]
        val planNodeJson = wholeStageTransformer.substraitPlanJson
        assert(
          !planNodeJson
            .replaceAll("\n", "")
            .replaceAll(" ", "")
            .contains("\"input\":{\"filter\":{"))
    }
  }

  test("test mergetree insert overwrite") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_insertoverwrite;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_insertoverwrite
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
                 |LOCATION '$dataHome/lineitem_mergetree_insertoverwrite'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_insertoverwrite
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | insert overwrite table lineitem_mergetree_insertoverwrite
                 | select * from lineitem where mod(l_orderkey,2) = 1
                 |""".stripMargin)
    val sql2 =
      s"""
         | select count(*) from lineitem_mergetree_insertoverwrite
         |
         |""".stripMargin
    assertResult(300001)(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0)
    )
  }

  test("test mergetree insert overwrite partitioned table with small table, static") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_insertoverwrite2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_insertoverwrite2
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
                 |LOCATION '$dataHome/lineitem_mergetree_insertoverwrite2'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_insertoverwrite2
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(
      s"""
         | insert overwrite table lineitem_mergetree_insertoverwrite2
         | select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
         |""".stripMargin)
    val sql2 =
      s"""
         | select count(*) from lineitem_mergetree_insertoverwrite2
         |
         |""".stripMargin
    assertResult(2418)(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0)
    )
  }

  test("test mergetree insert overwrite partitioned table with small table, dynamic") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_insertoverwrite3 PURGE;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_insertoverwrite3
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
                   |LOCATION '$dataHome/lineitem_mergetree_insertoverwrite3'
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table lineitem_mergetree_insertoverwrite3
                   | select * from lineitem
                   |""".stripMargin)

      spark.sql(
        s"""
           | insert overwrite table lineitem_mergetree_insertoverwrite3
           | select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      val sql2 =
        s"""
           | select count(*) from lineitem_mergetree_insertoverwrite3
           |
           |""".stripMargin
      assertResult(600572)(
        // total rows should remain unchanged
        spark.sql(sql2).collect().apply(0).get(0)
      )
    }
  }

  test("test mergetree table update") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_update;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_update
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
                 |LOCATION '$dataHome/lineitem_mergetree_update'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_update
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | update lineitem_mergetree_update set l_returnflag = 'Z' where l_orderkey = 12647
                 |""".stripMargin)

    {
      val sql1 =
        s"""
           | select count(*) from lineitem_mergetree_update where l_returnflag = 'Z'
           |
           |""".stripMargin

      val df = spark.sql(sql1)
      assertResult(1)(
        // in test data, there are only 1 row with l_orderkey = 12647
        df.collect().apply(0).get(0)
      )
    }

    val sql2 =
      s"""
         | select count(*) from lineitem_mergetree_update
         |
         |""".stripMargin
    assertResult(600572)(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0)
    )
  }

  test("test mergetree table delete") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_delete;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_delete
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
                 |LOCATION '$dataHome/lineitem_mergetree_delete'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_delete
                 | select * from lineitem
                 |""".stripMargin)
    val df1 = spark.sql(s"""
                           | delete from lineitem_mergetree_delete where l_orderkey = 12647
                           |""".stripMargin)

    {
      val df = spark.sql(s"""
                            | select count(*) from lineitem_mergetree_delete
                            |""".stripMargin)
      assertResult(600571)(
        df.collect().apply(0).get(0)
      )
    }

    {
      spark.sql(s"""
                   | delete from lineitem_mergetree_delete where mod(l_orderkey, 3) = 2
                   |""".stripMargin)
      val df3 = spark.sql(s"""
                             | select count(*) from lineitem_mergetree_delete
                             |""".stripMargin)
      assertResult(400089)(df3.collect().apply(0).get(0))
    }
  }

  test("test mergetree table upsert") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_upsert;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_upsert
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
                 |LOCATION '$dataHome/lineitem_mergetree_upsert'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_upsert
                 | select * from lineitem
                 |""".stripMargin)

    {
      val df0 = spark.sql(s"""
                             | select count(*) from lineitem_mergetree_upsert
                             |""".stripMargin)
      assertResult(600572)(df0.collect().apply(0).get(0))
    }

    upsertSourceTableAndCheck("lineitem_mergetree_upsert")
  }

  private def upsertSourceTableAndCheck(tableName: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into $tableName
          using (

            select l_orderkey, l_partkey, l_suppkey, l_linenumber,
              l_quantity, l_extendedprice, l_discount, l_tax,
               'Z' as `l_returnflag`,l_linestatus, l_shipdate, l_commitdate,
                l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in
              (select l_orderkey from lineitem group by l_orderkey having count(*) =1 )
              and l_orderkey < 100000

            union

            select l_orderkey + 10000000,
              l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
              l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate,
              l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in
             (select l_orderkey from lineitem group by l_orderkey having count(*) =1 )
             and l_orderkey < 100000
          ) as updates
          on updates.l_orderkey = $tableName.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from $tableName
                             |""".stripMargin)
      assertResult(600572 + 3506)(df1.collect().apply(0).get(0))
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_returnflag = 'Z'
                     |""".stripMargin)
      assertResult(3506)(df2.collect().apply(0).get(0))
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_orderkey > 10000000
                     |""".stripMargin)
      assertResult(3506)(df3.collect().apply(0).get(0))
    }
  }

  test("test mergetree write with orderby keys / primary keys") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey
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
                 |TBLPROPERTIES (orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_orderbykey'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey
                 | select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree_orderbykey")) {
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

        assertResult(6)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)
    }
  }

  test("test mergetree write with partition") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_partition;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_partition
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
                 |PARTITIONED BY (l_shipdate, l_returnflag)
                 |TBLPROPERTIES (orderByKey='l_orderkey',
                 |               primaryKey='l_orderkey')
                 |LOCATION '$dataHome/lineitem_mergetree_partition'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_mergetree_partition
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
      .insertInto("lineitem_mergetree_partition")

    // static partition
    spark.sql(s"""
                 | insert into lineitem_mergetree_partition
                 | PARTITION (l_shipdate=date'1995-01-21', l_returnflag = 'A')
                 | (l_orderkey,
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
                 | select l_orderkey,
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
                 |  where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree_partition"), compare = false) {
      df =>
        val result = df.collect()
        assertResult(4)(result.length)
        assertResult("A")(result(0).getString(0))
        assertResult("F")(result(0).getString(1))
        assertResult(3865234.0)(result(0).getDouble(2))

        assertResult("N")(result(2).getString(0))
        assertResult("O")(result(2).getString(1))
        assertResult(7454519.0)(result(2).getDouble(2))

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))
        assertResult(3745)(mergetreeScan.metrics("numFiles").value)

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
        assertResult(2)(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size)
        assertResult("l_shipdate")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns
            .head)
        assertResult("l_returnflag")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(1))
        val addFiles =
          fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(3836)(addFiles.size)
        assertResult(605363)(addFiles.map(_.rows).sum)
        assertResult(2)(addFiles.count(_.partitionValues("l_shipdate").equals("1992-06-01")))
        assertResult(4)(addFiles.count(_.partitionValues("l_shipdate").equals("1993-01-01")))
        assertResult(3)(addFiles.count(_.partitionValues("l_shipdate").equals("1995-01-21")))
    }
  }

  testSparkVersionLE33("test mergetree write with bucket table") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_bucket;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_bucket
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
                 |CLUSTERED BY (l_partkey)
                 |${if (spark32) "" else "SORTED BY (l_orderkey)"} INTO 4 BUCKETS
                 |LOCATION '$dataHome/lineitem_mergetree_bucket'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_bucket
                 | select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree_bucket")) {
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
          assertResult("l_orderkey")(
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
        assertResult(4)(addFiles.count(_.partitionValues("l_returnflag").equals("A")))
        assertResult(4)(addFiles.count(_.partitionValues("l_returnflag").equals("N")))
        assertResult(4)(addFiles.count(_.partitionValues("l_returnflag").equals("R")))
        assertResult(1)(
          addFiles.count(
            f => f.partitionValues("l_returnflag").equals("A") && f.bucketNum.equals("00000")))
    }
    // check part pruning effect of filter on bucket column
    val df = spark.sql(s"""
                          | select * from lineitem_mergetree_bucket where l_orderkey = 12647
                          | and l_shipdate = date'1997-06-02'
                          |""".stripMargin)
    df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val touchedParts = scanExec.head.getPartitions
      .flatMap(partition => partition.asInstanceOf[GlutenMergeTreePartition].partList)
      .map(_.name)
      .distinct
    assertResult(12)(touchedParts.size)

    // test upsert on partitioned & bucketed table
    upsertSourceTableAndCheck("lineitem_mergetree_bucket")

    // test insert overwrite on partitioned & bucketed table
    spark.sql("create table lineitem_3_rows like lineitem")
    spark.sql("insert into table lineitem_3_rows select * from lineitem where l_orderkey = 12643")
    spark.sql("insert overwrite table lineitem_mergetree_bucket select * from lineitem_3_rows")
    val df0 = spark.sql(s"""
                           | select count(*) from lineitem_mergetree_bucket
                           |""".stripMargin)
    assertResult(3)(df0.collect().apply(0).get(0))

  }

  test("GLUTEN-4749: Support to purge mergetree data for CH backend") {
    def createAndDropTable(
        tableName: String,
        tableLocation: String,
        isExternal: Boolean = false,
        purgeTable: Boolean = false): Unit = {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS $tableName;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE ${if (isExternal) "EXTERNAL" else ""} TABLE IF NOT EXISTS $tableName
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
                   |TBLPROPERTIES (orderByKey='l_shipdate,l_orderkey',
                   |               primaryKey='l_shipdate')
                   |${if (tableLocation.nonEmpty) "LOCATION '" + tableLocation + "'" else ""}
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table $tableName
                   | select * from lineitem
                   |""".stripMargin)

      spark.sql(s"""
                   |DROP TABLE IF EXISTS $tableName ${if (purgeTable) "PURGE" else ""};
                   |""".stripMargin)
    }

    def checkTableExists(
        tableName: String,
        tableLocation: String,
        exceptedExists: Boolean): Unit = {
      val tableList = spark
        .sql(s"""
                |show tables;
                |""".stripMargin)
        .collect()
      assert(!tableList.exists(_.getString(1).equals(tableName)))

      val deletedPathStr = if (tableLocation.nonEmpty) {
        tableLocation
      } else {
        warehouse + "/" + tableName
      }
      val deletedPath = new File(deletedPathStr)
      assertResult(exceptedExists)(deletedPath.exists())
    }

    // test non external table
    var tableName = "lineitem_mergetree_drop"
    var tableLocation = ""
    createAndDropTable(tableName, tableLocation)
    checkTableExists(tableName, tableLocation, exceptedExists = false)

    // test external table
    tableName = "lineitem_mergetree_external_drop"
    createAndDropTable(tableName, tableLocation, isExternal = true)
    checkTableExists(tableName, tableLocation, exceptedExists = false)

    // test table with the specified location
    tableName = "lineitem_mergetree_location_drop"
    tableLocation = dataHome + "/" + tableName
    createAndDropTable(tableName, tableLocation)
    checkTableExists(tableName, tableLocation, exceptedExists = true)

    tableName = "lineitem_mergetree_external_location_drop"
    tableLocation = dataHome + "/" + tableName
    createAndDropTable(tableName, tableLocation, isExternal = true)
    checkTableExists(tableName, tableLocation, exceptedExists = true)

    tableName = "lineitem_mergetree_location_purge"
    tableLocation = dataHome + "/" + tableName
    createAndDropTable(tableName, tableLocation, purgeTable = true)
    checkTableExists(tableName, tableLocation, exceptedExists = false)

    tableName = "lineitem_mergetree_external_location_purge"
    tableLocation = dataHome + "/" + tableName
    createAndDropTable(tableName, tableLocation, isExternal = true, purgeTable = true)
    checkTableExists(tableName, tableLocation, exceptedExists = false)
  }

  test("test mergetree CTAS simple") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_ctas1;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE lineitem_mergetree_ctas1
                 |USING clickhouse
                 |LOCATION '$dataHome/lineitem_mergetree_ctas1'
                 | as select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_mergetree_ctas1")) {
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
        assertResult(6)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)
    }
  }

  test("test mergetree CTAS partition") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_ctas2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_ctas2
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$dataHome/lineitem_mergetree_ctas2'
                 | as select * from lineitem
                 |""".stripMargin)

    checkQuery(q1("lineitem_mergetree_ctas2"))
  }

  test("test mergetree table with low cardinality column") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_lowcard;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_lowcard
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
                 |LOCATION '$dataHome/lineitem_mergetree_lowcard'
                 |TBLPROPERTIES('lowCardKey'='l_returnflag,L_LINESTATUS,l_quantity')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_lowcard
                 | select * from lineitem
                 |""".stripMargin)

    checkQuery(q1("lineitem_mergetree_lowcard"))
    val directory = new File(s"$dataHome/lineitem_mergetree_lowcard")
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    val columnsFile = new File(partDir, "columns.txt")
    val columns = Arm.withResource(Source.fromFile(columnsFile))(_.getLines().mkString)
    assert(columns.contains("`l_returnflag` LowCardinality(Nullable(String))"))
    assert(columns.contains("`l_linestatus` LowCardinality(Nullable(String))"))

    // test low card column in measure
    val sqlStr2 =
      s"""
         |SELECT
         |  max(l_returnflag), min(l_quantity)
         |FROM
         |    lineitem_mergetree_lowcard
         |GROUP BY
         |    l_linestatus
         |  order by l_linestatus
         |
         |""".stripMargin

    assertResult("R")(
      // total rows should remain unchanged
      spark.sql(sqlStr2).collect().apply(0).get(0)
    )

    // test select *
    val sqlStr3 =
      s"""
         |SELECT
         |  *
         |FROM
         |    lineitem_mergetree_lowcard limit 1
         |
         |""".stripMargin
    spark.sql(sqlStr3).collect()
  }

  test("test mergetree with primary keys filter") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey2
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
                 |TBLPROPERTIES (orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_orderbykey2'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey2
                 | select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q6("lineitem_mergetree_orderbykey2")) {
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

        assertResult(6)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)

        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assertResult(1)(plans.size)
        assertResult(17)(plans.head.metrics("selectedMarksPk").value)
        assertResult(74)(plans.head.metrics("totalMarksPk").value)
    }
  }

  test("test mergetree with order keys filter") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey3
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
                 |TBLPROPERTIES (orderByKey='l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_orderbykey3'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey3
                 | select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q6("lineitem_mergetree_orderbykey3")) {
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
        assertResult("l_shipdate")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKey)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKey
            .isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(6)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)

        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assertResult(1)(plans.size)
        assertResult(17)(plans.head.metrics("selectedMarksPk").value)
        assertResult(74)(plans.head.metrics("totalMarksPk").value)
    }
  }

  test(
    "GLUTEN-5061: Fix assert error when writing mergetree data with select * from table limit n") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_5061;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_5061
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
                 |LOCATION '$dataHome/lineitem_mergetree_5061'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_5061
                 | select * from lineitem limit 10
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    count(1)
         |FROM
         |    lineitem_mergetree_5061
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val result = df.collect()
        assertResult(1)(result.length)
        assertResult(10)(result(0).getLong(0))
      })
  }

  test("GLUTEN-5062: Add a UT to ensure that IN filtering can apply on CH primary key") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_5062;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_5062
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
                 |TBLPROPERTIES (orderByKey='l_returnflag,l_shipdate',
                 |               primaryKey='l_returnflag,l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_5062'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_5062
                 | select * from lineitem
                 |""".stripMargin)

    def checkSelectedMarksCnt(df: DataFrame, exceptedCnt: Long): Unit = {
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assertResult(1)(scanExec.size)

      val mergetreeScan = scanExec.head
      assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

      val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
      assertResult(mergetreeScan.metrics("totalMarksPk").value)(
        addFiles.map(_.marks).sum - addFiles.size)
      assertResult(exceptedCnt)(mergetreeScan.metrics("selectedMarksPk").value)
    }

    val sqlStr1 =
      s"""
         |SELECT
         |    sum(l_extendedprice)
         |FROM
         |    lineitem_mergetree_5062
         |WHERE
         |    l_shipdate in (date'1998-08-15', date'1993-12-05', date'1993-03-01')
         |""".stripMargin
    runSql(sqlStr1)(
      df => {
        val result = df.collect()
        assertResult(1)(result.length)
        assertResult("2.6480")(result(0).getDouble(0).toString.substring(0, 6))

        checkSelectedMarksCnt(df, 34)
      })

    val sqlStr2 =
      s"""
         |SELECT
         |    sum(l_extendedprice)
         |FROM
         |    lineitem_mergetree_5062
         |WHERE
         |    l_returnflag not in ('N', 'A')
         |""".stripMargin
    runSql(sqlStr2)(
      df => {
        val result = df.collect()
        assertResult(1)(result.length)
        assertResult("5.3379")(result(0).getDouble(0).toString.substring(0, 6))

        checkSelectedMarksCnt(df, 29)
      })
  }

  test("GLUTEN-5219: Fix the table metadata sync issue for the CH backend") {
    def checkQueryResult(tableName: String): Unit = {
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
      customCheckQuery(q1(tableName)) {
        df =>
          val scanExec = collect(df.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer => f
          }
          assertResult(1)(scanExec.size)

          val mergetreeScan = scanExec.head
          assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

          val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
          val addFiles =
            fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
          assertResult(6)(addFiles.size)
          assertResult(600572)(addFiles.map(_.rows).sum)
      }
    }

    // test with ctas
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_ctas_5219;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE lineitem_mergetree_ctas_5219
                 |USING clickhouse
                 |LOCATION '$dataHome/lineitem_mergetree_ctas_5219'
                 | as select * from lineitem
                 |""".stripMargin)

    checkQueryResult("lineitem_mergetree_ctas_5219")

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_ctas_5219;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE lineitem_mergetree_ctas_5219
                 |USING clickhouse
                 |TBLPROPERTIES (orderByKey='l_returnflag,l_shipdate',
                 |               primaryKey='l_returnflag,l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_ctas_5219_1'
                 | as select * from lineitem
                 |""".stripMargin)

    checkQueryResult("lineitem_mergetree_ctas_5219")

    var dataPath = new File(s"$dataHome/lineitem_mergetree_ctas_5219_1")
    assert(dataPath.isDirectory && dataPath.isDirectory)

    val fileFilter = new WildcardFileFilter("*_0_*")
    var dataFileList = dataPath.list(fileFilter)
    assertResult(6)(dataFileList.length)

    // test with the normal table
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_5219;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_5219
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
                 |TBLPROPERTIES (orderByKey='l_returnflag,l_shipdate',
                 |               primaryKey='l_returnflag,l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_5219'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_5219
                 | select * from lineitem
                 |""".stripMargin)

    checkQueryResult("lineitem_mergetree_5219")

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_5219;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_5219
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
                 |TBLPROPERTIES (orderByKey='l_shipdate',
                 |               primaryKey='l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_5219_1'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_5219
                 | select * from lineitem
                 |""".stripMargin)

    checkQueryResult("lineitem_mergetree_5219")

    dataPath = new File(s"$dataHome/lineitem_mergetree_5219_1")
    assert(dataPath.isDirectory && dataPath.isDirectory)

    dataFileList = dataPath.list(fileFilter)
    assertResult(6)(dataFileList.length)

    // re-create the same table
    for (i <- 0 until 10) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_5219_s purge;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE lineitem_mergetree_5219_s
                   |USING clickhouse
                   |LOCATION '$dataHome/lineitem_mergetree_5219_s'
                   | as select * from lineitem
                   |""".stripMargin)

      checkQueryResult("lineitem_mergetree_5219_s")
    }

    dataPath = new File(s"$dataHome/lineitem_mergetree_5219_s")
    assert(dataPath.isDirectory && dataPath.isDirectory)

    dataFileList = dataPath.list(fileFilter)
    assertResult(6)(dataFileList.length)
  }

  test("test mergetree with primary keys filter pruning by driver") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_pk_pruning_by_driver;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_pk_pruning_by_driver
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
                 |TBLPROPERTIES (orderByKey='l_shipdate')
                 |LOCATION '$dataHome/lineitem_mergetree_pk_pruning_by_driver'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_pk_pruning_by_driver
                 | select * from lineitem
                 |""".stripMargin)

    Seq(("true", 2), ("false", 3)).foreach(
      conf => {
        withSQLConf(CHConfig.runtimeSettings("enabled_driver_filter_mergetree_index") -> conf._1) {
          customCheckQuery(q6("lineitem_mergetree_pk_pruning_by_driver")) {
            df =>
              val scanExec = collect(df.queryExecution.executedPlan) {
                case f: FileSourceScanExecTransformer => f
              }
              assertResult(1)(scanExec.size)

              val mergetreeScan = scanExec.head
              assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

              val plans = collect(df.queryExecution.executedPlan) {
                case scanExec: BasicScanExecTransformer => scanExec
              }
              assertResult(1)(plans.size)
              assertResult(conf._2)(plans.head.getSplitInfos.size)
          }
        }
      })

    // GLUTEN-7670: fix enable 'files.per.partition.threshold'
    withSQLConf(
      CHConfig.runtimeSettings("enabled_driver_filter_mergetree_index") -> "true",
      CHConfig.prefixOf("files.per.partition.threshold") -> "10"
    ) {
      customCheckQuery(q6("lineitem_mergetree_pk_pruning_by_driver")) {
        df =>
          val scanExec = collect(df.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer => f
          }
          assertResult(1)(scanExec.size)

          val mergetreeScan = scanExec.head
          assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

          val plans = collect(df.queryExecution.executedPlan) {
            case scanExec: BasicScanExecTransformer => scanExec
          }
          assertResult(1)(plans.size)
          assertResult(1)(plans.head.getSplitInfos.size)
      }
    }
  }

  testSparkVersionLE33("test mergetree with primary keys filter pruning by driver with bucket") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_pk_pruning_by_driver_bucket;
                 |""".stripMargin)
    spark.sql(s"""
                 |DROP TABLE IF EXISTS orders_mergetree_pk_pruning_by_driver_bucket;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_pk_pruning_by_driver_bucket
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
                 |CLUSTERED by (l_orderkey)
                 |${if (spark32) "" else "SORTED BY (l_receiptdate)"} INTO 2 BUCKETS
                 |LOCATION '$dataHome/lineitem_mergetree_pk_pruning_by_driver_bucket'
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS orders_mergetree_pk_pruning_by_driver_bucket (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    double,
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 |USING clickhouse
                 |CLUSTERED by (o_orderkey)
                 |${if (spark32) "" else "SORTED BY (o_orderdate)"} INTO 2 BUCKETS
                 |LOCATION '$dataHome/orders_mergetree_pk_pruning_by_driver_bucket'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_pk_pruning_by_driver_bucket
                 | select * from lineitem
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table orders_mergetree_pk_pruning_by_driver_bucket
                 | select * from orders
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_shipmode,
         |    sum(
         |        CASE WHEN o_orderpriority = '1-URGENT'
         |            OR o_orderpriority = '2-HIGH' THEN
         |            1
         |        ELSE
         |            0
         |        END) AS high_line_count,
         |    sum(
         |        CASE WHEN o_orderpriority <> '1-URGENT'
         |            AND o_orderpriority <> '2-HIGH' THEN
         |            1
         |        ELSE
         |            0
         |        END) AS low_line_count
         |FROM
         |    orders_mergetree_pk_pruning_by_driver_bucket,
         |    lineitem_mergetree_pk_pruning_by_driver_bucket
         |WHERE
         |    o_orderkey = l_orderkey
         |    AND l_shipmode IN ('MAIL', 'SHIP')
         |    AND l_commitdate < l_receiptdate
         |    AND l_shipdate < l_commitdate
         |    AND l_receiptdate >= date'1994-01-01'
         |    AND l_receiptdate < date'1994-01-01' + interval 1 year
         |GROUP BY
         |    l_shipmode
         |ORDER BY
         |    l_shipmode;
         |""".stripMargin

    Seq(("true", 2), ("false", 2)).foreach(
      conf => {
        withSQLConf(CHConfig.runtimeSettings("enabled_driver_filter_mergetree_index") -> conf._1) {
          customCheckQuery(sqlStr -> 12) {
            df =>
              val scanExec = collect(df.queryExecution.executedPlan) {
                case f: BasicScanExecTransformer => f
              }
              assertResult(2)(scanExec.size)
              assertResult(conf._2)(scanExec(1).getSplitInfos.size)
          }
        }
      })
  }

  test("GLUTEN-6378: Support delta count optimizer for the MergeTree format") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_count_opti;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_count_opti
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
                 |PARTITIONED BY (l_shipdate, l_returnflag)
                 |TBLPROPERTIES (orderByKey='l_orderkey',
                 |               primaryKey='l_orderkey')
                 |LOCATION '$dataHome/lineitem_mergetree_count_opti'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_mergetree_count_opti
                 | select * from lineitem
                 |""".stripMargin)

    Seq("true", "false").foreach {
      skip =>
        withSQLConf("spark.databricks.delta.stats.skipping" -> skip) {
          val sqlStr =
            s"""
               |SELECT
               |    count(*) AS count_order
               |FROM
               |    lineitem_mergetree_count_opti
               |""".stripMargin
          runSql(sqlStr)(
            df => {
              val result = df.collect()
              assertResult(1)(result.length)
              assertResult("600572")(result(0).getLong(0).toString)

              // Spark 3.2 + Delta 2.0 does not support this feature
              if (!spark32) {
                assert(df.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])
              }
            })
        }
    }
  }

  test("test mergetree with column case sensitive") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS LINEITEM_MERGETREE_CASE_SENSITIVE;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS LINEITEM_MERGETREE_CASE_SENSITIVE
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
                 |TBLPROPERTIES (orderByKey='L_DISCOUNT')
                 |LOCATION '$dataHome/LINEITEM_MERGETREE_CASE_SENSITIVE'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_case_sensitive
                 | select * from lineitem
                 |""".stripMargin)

    checkQuery(q6("lineitem_mergetree_case_sensitive"))
  }

  test("test mergetree with partition with whitespace") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_partition_with_whitespace;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_partition_with_whitespace
                 |(
                 | l_returnflag    string,
                 | l_linestatus    string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$dataHome/lineitem_mergetree_partition_with_whitespace'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_partition_with_whitespace
                 | (l_returnflag, l_linestatus) values ('a A', 'abc')
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT * from lineitem_mergetree_partition_with_whitespace
         |""".stripMargin
    runSql(sqlStr) { _ => }
  }

  test("GLUTEN-7358: Optimize the strategy of the partition split according to the files count") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_split;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_split
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
                 |LOCATION '$dataHome/lineitem_split'
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table lineitem_split
                 | select * from lineitem
                 |""".stripMargin)
    Seq(("-1", 3), ("3", 3), ("6", 1)).foreach(
      conf => {
        withSQLConf(CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD -> conf._1) {
          val sql =
            s"""
               |select count(1), min(l_returnflag) from lineitem_split
               |""".stripMargin
          runSql(sql) {
            df =>
              val result = df.collect()
              assertResult(1)(result.length)
              assertResult("600572")(result(0).getLong(0).toString)
              val scanExec = collect(df.queryExecution.executedPlan) {
                case f: FileSourceScanExecTransformer => f
              }
              assert(scanExec.head.getPartitions.size == conf._2)
          }
        }
      })
  }

  test(
    "GLUTEN-7812: Fix the query failed for the mergetree format " +
      "when the 'spark.databricks.delta.stats.skipping' is off") {
    // Spark 3.2 + Delta 2.0 doesn't not support this feature
    if (!spark32) {
      withSQLConf(("spark.databricks.delta.stats.skipping", "false")) {
        spark.sql(s"""
                     |DROP TABLE IF EXISTS lineitem_mergetree_stats_skipping;
                     |""".stripMargin)

        spark.sql(s"""
                     |CREATE TABLE IF NOT EXISTS lineitem_mergetree_stats_skipping
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
                     |TBLPROPERTIES (orderByKey='l_orderkey',
                     |               primaryKey='l_orderkey')
                     |LOCATION '$dataHome/lineitem_mergetree_stats_skipping'
                     |""".stripMargin)

        // dynamic partitions
        spark.sql(s"""
                     | insert into table lineitem_mergetree_stats_skipping
                     | select * from lineitem
                     |""".stripMargin)

        val sqlStr =
          s"""
             |SELECT
             |    o_orderpriority,
             |    count(*) AS order_count
             |FROM
             |    orders
             |WHERE
             |    o_orderdate >= date'1993-07-01'
             |    AND o_orderdate < date'1993-07-01' + interval 3 month
             |    AND EXISTS (
             |        SELECT
             |            *
             |        FROM
             |            lineitem
             |        WHERE
             |            l_orderkey = o_orderkey
             |            AND l_commitdate < l_receiptdate)
             |GROUP BY
             |    o_orderpriority
             |ORDER BY
             |    o_orderpriority;
             |
             |""".stripMargin
        runSql(sqlStr)(
          df => {
            val result = df.collect()
            assertResult(5)(result.length)
            assertResult("1-URGENT")(result(0).getString(0))
            assertResult(999)(result(0).getLong(1))
            assertResult("2-HIGH")(result(1).getString(0))
            assertResult(997)(result(1).getLong(1))
            assertResult("5-LOW")(result(4).getString(0))
            assertResult(1077)(result(4).getLong(1))
          })
      }
    }
  }
}
