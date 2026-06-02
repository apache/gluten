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

import org.apache.gluten.backendsapi.clickhouse.RuntimeSettings
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.gluten.delta.DeltaStatsUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex

import io.delta.tables.DeltaTable

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseDeltaParquetWriteSuite extends ParquetTPCHSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, spark35.toString)
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set(RuntimeSettings.MERGE_AFTER_INSERT.key, "false")
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  }

  private val q1SchemaString: String =
    s""" l_orderkey      bigint,
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
       | l_comment       string""".stripMargin

  private def doInsert(drop: String, create: String, insert: String): Unit = {
    spark.sql(drop)
    spark.sql(create)
    spark.sql(insert)
  }
  private def drop(table: String): String = s"DROP TABLE IF EXISTS $table"
  private def createLineitem(table: String): String =
    s"""CREATE TABLE IF NOT EXISTS $table ($q1SchemaString) USING delta
       |TBLPROPERTIES (write.format.default = 'parquet')
       |LOCATION '$dataHome/$table'
       |""".stripMargin

  test("test parquet table write with the delta") {

    def insert(table: String): String =
      s"insert into table $table select /*+ REPARTITION(5) */ * from lineitem"

    val table = "lineitem_delta_parquet"
    doInsert(drop(table), createLineitem(table), insert(table))

    customCheckQuery(q1(table)) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
          case w: WholeStageTransformer => w
        }
        assert(plans.size === 4)

        val parquetScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
        assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet "))

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil)
        assert(addFiles.size === 5)
    }

    if (spark35) {
      val vanillaTable = "lineitem_delta_parquet_vanilla"
      withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")) {
        doInsert(drop(vanillaTable), createLineitem(vanillaTable), insert(vanillaTable))
      }
      val expected = DeltaStatsUtils
        .statsDF(
          spark,
          s"$dataHome/$vanillaTable/_delta_log/00000000000000000001.json",
          q1SchemaString)
        .collect()

      checkAnswer(
        DeltaStatsUtils.statsDF(
          spark,
          s"$dataHome/$table/_delta_log/00000000000000000001.json",
          q1SchemaString),
        expected
      )
    }
  }

  test("test parquet insert overwrite with the delta") {
    def insert(table: String): String =
      s"insert into table $table select  * from lineitem"

    val table = "lineitem_delta_parquet_insertoverwrite"
    doInsert(drop(table), createLineitem(table), insert(table))
    spark.sql(s"""
                 | insert overwrite table $table
                 | select * from lineitem where mod(l_orderkey,2) = 1
                 |""".stripMargin)

    // total rows should remain unchanged
    assert(spark.sql(s"select count(*) from $table").collect().apply(0).get(0) === 300001)
  }

  test("test parquet insert overwrite partitioned table with small table, static with delta") {
    val table = "lineitem_delta_parquet_insertoverwrite2"
    doInsert(
      drop(table),
      s"""
         |CREATE TABLE IF NOT EXISTS $table ($q1SchemaString) USING delta
         |PARTITIONED BY (l_shipdate)
         |LOCATION '$dataHome/$table'
         |""".stripMargin,
      s"""
         | insert into table $table select * from lineitem
         | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
         |""".stripMargin
    )
    spark.sql(s"""
                 | insert overwrite table $table select * from lineitem
                 | where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
                 |""".stripMargin)
    // total rows should remain unchanged
    assert(spark.sql(s"select count(*) from $table").collect().apply(0).get(0) === 2418)
  }

  test("test parquet insert overwrite partitioned table with small table, dynamic with delta") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_parquet_insertoverwrite3 PURGE;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_insertoverwrite3
                   |($q1SchemaString)
                   |USING delta
                   |PARTITIONED BY (l_shipdate)
                   |LOCATION '$dataHome/lineitem_delta_parquet_insertoverwrite3'
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table lineitem_delta_parquet_insertoverwrite3
                   | select * from lineitem
                   | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                   |""".stripMargin)

      spark.sql(
        s"""
           | insert overwrite table lineitem_delta_parquet_insertoverwrite3
           | select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      val sql2 =
        s"""
           | select count(*) from lineitem_delta_parquet_insertoverwrite3
           |
           |""".stripMargin
      assert(
        // total rows should remain unchanged
        spark.sql(sql2).collect().apply(0).get(0) === 21875
      )
    }
  }

  test("test parquet table update with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_update;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_update
                 |($q1SchemaString)
                 |USING delta
                 |LOCATION '$dataHome/lineitem_delta_parquet_update'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_update
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)

    spark.sql(
      s"""
         | update lineitem_delta_parquet_update set l_returnflag = 'Z' where l_orderkey = 12647
         |""".stripMargin)

    {
      val sql1 =
        s"""
           | select count(*) from lineitem_delta_parquet_update where l_returnflag = 'Z'
           |
           |""".stripMargin

      val df = spark.sql(sql1)
      val result = df.collect()
      assert(
        // in test data, there are only 1 row with l_orderkey = 12647
        result.apply(0).get(0) === 1
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size === 1)

      val parquetScan = scanExec.head
      assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet"))

      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil)
      assert(addFiles.size === 6)
    }

    val sql2 =
      s"""
         | select count(*) from lineitem_delta_parquet_update
         |
         |""".stripMargin

    // total rows should remain unchanged
    assert(spark.sql(sql2).collect().apply(0).get(0) === 600572)
  }

  test("test parquet table delete with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_delete;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_delete
                 |($q1SchemaString)
                 |USING delta
                 |LOCATION '$dataHome/lineitem_delta_parquet_delete'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_delete
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)
    val df1 = spark.sql(s"""
                           | delete from lineitem_delta_parquet_delete where l_orderkey = 12647
                           |""".stripMargin)

    {
      val df = spark.sql(s"""
                            | select sum(l_linenumber) from lineitem_delta_parquet_delete
                            |""".stripMargin)
      val result = df.collect()
      assert(
        result.apply(0).get(0) === 1802445
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      val parquetScan = scanExec.head
      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil)
      assert(addFiles.size === 6)
    }

    {
      spark.sql(s"""
                   | delete from lineitem_delta_parquet_delete where mod(l_orderkey, 3) = 2
                   |""".stripMargin)
      val df3 = spark.sql(s"""
                             | select sum(l_linenumber) from lineitem_delta_parquet_delete
                             |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) === 1200671
      )
    }
  }

  test("test parquet table upsert with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_upsert;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_upsert
                 |($q1SchemaString)
                 |USING delta
                 |LOCATION '$dataHome/lineitem_delta_parquet_upsert'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_upsert
                 | select * from lineitem
                 |""".stripMargin)

    {
      val df0 = spark.sql(s"""
                             | select sum(l_linenumber) from lineitem_delta_parquet_upsert
                             |""".stripMargin)
      assert(
        df0.collect().apply(0).get(0) === 1802446
      )
    }

    upsertSourceTableAndCheck("lineitem_delta_parquet_upsert")
  }

  private def upsertSourceTableAndCheck(tableName: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into $tableName
          using (

            select l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
           'Z' as `l_returnflag`,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

            union

            select l_orderkey + 10000000,
            l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

          ) as updates
          on updates.l_orderkey = $tableName.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from $tableName
                             |""".stripMargin)
      assert(
        df1.collect().apply(0).get(0) === 600572 + 3506
      )
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_returnflag = 'Z'
                     |""".stripMargin)
      assert(
        df2.collect().apply(0).get(0) === 3506
      )
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_orderkey > 10000000
                     |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) === 3506
      )
    }
  }

  test("test parquet write with partition + delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_partition;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_partition
                 |($q1SchemaString)
                 |USING delta
                 |PARTITIONED BY (l_shipdate, l_returnflag)
                 |LOCATION '$dataHome/lineitem_delta_parquet_partition'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_partition
                 | select * from lineitem
                 | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
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
      .format("delta")
      .mode(SaveMode.Append)
      .insertInto("lineitem_delta_parquet_partition")

    // static partition
    spark.sql(
      s"""
         | insert into lineitem_delta_parquet_partition PARTITION (l_shipdate=date'1993-02-21',
         | l_returnflag = 'A')
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
    customCheckQuery(q1("lineitem_delta_parquet_partition"), compare = false) {
      df =>
        val result = df.collect()
        assert(result.length === 2)
        assert(result(0).getString(0).equals("A"))
        assert(result(0).getString(1).equals("F"))
        assert(result(0).getDouble(2) === 368009.0)

        assert(result(1).getString(0).equals("R"))
        assert(result(1).getString(1).equals("F"))
        assert(result(1).getDouble(2) === 312371.0)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size === 1)

        val parquetScan = scanExec.head
        assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet"))
        assert(parquetScan.metrics("numFiles").value === 201)

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil)

        assert(addFiles.size === 201)
        assert(addFiles.count(_.partitionValues("l_shipdate").equals("1993-03-31")) === 2)
        assert(addFiles.count(_.partitionValues("l_shipdate").equals("1993-01-01")) === 4)
        assert(addFiles.count(_.partitionValues("l_shipdate").equals("1993-02-21")) === 3)
    }
  }

  test("test parquet CTAS simple with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_ctas1;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE lineitem_delta_parquet_ctas1
                 |USING delta
                 |LOCATION '$dataHome/lineitem_delta_parquet_ctas1'
                 | as select * from lineitem
                 |""".stripMargin)

    customCheckQuery(q1("lineitem_delta_parquet_ctas1")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size === 1)

        val parquetScan = scanExec.head
        assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet"))

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil)
        assert(addFiles.size === 1)
    }
  }

  test("test parquet CTAS complex with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_ctas2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_ctas2
                 |USING delta
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$dataHome/lineitem_delta_parquet_ctas2'
                 | as select * from lineitem
                 |""".stripMargin)
    checkQuery(q1("lineitem_delta_parquet_ctas2"))
  }

  test("test path based parquet write with the delta") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_filebased"
    clearDataPath(dataPath)
    val sourceDF = spark.sql("select * from lineitem")
    spark.sql(s"CREATE TABLE delta.`$dataPath` ($q1SchemaString) USING delta")

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(dataPath)

    customCheckQuery(q1(s"delta.`$dataPath`")) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
          case w: WholeStageTransformer => w
        }
        assert(plans.size === 4)

        val parquetScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
        assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet"))

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil)
        assert(addFiles.size === 1)
    }

    val result = spark.read
      .format("delta")
      .load(dataPath)
      .where("l_shipdate = date'1998-09-02'")
      .count()
    assert(result === 183)
  }

  test(
    "test path based parquet insert overwrite partitioned table with small table, static with delta") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_insertoverwrite2"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .partitionBy("l_shipdate")
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
         |""".stripMargin)
    sourceDF1.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy("l_shipdate")
      .save(dataPath)

    val result = spark.read
      .format("delta")
      .load(dataPath)
      .count()
    assert(result === 2418)
  }

  test(
    "test path based parquet insert overwrite partitioned table with small table, dynamic with delta") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      val dataPath = s"$dataHome/lineitem_delta_parquet_insertoverwrite3"
      clearDataPath(dataPath)

      val sourceDF = spark.sql(s"""
                                  |select * from lineitem
                                  |where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                                  |""".stripMargin)

      sourceDF.write
        .format("delta")
        .mode(SaveMode.Append)
        .partitionBy("l_shipdate")
        .save(dataPath)

      val sourceDF1 = spark.sql(
        s"""
           |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      sourceDF1.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .partitionBy("l_shipdate")
        .save(dataPath)

      val result = spark.read
        .format("delta")
        .load(dataPath)
        .count()
      assert(result === 21875)
    }
  }

  test("test path based parquet update with the delta") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_update"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select /*+ REPARTITION(6) */ * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)

    spark.sql(s"""
                 | update delta.`$dataPath` set l_returnflag = 'Z' where l_orderkey = 12647
                 |""".stripMargin)

    {
      val df = spark.read
        .format("delta")
        .load(dataPath)
        .where("l_returnflag = 'Z'")
      assert(df.count() === 1)
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size === 1)

      val parquetScan = scanExec.head
      assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet"))

      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil)
      assert(addFiles.size === 6)
    }

    val clickhouseTable = DeltaTable.forPath(spark, dataPath)
    clickhouseTable.updateExpr("l_orderkey = 10086", Map("l_returnflag" -> "'X'"))

    {
      val df = spark.read
        .format("delta")
        .load(dataPath)
        .where("l_returnflag = 'X'")
      assert(df.count() === 1)
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size === 1)

      val parquetScan = scanExec.head
      assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet"))

      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil)
      assert(addFiles.size === 6)
    }

    val df = spark.read
      .format("delta")
      .load(dataPath)
    assert(df.count() === 600572)
  }

  test("test path based parquet delete with the delta") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_delete"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select /*+ REPARTITION(6) */ * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)

    spark.sql(s"""
                 | delete from delta.`$dataPath` where l_orderkey = 12647
                 |""".stripMargin)
    val df = spark.read
      .format("delta")
      .load(dataPath)
    assert(df.count() === 600571)
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val parquetScan = scanExec.head
    val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
    val addFiles = fileIndex.matchingFiles(Nil, Nil)
    assert(addFiles.size === 6)

    val clickhouseTable = DeltaTable.forPath(spark, dataPath)
    clickhouseTable.delete("mod(l_orderkey, 3) = 2")
    val df1 = spark.read
      .format("delta")
      .load(dataPath)
    assert(df1.count() === 400089)
  }

  test("test path based parquet upsert with the delta") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_upsert"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select /*+ REPARTITION(6) */ * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df0 = spark.sql(s"""
                           | select count(*) from delta.`$dataPath`
                           |""".stripMargin)
    assert(
      df0.collect().apply(0).get(0) === 600572
    )
    upsertPathBasedSourceTableAndCheck(dataPath)
  }

  private def upsertPathBasedSourceTableAndCheck(dataPath: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into delta.`$dataPath`
          using (

            select l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
           'Z' as `l_returnflag`,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

            union

            select l_orderkey + 10000000,
            l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

          ) as updates
          on updates.l_orderkey = delta.`$dataPath`.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from delta.`$dataPath`
                             |""".stripMargin)
      assert(
        df1.collect().apply(0).get(0) === 600572 + 3506
      )
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from delta.`$dataPath` where l_returnflag = 'Z'
                     |""".stripMargin)
      assert(
        df2.collect().apply(0).get(0) === 3506
      )
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from delta.`$dataPath` where l_orderkey > 10000000
                     |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) === 3506
      )
    }
  }

  test("test path based parquet write with partition + delta") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_partition"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .partitionBy("l_shipdate", "l_returnflag")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
         |""".stripMargin)

    sourceDF1.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)

    customCheckQuery(q1(s"delta.`$dataPath`"), compare = false) {
      df =>
        val result = df.collect()
        assert(result.length === 2)
        assert(result(0).getString(0).equals("A"))
        assert(result(0).getString(1).equals("F"))
        assert(result(0).getDouble(2) === 306633.0)

        assert(result(1).getString(0).equals("R"))
        assert(result(1).getString(1).equals("F"))
        assert(result(1).getDouble(2) === 312371.0)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size === 1)

        val parquetScan = scanExec.head
        assert(parquetScan.nodeName.startsWith("FileSourceScanExecTransformer parquet"))
        assert(parquetScan.metrics("numFiles").value === 200)

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil)

        assert(addFiles.size === 200)
        assert(addFiles.count(_.partitionValues("l_shipdate").equals("1993-03-31")) === 2)
        assert(addFiles.count(_.partitionValues("l_shipdate").equals("1993-01-01")) === 4)
    }
  }

  test("test path based parquet CTAS with delta") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_ctas"
    clearDataPath(dataPath)

    spark.sql(s"""
                 |CREATE TABLE delta.`$dataPath`
                 |USING delta
                 |PARTITIONED BY (l_linestatus)
                 | as select * from lineitem
                 |""".stripMargin)

    checkQuery(q1(s"delta.`$dataPath`"))
  }

  test("test parquet optimize basic") {
    withSQLConf("spark.databricks.delta.optimize.maxFileSize" -> "20000000") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_parquet_optimize;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_optimize
                   |USING delta
                   |LOCATION '$dataHome/lineitem_delta_parquet_optimize'
                   | as select  /*+ REPARTITION(20) */ * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_delta_parquet_optimize")
      val ret = spark.sql("select count(*) from lineitem_delta_parquet_optimize").collect()
      assert(ret.apply(0).get(0) === 600572)

      assert(
        countFiles(new File(s"$dataHome/lineitem_delta_parquet_optimize")) === 24
      )
    }
  }

  def countFiles(directory: File): Int = {
    if (directory.exists && directory.isDirectory && !directory.getName.equals("_commits")) {
      val files = directory.listFiles
      val count = files
        .filter(!_.getName.endsWith(".crc"))
        .filter(!_.getName.endsWith("vacuum_info"))
        .count(_.isFile) + files.filter(_.isDirectory).map(countFiles).sum
      count
    } else {
      0
    }
  }

  test("test parquet optimize partitioned by one low card column") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_optimize_p2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_optimize_p2
                 |USING delta
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$dataHome/lineitem_delta_parquet_optimize_p2'
                 | as select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)

    spark.sparkContext.setJobGroup("test3", "test3")
    spark.sql("optimize lineitem_delta_parquet_optimize_p2")
    val job_ids = spark.sparkContext.statusTracker.getJobIdsForGroup("test3")
    if (spark32) {
      assert(job_ids.length === 7) // WILL trigger actual merge job
    } else {
      assert(job_ids.length === 8) // WILL trigger actual merge job
    }

    spark.sparkContext.clearJobGroup()

    val ret = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p2").collect()
    assert(ret.apply(0).get(0) === 600572)

    assert(countFiles(new File(s"$dataHome/lineitem_delta_parquet_optimize_p2")) === 23)
    spark.sql("VACUUM lineitem_delta_parquet_optimize_p2 RETAIN 0 HOURS")
    if (spark32) {
      assert(countFiles(new File(s"$dataHome/lineitem_delta_parquet_optimize_p2")) === 5)
    } else {
      assert(countFiles(new File(s"$dataHome/lineitem_delta_parquet_optimize_p2")) === 7)
    }

    val ret2 = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p2").collect()
    assert(ret2.apply(0).get(0) === 600572)
  }

  test("test parquet optimize parallel delete") {
    withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_parquet_optimize_p4;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_optimize_p4
                   |USING delta
                   |PARTITIONED BY (l_linenumber,l_returnflag)
                   |LOCATION '$dataHome/lineitem_delta_parquet_optimize_p4'
                   | as select /*+ REPARTITION(6) */ * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_delta_parquet_optimize_p4")
      val ret = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p4").collect()
      assert(ret.apply(0).get(0) === 600572)

      assert(countFiles(new File(s"$dataHome/lineitem_delta_parquet_optimize_p4")) === 149)
      spark.sql("VACUUM lineitem_delta_parquet_optimize_p4 RETAIN 0 HOURS")
      if (spark32) {
        assert(countFiles(new File(s"$dataHome/lineitem_delta_parquet_optimize_p4")) === 23)
      } else {
        assert(countFiles(new File(s"$dataHome/lineitem_delta_parquet_optimize_p4")) === 25)
      }

      val ret2 = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p4").collect()
      assert(ret2.apply(0).get(0) === 600572)
    }
  }

  test("test parquet optimize with the path based table") {
    val dataPath = s"$dataHome/lineitem_delta_parquet_optimize_path_based"
    clearDataPath(dataPath)
    withSQLConf(
      "spark.databricks.delta.optimize.maxFileSize" -> "1100000",
      "spark.databricks.delta.optimize.minFileSize" -> "838000") {

      val sourceDF = spark.sql(s"""
                                  |select /*+ REPARTITION(50) */ * from lineitem
                                  |""".stripMargin)

      sourceDF.write
        .format("delta")
        .mode(SaveMode.Append)
        .save(dataPath)

      assert(countFiles(new File(dataPath)) === 51)

      val clickhouseTable = DeltaTable.forPath(spark, dataPath)
      clickhouseTable.optimize().executeCompaction()

      // There are 75 parquet files + 2 json files after compaction
      assert(countFiles(new File(dataPath)) === 77)

      clickhouseTable.vacuum(0.0)
      if (spark32) {
        assert(countFiles(new File(dataPath)) === 27)
      } else {
        // There are 25 parquet files + 4 json files after vacuum
        assert(countFiles(new File(dataPath)) === 29)
      }

      val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
      assert(ret.apply(0).get(0) === 600572)
    }

    withSQLConf(
      "spark.databricks.delta.optimize.maxFileSize" -> "10000000",
      "spark.databricks.delta.optimize.minFileSize" -> "1100000") {

      val clickhouseTable = DeltaTable.forPath(spark, dataPath)
      clickhouseTable.optimize().executeCompaction()

      clickhouseTable.vacuum(0.0)
      if (spark32) {
        assert(countFiles(new File(dataPath)) === 6)
      } else {
        // There are 3 parquet files + 7 json files + 2 check point files after vacuum
        assert(countFiles(new File(dataPath)) === 12)
      }

      val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
      assert(ret.apply(0).get(0) === 600572)
    }

    // now merge all parts (testing merging from merged parts)
    val clickhouseTable = DeltaTable.forPath(spark, dataPath)
    clickhouseTable.optimize().executeCompaction()

    clickhouseTable.vacuum(0.0)
    if (spark32) {
      assert(countFiles(new File(dataPath)) === 5)
    } else {
      // There are 1 parquet file + 10 json files + 2 check point files after vacuum
      assert(countFiles(new File(dataPath)) === 13)
    }

    val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
    assert(ret.apply(0).get(0) === 600572)
  }

  // TODO: after rebase-25.12, support 'reorg' command for delta dv + partition
  ignore("Gluten-9697: Add 'reorg' command ut for the mergetree + delta dv") {
    val tableName = "mergetree_delta_dv_reorg"
    withTable(tableName) {
      withTempDir {
        dirName =>
          val s = createTableBuilder(tableName, "delta", s"$dirName/$tableName")
            .withProps(Map("delta.enableDeletionVectors" -> "'true'"))
            .withTableKey("lineitem")
            .build()
          spark.sql(s)

          spark.sql(s"""
                       |insert into table $tableName
                       |select /*+ REPARTITION(6) */ * from lineitem
                       |""".stripMargin)

          spark.sql(s"""
                       |delete from $tableName
                       |where mod(l_orderkey, 3) = 2
                       |""".stripMargin)

          var df = spark.sql(s"""
                                | select sum(l_linenumber) from $tableName
                                |""".stripMargin)
          var result = df.collect()
          assert(
            result(0).get(0) === 1200671
          )
          checkFallbackOperators(df, 0)

          spark.sql(s"""
                       | REORG TABLE $tableName APPLY (PURGE)
                       |""".stripMargin)
          df = spark.sql(s"""
                            | select sum(l_linenumber) from $tableName
                            |""".stripMargin)
          result = df.collect()
          assert(
            result(0).get(0) === 1200671
          )
          val scanExec = collect(df.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer => f
          }
          val parquetScan = scanExec.head
          val fileIndex = parquetScan.relation.location.asInstanceOf[PreparedDeltaFileIndex]
          val addFiles = fileIndex.preparedScan.files
          assert(addFiles.size === 1)
          assert(addFiles(0).deletionVector === null)
      }
    }
  }

  // TODO: after rebase-25.12, fix reorg purge command in delta 3.3.1
  ignore("Gluten-9697: Add 'reorg' command ut for delta dv + partition") {
    val tableName = "mergetree_delta_dv_reorg_partition"
    spark.sql(s"""
                 |DROP TABLE IF EXISTS $tableName;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS $tableName
                 |(${table2columns.get("lineitem").get(true)})
                 |USING delta
                 |PARTITIONED BY (l_returnflag)
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$dataHome/$tableName'
                 |""".stripMargin)
    spark.sql(s"""
                 |insert into table $tableName
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)
    spark.sql(s"""
                 |delete from $tableName
                 | where mod(l_orderkey, 3) = 1
                 |""".stripMargin)
    var df = spark.sql(s"""
                          |select sum(l_linenumber) from $tableName
                          |""".stripMargin)
    var result = df.collect()
    assert(
      result(0).get(0) === 1201486
    )
    checkFallbackOperators(df, 0)
    spark.sql(s"""
                 |REORG TABLE $tableName APPLY (PURGE)
                 |""".stripMargin)
    df = spark.sql(s"""
                      |select sum(l_linenumber) from $tableName
                      |""".stripMargin)
    result = df.collect()
    assert(
      result(0).get(0) === 1201486
    )
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val parquetScan = scanExec.head
    val fileIndex = parquetScan.relation.location.asInstanceOf[PreparedDeltaFileIndex]
    val addFiles = fileIndex.preparedScan.files
    assert(addFiles.size === 3)
    assert(addFiles.forall(_.deletionVector === null))
  }
}
// scalastyle:off line.size.limit
