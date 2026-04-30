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
import org.apache.gluten.execution._
import org.apache.gluten.utils.Arm

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.functions._

import io.delta.tables.ClickhouseTable
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.scalatest.DoNotDiscover

import java.io.File

import scala.io.Source

@DoNotDiscover
class GlutenClickHouseMergeTreePathBasedWriteSuite extends CreateMergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.ui.enabled", "true")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
      .set(RuntimeSettings.MIN_INSERT_BLOCK_SIZE_ROWS.key, "100000")
      .set(RuntimeSettings.MERGE_AFTER_INSERT.key, "false")
      .setCHSettings("input_format_parquet_max_block_size", 8192)

  }

  test("test mergetree path based write") {
    val dataPath = s"$dataHome/lineitem_filebased"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE clickhouse.`$dataPath` (
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
                 |               primaryKey='l_returnflag',
                 |               lowCardKey='l_returnflag,l_linestatus')
                 |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Overwrite)
      .save(dataPath)

    customCheckQuery(q1(s"clickhouse.`$dataPath`")) {
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
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKey.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKey.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
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

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .where("l_shipdate = date'1998-09-02'")
      .count()
    assertResult(183)(result)
  }

  test("test mergetree path based write with dataframe api") {
    val dataPath = s"$dataHome/lineitem_filebased_df"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Overwrite)
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath)

    customCheckQuery(q1(s"clickhouse.`$dataPath`")) {
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
        assertResult("l_shipdate,l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKey)
        assertResult("l_shipdate")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKey)
        assertResult("l_returnflag,l_linestatus")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .lowCardKey)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
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

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .where("l_shipdate = date'1998-09-02'")
      .count()
    assertResult(183L)(result)
  }

  test("test mergetree path based insert overwrite partitioned table with small table, static") {
    val dataPath = s"$dataHome/lineitem_mergetree_insertoverwrite2"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .partitionBy("l_shipdate")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
         |""".stripMargin)
    sourceDF1.write
      .format("clickhouse")
      .mode(SaveMode.Overwrite)
      .partitionBy("l_shipdate")
      .save(dataPath)

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .count()
    assertResult(2418)(result)
  }

  test("test mergetree path based insert overwrite partitioned table with small table, dynamic") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      val dataPath = s"$dataHome/lineitem_mergetree_insertoverwrite3"
      clearDataPath(dataPath)

      val sourceDF = spark.sql(s"""
                                  |select * from lineitem
                                  |""".stripMargin)

      sourceDF.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .partitionBy("l_shipdate")
        .option("clickhouse.orderByKey", "l_orderkey")
        .option("clickhouse.primaryKey", "l_orderkey")
        .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
        .save(dataPath)

      val sourceDF1 = spark.sql(
        s"""
           |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      sourceDF1.write
        .format("clickhouse")
        .mode(SaveMode.Overwrite)
        .partitionBy("l_shipdate")
        .save(dataPath)

      val result = spark.read
        .format("clickhouse")
        .load(dataPath)
        .count()
      assertResult(600572)(result)
    }
  }

  test("test mergetree path based table update") {
    val dataPath = s"$dataHome/lineitem_mergetree_update"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    /**
     * TODO: new test for (spark.databricks.delta.stats.skipping -> true)
     *
     * Since one pipeline write will collect stats, so that pruning will be more accurate in point
     * query. Let's add a new test when we implement lightweight update and delete.
     */
    withSQLConf(("spark.databricks.delta.stats.skipping", "false")) {
      spark.sql(s"""
                   | update clickhouse.`$dataPath` set l_returnflag = 'Z' where l_orderkey = 12647
                   |""".stripMargin)

      {
        val df = spark.read
          .format("clickhouse")
          .load(dataPath)
          .where("l_returnflag = 'Z'")
        assertResult(1)(df.count())
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
        assertResult(600572)(addFiles.map(_.rows).sum)
        // 5 parts belong to the first batch
        // 1 parts belong to the second batch (1 actual updated part).
        assertResult(6)(addFiles.size)
        val filePaths =
          addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
        assertResult(2)(filePaths.size)
        assertResult(Array(1, 5))(filePaths.values.map(paths => paths.size).toArray.sorted)
      }

      val clickhouseTable = ClickhouseTable.forPath(spark, dataPath)
      clickhouseTable.updateExpr("l_orderkey = 10086", Map("l_returnflag" -> "'X'"))

      {
        val df = spark.read
          .format("clickhouse")
          .load(dataPath)
          .where("l_returnflag = 'X'")
        assertResult(1)(df.count())
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(600572)(addFiles.map(_.rows).sum)

        // 4 parts belong to the first batch
        // 2 parts belong to the second batch (1 actual updated part, 1 passively updated).
        assertResult(6)(addFiles.size)
        val filePaths =
          addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
        assertResult(2)(filePaths.size)
      }
    }
    val df = spark.read
      .format("clickhouse")
      .load(dataPath)
    assertResult(600572)(df.count())
  }

  test("test mergetree path based table delete") {
    val dataPath = s"$dataHome/lineitem_mergetree_delete"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    /**
     * TODO: new test for (spark.databricks.delta.stats.skipping -> true)
     *
     * Since one pipeline write will collect stats, so that pruning will be more accurate in point
     * query. Let's add a new test when we implement lightweight update and delete.
     */
    withSQLConf(("spark.databricks.delta.stats.skipping", "false")) {
      spark.sql(s"""
                   | delete from clickhouse.`$dataPath` where l_orderkey = 12647
                   |""".stripMargin)
      val df = spark.read
        .format("clickhouse")
        .load(dataPath)
      assertResult(600571)(df.count())
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      val mergetreeScan = scanExec.head
      val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
      // 5 parts belong to the first batch
      // 1 parts belong to the second batch (1 actual updated part).
      assertResult(6)(addFiles.size)
      val filePaths = addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
      assertResult(2)(filePaths.size)
      assertResult(Array(1, 5))(filePaths.values.map(paths => paths.size).toArray.sorted)

      val clickhouseTable = ClickhouseTable.forPath(spark, dataPath)
      clickhouseTable.delete("mod(l_orderkey, 3) = 2")
      val df1 = spark.read
        .format("clickhouse")
        .load(dataPath)
      assertResult(400089)(df1.count())
    }
  }

  test("test mergetree path based table upsert") {
    val dataPath = s"$dataHome/lineitem_mergetree_upsert"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df0 = spark.sql(s"""
                           | select count(*) from clickhouse.`$dataPath`
                           |""".stripMargin)
    assertResult(600572)(
      df0.collect().apply(0).get(0)
    )
    upsertSourceTableAndCheck(dataPath)
  }

  private def upsertSourceTableAndCheck(dataPath: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into clickhouse.`$dataPath`
          using (

            select l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
              l_extendedprice, l_discount, l_tax, 'Z' as `l_returnflag`,
              l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
              l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in
              (select l_orderkey from lineitem group by l_orderkey having count(*) =1 )
               and l_orderkey < 100000

            union

            select l_orderkey + 10000000,
              l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
              l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate, l_commitdate,
              l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in
              (select l_orderkey from lineitem group by l_orderkey having count(*) =1 )
              and l_orderkey < 100000

          ) as updates
          on updates.l_orderkey = clickhouse.`$dataPath`.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from clickhouse.`$dataPath`
                             |""".stripMargin)
      assertResult(600572 + 3506)(
        df1.collect().apply(0).get(0)
      )
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from clickhouse.`$dataPath` where l_returnflag = 'Z'
                     |""".stripMargin)
      assertResult(3506)(
        df2.collect().apply(0).get(0)
      )
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from clickhouse.`$dataPath` where l_orderkey > 10000000
                     |""".stripMargin)
      assertResult(3506)(
        df3.collect().apply(0).get(0)
      )
    }
  }

  test("test mergetree path based write with orderby keys / primary keys") {
    val dataPath = s"$dataHome/lineitem_mergetree_orderbykey"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .mode(SaveMode.Append)
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

    val df = spark.read
      .format("clickhouse")
      .load(dataPath)
      .where("""
               |l_shipdate >= date'1994-01-01'
               |    AND l_shipdate < date'1994-01-01' + interval 1 year
               |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
               |    AND l_quantity < 24
               |""".stripMargin)
      .agg(sum("l_linenumber").alias("res"))
    val result = df.collect()
    assertResult(34842)(result(0).getLong(0))
  }

  test("test mergetree path based write with partition") {
    val dataPath = s"$dataHome/lineitem_mergetree_partition"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .partitionBy("l_shipdate", "l_returnflag")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
         |""".stripMargin)

    sourceDF1.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    customCheckQuery(q1(s"clickhouse.`$dataPath`"), compare = false) {
      df =>
        val result = df.collect()
        assertResult(4)(result.length)
        assertResult("A")(result(0).getString(0))
        assertResult("F")(result(0).getString(1))
        assertResult(3803858.0)(result(0).getDouble(2))

        assertResult("N")(result(2).getString(0))
        assertResult("O")(result(2).getString(1))
        assertResult(7454519.0)(result(2).getDouble(2))

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))
        assertResult(3744)(mergetreeScan.metrics("numFiles").value)

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
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(3835)(addFiles.size)
        assertResult(602945)(addFiles.map(_.rows).sum)
        assertResult(2)(addFiles.count(_.partitionValues("l_shipdate").equals("1992-06-01")))
        assertResult(4)(addFiles.count(_.partitionValues("l_shipdate").equals("1993-01-01")))
        assertResult(2)(addFiles.count(_.partitionValues("l_shipdate").equals("1995-01-21")))
    }
  }

  testSparkVersionLE33("test mergetree path based write with bucket table") {
    val dataPath = s"$dataHome/lineitem_mergetree_bucket"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .partitionBy("l_returnflag")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .option("clickhouse.numBuckets", "4")
      .option("clickhouse.bucketColumnNames", "l_partkey")
      .mode(SaveMode.Append)
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
        val buckets = ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption
        assert(buckets.isDefined)
        assertResult(4)(buckets.get.numBuckets)
        assertResult("l_orderkey")(
          buckets.get.sortColumnNames
            .mkString(","))
        assertResult("l_partkey")(
          buckets.get.bucketColumnNames
            .mkString(","))
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
                          | select * from clickhouse.`$dataPath` where l_orderkey = 12647
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
    upsertSourceTableAndCheck(dataPath)
  }

  test("test mergetree path based CTAS simple") {
    val dataPath = s"$dataHome/lineitem_mergetree_ctas1"
    clearDataPath(dataPath)

    spark.sql(s"""
                 |CREATE TABLE clickhouse.`$dataPath`
                 |USING clickhouse
                 | as select * from lineitem
                 |""".stripMargin)

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

  test("test mergetree path based CTAS partition") {
    val dataPath = s"$dataHome/lineitem_mergetree_ctas2"
    clearDataPath(dataPath)

    spark.sql(s"""
                 |CREATE TABLE clickhouse.`$dataPath`
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 | as select * from lineitem
                 |""".stripMargin)

    checkQuery(q1(s"clickhouse.`$dataPath`"))

  }

  test("test mergetree path based table with low cardinality column") {
    val dataPath = s"$dataHome/lineitem_mergetree_lowcard"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.lowCardKey", "l_returnflag,L_LINESTATUS,l_quantity")
      .mode(SaveMode.Append)
      .save(dataPath)

    checkQuery(q1(s"clickhouse.`$dataPath`"))
    val directory = new File(dataPath)
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
         |    clickhouse.`$dataPath`
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
         |    clickhouse.`$dataPath` limit 1
         |
         |""".stripMargin
    spark.sql(sqlStr3).collect()
  }

  test("test mergetree path based table with primary keys filter") {
    val dataPath = s"$dataHome/lineitem_mergetree_orderbykey2"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .mode(SaveMode.Append)
      .save(dataPath)

    customCheckQuery(q6(s"clickhouse.`$dataPath`")) {
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

  test("test simple minmax index") {
    val dataPath = s"$dataHome/lineitem_mergetree_minmax"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.minmaxIndexKey", "l_receiptdate")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df = spark
      .sql(s"""
              |select count(*) from clickhouse.`$dataPath` where l_receiptdate = '1998-12-27'
              |""".stripMargin)

    val ret = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assertResult(1)(scanExec.size)
    val mergetreeScan = scanExec.head

    assertResult(1)(ret.apply(0).get(0))
    val marks = mergetreeScan.metrics("selectedMarks").value
    assertResult(1)(marks)

    val directory = new File(dataPath)
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(
      partDir.listFiles().exists(p => p.getName.contains("skp_idx__minmax_l_receiptdate.idx2")))
  }

  test("test simple bloom filter index") {
    val dataPath = s"$dataHome/lineitem_mergetree_bf"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.bloomfilterIndexKey", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df = spark
      .sql(s"""
              |select count(*) from clickhouse.`$dataPath` where l_orderkey = '600000'
              |""".stripMargin)
    val ret = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assertResult(1)(scanExec.size)
    val mergetreeScan = scanExec.head
    assertResult(2)(ret.apply(0).get(0))
    val marks = mergetreeScan.metrics("selectedMarks").value
    assertResult(1)(marks)

    val directory = new File(dataPath)
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(
      partDir.listFiles().exists(p => p.getName.contains("skp_idx__bloomfilter_l_orderkey.idx")))
  }

  test("test simple set index") {
    val dataPath = s"$dataHome/lineitem_mergetree_set"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.setIndexKey", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df = spark
      .sql(s"""
              |select count(*) from clickhouse.`$dataPath` where l_orderkey = '600000'
              |""".stripMargin)

    val ret = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assertResult(1)(scanExec.size)
    val mergetreeScan = scanExec.head
    assertResult(2)(ret.apply(0).get(0))
    val marks = mergetreeScan.metrics("selectedMarks").value
    assertResult(1)(marks)

    val directory = new File(dataPath)
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(partDir.listFiles().exists(p => p.getName.contains("skp_idx__set_l_orderkey.idx")))
  }

  test("GLUTEN-5219: Fix the table metadata sync issue for the CH backend") {
    def checkQueryResult(tableName: String): Unit = {
      customCheckQuery(q1(s"clickhouse.`$tableName`")) {
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

    val dataPath = s"$dataHome/lineitem_mergetree_ctas_5219"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    checkQueryResult(dataPath)

    val dataPath1 = s"$dataHome/lineitem_mergetree_ctas_5219"
    clearDataPath(dataPath1)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath1)

    checkQueryResult(dataPath1)

    var dataPathFile = new File(dataPath1)
    assert(dataPathFile.isDirectory && dataPathFile.isDirectory)

    val fileFilter = new WildcardFileFilter("*_0_*")
    var dataFileList = dataPathFile.list(fileFilter)
    assertResult(6)(dataFileList.length)

    // re-create the same table
    val dataPath2 = s"$dataHome/lineitem_mergetree_5219_s"
    for (i <- 0 until 10) {
      clearDataPath(dataPath2)

      sourceDF.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .save(dataPath2)

      checkQueryResult(dataPath2)
    }

    dataPathFile = new File(dataPath2)
    assert(dataPathFile.isDirectory && dataPathFile.isDirectory)

    dataFileList = dataPathFile.list(fileFilter)
    assertResult(6)(dataFileList.length)
  }

  test("GLUTEN-6378: Support delta count optimizer for the MergeTree format") {
    val dataPath = s"$dataHome/lineitem_mergetree_count_opti"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .partitionBy("l_shipdate", "l_returnflag")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df = spark.read
      .format("clickhouse")
      .load(dataPath)
      .groupBy()
      .count()
    val result = df.collect()
    assertResult(600572)(result(0).getLong(0))
    // Spark 3.2 + Delta 2.0 does not support this feature
    if (!spark32) {
      assert(df.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])
    }
  }

  test(
    "GLUTEN-7344: Fix the error default database name and table " +
      "name for the mergetree file format when using path based") {
    val dataPath = s"$dataHome/lineitem_filebased_7344"
    clearDataPath(dataPath)
    val dataPath1 = s"$dataHome/lineitem_filebased_7344_1"
    clearDataPath(dataPath1)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath1)

    {
      val df = spark.read
        .format("clickhouse")
        .load(dataPath)
      val result = df.collect()
      assertResult(600572)(result.length)

      val plans = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      val partitions = plans.head.getPartitions
      assert(partitions.nonEmpty)
      assert(partitions.head.isInstanceOf[GlutenMergeTreePartition])
      val mergeTreePartition = partitions.head.asInstanceOf[GlutenMergeTreePartition]
      assertResult(mergeTreePartition.database)(StorageMeta.DEFAULT_PATH_BASED_DATABASE)
      assertResult(mergeTreePartition.table)(dataPath)
    }
    {
      val df1 = spark.read
        .format("clickhouse")
        .load(dataPath1)
      val result1 = df1.collect()
      assertResult(600572)(result1.length)

      val plans1 = collect(df1.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      val partitions1 = plans1.head.getPartitions
      assert(partitions1.nonEmpty)
      assert(partitions1.head.isInstanceOf[GlutenMergeTreePartition])
      val mergeTreePartition1 = partitions1.head.asInstanceOf[GlutenMergeTreePartition]
      assertResult(mergeTreePartition1.database)(StorageMeta.DEFAULT_PATH_BASED_DATABASE)
      assertResult(mergeTreePartition1.table)(dataPath1)
    }
  }
}
