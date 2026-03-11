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
package org.apache.spark.gluten.delta

import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArrayFormat
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.DeletionVectorWriteTransformer
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.functions.col

import org.apache.hadoop.fs.Path

class GlutenDeltaParquetDeletionVectorSuite extends ParquetSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

  ignore("test parquet table delete with the delta DV") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_delete_dv;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_delete_dv
                 |($q1SchemaString)
                 |USING delta
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$dataHome/lineitem_delta_parquet_delete_dv'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_delete_dv
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | delete from lineitem_delta_parquet_delete_dv
                 | where l_orderkey = 3
                 |""".stripMargin)

    val df = spark.sql(s"""
                          | select sum(l_linenumber) from lineitem_delta_parquet_delete_dv
                          |""".stripMargin)
    val result = df.collect()
    assert(
      result.apply(0).get(0) === 1802425
    )
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val parquetScan = scanExec.head
    val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
    val addFiles = fileIndex.matchingFiles(Nil, Nil)
    assert(addFiles.size === 6)

    spark.sql(s"""
                 | delete from lineitem_delta_parquet_delete_dv where mod(l_orderkey, 3) = 2
                 |""".stripMargin)

    val df3 = spark.sql(s"""
                           | select sum(l_linenumber) from lineitem_delta_parquet_delete_dv
                           |""".stripMargin)
    assert(
      df3.collect().apply(0).get(0) === 1200650
    )
  }

  ignore("test parquet table delete + update with the delta DV") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_update_dv;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_update_dv
                 |($q1SchemaString)
                 |USING delta
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$dataHome/lineitem_delta_parquet_update_dv'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_update_dv
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(
      s"""
         | update lineitem_delta_parquet_update_dv set l_returnflag = 'AAA' where l_orderkey < 200
         |""".stripMargin)

    val df =
      spark.sql(s"""
                   | select sum(l_linenumber)
                   | from lineitem_delta_parquet_update_dv
                   | where l_returnflag = 'AAA'
                   |""".stripMargin)
    val result = df.collect()
    assert(
      result.apply(0).get(0) === 662
    )
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.nonEmpty)
  }

  test("test ObjectHashAggregateExec(bitmapaggregator) no fallback") {
    val table_name = "dv_fallback"
    withTable(table_name) {
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        spark.sql(s"""
                     |CREATE TABLE IF NOT EXISTS $table_name
                     |($q1SchemaString)
                     |USING delta
                     |TBLPROPERTIES (delta.enableDeletionVectors='true')
                     |LOCATION '$dataHome/$table_name'
                     |""".stripMargin)

        spark.sql(s"""
                     | insert into table $table_name select * from lineitem
                     |""".stripMargin)

        def createBitmapSetAggregator(indexColumn: Column): Column = {
          val func = new BitmapAggregator(indexColumn.expr, RoaringBitmapArrayFormat.Portable)
          new Column(func.toAggregateExpression(isDistinct = false))
        }

        val aggColumns = Seq(createBitmapSetAggregator(col("l_orderkey")))

        val aggregated = sql(s"select l_orderkey,l_shipdate from $table_name")
          .groupBy(col("l_shipdate"))
          .agg(aggColumns.head, aggColumns.tail: _*)
          .select("*")
          .toDF()
        aggregated.collect()
        val bitMapAggregator = aggregated.queryExecution.executedPlan.collect {
          case agg: ObjectHashAggregateExec => agg
        }
        assert(bitMapAggregator.isEmpty)
      }
    }
  }

  ignore("test delta DV write") {
    val table_name = "dv_write_test"
    withTable(table_name) {
      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS $table_name
                   |($q1SchemaString)
                   |USING delta
                   |TBLPROPERTIES (delta.enableDeletionVectors='true')
                   |LOCATION '$dataHome/$table_name'
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table $table_name select * from lineitem
                   |""".stripMargin)

      spark.sql(s"""
                   | delete from $table_name
                   | where mod(l_orderkey, 3) = 1 and l_orderkey < 100
                   |""".stripMargin)

      val df = spark.sql(s"""
                            | select sum(l_linenumber) from $table_name
                            |""".stripMargin)
      val result = df.collect()
      assertResult(1802335)(result.apply(0).get(0))

      spark.sql(s"""
                   | update $table_name
                   | set l_orderkey = 1 where l_orderkey > 0
                   |""".stripMargin)

      spark.sql(s""" select count(*) from $table_name """.stripMargin).show()

      val df2 = spark.sql(s"""
                             | select sum(l_orderkey) from $table_name
                             |""".stripMargin)
      val result2 = df2.collect()
      assertResult(600536)(result2.apply(0).get(0))
    }
  }

  def testBasic(prefix: Boolean = false)(deleteCallback: => Unit)(updateCallback: => Unit): Unit = {
    val prefix_str = if (prefix) {
      ", delta.randomizeFilePrefixes=true"
    } else {
      ""
    }

    val tableName = "dv_write_test"
    withTable(tableName) {
      withTempDir {
        dirName =>
          spark.sql(s"""
                       |CREATE TABLE IF NOT EXISTS $tableName
                       |($q1SchemaString)
                       |USING delta
                       |TBLPROPERTIES (delta.enableDeletionVectors='true' $prefix_str)
                       |LOCATION '$dirName/$tableName'
                       |""".stripMargin)

          spark.sql(s"""insert into table $tableName select * from lineitem """.stripMargin)

          spark.sql(s"""
                       | delete from $tableName
                       | where mod(l_orderkey, 3) = 1 and l_orderkey < 100
                       |""".stripMargin)

          deleteCallback

          val df = spark.sql(s"""select sum(l_linenumber) from $tableName """.stripMargin)
          val result = df.collect()
          assertResult(1802335)(result.apply(0).get(0))

          spark.sql(s"""update $tableName set l_orderkey = 1 where l_orderkey > 0 """.stripMargin)

          updateCallback
          val df2 = spark.sql(s"""select sum(l_orderkey) from $tableName""".stripMargin)
          val result2 = df2.collect()
          assertResult(600536)(result2.apply(0).get(0))
      }
    }
  }

  test("test delta DV write use native writer") {
    var counter = DeletionVectorWriteTransformer.COUNTER.get()
    testBasic() {
      counter = DeletionVectorWriteTransformer.COUNTER.get()
      assertResult(true)(counter > 0)
    } {
      assertResult(true)(DeletionVectorWriteTransformer.COUNTER.get() > counter)
    }
  }

  test("test delta DV write use native writer with prefix") {
    var counter = DeletionVectorWriteTransformer.COUNTER.get()
    testBasic(prefix = true) {
      counter = DeletionVectorWriteTransformer.COUNTER.get()
      assertResult(true)(counter > 0)
    } {
      assertResult(true)(DeletionVectorWriteTransformer.COUNTER.get() > counter)
    }
  }

  for (targetDVFileSize <- Seq(2, 200, 2000000)) {
    ignore(
      s"DELETE with DVs - packing multiple DVs into one file: target max DV file " +
        s"size=$targetDVFileSize") {
      withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
        DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
        DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key -> targetDVFileSize.toString,
        "spark.sql.shuffle.partitions" -> "1"
      ) {
        withTempDir {
          dirName =>
            // Create table with 100 files of 2 rows each.
            val numFiles = 100
            val path = dirName.getAbsolutePath
            spark.range(0, 200, step = 1, numPartitions = numFiles).write.format("delta").save(path)
            val tableName = s"delta.`$path`"
            val numFilesWithDVs = 10
            val numDeletedRows = numFilesWithDVs * 1
            spark.sql(s"DELETE FROM $tableName WHERE id % 2 = 0 AND id < 20")

            // Verify the expected number of AddFiles with DVs
            val allFiles = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot.allFiles.collect()
            assert(allFiles.length === numFiles)
            val addFilesWithDV = allFiles.filter(_.deletionVector != null)
            assert(addFilesWithDV.length === numFilesWithDVs)
            assert(addFilesWithDV.map(_.deletionVector.cardinality).sum == numDeletedRows)

            var expectedDVFileCount = 0
            targetDVFileSize match {
              // Each AddFile will have its own DV file
              case 2 => expectedDVFileCount = numFilesWithDVs
              // Each DV size is about 34bytes according the latest format.
              case 200 => expectedDVFileCount = numFilesWithDVs / (200 / 34).floor.toInt
              // Expect all DVs in one file
              case 2000000 => expectedDVFileCount = 1
              case default =>
                throw new IllegalStateException(s"Unknown target DV file size: $default")
            }
            // Expect all DVs are written in one file
            assert(
              addFilesWithDV.map(_.deletionVector.absolutePath(new Path(path))).toSet.size ===
                expectedDVFileCount
            )
        }
      }
    }
  }

  ignore("test parquet partition table delete with the delta DV") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_partition_parquet_delete_dv;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_partition_parquet_delete_dv
                   |($q1SchemaString)
                   |USING delta
                   |PARTITIONED BY (l_returnflag)
                   |TBLPROPERTIES (delta.enableDeletionVectors='true')
                   |LOCATION '$dataHome/lineitem_delta_partition_parquet_delete_dv'
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table lineitem_delta_partition_parquet_delete_dv
                   | select /*+ REPARTITION(6) */ * from lineitem
                   |""".stripMargin)

      spark.sql(s"""
                   | delete from lineitem_delta_partition_parquet_delete_dv
                   | where mod(l_orderkey, 3) = 1
                   |""".stripMargin)

      val df =
        spark.sql(s"""
                     | select sum(l_linenumber) from lineitem_delta_partition_parquet_delete_dv
                     |""".stripMargin)
      val result = df.collect()
      assert(
        result.apply(0).get(0) === 1201486
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.nonEmpty)
    }
  }

  ignore("test parquet table upsert with the delta DV") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_upsert_dv;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_upsert_dv
                 |($q1SchemaString)
                 |USING delta
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$dataHome/lineitem_delta_parquet_upsert_dv'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_upsert_dv
                 | select * from lineitem
                 |""".stripMargin)

    val df0 = spark.sql(s"""
                           | select sum(l_linenumber) from lineitem_delta_parquet_upsert_dv
                           |""".stripMargin)
    assert(
      df0.collect().apply(0).get(0) === 1802446
    )
    upsertSourceTableAndCheck("lineitem_delta_parquet_upsert_dv")
  }

  private def upsertSourceTableAndCheck(tableName: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into $tableName
          using (
            select
              l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
              l_discount, l_tax, 'Z' as `l_returnflag`, l_linestatus, l_shipdate, l_commitdate,
              l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem
            where
              l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1)
              and l_orderkey < 100000
            union
            select
              l_orderkey + 10000000, l_partkey, l_suppkey, l_linenumber, l_quantity,
              l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate,
              l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem
            where
              l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1)
              and l_orderkey < 100000
          ) as updates
          on updates.l_orderkey = $tableName.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    val df1 = spark.sql(s"""
                           | select sum(l_linenumber) from $tableName
                           |""".stripMargin)
    assert(
      df1.collect().apply(0).get(0) === 1805952
    )

    val df2 =
      spark.sql(s"""
                   | select count(*) from $tableName where l_returnflag = 'Z'
                   |""".stripMargin)
    assert(
      df2.collect().apply(0).get(0) === 3506
    )

    val df3 =
      spark.sql(s"""
                   | select count(*) from $tableName where l_orderkey > 10000000
                   |""".stripMargin)
    assert(
      df3.collect().apply(0).get(0) === 3506
    )
  }
}
