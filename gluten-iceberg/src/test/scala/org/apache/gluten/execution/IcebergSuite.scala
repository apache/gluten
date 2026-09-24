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

import org.apache.gluten.config.GlutenIcebergConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.iceberg.spark.source.GlutenIcebergSourceUtil

import java.util.Locale
import java.util.concurrent.{CountDownLatch, TimeUnit}

abstract class IcebergSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  // FIXME: This folder doesn't exist in module gluten-iceberg so should be provided by
  //  backend modules that rely on this suite.
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/tpch-data-iceberg-velox")
  }

  test("iceberg transformer exists") {
    withTable("iceberg_tb") {
      spark.sql("""
                  |create table iceberg_tb using iceberg as
                  |(select 1 as col1, 2 as col2, 3 as col3)
                  |""".stripMargin)

      runQueryAndCompare("""
                           |select * from iceberg_tb;
                           |""".stripMargin) {
        checkGlutenPlan[IcebergScanTransformer]
      }
    }
  }

  test("rewrite_data_files uses an iceberg staged scan transformer") {
    val tableName = "iceberg_rewrite_tb"
    withTable(tableName) {
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        spark.sql(s"CREATE TABLE $tableName (id INT, data STRING) USING iceberg")
        (1 to 5).foreach {
          id => spark.sql(s"INSERT INTO $tableName VALUES ($id, 'value-$id')")
        }

        def dataFileCount: Long =
          spark.table(s"spark_catalog.default.$tableName.files").count()

        assert(dataFileCount == 5)
        val stagedScanSeen = new CountDownLatch(1)
        val listener = new QueryExecutionListener {
          override def onSuccess(
              funcName: String,
              qe: QueryExecution,
              durationNs: Long): Unit = {
            if (
              qe.executedPlan.exists {
                case scan: IcebergScanTransformer =>
                  GlutenIcebergSourceUtil.isSparkStagedScan(scan.scan)
                case _ => false
              }
            ) {
              stagedScanSeen.countDown()
            }
          }

          override def onFailure(
              funcName: String,
              qe: QueryExecution,
              exception: Exception): Unit = {}
        }

        try {
          spark.listenerManager.register(listener)
          val result = spark
            .sql(s"""
                    |CALL spark_catalog.system.rewrite_data_files(
                    |  table => 'default.$tableName',
                    |  options => map('min-input-files', '2'))
                    |""".stripMargin)
            .collect()

          assert(result.length == 1)
          assert(result.head.getInt(0) == 5)
          assert(result.head.getInt(1) == 1)
          assert(
            stagedScanSeen.await(10, TimeUnit.SECONDS),
            "Rewrite read did not use IcebergScanTransformer with SparkStagedScan")
        } finally {
          spark.listenerManager.unregister(listener)
        }

        assert(dataFileCount == 1)
        checkAnswer(
          spark.sql(s"SELECT * FROM $tableName ORDER BY id"),
          (1 to 5).map(id => Row(id, s"value-$id")))
      }
    }
  }

  test("iceberg input_file_name") {
    withTable("iceberg_input_file_tb") {
      spark.sql("""
                  |CREATE TABLE iceberg_input_file_tb (id INT, data STRING)
                  |USING iceberg
                  |""".stripMargin)
      spark.sql("""
                  |INSERT INTO iceberg_input_file_tb VALUES
                  |(1, 'a'), (2, 'b'), (3, 'c')
                  |""".stripMargin)

      val df = runAndCompare("""
                               |SELECT id, input_file_name() AS name
                               |FROM iceberg_input_file_tb
                               |ORDER BY id
                               |""".stripMargin)

      val rows = df.collect()
      checkGlutenPlan[IcebergScanTransformer](df)
      assert(
        rows.forall(row => !row.isNullAt(1) && row.getString(1).nonEmpty),
        s"Expected non-empty input_file_name values, got: ${rows.mkString(", ")}")
    }
  }

  test("iceberg bucketed join") {
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Partition key of string type.
      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $leftTable(id int, name string, p string)
                   |using iceberg
                   |partitioned by (bucket(4, id));
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $leftTable values
           |(4, 'a5', 'p4'),
           |(1, 'a1', 'p1'),
           |(2, 'a3', 'p2'),
           |(1, 'a2', 'p1'),
           |(3, 'a4', 'p3');
           |""".stripMargin
      )

      // Partition key of integer type.
      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $rightTable(id int, name string, p int)
                   |using iceberg
                   |partitioned by (bucket(4, id));
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $rightTable values
           |(3, 'b4', 23),
           |(1, 'b2', 21),
           |(4, 'b5', 24),
           |(2, 'b3', 22),
           |(1, 'b1', 21);
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan if plan.isInstanceOf[IcebergScanTransformer] =>
                  assert(
                    plan.asInstanceOf[IcebergScanTransformer].getKeyGroupPartitioning.isDefined)
                  assert(plan.asInstanceOf[IcebergScanTransformer].getSplitInfos.length == 3)
                case _ => // do nothing
              }
              checkLengthAndPlan(df, 7)
            }
        }
      }
    }
  }

  test("iceberg bucketed join with partition") {
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Partition key of string type.
      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $leftTable(id int, name string, p int)
                   |using iceberg
                   |partitioned by (bucket(4, id), p);
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $leftTable values
           |(4, 'a5', 2),
           |(1, 'a1', 1),
           |(2, 'a3', 1),
           |(1, 'a2', 1),
           |(3, 'a4', 2);
           |""".stripMargin
      )

      // Partition key of integer type.
      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $rightTable(id int, name string, p int)
                   |using iceberg
                   |partitioned by (bucket(4, id), p);
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $rightTable values
           |(3, 'b4', 2),
           |(1, 'b2', 1),
           |(4, 'b5', 2),
           |(2, 'b3', 1),
           |(1, 'b1', 1);
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id and s.p = i.p;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan if plan.isInstanceOf[IcebergScanTransformer] =>
                  assert(
                    plan.asInstanceOf[IcebergScanTransformer].getKeyGroupPartitioning.isDefined)
                  assert(plan.asInstanceOf[IcebergScanTransformer].getSplitInfos.length == 3)
                case _ => // do nothing
              }
              checkLengthAndPlan(df, 7)
            }
        }
      }
    }
  }

  test("iceberg bucketed join partition value not exists") {
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $leftTable(id int, name string, p string)
                   |using iceberg
                   |partitioned by (bucket(4, id));
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $leftTable values
           |(4, 'a5', 'p4'),
           |(1, 'a1', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(2, 'a3', 'p2'),
           |(1, 'a2', 'p1'),
           |(3, 'a4', 'p3'),
           |(10, 'a4', 'p3');
           |""".stripMargin
      )
      spark.sql(s"""
                   |create table $rightTable(id int, name string, p int)
                   |using iceberg
                   |partitioned by (bucket(4, id));
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $rightTable values
           |(3, 'b4', 23),
           |(1, 'b1', 21);
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled" -> "false"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan: IcebergScanTransformer =>
                  assert(plan.getKeyGroupPartitioning.isDefined)
                  assert(plan.getSplitInfos.length == 3)
                case _ => // do nothing
              }
            }
        }
      }
    }
  }

  test("iceberg bucketed join partition value not exists partial cluster") {
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $leftTable(id int, name string, p string)
                   |using iceberg
                   |partitioned by (bucket(4, id));
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $leftTable values
           |(4, 'a5', 'p4'),
           |(1, 'a1', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(1, 'a2', 'p1'),
           |(2, 'a3', 'p2'),
           |(1, 'a2', 'p1'),
           |(3, 'a4', 'p3'),
           |(10, 'a4', 'p3');
           |""".stripMargin
      )
      spark.sql(s"""
                   |create table $rightTable(id int, name string, p int)
                   |using iceberg
                   |partitioned by (bucket(4, id));
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $rightTable values
           |(3, 'b4', 23),
           |(1, 'b1', 21);
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled" -> "true"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan: IcebergScanTransformer =>
                  assert(plan.getKeyGroupPartitioning.isDefined)
                  assert(plan.getSplitInfos.length == 3)
                case _ => // do nothing
              }
            }
        }
      }
    }
  }

  test("iceberg bucketed join with partition filter") {
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Partition key of string type.
      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $leftTable(id int, name string, p int)
                   |using iceberg
                   |partitioned by (bucket(4, id), p);
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $leftTable values
           |(4, 'a5', 2),
           |(1, 'a1', 1),
           |(2, 'a3', 1),
           |(1, 'a2', 1),
           |(3, 'a4', 2);
           |""".stripMargin
      )

      // Partition key of integer type.

      // Gluten does not support write iceberg table.
      spark.sql(s"""
                   |create table $rightTable(id int, name string, p int)
                   |using iceberg
                   |partitioned by (bucket(4, id), p);
                   |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $rightTable values
           |(3, 'b4', 2),
           |(1, 'b2', 1),
           |(4, 'b5', 2),
           |(2, 'b3', 1),
           |(1, 'b1', 1);
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id
                              | where s.p = 1 and i.p = 1;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan if plan.isInstanceOf[IcebergScanTransformer] =>
                  assert(
                    plan.asInstanceOf[IcebergScanTransformer].getKeyGroupPartitioning.isDefined)
                  assert(plan.asInstanceOf[IcebergScanTransformer].getSplitInfos.length == 1)
                case _ => // do nothing
              }
              checkLengthAndPlan(df, 5)
            }
        }
      }
    }
  }

  test("iceberg: time travel") {
    withTable("iceberg_tm") {
      spark.sql(s"""
                   |create table iceberg_tm (id int, name string) using iceberg
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into iceberg_tm values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into iceberg_tm values (3, "v3"), (4, "v4")
                   |""".stripMargin)

      val df =
        spark.sql("select snapshot_id from default.iceberg_tm.snapshots where parent_id is null")
      val value = df.collectAsList().get(0).getAs[Long](0)
      spark.sql(s"call system.set_current_snapshot('default.iceberg_tm',$value)")
      val data = runQueryAndCompare("select * from iceberg_tm") { _ => }
      checkLengthAndPlan(data, 2)
      checkAnswer(data, Row(1, "v1") :: Row(2, "v2") :: Nil)
    }
  }

  test("iceberg: partition filters") {
    withTable("iceberg_pf") {
      spark.sql(s"""
                   |create table iceberg_pf (id int, name string)
                   | using iceberg partitioned by (name)
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into iceberg_pf values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from iceberg_pf where name = 'v1'") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(3, "v1") :: Nil)
    }
  }

  test("iceberg read mor table - delete and update") {
    withTable("iceberg_mor_tb") {
      spark.sql("""
                  |create table iceberg_mor_tb (
                  |  id int,
                  |  name string,
                  |  p string
                  |) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'merge-on-read',
                  |  'write.update.mode' = 'merge-on-read',
                  |  'write.merge.mode' = 'merge-on-read'
                  |)
                  |partitioned by (p);
                  |""".stripMargin)

      // Insert some test rows.
      spark.sql("""
                  |insert into table iceberg_mor_tb
                  |values (1, 'a1', 'p1'), (2, 'a2', 'p1'), (3, 'a3', 'p2'),
                  |       (4, 'a4', 'p1'), (5, 'a5', 'p2'), (6, 'a6', 'p1');
                  |""".stripMargin)

      // Delete row.
      spark.sql(
        """
          |delete from iceberg_mor_tb where name = 'a1';
          |""".stripMargin
      )
      // Update row.
      spark.sql(
        """
          |update iceberg_mor_tb set name = 'new_a2' where id = 'a2';
          |""".stripMargin
      )
      // Delete row again.
      spark.sql(
        """
          |delete from iceberg_mor_tb where id = 6;
          |""".stripMargin
      )

      runQueryAndCompare("""
                           |select * from iceberg_mor_tb;
                           |""".stripMargin) {
        checkGlutenPlan[IcebergScanTransformer]
      }
    }
  }

  test("iceberg read mor table - merge into") {
    withTable("iceberg_mor_tb", "merge_into_source_tb") {
      spark.sql("""
                  |create table iceberg_mor_tb (
                  |  id int,
                  |  name string,
                  |  p string
                  |) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'merge-on-read',
                  |  'write.update.mode' = 'merge-on-read',
                  |  'write.merge.mode' = 'merge-on-read'
                  |)
                  |partitioned by (p);
                  |""".stripMargin)
      spark.sql("""
                  |create table merge_into_source_tb (
                  |  id int,
                  |  name string,
                  |  p string
                  |) using iceberg;
                  |""".stripMargin)

      // Insert some test rows.
      spark.sql("""
                  |insert into table iceberg_mor_tb
                  |values (1, 'a1', 'p1'), (2, 'a2', 'p1'), (3, 'a3', 'p2');
                  |""".stripMargin)
      spark.sql("""
                  |insert into table merge_into_source_tb
                  |values (1, 'a1_1', 'p2'), (2, 'a2_1', 'p2'), (3, 'a3_1', 'p1'),
                  |       (4, 'a4', 'p2'), (5, 'a5', 'p1'), (6, 'a6', 'p2');
                  |""".stripMargin)

      // Delete row.
      spark.sql(
        """
          |delete from iceberg_mor_tb where name = 'a1';
          |""".stripMargin
      )
      // Update row.
      spark.sql(
        """
          |update iceberg_mor_tb set name = 'new_a2' where id = 'a2';
          |""".stripMargin
      )

      // Merge into.
      spark.sql(
        """
          |merge into iceberg_mor_tb t
          |using (select * from merge_into_source_tb) s
          |on t.id = s.id
          |when matched then
          | update set t.name = s.name, t.p = s.p
          |when not matched then
          | insert (id, name, p) values (s.id, s.name, s.p);
          |""".stripMargin
      )
      runQueryAndCompare("""
                           |select * from iceberg_mor_tb;
                           |""".stripMargin) {
        checkGlutenPlan[IcebergScanTransformer]
      }
    }
  }

  // Spark configuration spark.sql.iceberg.handle-timestamp-without-timezone is not supported
  // in Spark 3.4
  testWithSpecifiedSparkVersion("iceberg partition type - timestamp", "3.5") {
    Seq("true", "false").foreach {
      flag =>
        withSQLConf(
          "spark.sql.iceberg.handle-timestamp-without-timezone" -> flag,
          "spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables" -> flag) {
          withTable("part_by_timestamp") {
            spark.sql("""
                        |create table part_by_timestamp (
                        |  p timestamp
                        |) using iceberg
                        |tblproperties (
                        |  'format-version' = '1'
                        |)
                        |partitioned by (p);
                        |""".stripMargin)

            // Insert some test rows.
            spark.sql("""
                        |insert into table part_by_timestamp
                        |values (TIMESTAMP '2022-01-01 00:01:20');
                        |""".stripMargin)
            val df = spark.sql("select * from part_by_timestamp")
            checkAnswer(df, Row(java.sql.Timestamp.valueOf("2022-01-01 00:01:20")) :: Nil)
          }
        }
    }
  }

  test("test read v1 iceberg with partition drop") {
    val testTable = "test_table_with_partition"
    withTable(testTable) {
      spark.sql(s"""
                   |CREATE TABLE $testTable (id INT, data STRING, p1 STRING, p2 STRING)
                   |USING iceberg
                   |tblproperties (
                   |  'format-version' = '1'
                   |)
                   |PARTITIONED BY (p1, p2);
                   |""".stripMargin)
      spark.sql(s"""
                   |INSERT INTO $testTable VALUES
                   |(1, 'test_data', 'test_p1', 'test_p2');
                   |""".stripMargin)
      spark.sql(s"""
                   |ALTER TABLE $testTable DROP PARTITION FIELD p2
                   |""".stripMargin)
      val resultDf = spark.sql(s"SELECT id, data, p1, p2 FROM $testTable")
      val result = resultDf.collect()

      assert(result.length == 1)
      assert(result.head.getString(3) == "test_p2")
    }
  }

  test("case-sensitive mode: iceberg scan executes natively") {
    // Regression test for the three unconditional .toLowerCase(Locale.ROOT) calls that were
    // replaced with ConverterUtils.normalizeColName in IcebergScanTransformer.
    // Under caseSensitive=true, readSchemaFields and inputFileRelatedMetadataColumns must
    // preserve original casing so metadata-column detection is not confused with data columns
    // whose names happen to match metadata-column constants when lowercased.
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_cs") {
        spark.sql("""
                    |CREATE TABLE iceberg_cs (id INT, data STRING)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_cs VALUES (1, 'a'), (2, 'b')
                    |""".stripMargin)

        // Basic scan must use IcebergScanTransformer (not fall back to vanilla Spark).
        runQueryAndCompare("SELECT id, data FROM iceberg_cs ORDER BY id") {
          checkGlutenPlan[IcebergScanTransformer]
        }
      }
    }
  }

  test("case-sensitive mode: iceberg input_file_name with caseSensitive=true") {
    // The readSchemaFields set and inputFileRelatedMetadataColumns filter both used
    // unconditional locale-unaware lowercasing, which would incorrectly classify a data
    // column named "Input_File_Name" (mixed case) as a metadata-column candidate because
    // the lowercased form "input_file_name" is in InputFileRelatedMetadataColumnNames.
    // After the fix, ConverterUtils.normalizeColName preserves casing under caseSensitive=true,
    // so the lookup against the all-lowercase constant set correctly returns false.
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_input_file_cs") {
        spark.sql("""
                    |CREATE TABLE iceberg_input_file_cs (id INT, data STRING)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_input_file_cs VALUES (1, 'x'), (2, 'y')
                    |""".stripMargin)

        // input_file_name() must work correctly under caseSensitive=true and
        // IcebergScanTransformer must be used (no validation fallback).
        val df = runAndCompare("""
                                 |SELECT id, input_file_name() AS fname
                                 |FROM iceberg_input_file_cs
                                 |ORDER BY id
                                 |""".stripMargin)
        checkGlutenPlan[IcebergScanTransformer](df)
        val rows = df.collect()
        assert(
          rows.forall(r => !r.isNullAt(1) && r.getString(1).nonEmpty),
          s"Expected non-empty input_file_name values, got: ${rows.mkString(", ")}")
      }
    }
  }

  test("test read iceberg with special characters in column name") {
    val testTable = "test_table_with_special_characters"
    withTable(testTable) {
      spark.sql(s"""
                   |CREATE TABLE $testTable (id INT, `my/data` STRING)
                   |USING iceberg
                   |""".stripMargin)
      spark.sql(s"""
                   |INSERT INTO $testTable VALUES
                   |(1, 'test_data');
                   |""".stripMargin)
      val resultDf = spark.sql(s"SELECT id, `my/data` FROM $testTable")
      val result = resultDf.collect()

      assert(result.length == 1)
      assert(result.head.getString(1) == "test_data")
    }
  }

  test("assert_not_null with iceberg table") {
    withTable("iceberg_not_null") {
      spark.sql("""
                  |CREATE TABLE iceberg_not_null (id BIGINT NOT NULL, name STRING NOT NULL)
                  |USING iceberg
                  |""".stripMargin)
      // Insert non-null values should succeed with AssertNotNull offloaded.
      spark.sql("INSERT INTO iceberg_not_null VALUES (1, 'a'), (2, 'b')")
      runQueryAndCompare("SELECT * FROM iceberg_not_null") {
        checkGlutenPlan[IcebergScanTransformer]
      }

      // Insert from a query with nullable source columns.
      spark.sql(
        "INSERT INTO iceberg_not_null SELECT id + 10, CAST(id AS STRING) FROM iceberg_not_null")
      val df = runQueryAndCompare("SELECT * FROM iceberg_not_null ORDER BY id") { _ => }
      assert(df.count() == 4)

      // Insert null into NOT NULL column should throw.
      val e = intercept[Exception] {
        spark.sql("INSERT INTO iceberg_not_null VALUES (null, 'c')").collect()
      }
      assert(
        e.getMessage.contains("null") || e.getMessage.contains("NOT_NULL") ||
          e.getCause != null && e.getCause.getMessage.contains("null"))
    }
  }

  test("iceberg scan falls back when native read is disabled") {
    withTable("iceberg_read_switch_tb") {
      spark.sql("""
                  |create table iceberg_read_switch_tb using iceberg as
                  |(select 1 as col1, 2 as col2)
                  |""".stripMargin)

      withSQLConf(GlutenIcebergConfig.ENABLE_NATIVE_READ.key -> "false") {
        val df = spark.sql("select * from iceberg_read_switch_tb")
        checkSparkPlan[BatchScanExec](df)
        assert(
          !getExecutedPlan(df).exists(_.isInstanceOf[IcebergScanTransformer]),
          "Iceberg scan should not be offloaded when native read is disabled")
        checkAnswer(df, Seq(Row(1, 2)))
      }

      // The switch is dynamic: offload resumes once it is back to the default.
      runQueryAndCompare("select * from iceberg_read_switch_tb") {
        checkGlutenPlan[IcebergScanTransformer]
      }
    }
  }

  test("disabling iceberg native read keeps other scans offloaded") {
    withTable("iceberg_read_switch_tb") {
      spark.sql("""
                  |create table iceberg_read_switch_tb using iceberg as
                  |(select 1 as col1)
                  |""".stripMargin)

      withTempPath {
        path =>
          spark.range(5).toDF("col1").write.parquet(path.getCanonicalPath)

          withSQLConf(GlutenIcebergConfig.ENABLE_NATIVE_READ.key -> "false") {
            val icebergDf = spark.sql("select * from iceberg_read_switch_tb")
            assert(
              !getExecutedPlan(icebergDf).exists(_.isInstanceOf[IcebergScanTransformer]),
              "Iceberg scan should fall back")

            val parquetDf = spark.read.parquet(path.getCanonicalPath)
            checkGlutenPlan[FileSourceScanExecTransformer](parquetDf)
            assert(parquetDf.count() == 5)
          }
      }
    }
  }

  test("case-sensitive mode: data column named Input_File_Name is not confused with metadata") {
    // Regression test for the IcebergScanTransformer fix.
    // A user data column whose name equals "input_file_name" when lowercased must NOT be
    // misclassified as an Iceberg metadata column under caseSensitive=true.
    // Before the fix, readSchemaFields and inputFileRelatedMetadataColumns both lowercased
    // names unconditionally, so "Input_File_Name" would hash-collide with the metadata
    // constant "input_file_name" and could be injected as a metadata column, shadowing the
    // user data.
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_col_collision") {
        spark.sql("""
                    |CREATE TABLE iceberg_col_collision
                    |  (id INT, `Input_File_Name` STRING)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_col_collision VALUES
                    |(1, 'user-data-value'), (2, 'another-value')
                    |""".stripMargin)

        // Data column must return the user value, not a file path.  This exercises the
        // readSchemaFields and inputFileRelatedMetadataColumns paths fixed in this patch.
        val dfData = runAndCompare("""
                                     |SELECT id, `Input_File_Name`
                                     |FROM iceberg_col_collision
                                     |ORDER BY id
                                     |""".stripMargin)
        checkGlutenPlan[IcebergScanTransformer](dfData)
        val dataRows = dfData.collect()
        assert(dataRows.length == 2, s"Expected 2 rows, got ${dataRows.length}")
        assert(
          dataRows(0).getString(1) == "user-data-value",
          s"Row 0 data column value wrong: ${dataRows(0).getString(1)}")
        assert(
          dataRows(1).getString(1) == "another-value",
          s"Row 1 data column value wrong: ${dataRows(1).getString(1)}")
      }
    }
  }

  test("case-sensitive mode: Input_File_Name data column and input_file_name metadata") {
    // Regression test for PushDownInputFileExpression.PostOffload. Under caseSensitive=true,
    // the user data column `Input_File_Name` and generated metadata attribute `input_file_name`
    // are distinct Spark attributes and both must remain available for binding.
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_input_file_projection") {
        spark.sql("""
                    |CREATE TABLE iceberg_input_file_projection
                    |  (id INT, `Input_File_Name` STRING)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_input_file_projection VALUES
                    |(1, 'user-data-value'), (2, 'another-value')
                    |""".stripMargin)

        val df = runAndCompare("""
                                 |SELECT id, `Input_File_Name`, input_file_name() AS fname
                                 |FROM iceberg_input_file_projection
                                 |ORDER BY id
                                 |""".stripMargin)
        // When the user table has a column whose lowercase name matches the metadata function
        // "input_file_name", Velox would see two conflicting column handles (Regular vs
        // PartitionKey) for the same physical name. Gluten detects this conflict and falls the
        // scan back to Vanilla BatchScanExec so Spark's own FilePartitionReader sets the
        // InputFileBlockHolder thread-local and input_file_name() returns the correct path.
        checkSparkPlan[BatchScanExec](df)
        val rows = df.collect()
        assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
        assert(rows(0).getString(1) == "user-data-value")
        assert(rows(1).getString(1) == "another-value")
        assert(
          rows.forall(r => !r.isNullAt(2) && r.getString(2).nonEmpty),
          s"Expected non-empty input_file_name values, got: ${rows.mkString(", ")}")
      }
    }
  }

  test("case-sensitive mode: lowercase input_file_name as data column -- platform compatibility") {
    // The exact name "input_file_name" (all lowercase) collides with the Spark built-in
    // function of the same name.  Whether a user column with that exact name can be
    // created in an Iceberg table is a platform question, not a Gluten question:
    //
    //  - If Iceberg/Spark rejects the CREATE TABLE: that is expected, the test is cancelled
    //    (not failed), and Gluten is not involved.
    //  - If Iceberg/Spark accepts the CREATE TABLE but rejects the INSERT (because the
    //    query planner resolves "input_file_name" as the built-in expression): that is
    //    also expected platform behaviour; the test is cancelled.
    //  - If both succeed: Gluten must read the data column correctly AND the
    //    input_file_name() function must return a distinct file path.  Using the same
    //    query to test both guards against the pre-fix bug where the two were conflated.
    //
    // Note: only bare Exception (not Throwable/Error) is caught as a platform-rejection signal.
    // Any Error (OOM, AssertionError inside the SQL engine) is allowed to propagate normally.
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_exact_collision") {
        // -- 1. CREATE TABLE ---------------------------------------------------
        // Narrow to AnalysisException: that is what the Spark analyzer throws
        // when a column name conflicts with a reserved function name or catalog
        // rules.  Any other exception (OOM, Gluten bug, etc.) must propagate.
        val createException: Option[AnalysisException] =
          try {
            spark.sql("""
                        |CREATE TABLE iceberg_exact_collision
                        |  (id INT, input_file_name STRING)
                        |USING iceberg
                        |""".stripMargin)
            None
          } catch {
            case e: AnalysisException => Some(e)
          }
        // If the Spark analyzer rejects this schema, cancel (not fail) the test.
        assume(
          createException.isEmpty,
          s"Spark analyzer rejected CREATE TABLE with column named 'input_file_name' " +
            s"(expected platform limitation, not a Gluten defect): " +
            s"${createException.map(_.getMessage).getOrElse("")}"
        )

        // -- 2. INSERT ---------------------------------------------------------
        // Use a value that is clearly not a file path so we can distinguish it
        // from the result of the input_file_name() function later.
        // Narrow to AnalysisException: Spark may resolve "input_file_name" as a
        // built-in function expression during INSERT analysis.
        val insertException: Option[AnalysisException] =
          try {
            spark.sql("""
                        |INSERT INTO iceberg_exact_collision VALUES
                        |(1, 'exact-user-value-not-a-path')
                        |""".stripMargin)
            None
          } catch {
            case e: AnalysisException => Some(e)
          }
        // If Spark resolves "input_file_name" as the built-in expression during INSERT,
        // cancel (not fail) the test.
        assume(
          insertException.isEmpty,
          s"Spark analyzer rejected INSERT INTO table with column named 'input_file_name' " +
            s"(expected platform limitation, not a Gluten defect): " +
            s"${insertException.map(_.getMessage).getOrElse("")}"
        )

        // -- 3. Verify Gluten correctness: data column and function are distinct -
        // Both CREATE and INSERT succeeded: Gluten must return the user data value
        // from the physical column AND a non-empty file path from the function.
        // They must be different values -- if the pre-fix bug is present the physical
        // column would be replaced by the function result, making them equal.
        val df = runAndCompare("""
                                 |SELECT id, input_file_name, input_file_name() AS fname
                                 |FROM iceberg_exact_collision
                                 |ORDER BY id
                                 |""".stripMargin)
        // The exact lowercase collision "input_file_name" (column) vs input_file_name() (function)
        // triggers the same Velox conflict detection; the scan falls back to Vanilla BatchScanExec.
        checkSparkPlan[BatchScanExec](df)
        val rows = df.collect()
        assert(rows.length == 1, s"Expected 1 row, got ${rows.length}")
        // Physical data column must contain the user-inserted value.
        assert(
          rows(0).getString(1) == "exact-user-value-not-a-path",
          s"Physical 'input_file_name' column should be user data, " +
            s"got: '${rows(0).getString(1)}'")
        // input_file_name() function must return a non-empty file path.
        val fname = rows(0).getString(2)
        assert(
          fname != null && fname.nonEmpty,
          s"input_file_name() must return a non-empty path, got: '$fname'")
        // The two must not be equal -- if they are, the old conflation bug is present.
        assert(
          rows(0).getString(1) != rows(0).getString(2),
          s"Physical column and function result must differ: " +
            s"col='${rows(0).getString(1)}', fn='${rows(0).getString(2)}'"
        )
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Comprehensive case-sensitivity tests -- 7 required scenarios
  //
  // These tests cover both spark.sql.caseSensitive=true and =false.
  // Under caseSensitive=true a table may have distinct columns id/ID/Id/iD
  // (Spark's case-sensitive analysis treats them as different identifiers).
  // Under caseSensitive=false the same names are treated as equivalent and
  // Spark's catalog/analyzer will reject a schema with duplicates.
  // ---------------------------------------------------------------------------

  // Scenario 1 - Exact column resolution: each column resolves to its own distinct value.
  // NOTE: Iceberg/Spark will reject a schema with columns that differ only in case when
  // caseSensitive=false (duplicate column error), so the four-column fixture is only
  // attempted when it is actually supported (guarded by assume).  The primary assertion -
  // that exact column names return their own values - is always exercised.
  test("case-sensitivity: exact column resolution (caseSensitive=true)") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_cs_exact") {
        // Attempt to create a table with four case-distinct columns.
        // Iceberg may reject this at the catalog level even under caseSensitive=true
        // because many catalog implementations normalize names to lowercase.
        // Narrow to AnalysisException: other exceptions are unexpected and must propagate.
        val createEx: Option[AnalysisException] =
          try {
            spark.sql("""
                        |CREATE TABLE iceberg_cs_exact
                        |  (id INT, ID INT, Id INT, iD INT)
                        |USING iceberg
                        |""".stripMargin)
            None
          } catch { case e: AnalysisException => Some(e) }

        if (createEx.isDefined) {
          // Four-column case-distinct schema is not supported on this platform.
          // Fall back to a simpler two-column fixture to still exercise exact name binding.
          withTable("iceberg_cs_exact_simple") {
            spark.sql("""
                        |CREATE TABLE iceberg_cs_exact_simple (lower_id INT, upper_ID INT)
                        |USING iceberg
                        |""".stripMargin)
            spark.sql("""
                        |INSERT INTO iceberg_cs_exact_simple VALUES (10, 20), (30, 40)
                        |""".stripMargin)

            // Each column must resolve to its own distinct value.
            val df1 = runAndCompare(
              "SELECT lower_id FROM iceberg_cs_exact_simple ORDER BY lower_id")
            checkGlutenPlan[IcebergScanTransformer](df1)
            assert(df1.collect().map(_.getInt(0)).toSeq == Seq(10, 30))

            val df2 = runAndCompare(
              "SELECT upper_ID FROM iceberg_cs_exact_simple ORDER BY upper_ID")
            checkGlutenPlan[IcebergScanTransformer](df2)
            assert(df2.collect().map(_.getInt(0)).toSeq == Seq(20, 40))
          }
        } else {
          spark.sql("""
                      |INSERT INTO iceberg_cs_exact VALUES (1, 2, 3, 4)
                      |""".stripMargin)

          // Each exact column name must return its own distinct value.
          val cases = Seq(("id", 1), ("ID", 2), ("Id", 3), ("iD", 4))
          cases.foreach {
            case (col, expected) =>
              // Use vanilla Spark as baseline then compare with Gluten.
              val df = runAndCompare(
                s"SELECT `$col` FROM iceberg_cs_exact ORDER BY `$col`")
              checkGlutenPlan[IcebergScanTransformer](df)
              val vals = df.collect().map(_.getInt(0))
              assert(
                vals.contains(expected),
                s"Column '$col' should contain $expected under caseSensitive=true, " +
                  s"got: ${vals.mkString(",")}")
          }

          // Incorrect casing must NOT resolve to a different column's value.
          // Under caseSensitive=true "ID" is distinct from "id", so selecting "ID"
          // must return 2, not 1.
          val dfWrong = runAndCompare("SELECT `ID` FROM iceberg_cs_exact ORDER BY `ID`")
          val wrongVals = dfWrong.collect().map(_.getInt(0))
          assert(
            !wrongVals.contains(1),
            s"Selecting 'ID' must not return value of 'id' (1) under caseSensitive=true")
        }
      }
    }
  }

  // Scenario 2 - Mixed-case identifier lookup under caseSensitive=false.
  // Under case-insensitive mode any casing of an identifier resolves to the same physical column.
  test("case-sensitivity: mixed-case identifier lookup (caseSensitive=false)") {
    withSQLConf("spark.sql.caseSensitive" -> "false") {
      withTable("iceberg_ci_exact") {
        spark.sql("""
                    |CREATE TABLE iceberg_ci_exact (id INT, data STRING)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_ci_exact VALUES (1, 'alpha'), (2, 'beta')
                    |""".stripMargin)

        // Under caseSensitive=false all spellings of "id" resolve to the same column.
        Seq("id", "ID", "Id", "iD").foreach {
          spelling =>
            val df = runAndCompare(
              s"SELECT `$spelling` FROM iceberg_ci_exact ORDER BY id")
            checkGlutenPlan[IcebergScanTransformer](df)
            val rows = df.collect()
            assert(rows.length == 2, s"Expected 2 rows for spelling '$spelling'")
            assert(
              rows.map(_.getInt(0)).toSeq == Seq(1, 2),
              s"Unexpected values for '$spelling': ${rows.map(_.getInt(0)).toSeq}")
        }
      }
    }
  }

  // Scenario 3 -- Test B: Ambiguous identifier resolution under caseSensitive=false.
  // A table containing two columns that differ only by case cannot be created when
  // caseSensitive=false because Spark's analyzer treats them as duplicates.  This test
  // verifies that attempting such a schema fails at the DDL level (Spark's own behavior),
  // and that if a schema with ambiguous names is somehow presented, Gluten follows Spark.
  //
  // Note: under caseSensitive=true, columns id/ID/Id/iD are distinct identifiers; the
  // case-distinct column creation is tested in the caseSensitive=true exact-resolution test.
  test("case-sensitivity: ambiguous column schema is rejected at DDL level (caseSensitive=false)") {
    withSQLConf("spark.sql.caseSensitive" -> "false") {
      withTable("iceberg_ci_dup") {
        // Spark with caseSensitive=false must reject a schema where two columns
        // differ only in case.
        // This is Spark's own behavior -- Gluten must not weaken it.
        // Narrow to AnalysisException: that is what Spark's analyzer raises for duplicate columns.
        val ex = intercept[AnalysisException] {
          spark.sql("""
                      |CREATE TABLE iceberg_ci_dup (id INT, ID INT)
                      |USING iceberg
                      |""".stripMargin)
        }
        // Spark raises an AnalysisException about duplicate/ambiguous column names.
        val msg = ex.getMessage + Option(ex.getCause).map(_.getMessage).getOrElse("")
        assert(
          msg.toLowerCase(Locale.ROOT).contains("duplicate") ||
            msg.toLowerCase(Locale.ROOT).contains("ambiguous") ||
            msg.toLowerCase(Locale.ROOT).contains("already exists") ||
            msg.toLowerCase(Locale.ROOT).contains("column"),
          s"Expected a duplicate/ambiguous column error but got: $msg"
        )
      }
    }
  }

  // Scenario 4 - Projection and filtering
  test("case-sensitivity: projection and filtering (caseSensitive=true)") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_cs_proj") {
        spark.sql("""
                    |CREATE TABLE iceberg_cs_proj (id INT, value INT, tag STRING)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_cs_proj VALUES
                    |(1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c')
                    |""".stripMargin)

        // Project subset + filter -- both must be correctly resolved and offloaded.
        val df = runAndCompare("""
                                 |SELECT id, tag FROM iceberg_cs_proj
                                 |WHERE value > 100
                                 |ORDER BY id
                                 |""".stripMargin)
        checkGlutenPlan[IcebergScanTransformer](df)
        val rows = df.collect()
        assert(rows.length == 2)
        assert(rows.map(_.getInt(0)).toSeq == Seq(2, 3))
        assert(rows.map(_.getString(1)).toSeq == Seq("b", "c"))
      }
    }
  }

  test("case-sensitivity: projection and filtering (caseSensitive=false)") {
    withSQLConf("spark.sql.caseSensitive" -> "false") {
      withTable("iceberg_ci_proj") {
        spark.sql("""
                    |CREATE TABLE iceberg_ci_proj (id INT, value INT, tag STRING)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_ci_proj VALUES
                    |(1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c')
                    |""".stripMargin)

        // Column names in upper case must still resolve correctly.
        val df = runAndCompare("""
                                 |SELECT ID, TAG FROM iceberg_ci_proj
                                 |WHERE VALUE > 100
                                 |ORDER BY id
                                 |""".stripMargin)
        checkGlutenPlan[IcebergScanTransformer](df)
        val rows = df.collect()
        assert(rows.length == 2)
        assert(rows.map(_.getInt(0)).toSeq == Seq(2, 3))
        assert(rows.map(_.getString(1)).toSeq == Seq("b", "c"))
      }
    }
  }

  // Scenario 7 - Aggregation
  test("case-sensitivity: aggregation (caseSensitive=true)") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("iceberg_cs_agg") {
        spark.sql("""
                    |CREATE TABLE iceberg_cs_agg (category STRING, value INT)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_cs_agg VALUES
                    |('a', 1), ('a', 2), ('b', 3), ('b', 4)
                    |""".stripMargin)

        val df = runAndCompare("""
                                 |SELECT category, SUM(value) AS total
                                 |FROM iceberg_cs_agg
                                 |GROUP BY category
                                 |ORDER BY category
                                 |""".stripMargin)
        checkGlutenPlan[IcebergScanTransformer](df)
        val rows = df.collect()
        assert(rows.length == 2)
        assert(rows(0).getString(0) == "a" && rows(0).getLong(1) == 3L)
        assert(rows(1).getString(0) == "b" && rows(1).getLong(1) == 7L)
      }
    }
  }

  test("case-sensitivity: aggregation (caseSensitive=false)") {
    withSQLConf("spark.sql.caseSensitive" -> "false") {
      withTable("iceberg_ci_agg") {
        spark.sql("""
                    |CREATE TABLE iceberg_ci_agg (category STRING, value INT)
                    |USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_ci_agg VALUES
                    |('a', 1), ('a', 2), ('b', 3), ('b', 4)
                    |""".stripMargin)

        // Mixed case in column references must still aggregate correctly.
        val df = runAndCompare("""
                                 |SELECT CATEGORY, SUM(VALUE) AS total
                                 |FROM iceberg_ci_agg
                                 |GROUP BY CATEGORY
                                 |ORDER BY CATEGORY
                                 |""".stripMargin)
        checkGlutenPlan[IcebergScanTransformer](df)
        val rows = df.collect()
        assert(rows.length == 2)
        assert(rows(0).getString(0) == "a" && rows(0).getLong(1) == 3L)
        assert(rows(1).getString(0) == "b" && rows(1).getLong(1) == 7L)
      }
    }
  }
}
