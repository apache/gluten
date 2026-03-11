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
import org.apache.spark.sql.Row

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

  testWithMinSparkVersion("iceberg bucketed join", "3.4") {
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

  testWithMinSparkVersion("iceberg bucketed join with partition", "3.4") {
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

  testWithMinSparkVersion("iceberg bucketed join partition value not exists", "3.4") {
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

  testWithMinSparkVersion(
    "iceberg bucketed join partition value not exists partial cluster",
    "3.4") {
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

  testWithMinSparkVersion("iceberg bucketed join with partition filter", "3.4") {
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
      val value = df.collectAsList().get(0).getAs[Long](0);
      spark.sql(s"call system.set_current_snapshot('default.iceberg_tm',$value)");
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
  testWithSpecifiedSparkVersion("iceberg partition type - timestamp", "3.3", "3.5") {
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
}
