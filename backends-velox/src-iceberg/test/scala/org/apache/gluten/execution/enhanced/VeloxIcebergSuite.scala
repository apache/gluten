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
package org.apache.gluten.execution.enhanced

import org.apache.gluten.execution._
import org.apache.gluten.tags.EnhancedFeaturesTest

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.gluten.TestUtils

@EnhancedFeaturesTest
class VeloxIcebergSuite extends IcebergSuite {

  import testImplicits._

  test("iceberg insert") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
    }
  }

  test("iceberg insert partition table identity transform") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (a);
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
      assert(result(0).get(1) == 189)
    }
  }

  test("iceberg insert partition table with uppercase partition name") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(A int, b int)
                  |using iceberg
                  |partitioned by (A);
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1, 1)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      checkAnswer(spark.sql("select * from iceberg_tb2"), Seq(Row(1, 1)))

      val filePath = spark
        .sql("select * from default.iceberg_tb2.files")
        .select("file_path")
        .collect()
        .apply(0)
        .getString(0)
      val partitionPath = filePath.split('/').init.last
      assert(partitionPath == "A=1")
    }
  }

  test("iceberg read cow table - delete") {
    withTable("iceberg_cow_tb") {
      spark.sql("""
                  |create table iceberg_cow_tb (
                  |  id int,
                  |  name string,
                  |  p string
                  |) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |);
                  |""".stripMargin)

      // Insert some test rows.
      spark.sql("""
                  |insert into table iceberg_cow_tb
                  |values (1, 'a1', 'p1'), (2, 'a2', 'p1'), (3, 'a3', 'p2'),
                  |       (4, 'a4', 'p1'), (5, 'a5', 'p2'), (6, 'a6', 'p1');
                  |""".stripMargin)

      // Delete row.
      val df = spark.sql(
        """
          |delete from iceberg_cow_tb where name = 'a1';
          |""".stripMargin
      )
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergReplaceDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_cow_tb;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 5)

    }
  }

  test("iceberg insert partition table bucket transform") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (bucket(16, a));
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
      assert(result(0).get(1) == 189)
    }
  }

  test("iceberg insert partition table truncate transform") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (truncate(16, a));
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
      assert(result(0).get(1) == 189)
    }
  }

  test("iceberg insert overwrite") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tb2 values (1)")

      // Overwrite table
      val df = spark.sql("""
                           |insert overwrite table iceberg_tb2 values (2)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergOverwriteByExpressionExec])

      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 2)
    }
  }

  test("iceberg create table as select") {
    withTable("iceberg_tb1", "iceberg_tb2") {
      spark.sql("""
                  |create table iceberg_tb1 (a int, pt int) using iceberg
                  |partitioned by (pt)
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tb1 values (1, 1), (2, 2)")

      // CTAS
      val sqlStr = """
                     |create table iceberg_tb2 using iceberg
                     |partitioned by (pt)
                     |as select * from iceberg_tb1
                     |""".stripMargin

      TestUtils.checkExecutedPlanContains[VeloxIcebergAppendDataExec](spark, sqlStr)

      checkAnswer(
        spark.sql("select * from iceberg_tb2 order by a"),
        Seq(Row(1, 1), Row(2, 2))
      )
    }
  }

  test("check iceberg write c2r") {
    withTable("iceberg_tbl") {
      spark.sql("""
                  |create table if not exists iceberg_tbl (a int, pt int) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |)
                  |partitioned by (pt)
                  |""".stripMargin)

      def checkColumnarToRow(df: DataFrame, num: Int): Unit = {
        assert(
          collect(
            df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
            case p if p.isInstanceOf[ColumnarToRowExecBase] => p
          }.size == num)
      }

      // insert partitioned table
      var df = spark.sql("insert into table iceberg_tbl values (1, 1), (2, 1), (3, 1), (4, 2)")
      checkAnswer(
        spark.sql("select * from iceberg_tbl order by a"),
        Seq(Row(1, 1), Row(2, 1), Row(3, 1), Row(4, 2)))
      checkColumnarToRow(df, 0)

      // delete partitioned table
      df = spark.sql("delete from iceberg_tbl where a = 1")
      checkAnswer(
        spark.sql("select * from iceberg_tbl order by a"),
        Seq(Row(2, 1), Row(3, 1), Row(4, 2)))
      checkColumnarToRow(df, 0)

      // overwrite partitioned table
      df = spark.sql("insert overwrite table iceberg_tbl values (5, 1)")
      checkAnswer(spark.sql("select * from iceberg_tbl order by a"), Seq(Row(5, 1)))
      checkColumnarToRow(df, 0)
    }
  }

  test("iceberg dynamic insert overwrite partition") {
    withTable("iceberg_tbl") {
      spark.sql("""
                  |create table if not exists iceberg_tbl (a int, pt int) using iceberg
                  |partitioned by (pt)
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tbl values (1, 1), (2, 2)")

      withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
        val df = spark.sql("insert overwrite table iceberg_tbl values (11, 1)")
        assert(
          df.queryExecution.executedPlan
            .asInstanceOf[CommandResultExec]
            .commandPhysicalPlan
            .isInstanceOf[VeloxIcebergOverwritePartitionsDynamicExec])
        checkAnswer(
          spark.sql("select * from iceberg_tbl order by pt"),
          Seq(Row(11, 1), Row(2, 2))
        )
      }
    }
  }

  test("iceberg write metrics") {
    withTable("iceberg_tbl") {
      spark.sql("create table if not exists iceberg_tbl (id int) using iceberg".stripMargin)
      val df = spark.sql("insert into iceberg_tbl values 1")
      val metrics =
        df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan.metrics
      val statusStore = spark.sharedState.statusStore
      val lastExecId = statusStore.executionsList().last.executionId
      val executionMetrics = statusStore.executionMetrics(lastExecId)

      // TODO: fix https://github.com/apache/incubator-gluten/issues/11510
      assert(executionMetrics(metrics("numWrittenFiles").id).toLong == 0)
    }
  }

  test("iceberg write file name") {
    withTable("iceberg_tbl") {
      spark.sql("create table if not exists iceberg_tbl (id int) using iceberg")
      spark.sql("insert into iceberg_tbl values 1")

      val filePath = spark
        .sql("select * from default.iceberg_tbl.files")
        .select("file_path")
        .collect()
        .apply(0)
        .getString(0)

      val fileName = filePath.split('/').last
      // Expected format: {partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}.parquet
      // Example: 00000-0-query_id-0-00001.parquet
      assert(
        fileName.matches("\\d{5}-\\d+-.*-\\d{5}\\.parquet"),
        s"File name does not match expected format: $fileName")
    }
  }

  test("iceberg stream write to table") {
    withTable("iceberg_tbl") {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE iceberg_tbl (a INT, b STRING) USING iceberg")
          TestUtils.checkExecutedPlanContains[VeloxIcebergWriteToDataSourceV2Exec](spark) {
            val inputData = MemoryStream[(Int, String)]
            val stream = inputData
              .toDS()
              .toDF("a", "b")
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .format("iceberg")
              .toTable("iceberg_tbl")

            val query = () => spark.sql("SELECT * FROM iceberg_tbl ORDER BY a")
            try {
              inputData.addData((1, "a"))
              stream.processAllAvailable()
              checkAnswer(query(), Seq(Row(1, "a")))

              inputData.addData((2, "b"))
              stream.processAllAvailable()
              checkAnswer(query(), Seq(Row(1, "a"), Row(2, "b")))
            } finally {
              stream.stop()
            }
          }

      }
    }
  }
}
