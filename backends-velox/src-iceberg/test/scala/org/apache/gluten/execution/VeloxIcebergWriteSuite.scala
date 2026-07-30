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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.GlutenImplicits._
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.gluten.TestUtils

class VeloxIcebergWriteSuite extends VeloxIcebergTestBase {

  import testImplicits._

  test("append to an unpartitioned table") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098)
                           |""".stripMargin)
      checkCommandPlan[VeloxIcebergAppendDataExec](df)
      checkAnswer(spark.sql("select * from iceberg_tb2"), Seq(Row(1098)))
    }
  }

  test("append to a table partitioned by identity") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (a);
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      checkCommandPlan[VeloxIcebergAppendDataExec](df)
      checkAnswer(spark.sql("select * from iceberg_tb2"), Seq(Row(1098, 189)))
    }
  }

  test("preserve uppercase identity partition names") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(A int, b int)
                  |using iceberg
                  |partitioned by (A);
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1, 1)
                           |""".stripMargin)
      checkCommandPlan[VeloxIcebergAppendDataExec](df)
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

  test("append to a table partitioned by bucket") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (bucket(16, a));
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      checkCommandPlan[VeloxIcebergAppendDataExec](df)
      checkAnswer(spark.sql("select * from iceberg_tb2"), Seq(Row(1098, 189)))
    }
  }

  test("append to a table partitioned by truncate") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (truncate(16, a));
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      checkCommandPlan[VeloxIcebergAppendDataExec](df)
      checkAnswer(spark.sql("select * from iceberg_tb2"), Seq(Row(1098, 189)))
    }
  }

  test("overwrite a table by expression") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tb2 values (1)")

      // Overwrite table
      val df = spark.sql("""
                           |insert overwrite table iceberg_tb2 values (2)
                           |""".stripMargin)
      checkCommandPlan[VeloxIcebergOverwriteByExpressionExec](df)
      checkAnswer(spark.sql("select * from iceberg_tb2"), Seq(Row(2)))
    }
  }

  test("create a partitioned table as select") {
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

  test("keep native write plans columnar") {
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

      def checkNoColumnarToRow(df: DataFrame): Unit = {
        assert(
          collect(commandPhysicalPlan(df)) {
            case p if p.isInstanceOf[ColumnarToRowExecBase] => p
          }.isEmpty)
      }

      // insert partitioned table
      var df = spark.sql("insert into table iceberg_tbl values (1, 1), (2, 1), (3, 1), (4, 2)")
      checkAnswer(
        spark.sql("select * from iceberg_tbl order by a"),
        Seq(Row(1, 1), Row(2, 1), Row(3, 1), Row(4, 2)))
      checkNoColumnarToRow(df)

      // delete partitioned table
      df = spark.sql("delete from iceberg_tbl where a = 1")
      checkAnswer(
        spark.sql("select * from iceberg_tbl order by a"),
        Seq(Row(2, 1), Row(3, 1), Row(4, 2)))
      checkNoColumnarToRow(df)

      // overwrite partitioned table
      df = spark.sql("insert overwrite table iceberg_tbl values (5, 1)")
      checkAnswer(spark.sql("select * from iceberg_tbl order by a"), Seq(Row(5, 1)))
      checkNoColumnarToRow(df)
    }
  }

  test("dynamically overwrite partitions") {
    withTable("iceberg_tbl") {
      spark.sql("""
                  |create table if not exists iceberg_tbl (a int, pt int) using iceberg
                  |partitioned by (pt)
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tbl values (1, 1), (2, 2)")

      withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
        val df = spark.sql("insert overwrite table iceberg_tbl values (11, 1)")
        checkCommandPlan[VeloxIcebergOverwritePartitionsDynamicExec](df)
        checkAnswer(
          spark.sql("select * from iceberg_tbl order by pt"),
          Seq(Row(11, 1), Row(2, 2))
        )
      }
    }
  }

  test("report the number of written files") {
    withTable("iceberg_tbl") {
      spark.sql("create table if not exists iceberg_tbl (id int) using iceberg".stripMargin)
      val df = spark.sql("insert into iceberg_tbl values 1")
      val metrics = commandPhysicalPlan(df).metrics
      val statusStore = spark.sharedState.statusStore
      val lastExecId = statusStore.executionsList().last.executionId
      val executionMetrics = statusStore.executionMetrics(lastExecId)

      assert(executionMetrics(metrics("numWrittenFiles").id).toLong == 1)
    }
  }

  test("use Iceberg-compatible data file names") {
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

  test("append a streaming query to a table") {
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

  test("fall back to Spark writes for tables with a sort order") {
    withTable("iceberg_sorted_tbl") {
      spark.sql("CREATE TABLE iceberg_sorted_tbl (a INT, b STRING) USING iceberg")
      spark.sql("ALTER TABLE iceberg_sorted_tbl WRITE ORDERED BY a")

      val df = spark.sql("INSERT INTO iceberg_sorted_tbl VALUES (1, 'hello'), (2, 'world')")

      // Should fallback to vanilla Spark's AppendDataExec.
      val commandPlan = commandPhysicalPlan(df)
      assert(commandPlan.isInstanceOf[AppendDataExec])
      assert(!commandPlan.isInstanceOf[VeloxIcebergAppendDataExec])

      checkAnswer(
        spark.sql("SELECT * FROM iceberg_sorted_tbl ORDER BY a"),
        Seq(Row(1, "hello"), Row(2, "world")))

      // Verify fallbackSummary reports the sort order fallback reason.
      val summary = df.fallbackSummary()
      assert(
        summary.fallbackNodeToReason.exists(
          _.values.exists(_.contains("Not support write table with sort order"))))
    }
  }
}
