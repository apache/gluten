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
import org.apache.spark.sql.execution.GlutenImplicits._
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec
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

      // TODO: fix https://github.com/apache/gluten/issues/11510
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

  test("iceberg native write fallback when validation fails - sort order") {
    withTable("iceberg_sorted_tbl") {
      spark.sql("CREATE TABLE iceberg_sorted_tbl (a INT, b STRING) USING iceberg")
      spark.sql("ALTER TABLE iceberg_sorted_tbl WRITE ORDERED BY a")

      val df = spark.sql("INSERT INTO iceberg_sorted_tbl VALUES (1, 'hello'), (2, 'world')")

      // Should fallback to vanilla Spark's AppendDataExec.
      val commandPlan =
        df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
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

  test("iceberg read cow table - update after schema evolution") {
    withTable("iceberg_cow_update_evolved_tb") {
      spark.sql("""
                  |create table iceberg_cow_update_evolved_tb (
                  |  id int,
                  |  name string,
                  |  age int
                  |) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |)
                  |""".stripMargin)

      spark.sql("""
                  |alter table iceberg_cow_update_evolved_tb
                  |add columns (salary decimal(10, 2))
                  |""".stripMargin)

      spark.sql("""
                  |insert into table iceberg_cow_update_evolved_tb values
                  |  (1, 'Name1', 23, 3400.00),
                  |  (2, 'Name2', 30, 5500.00),
                  |  (3, 'Name3', 35, 6500.00)
                  |""".stripMargin)

      val df = spark.sql("""
                           |update iceberg_cow_update_evolved_tb
                           |set name = 'Name4'
                           |where id = 1
                           |""".stripMargin)

      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergReplaceDataExec])

      checkAnswer(
        spark.sql("""
                    |select id, name, age, salary
                    |from iceberg_cow_update_evolved_tb
                    |order by id
                    |""".stripMargin),
        Seq(
          Row(1, "Name4", 23, new java.math.BigDecimal("3400.00")),
          Row(2, "Name2", 30, new java.math.BigDecimal("5500.00")),
          Row(3, "Name1", 35, new java.math.BigDecimal("6500.00"))
        )
      )
    }
  }
  test("iceberg show stats has non-empty min max for store sales table") {
    withTable("store_sales_10_rows") {
      spark.sql("""
                  |CREATE TABLE store_sales_10_rows (
                  |  ss_sold_date_sk INT,
                  |  ss_sold_time_sk INT,
                  |  ss_item_sk INT,
                  |  ss_customer_sk INT,
                  |  ss_cdemo_sk INT,
                  |  ss_hdemo_sk INT,
                  |  ss_addr_sk INT,
                  |  ss_store_sk INT,
                  |  ss_promo_sk INT,
                  |  ss_ticket_number BIGINT,
                  |  ss_quantity INT,
                  |  ss_wholesale_cost DECIMAL(7,2),
                  |  ss_list_price DECIMAL(7,2),
                  |  ss_sales_price DECIMAL(7,2),
                  |  ss_ext_discount_amt DECIMAL(7,2),
                  |  ss_ext_sales_price DECIMAL(7,2),
                  |  ss_ext_wholesale_cost DECIMAL(7,2),
                  |  ss_ext_list_price DECIMAL(7,2),
                  |  ss_ext_tax DECIMAL(7,2),
                  |  ss_coupon_amt DECIMAL(7,2),
                  |  ss_net_paid DECIMAL(7,2),
                  |  ss_net_paid_inc_tax DECIMAL(7,2),
                  |  ss_net_profit DECIMAL(7,2)
                  |) USING iceberg
                  |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO store_sales_10_rows VALUES
          |(2450899, null, 174781, null, null, 5105, 712262, null, null, 875206344, null, 75.64, 105.13, null, 0.00, null, 3328.16, 4625.72, null, 0.00, null, null, null),
          |(2450899, 45381, 240260, 63498438, 1296795, 5105, 712262, 542, 1925, 875206344, 13, 5.12, 7.27, 2.18, 0.00, 28.34, 66.56, 94.51, 1.98, 0.00, 28.34, 30.32, -38.22),
          |(2450899, 45381, 360506, 63498438, 1296795, 5105, 712262, 542, 332, 875206344, 69, 36.45, 70.34, 16.17, 0.00, 1115.73, 2515.05, 4853.46, 22.31, 0.00, 1115.73, 1138.04, -1399.32),
          |(2450899, 45381, 197360, 63498438, 1296795, 5105, 712262, 542, 1486, 875206344, 50, 92.87, 167.16, 58.50, 0.00, 2925.00, 4643.50, 8358.00, 204.75, 0.00, 2925.00, 3129.75, -1718.50),
          |(2450899, 45381, 58255, 63498438, 1296795, 5105, 712262, 542, 359, 875206344, 100, 85.99, 105.76, 47.59, 523.49, 4759.00, 8599.00, 10576.00, 296.48, 523.49, 4235.51, 4531.99, -4363.49),
          |(2450899, 45381, 219500, 63498438, 1296795, 5105, 712262, 542, 8, 875206344, 77, 80.61, 121.72, 26.77, 2020.06, 2061.29, 6206.97, 9372.44, 1.64, 2020.06, 41.23, 42.87, -6165.74),
          |(2450899, 45381, 60157, 63498438, 1296795, 5105, 712262, 542, 484, 875206344, 9, 44.58, 62.85, 50.28, 0.00, 452.52, 401.22, 565.65, 27.15, 0.00, 452.52, 479.67, 51.30),
          |(2450899, 45381, 132362, 63498438, 1296795, 5105, 712262, 542, 1575, 875206344, 86, 30.79, 31.71, 25.68, 0.00, 2208.48, 2647.94, 2727.06, 22.08, 0.00, 2208.48, 2230.56, -439.46),
          |(2450899, 45381, 41590, 63498438, 1296795, 5105, 712262, 542, 441, 875206344, 40, 79.99, 137.58, 12.38, 0.00, 495.20, 3199.60, 5503.20, 9.90, 0.00, 495.20, 505.10, -2704.40)
          |""".stripMargin)

      spark.sql("ANALYZE store_sales_10_rows")

      withSQLConf("spark.sql.statistics.ignoreStatsCalculatorFailures" -> "false") {
        val stats = spark.sql("SHOW STATS FOR store_sales_10_rows")

        val statsByColumn =
          stats.collect().map(row => row.getAs[String]("column_name") -> row).toMap

        Seq(
          "ss_sold_date_sk",
          "ss_item_sk",
          "ss_ticket_number",
          "ss_quantity",
          "ss_wholesale_cost",
          "ss_list_price",
          "ss_sales_price",
          "ss_ext_sales_price",
          "ss_net_profit"
        ).foreach {
          colName =>
            val row = statsByColumn(colName)

            assert(
              Option(row.getAs[Any]("min")).exists(_.toString.nonEmpty),
              s"Expected non-empty min for $colName in SHOW STATS output: $row")

            assert(
              Option(row.getAs[Any]("max")).exists(_.toString.nonEmpty),
              s"Expected non-empty max for $colName in SHOW STATS output: $row")
        }
      }
    }
  }
}
