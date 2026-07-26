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

import org.apache.gluten.execution.{IcebergScanTransformer, IcebergSuite}
import org.apache.gluten.tags.EnhancedFeaturesTest

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.CommandResultExec

@EnhancedFeaturesTest
class VeloxIcebergDeletionVectorSuite extends IcebergSuite {

  import testImplicits._

  test("read Spark deletion vector and union it in a native delete") {
    withTable("iceberg_dv_delete") {
      createV3Table("iceberg_dv_delete")
      spark.sql("INSERT INTO iceberg_dv_delete VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")

      withSQLConf("spark.gluten.sql.enable.enhancedFeatures" -> "false") {
        spark.sql("DELETE FROM iceberg_dv_delete WHERE id = 1").collect()
      }

      val sparkDvRead = spark.sql("SELECT * FROM iceberg_dv_delete ORDER BY id")
      checkAnswer(sparkDvRead, Seq(Row(2, "b"), Row(3, "c"), Row(4, "d")))
      assert(sparkDvRead.queryExecution.executedPlan.collect {
        case _: IcebergScanTransformer => true
      }.nonEmpty)

      val nativeDelete = spark.sql("DELETE FROM iceberg_dv_delete WHERE id = 2")
      assertNativeDeltaWrite(nativeDelete)
      checkAnswer(
        spark.sql("SELECT * FROM iceberg_dv_delete ORDER BY id"),
        Seq(Row(3, "c"), Row(4, "d")))

      checkAnswer(
        spark.sql("""
                    |SELECT file_format, record_count
                    |FROM iceberg_dv_delete.delete_files
                    |""".stripMargin),
        Seq(Row("PUFFIN", 2L))
      )
    }
  }

  test("native update and merge commit data and deletion vectors") {
    withTable("iceberg_dv_mutation") {
      createV3Table("iceberg_dv_mutation")
      spark.sql("INSERT INTO iceberg_dv_mutation VALUES (1, 'a'), (2, 'b')")

      val update = spark.sql("UPDATE iceberg_dv_mutation SET data = 'updated' WHERE id = 1")
      assertNativeDeltaWrite(update)

      withTempView("iceberg_dv_source") {
        Seq((2, "merged"), (3, "inserted")).toDF("id", "data").createOrReplaceTempView(
          "iceberg_dv_source")
        val merge = spark.sql("""
                                |MERGE INTO iceberg_dv_mutation target
                                |USING iceberg_dv_source source
                                |ON target.id = source.id
                                |WHEN MATCHED THEN UPDATE SET data = source.data
                                |WHEN NOT MATCHED THEN INSERT (id, data)
                                |VALUES (source.id, source.data)
                                |""".stripMargin)
        assertNativeDeltaWrite(merge)
      }

      checkAnswer(
        spark.sql("SELECT * FROM iceberg_dv_mutation ORDER BY id"),
        Seq(Row(1, "updated"), Row(2, "merged"), Row(3, "inserted")))
      assert(
        spark
          .sql("""
                 |SELECT count(*)
                 |FROM iceberg_dv_mutation.delete_files
                 |WHERE file_format = 'PUFFIN' AND referenced_data_file IS NOT NULL
                 |""".stripMargin)
          .head()
          .getLong(0) > 0)
    }
  }

  test("native deletion vectors preserve partitions and filter split ranges") {
    withTable("iceberg_dv_partitioned") {
      spark.sql("""
                  |CREATE TABLE iceberg_dv_partitioned (id INT, data STRING, p INT)
                  |USING iceberg
                  |PARTITIONED BY (p)
                  |TBLPROPERTIES (
                  |  'format-version' = '3',
                  |  'write.delete.mode' = 'merge-on-read',
                  |  'write.update.mode' = 'merge-on-read',
                  |  'write.merge.mode' = 'merge-on-read',
                  |  'read.split.target-size' = '1024',
                  |  'write.parquet.row-group-size-bytes' = '1024'
                  |)
                  |""".stripMargin)
      withSQLConf("spark.gluten.sql.enable.enhancedFeatures" -> "false") {
        spark
          .range(0, 5000)
          .selectExpr(
            "cast(id as int) as id",
            "concat(id, repeat('x', 128)) as data",
            "cast(id % 2 as int) as p")
          .writeTo("iceberg_dv_partitioned")
          .append()
      }

      val delete = spark.sql("DELETE FROM iceberg_dv_partitioned WHERE id IN (10, 51)")
      assertNativeDeltaWrite(delete)

      withSQLConf("spark.sql.files.maxPartitionBytes" -> "256") {
        val splitRead = spark.sql("""
                                    |SELECT id, p
                                    |FROM iceberg_dv_partitioned
                                    |WHERE id BETWEEN 9 AND 52
                                    |ORDER BY id
                                    |""".stripMargin)
        checkAnswer(splitRead, (9 to 52).filterNot(Set(10, 51)).map(id => Row(id, id % 2)))
        val scan = splitRead.queryExecution.executedPlan.collectFirst {
          case icebergScan: IcebergScanTransformer => icebergScan
        }.get
        assert(scan.getSplitInfos.length > 2)
      }
      checkAnswer(
        spark.sql("""
                    |SELECT count(*)
                    |FROM iceberg_dv_partitioned.delete_files
                    |WHERE file_format <> 'PUFFIN' OR referenced_data_file IS NULL
                    |""".stripMargin),
        Seq(Row(0L))
      )
    }
  }

  private def createV3Table(table: String): Unit = {
    spark.sql(s"""
                 |CREATE TABLE $table (id INT, data STRING)
                 |USING iceberg
                 |TBLPROPERTIES (
                 |  'format-version' = '3',
                 |  'write.delete.mode' = 'merge-on-read',
                 |  'write.update.mode' = 'merge-on-read',
                 |  'write.merge.mode' = 'merge-on-read'
                 |)
                 |""".stripMargin)
  }

  private def assertNativeDeltaWrite(df: DataFrame): Unit = {
    val commandPlan =
      df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
    assert(commandPlan.collect {
      case plan if plan.getClass.getSimpleName == "VeloxIcebergWriteDeltaExec" => true
    }.nonEmpty)
  }
}
