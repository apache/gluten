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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.CommandResultExec

import org.apache.hadoop.fs.Path
import org.apache.iceberg.{DataFile, SortOrderComparators, Table}
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.shaded.org.apache.parquet.example.data.Group
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.iceberg.spark.Spark3Util

import scala.collection.JavaConverters._

@EnhancedFeaturesTest
class VeloxIcebergSortOrderSuite extends WholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.gluten.sql.enable.enhancedFeatures", "true")
    .set("spark.gluten.sql.columnar.maxBatchSize", "64")
    .set("spark.sql.ansi.enabled", "false")
    .set("spark.sql.shuffle.partitions", "4")
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set(
      "spark.sql.catalog.spark_catalog.warehouse",
      s"file://${getClass.getResource("/").getPath}/iceberg-sort-order-warehouse")

  private def input: DataFrame = spark
    .range(0, 1024, 1, 4)
    .selectExpr("CAST(pmod(id * 37, 1024) AS INT) AS id")
    .selectExpr(
      "id",
      "CASE WHEN id % 7 = 0 THEN NULL ELSE id % 5 END AS category",
      "named_struct('rank', id % 11) AS nested",
      "concat(sha2(CAST(id AS STRING), 256), sha2(CAST(id + 1024 AS STRING), 256)) AS payload"
    )

  private def createTable(partitioning: String = ""): Unit = {
    spark.sql(s"""
                 |CREATE TABLE iceberg_sort_order (
                 |  id INT, category INT, nested STRUCT<rank: INT>, payload STRING
                 |) USING iceberg $partitioning
                 |TBLPROPERTIES ('write.parquet.compression-codec' = 'uncompressed')
                 |""".stripMargin)
  }

  private def nativeWrite(sql: String): IcebergWriteExec = {
    val result = spark.sql(sql)
    val plan =
      result.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
    assert(plan.isInstanceOf[IcebergWriteExec], s"Expected native Iceberg write: $plan")
    plan.asInstanceOf[IcebergWriteExec]
  }

  private def readFiles(table: Table): Vector[(DataFile, Vector[Record])] = {
    table.refresh()
    val tasks = table.newScan().planFiles()
    val files =
      try {
        tasks.asScala.map(_.file().copy()).toVector
      } finally {
        tasks.close()
      }
    files.map {
      file =>
        val reader = ParquetReader
          .builder[Group](new GroupReadSupport(), new Path(file.path().toString))
          .withConf(spark.sessionState.newHadoopConf())
          .build()
        val rows =
          try {
            Iterator.continually(reader.read()).takeWhile(_ != null).map {
              group =>
                val row = GenericRecord.create(table.schema())
                row.setField("id", Int.box(group.getInteger("id", 0)))
                if (group.getFieldRepetitionCount("category") > 0) {
                  row.setField("category", Int.box(group.getInteger("category", 0)))
                }
                val nested = GenericRecord.create(table.schema().findType("nested").asStructType())
                nested.setField("rank", Int.box(group.getGroup("nested", 0).getInteger("rank", 0)))
                row.setField("nested", nested)
                row.setField("payload", group.getString("payload", 0))
                row: Record
            }.toVector
          } finally {
            reader.close()
          }
        assert(rows.nonEmpty)
        (file, rows)
    }
  }

  private def assertSortedFiles(global: Boolean): Vector[(DataFile, Vector[Record])] = {
    val table = Spark3Util.loadIcebergTable(spark, "iceberg_sort_order")
    val files = readFiles(table)
    assert(files.size > 1, "Sorting must be tested across multiple output files")
    val comparator = SortOrderComparators.forSchema(table.schema(), table.sortOrder())
    files.foreach {
      case (file, rows) =>
        assert(file.sortOrderId() == table.sortOrder().orderId())
        rows.sliding(2).filter(_.size == 2).foreach {
          pair =>
            assert(comparator.compare(pair.head, pair.last) <= 0, s"Unsorted file: ${file.path()}")
        }
    }
    val ids = files.flatMap(_._2.map(_.getField("id").asInstanceOf[Int])).sorted
    assert(ids == (0 until 1024).toVector)

    if (global) {
      files.groupBy(_._1.partition().toString).values.foreach {
        partitionFiles =>
          val ranges = partitionFiles.map(_._2).sortWith {
            (left, right) => comparator.compare(left.head, right.head) < 0
          }
          ranges.sliding(2).filter(_.size == 2).foreach {
            pair =>
              assert(
                comparator.compare(pair.head.last, pair.last.head) <= 0,
                "Output files have overlapping sort-key ranges")
          }
      }
    }
    files
  }

  for {
    (name, partitioning, ordering, global) <- Seq(
      ("global", "", "ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST", true),
      ("local", "", "LOCALLY ORDERED BY category DESC NULLS FIRST, id ASC NULLS LAST", false),
      (
        "partitioned",
        "PARTITIONED BY (bucket(2, id))",
        "ORDERED BY category DESC NULLS LAST, id ASC",
        true),
      (
        "transformed",
        "",
        "ORDERED BY truncate(3, category) DESC NULLS FIRST, id ASC",
        true),
      ("nested", "", "ORDERED BY nested.rank ASC, id DESC", true)
    )
  } {
    test(s"iceberg native $name sort order") {
      withTable("iceberg_sort_order") {
        withTempView("sort_order_input") {
          createTable(partitioning)
          spark.sql(s"ALTER TABLE iceberg_sort_order WRITE $ordering")
          if (!global) {
            spark.sql("""
                        |ALTER TABLE iceberg_sort_order SET TBLPROPERTIES (
                        |  'write.distribution-mode' = 'none')
                        |""".stripMargin)
          }
          input.createOrReplaceTempView("sort_order_input")
          val write = nativeWrite("INSERT INTO iceberg_sort_order SELECT * FROM sort_order_input")
          if (name == "global" || name == "local") {
            assert(collect(write.query) { case s: SortExecTransformer => s }.nonEmpty)
          }
          assertSortedFiles(global)
        }
      }
    }
  }

  test("iceberg sort order survives file rotation and overwrite") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("iceberg_sort_order") {
        withTempView("sort_order_input") {
          createTable()
          spark.sql("ALTER TABLE iceberg_sort_order WRITE ORDERED BY id DESC")
          spark.sql("""
                      |ALTER TABLE iceberg_sort_order SET TBLPROPERTIES (
                      |  'write.target-file-size-bytes' = '8192',
                      |  'write.parquet.page-size-bytes' = '1024')
                      |""".stripMargin)
          input.createOrReplaceTempView("sort_order_input")
          nativeWrite("INSERT INTO iceberg_sort_order SELECT * FROM sort_order_input")
          val files = assertSortedFiles(global = true)
          assert(files.size > 4, "Expected file rotation within write tasks")

          spark.sql("ALTER TABLE iceberg_sort_order WRITE ORDERED BY category, id")
          nativeWrite("INSERT OVERWRITE iceberg_sort_order SELECT * FROM sort_order_input")
          assertSortedFiles(global = true)
        }
      }
    }
  }

  test("iceberg copy-on-write UPDATE preserves sort order") {
    withTable("iceberg_sort_order") {
      withTempView("sort_order_input") {
        createTable()
        spark.sql(
          "ALTER TABLE iceberg_sort_order WRITE ORDERED BY category DESC NULLS LAST, id ASC")
        input.createOrReplaceTempView("sort_order_input")
        nativeWrite("INSERT INTO iceberg_sort_order SELECT * FROM sort_order_input")
        nativeWrite("UPDATE iceberg_sort_order SET category = 20 - category WHERE id % 2 = 0")
        assertSortedFiles(global = false)
      }
    }
  }

  for (aqe <- Seq(false, true)) {
    test(s"iceberg local sort order survives native hash aggregation: aqe=$aqe") {
      withSQLConf("spark.sql.adaptive.enabled" -> aqe.toString) {
        withTable("iceberg_sort_order") {
          withTempView("sort_order_input") {
            createTable()
            spark.sql("ALTER TABLE iceberg_sort_order WRITE LOCALLY ORDERED BY id")
            spark.sql("""
                        |ALTER TABLE iceberg_sort_order SET TBLPROPERTIES (
                        |  'write.distribution-mode' = 'none')
                        |""".stripMargin)
            input.createOrReplaceTempView("sort_order_input")
            val write = nativeWrite("""
                                      |INSERT INTO iceberg_sort_order
                                      |SELECT id, first(category), first(nested), max(payload)
                                      |FROM sort_order_input GROUP BY id
                                      |""".stripMargin)
            assert(collect(write.query) { case a: HashAggregateExecTransformer => a }.nonEmpty)
            assertSortedFiles(global = false)
            assert(collect(write.query) { case s: SortExecTransformer => s }.nonEmpty)
          }
        }
      }
    }
  }
}
