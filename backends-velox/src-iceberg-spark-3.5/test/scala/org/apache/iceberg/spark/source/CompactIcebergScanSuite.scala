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
package org.apache.iceberg.spark.source

import org.apache.gluten.config.{GlutenConfig, GlutenIcebergConfig}
import org.apache.gluten.execution.{IcebergScanTransformer, WholeStageTransformerSuite}

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

import org.apache.iceberg.spark.Spark3Util
import org.scalatest.time.{Seconds, Span}

import java.lang.ref.WeakReference

import scala.collection.mutable.ArrayBuffer

class CompactIcebergScanSuite extends WholeStageTransformerSuite {
  override protected val resourcePath = "/tpch-data-parquet"
  override protected val fileFormat = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")
    .set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set(
      "spark.sql.catalog.spark_catalog.warehouse",
      getClass.getResource("/").toString + "compact-iceberg-warehouse")
    .set("spark.sql.iceberg.data-planning-mode", "local")
    .set("spark.sql.iceberg.locality.enabled", "false")
    .set(GlutenIcebergConfig.EARLY_TASK_ENCODING_ENABLED.key, "true")

  override protected def createSparkSession =
    org.apache.spark.sql.CompactIcebergTestSession.create(sparkConf)

  private def withFixture(partitioned: Boolean = true, format: String = "parquet")(
      f: String => Unit): Unit = {
    val name = "compact_iceberg_test"
    withTable(name) {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        val partitioning = if (partitioned) "PARTITIONED BY (p)" else ""
        sql(s"""CREATE TABLE $name (id INT, p INT, data STRING) USING iceberg $partitioning
          TBLPROPERTIES ('format-version'='2', 'write.format.default'='$format',
            'write.delete.mode'='merge-on-read')""")
        sql(s"INSERT INTO $name VALUES (1, 10, 'a'), (2, 10, null), (3, 20, 'c')")
        sql(s"INSERT INTO $name VALUES (4, 10, 'd'), (5, null, 'e'), (6, 20, 'f')")
      }
      f(name)
    }
  }

  private def read(name: String): DataFrame =
    spark.read.option("split-size", "1048576").table(name)

  private def scan(df: DataFrame): CompactIcebergBatchScan = {
    val scans = df.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation
          if relation.scan.isInstanceOf[CompactIcebergBatchScan] =>
        relation.scan.asInstanceOf[CompactIcebergBatchScan]
    }
    assert(scans.size == 1, df.queryExecution.optimizedPlan.toString())
    scans.head
  }

  private def assertNoTaskCaches(compact: CompactIcebergBatchScan): Unit = {
    Seq("tasks", "taskGroups").foreach {
      name =>
        val field = classOf[SparkBatchQueryScan].getSuperclass.getDeclaredField(name)
        field.setAccessible(true)
        assert(field.get(compact) == null, s"$name retained Java planning objects")
    }
  }

  private def checkResult(query: DataFrame, expected: Seq[Row]): Unit = {
    // QueryTest's default also executes a separate toRDD plan, which may use a vanilla scan.
    org.apache.spark.sql.QueryTest.checkAnswer(query, expected, checkToRDD = false)
  }

  test("compact native reads preserve projection, residuals, null partitions and reuse") {
    withFixture() {
      name =>
        val query = read(name).where("id >= 2 AND id < 6").select("data", "id")
        val compact = scan(query)
        val stats = compact.estimateStatistics()
        val partitions = compact.nativeBatch().planInputPartitions()
        assert(partitions.nonEmpty && compact.usesCompactPlanning)
        assertNoTaskCaches(compact)
        val again = compact.nativeBatch().planInputPartitions()
        assert(partitions.zip(again).forall { case (a, b) => a eq b })
        assert(compact.estimateStatistics().numRows() == stats.numRows())
        checkResult(query, Seq(Row(null, 2), Row("c", 3), Row("d", 4), Row("e", 5)))
        checkGlutenPlanCount[IcebergScanTransformer](query, 1)
        assert(compact.usesCompactPlanning)
        assertNoTaskCaches(compact)
        checkResult(query, Seq(Row(null, 2), Row("c", 3), Row("d", 4), Row("e", 5)))
        assertNoTaskCaches(compact)
    }
  }

  test("compact unpartitioned scans and empty filtered scans execute") {
    withFixture(partitioned = false) {
      name =>
        val query = read(name).where("id >= 4").select("id")
        val compact = scan(query)
        checkResult(query, Seq(Row(4), Row(5), Row(6)))
        assert(compact.usesCompactPlanning)
        assertNoTaskCaches(compact)
        checkResult(read(name).where("id > 100"), Seq.empty)
    }
  }

  test("prepared native batches release Iceberg file tasks before execution") {
    withFixture() {
      name =>
        val table = Spark3Util.loadIcebergTable(spark, name)
        val snapshot = table.currentSnapshot()
        val options = new java.util.HashMap[String, String]()
        options.put("snapshot-id", snapshot.snapshotId().toString)
        options.put("split-size", "1048576")
        val conf = new org.apache.iceberg.spark.SparkReadConf(spark, table, null, options)
        val references = ArrayBuffer.empty[WeakReference[org.apache.iceberg.FileScanTask]]
        val underlying = org.mockito.Mockito.mock(classOf[org.apache.iceberg.BatchScan])
        org.mockito.Mockito.when(underlying.snapshot()).thenReturn(snapshot)
        org.mockito.Mockito.when(underlying.targetSplitSize()).thenReturn(1048576L)
        org.mockito.Mockito.when(underlying.splitLookback()).thenReturn(1)
        org.mockito.Mockito.when(underlying.splitOpenFileCost()).thenReturn(4194304L)
        org.mockito.Mockito.when(underlying.planFiles()).thenAnswer(
          _ =>
            org.apache.iceberg.io.CloseableIterable.transform[
              org.apache.iceberg.FileScanTask,
              org.apache.iceberg.ScanTask](
              table.newScan().useSnapshot(snapshot.snapshotId()).planFiles(),
              task => {
                references += new WeakReference(task)
                task
              }))
        val original = new SparkBatchQueryScan(
          spark,
          table,
          underlying,
          conf,
          table.schema(),
          java.util.Collections.singletonList(
            org.apache.iceberg.expressions.Expressions.alwaysTrue()),
          () => null)
        val compact = CompactIcebergBatchScan.wrap(spark, original, options)
          .asInstanceOf[CompactIcebergBatchScan]
        val batch = compact.nativeBatch()
        val partitions = batch.planInputPartitions()
        assert(partitions.nonEmpty && references.nonEmpty)
        assertNoTaskCaches(compact)
        eventually(timeout(Span(10, Seconds))) {
          System.gc()
          assert(references.forall(_.get() == null), "Prepared scan retained Iceberg file tasks")
        }
        assert(compact.estimateStatistics().numRows().getAsLong == 6)
        assert(partitions.zip(batch.planInputPartitions()).forall { case (a, b) => a eq b })
        val files = partitions.map {
          case partition: CompactIcebergInputPartition =>
            assert(partition.split.toProtobuf.getItemsCount == partition.fileCount)
            partition.fileCount
        }.sum
        assert(files >= references.size)
        org.mockito.Mockito.verify(underlying, org.mockito.Mockito.times(1)).planFiles()
    }
  }

  test("compact scan pins the selected snapshot before a concurrent append") {
    withFixture() {
      name =>
        val query = read(name).where("id > 0").select("id")
        val compact = scan(query)
        val rows = compact.estimateStatistics().numRows()
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          sql(s"INSERT INTO $name VALUES (7, 30, 'g')")
        }
        checkResult(query, (1 to 6).map(Row(_)))
        assert(compact.estimateStatistics().numRows() == rows)
        assertNoTaskCaches(compact)
        checkResult(read(name).select("id"), (1 to 7).map(Row(_)))
    }
  }

  test("explicit snapshot and as-of reads keep the selected data") {
    withFixture() {
      name =>
        val table = Spark3Util.loadIcebergTable(spark, name)
        val first = table.snapshot(table.currentSnapshot().parentId())
        Seq(
          "snapshot-id" -> first.snapshotId().toString,
          "as-of-timestamp" -> first.timestampMillis().toString).foreach {
          case (option, value) =>
            val query = spark.read.option("split-size", "1048576")
              .option(option, value).table(name).select("id", "p")
            val compact = scan(query)
            checkResult(query, Seq(Row(1, 10), Row(2, 10), Row(3, 20)))
            assertNoTaskCaches(compact)
        }
        val query = read(name).where("p IS NULL OR p = 10").select("id", "p")
        val compact = scan(query)
        checkResult(query, Seq(Row(1, 10), Row(2, 10), Row(4, 10), Row(5, null)))
        assertNoTaskCaches(compact)
    }
  }

  test("compact statistics preserve the snapshot summary fast path") {
    withFixture() {
      name =>
        val table = new SparkTable(Spark3Util.loadIcebergTable(spark, name), false)
        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Collections.singletonMap("split-size", "1048576"))
        val compact = new CompactIcebergTable(spark, table).newScanBuilder(options)
          .build().asInstanceOf[CompactIcebergBatchScan]
        assert(compact.estimateStatistics().numRows().getAsLong == 6)
        assertNoTaskCaches(compact)
        // No task planning is needed to report this estimate.
        val resultField =
          classOf[CompactIcebergBatchScan].getDeclaredFields.find(_.getName == "result").get
        resultField.setAccessible(true)
        assert(resultField.get(compact) == None)
    }
  }

  test("physical fallback after compact statistics uses the ordinary Iceberg reader") {
    withFixture() {
      name =>
        withSQLConf(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false") {
          val query = read(name).where("id > 3").select("id")
          val compact = scan(query)
          compact.estimateStatistics()
          checkResult(query, Seq(Row(4), Row(5), Row(6)))
          checkGlutenPlanCount[IcebergScanTransformer](query, 0)
          assert(
            compact.toBatch().planInputPartitions().forall(_.isInstanceOf[SparkInputPartition]))
        }
    }
  }

  test("delete files and non-Parquet files use existing planning") {
    withFixture() {
      name =>
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          sql(s"DELETE FROM $name WHERE id = 2")
        }
        val query = read(name).select("id")
        val compact = scan(query)
        assert(!compact.usesCompactPlanning)
        checkResult(query, Seq(1, 3, 4, 5, 6).map(Row(_)))
    }
    withFixture(format = "orc") {
      name =>
        val query = read(name).select("id")
        assert(!scan(query).usesCompactPlanning)
        checkResult(query, (1 to 6).map(Row(_)))
    }
  }

  test("partition coalescing uses existing planning") {
    withFixture() {
      name =>
        withSQLConf(GlutenConfig.COLUMNAR_BATCHSCAN_MAX_INPUT_PARTITIONS.key -> "1") {
          val query = read(name).select("id")
          assert(!scan(query).usesCompactPlanning)
          checkResult(query, (1 to 6).map(Row(_)))
        }
    }
  }

  test("disabled option, metadata expressions, joins, CBO and schema evolution stay unchanged") {
    withFixture() {
      name =>
        def assertOrdinary(query: DataFrame): Unit = {
          assert(!query.queryExecution.optimizedPlan.exists {
            case relation: DataSourceV2ScanRelation =>
              relation.scan.isInstanceOf[CompactIcebergBatchScan]
            case _ => false
          })
          query.collect()
        }
        withSQLConf(GlutenIcebergConfig.EARLY_TASK_ENCODING_ENABLED.key -> "false") {
          assertOrdinary(read(name).select("id"))
        }
        assertOrdinary(read(name).selectExpr("id", "input_file_name()"))
        assertOrdinary(read(name).as("a").join(read(name).as("b"), "id"))
        withSQLConf("spark.sql.cbo.enabled" -> "true") {
          assertOrdinary(read(name).select("id"))
        }
        val table = Spark3Util.loadIcebergTable(spark, name)
        table.updateSchema().renameColumn("data", "renamed").commit()
        spark.catalog.refreshTable(name)
        assertOrdinary(read(name).select("id", "renamed"))
    }
  }

  test("fallback produces safe rows when Iceberg vectorization is disabled") {
    withFixture() {
      name =>
        withSQLConf(
          GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false",
          "spark.sql.iceberg.vectorization.enabled" -> "false") {
          val query = read(name).where("id > 0").select("id")
          scan(query).estimateStatistics()
          checkResult(query, (1 to 6).map(Row(_)))
          checkGlutenPlanCount[IcebergScanTransformer](query, 0)
        }
    }
  }

  test("a deferred scan remains executable if Gluten is disabled after planning") {
    withFixture() {
      name =>
        val query = read(name).where("id > 0").select("id")
        val compact = scan(query)
        val physical = query.queryExecution.sparkPlan
        assert(physical.exists(_.isInstanceOf[
          org.apache.spark.sql.execution.datasources.v2.DeferredIcebergScanExec]))
        assertNoTaskCaches(compact)
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          checkResult(query, (1 to 6).map(Row(_)))
        }
    }
  }

  test("AQE and whole-query fallback preserve results") {
    withFixture() {
      name =>
        withSQLConf("spark.sql.adaptive.enabled" -> "true") {
          val query = read(name).where("id % 2 = 0").groupBy("p").sum("id")
          val compact = scan(query)
          checkResult(query, Seq(Row(10, 6L), Row(20, 6L)))
          assertNoTaskCaches(compact)
        }
        withSQLConf(
          GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "0",
          GlutenConfig.COLUMNAR_FALLBACK_PREFER_COLUMNAR.key -> "false") {
          val query = read(name).where("id > 0").select("id")
          scan(query).estimateStatistics()
          checkResult(query, (1 to 6).map(Row(_)))
          checkGlutenPlanCount[IcebergScanTransformer](query, 0)
        }
    }
  }

  test("task retries reuse encoded work") {
    withFixture() {
      name =>
        val query = read(name).where("id > 0").select("id")
        val compact = scan(query)
        val partitions = compact.nativeBatch().planInputPartitions()
        val result = query.rdd.mapPartitions {
          rows =>
            if (TaskContext.get().attemptNumber() == 0) {
              throw new IllegalStateException("Injected first-attempt failure")
            }
            rows
        }.collect().map(_.getInt(0)).sorted
        assert(result.toSeq == (1 to 6))
        assertNoTaskCaches(compact)
        assert(partitions.zip(compact.nativeBatch().planInputPartitions()).forall {
          case (a, b) => a eq b
        })
    }
  }

  test("planning failures close resources and do not publish partial work") {
    withFixture() {
      name =>
        val table = Spark3Util.loadIcebergTable(spark, name)
        val snapshot = table.currentSnapshot()
        val options = new java.util.HashMap[String, String]()
        options.put("snapshot-id", snapshot.snapshotId().toString)
        options.put("split-size", "1048576")
        val readConf = new org.apache.iceberg.spark.SparkReadConf(spark, table, null, options)
        val calls = new java.util.concurrent.atomic.AtomicInteger()
        val closes = new java.util.concurrent.atomic.AtomicInteger()
        val underlying = org.mockito.Mockito.mock(classOf[org.apache.iceberg.BatchScan])
        org.mockito.Mockito.when(underlying.snapshot()).thenReturn(snapshot)
        org.mockito.Mockito.when(underlying.targetSplitSize()).thenReturn(1048576L)
        org.mockito.Mockito.when(underlying.splitLookback()).thenReturn(1)
        org.mockito.Mockito.when(underlying.splitOpenFileCost()).thenReturn(4194304L)
        org.mockito.Mockito.when(underlying.planFiles()).thenAnswer(
          _ => {
            val attempt = calls.incrementAndGet()
            val raw = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()
            new org.apache.iceberg.io.CloseableIterable[org.apache.iceberg.FileScanTask] {
              override def iterator(): org.apache.iceberg.io.CloseableIterator[
                org.apache.iceberg.FileScanTask] = {
                val iter = raw.iterator()
                new org.apache.iceberg.io.CloseableIterator[org.apache.iceberg.FileScanTask] {
                  private var count = 0
                  override def hasNext: Boolean = iter.hasNext
                  override def next(): org.apache.iceberg.FileScanTask = {
                    count += 1
                    if (attempt == 1 && count == 3) {
                      throw new java.io.IOException("Injected manifest read failure")
                    }
                    iter.next()
                  }
                  override def close(): Unit = iter.close()
                }
              }
              override def close(): Unit = {
                closes.incrementAndGet()
                raw.close()
              }
            }
          })
        val original = new SparkBatchQueryScan(
          spark,
          table,
          underlying,
          readConf,
          table.schema(),
          java.util.Collections.singletonList(
            org.apache.iceberg.expressions.Expressions.alwaysTrue()),
          () => null
        )
        val compact = CompactIcebergBatchScan.wrap(spark, original, options)
          .asInstanceOf[CompactIcebergBatchScan]
        Thread.currentThread().interrupt()
        try {
          intercept[InterruptedException](compact.nativeBatch())
        } finally {
          Thread.interrupted()
        }
        assert(calls.get() == 0)
        val failure = intercept[java.io.UncheckedIOException](compact.nativeBatch())
        assert(failure.getCause.getMessage == "Injected manifest read failure")
        assert(closes.get() == 1)
        assertNoTaskCaches(compact)
        assert(compact.nativeBatch().planInputPartitions().nonEmpty)
        assert(calls.get() == 2 && closes.get() == 2)
        compact.estimateStatistics()
        compact.nativeBatch()
        assert(calls.get() == 2)
        assertNoTaskCaches(compact)
    }
  }
}
