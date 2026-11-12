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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig, VeloxLanceConfig}

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, QueryExecution, RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.gluten.LanceTestUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MapType, Metadata, MetadataBuilder, StructType, TimestampType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.task.TaskResources

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BitVector, FieldVector}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.lance.WriteParams
import org.lance.spark.utils.BlobUtils

import java.io.File
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Exercises the Lance write offload: writes to Lance tables run through [[LanceWriteExec]], whose
 * executors stream Velox batches to Lance over an Arrow C stream, while writes Velox cannot serve
 * fall back to lance-spark's row writer.
 */
class VeloxLanceWriteSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.ansi.enabled", "false")
      .set("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
      .set(VeloxLanceConfig.ENABLE_NATIVE_WRITE.key, "true")
  }

  /** Rows `start until start + n` across the column types the offload supports, with nulls. */
  private def rows(start: Long, n: Long, partitions: Int = 1): DataFrame =
    spark
      .range(start, start + n, 1, partitions)
      .selectExpr(
        "cast(id as int) as id",
        "id * 3 as l",
        "cast(id as double) / 2 as d",
        "case when id % 7 = 0 then null else concat('s', cast(id as string)) end as s",
        "cast(id as decimal(10, 2)) as dec",
        "date_add(date'2020-01-01', cast(id as int)) as dt",
        "timestamp_seconds(id * 1000) as ts",
        "id % 2 = 0 as b",
        "cast(concat('b', cast(id as string)) as binary) as bin"
      )

  private def read(uri: String): DataFrame = spark.read.format("lance").option("path", uri).load()

  private def withLanceTable(initial: DataFrame)(f: String => Unit): Unit = {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/t.lance"
        initial.write.format("lance").option("path", uri).save()
        f(uri)
    }
  }

  private val catalogs = new AtomicLong()

  /** Runs `f` with a Lance catalog over a temporary directory, passing the catalog's name. */
  private def withLanceCatalog(f: String => Unit): Unit = {
    withTempDir {
      dir =>
        // Spark caches a catalog by name, so each directory gets its own.
        val catalog = s"lance_write_${catalogs.incrementAndGet()}"
        withSQLConf(
          s"spark.sql.catalog.$catalog" -> "org.lance.spark.LanceNamespaceSparkCatalog",
          s"spark.sql.catalog.$catalog.impl" -> "dir",
          s"spark.sql.catalog.$catalog.root" -> dir.getCanonicalPath
        ) {
          spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.default")
          f(catalog)
        }
    }
  }

  private def sqlRows(start: Int, end: Int): String =
    s"SELECT cast(id as int) as id, concat('s', cast(id as string)) as s FROM range($start, $end)"

  private def expectedSqlRows(start: Int, end: Int): Seq[Row] =
    (start until end).map(i => Row(i, s"s$i"))

  /** Runs `f` and checks that its tasks released all the Arrow memory they allocated. */
  private def assertNoArrowLeak(f: => Unit): Unit = {
    val before = TaskResources.ACCUMULATED_LEAK_BYTES.get()
    f
    LanceTestUtils.waitForListeners(spark)
    assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == before, "Arrow memory leaked")
  }

  /** Whether `e` or one of its causes has a message containing `text`. */
  private def causedBy(e: Throwable, text: String): Boolean =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).exists {
      t => String.valueOf(t.getMessage).contains(text)
    }

  /** Runs `action` and returns the executed plans of the queries it ran. */
  private def executedPlans(action: => Unit): Seq[SparkPlan] = {
    val plans = ArrayBuffer[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        plans.synchronized(plans += qe.executedPlan)
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      action
      LanceTestUtils.waitForListeners(spark)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.synchronized(plans.toList)
  }

  /** The `major.minor` file format version of every data file in the table. */
  private def fileFormatVersions(uri: String): Seq[String] = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val dataset = org.lance.Dataset.open(uri, allocator)
      try {
        for {
          fragment <- dataset.getFragments.asScala.toSeq
          file <- fragment.metadata().getFiles.asScala
        } yield s"${file.getFileMajorVersion}.${file.getFileMinorVersion}"
      } finally dataset.close()
    } finally allocator.close()
  }

  private def assertOffloaded[T <: LanceWriteExec: scala.reflect.ClassTag](plans: Seq[SparkPlan])
      : Unit = {
    val cls = implicitly[scala.reflect.ClassTag[T]].runtimeClass
    assert(plans.exists(_.exists(cls.isInstance(_))), s"Expected ${cls.getSimpleName} in:\n$plans")
  }

  /** The row-to-columnar and columnar-to-row conversions in `plans`, adaptive plans included. */
  private def rowConversions(plans: Seq[SparkPlan]): Seq[SparkPlan] = {
    def nodes(plan: SparkPlan): Seq[SparkPlan] = plan +: (plan match {
      case adaptive: AdaptiveSparkPlanExec => nodes(adaptive.executedPlan)
      case stage: QueryStageExec => nodes(stage.plan)
      case other => other.children.flatMap(nodes)
    })
    plans.flatMap(nodes).filter {
      case _: ColumnarToRowTransition | _: RowToColumnarTransition => true
      case _ => false
    }
  }

  private def assertFellBack(plans: Seq[SparkPlan]): Unit = {
    assert(!plans.exists(_.exists(_.isInstanceOf[LanceWriteExec])), s"Unexpected offload:\n$plans")
    assert(
      plans.exists(_.exists {
        case _: AppendDataExec | _: OverwriteByExpressionExec => true
        case _ => false
      }),
      s"Expected a vanilla write in:\n$plans")
  }

  test("append offloads to Velox and keeps every value") {
    withLanceTable(rows(0, 50)) {
      uri =>
        val plans = executedPlans {
          assertNoArrowLeak {
            rows(50, 50).write.format("lance").mode("append").option("path", uri).save()
          }
        }
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        checkAnswer(read(uri), rows(0, 100))
    }
  }

  test("saving a new table through its path writes it natively") {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/t.lance"
        // Spark creates the table through lance-spark's staging catalog, then appends to it.
        val plans = executedPlans(rows(0, 30).write.format("lance").option("path", uri).save())
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        checkAnswer(read(uri), rows(0, 30))
    }
  }

  test("overwrite offloads to Velox and replaces the table") {
    withLanceTable(rows(0, 50)) {
      uri =>
        val plans = executedPlans {
          rows(100, 30).write.format("lance").mode("overwrite").option("path", uri).save()
        }
        assertOffloaded[VeloxLanceOverwriteByExpressionExec](plans)
        checkAnswer(read(uri), rows(100, 30))
    }
  }

  test("copies of a write node keep its batch write") {
    // SparkWrite.toBatch opens the table and pins its version on every call, and the planner copies
    // the node after validating it, so a copy must keep the batch write rather than build its own.
    withLanceTable(rows(0, 10)) {
      uri =>
        val plans = executedPlans {
          rows(10, 10).write.format("lance").mode("append").option("path", uri).save()
          rows(20, 10).write.format("lance").mode("overwrite").option("path", uri).save()
        }
        val nodes = plans.flatMap(_.collect { case w: LanceWriteExec => w })
        assert(nodes.exists(_.isInstanceOf[VeloxLanceAppendDataExec]), plans)
        assert(nodes.exists(_.isInstanceOf[VeloxLanceOverwriteByExpressionExec]), plans)
        nodes.foreach {
          node =>
            val copy = node.withNewQuery(node.query).asInstanceOf[LanceWriteExec]
            assert(copy ne node)
            assert(copy.batchWrite eq node.batchWrite)
        }
        checkAnswer(read(uri), rows(20, 10))
    }
  }

  test("write_mode=overwrite turns an offloaded append into an overwrite") {
    withLanceTable(rows(0, 50)) {
      uri =>
        val plans = executedPlans {
          rows(200, 20).write
            .format("lance")
            .mode("append")
            .option("write_mode", "overwrite")
            .option("path", uri)
            .save()
        }
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        checkAnswer(read(uri), rows(200, 20))
    }
  }

  test("several tasks and several batches per task") {
    withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "8") {
      withLanceTable(rows(0, 10)) {
        uri =>
          val plans = executedPlans {
            rows(10, 200, partitions = 4).write
              .format("lance")
              .mode("append")
              .option("path", uri)
              .save()
          }
          assertOffloaded[VeloxLanceAppendDataExec](plans)
          checkAnswer(read(uri), rows(0, 210))
      }
    }
  }

  test("an empty append behaves like lance-spark's own writer") {
    withLanceTable(rows(0, 20)) {
      uri =>
        def emptyAppend(): Option[String] =
          try {
            rows(0, 0).write.format("lance").mode("append").option("path", uri).save()
            None
          } catch {
            case e: Exception =>
              var cause: Throwable = e
              while (cause.getCause != null) cause = cause.getCause
              Some(cause.getMessage)
          }
        var vanilla: Option[String] = None
        withSQLConf(VeloxLanceConfig.ENABLE_NATIVE_WRITE.key -> "false") {
          vanilla = emptyAppend()
        }
        var native: Option[String] = None
        val plans = executedPlans { native = emptyAppend() }
        assert(native == vanilla, s"native: $native, vanilla: $vanilla")
        if (native.isEmpty) {
          assertOffloaded[VeloxLanceAppendDataExec](plans)
        }
        checkAnswer(read(uri), rows(0, 20))
    }
  }

  test("append to a table whose field ids are not contiguous") {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/t.lance"
        spark
          .range(0, 10)
          .selectExpr("cast(id as int) as a", "cast(id as int) as b", "cast(id * 10 as int) as c")
          .write
          .format("lance")
          .option("path", uri)
          .save()
        val allocator = new RootAllocator(Long.MaxValue)
        try {
          val dataset = org.lance.Dataset.open(uri, allocator)
          try dataset.dropColumns(Collections.singletonList("b"))
          finally dataset.close()
        } finally {
          allocator.close()
        }

        val plans = executedPlans {
          spark
            .range(10, 20)
            .selectExpr("cast(id as int) as a", "cast(id * 10 as int) as c")
            .write
            .format("lance")
            .mode("append")
            .option("path", uri)
            .save()
        }
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        checkAnswer(read(uri), (0 until 20).map(i => Row(i, i * 10)))
    }
  }

  test("nested struct and array columns round trip") {
    def nested(start: Long, n: Long): DataFrame =
      spark
        .range(start, start + n)
        .selectExpr(
          "cast(id as int) as id",
          "case when id % 6 = 0 then null " +
            "else named_struct('x', cast(id as int), 'y', concat('v', cast(id as string))) " +
            "end as st",
          "case when id % 5 = 0 then null " +
            "else array(cast(id as int), null, cast(id + 1 as int)) end as arr",
          "array(named_struct('k', cast(id as string), 'v', id * 2)) as arr_of_st"
        )
    withLanceTable(nested(0, 30)) {
      uri =>
        val plans = executedPlans {
          nested(30, 30).write.format("lance").mode("append").option("path", uri).save()
        }
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        checkAnswer(read(uri), nested(0, 60))
    }
  }

  test("appended fragments keep the table's pinned file format version") {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/t.lance"
        // 2.1 is older than Lance's default, so only the spec's resolved version writes 2.1 files.
        rows(0, 20).write
          .format("lance")
          .option("file_format_version", "2.1")
          .option("path", uri)
          .save()
        val plans = executedPlans {
          rows(20, 50).write
            .format("lance")
            .mode("append")
            .option("max_row_per_file", "10")
            .option("path", uri)
            .save()
        }
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        val versions = fileFormatVersions(uri)
        // The 20 initial rows plus at least five 10-row files from the append.
        assert(versions.size >= 6, s"Expected the append to roll files: $versions")
        assert(versions.forall(_ == "2.1"), s"Expected only 2.1 files: $versions")
        checkAnswer(read(uri), rows(0, 70))
    }
  }

  test("a failed write task leaves the table unchanged and no files behind") {
    withLanceTable(rows(0, 50)) {
      uri =>
        val dataFiles = new File(uri, "data").list().toSet
        // The range makes batches of 1,000 rows and the writer queues at most 4, so Lance has
        // finished several 1,000-row files before the row with id 19000 fails the task.
        val failing = rows(50, 20000).where("assert_true(id < 19000) is null")
        assertNoArrowLeak {
          val e = intercept[Exception] {
            failing.write
              .format("lance")
              .mode("append")
              .option("path", uri)
              .option("max_row_per_file", "1000")
              .save()
          }
          assert(causedBy(e, "is not true"), e)
        }
        checkAnswer(read(uri), rows(0, 50))
        // Aborting fails the stream, so Lance deletes the files it wrote, finished or not.
        // lance-spark's writer ends its stream instead, and leaves those files.
        assert(new File(uri, "data").list().toSet == dataFiles)
        // The table still takes writes.
        rows(50, 10).write.format("lance").mode("append").option("path", uri).save()
        checkAnswer(read(uri), rows(0, 60))
    }
  }

  test("reports rows, files and bytes written") {
    withLanceTable(rows(0, 10)) {
      uri =>
        val stageRecords = new AtomicLong()
        val stageBytes = new AtomicLong()
        val listener = new SparkListener {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
            Option(taskEnd.taskMetrics).foreach {
              metrics =>
                stageRecords.addAndGet(metrics.outputMetrics.recordsWritten)
                stageBytes.addAndGet(metrics.outputMetrics.bytesWritten)
            }
          }
        }
        LanceTestUtils.waitForListeners(spark)
        spark.sparkContext.addSparkListener(listener)
        val plans =
          try {
            executedPlans {
              rows(10, 100, partitions = 2).write
                .format("lance")
                .mode("append")
                .option("path", uri)
                .save()
            }
          } finally {
            spark.sparkContext.removeSparkListener(listener)
          }
        val node = plans.flatMap(_.collect { case w: VeloxLanceAppendDataExec => w }).head
        assert(node.metrics("recordsWritten").value == 100)
        assert(node.metrics("numWrittenFiles").value >= 2)
        assert(node.metrics("numWrittenBytes").value > 0)
        assert(stageRecords.get == 100)
        assert(stageBytes.get == node.metrics("numWrittenBytes").value)
    }
  }

  test("time of write leaves out the time the query takes to produce batches") {
    withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "1") {
      withLanceTable(rows(0, 10)) {
        uri =>
          // Each row sleeps for 500 ms in a JVM expression, and each batch holds one row, so the
          // stream waits about 2 seconds for the batches after the first.
          val slow = rows(10, 5).where(
            "reflect('java.lang.Thread', 'sleep', cast(id * 0 + 500 as bigint)) = 'null'")
          val plans = executedPlans {
            slow.write.format("lance").mode("append").option("path", uri).save()
          }
          val node = plans.flatMap(_.collect { case w: VeloxLanceAppendDataExec => w }).head
          val writeNs = node.metrics("writeWallNs").value
          assert(writeNs > 0 && writeNs < TimeUnit.SECONDS.toNanos(1), writeNs)
          checkAnswer(read(uri), rows(0, 15))
      }
    }
  }

  test("SQL INSERT INTO and INSERT OVERWRITE through a Lance catalog") {
    withLanceCatalog {
      catalog =>
        val table = s"$catalog.default.t"
        spark.sql(s"CREATE TABLE $table (id INT, s STRING) USING lance")

        val inserted = executedPlans(spark.sql(s"INSERT INTO $table ${sqlRows(0, 10)}"))
        assertOffloaded[VeloxLanceAppendDataExec](inserted)
        checkAnswer(spark.table(table), expectedSqlRows(0, 10))

        val overwritten = executedPlans(spark.sql(s"INSERT OVERWRITE $table ${sqlRows(5, 8)}"))
        assertOffloaded[VeloxLanceOverwriteByExpressionExec](overwritten)
        checkAnswer(spark.table(table), expectedSqlRows(5, 8))
    }
  }

  test("CREATE TABLE AS SELECT and REPLACE TABLE AS SELECT through a Lance catalog") {
    withLanceCatalog {
      catalog =>
        val table = s"$catalog.default.ctas"
        // Spark 3.5 and later write a staged table through a nested append or, for a replace on
        // Spark 4.x, a nested overwrite. Either is offloaded. Spark 3.4 writes it directly, so it
        // stays on lance-spark's writer.
        def assertNestedWriteOffloaded(plans: Seq[SparkPlan]): Unit =
          if (isSparkVersionGE("3.5")) assertOffloaded[LanceWriteExec](plans)
        val created =
          executedPlans(spark.sql(s"CREATE TABLE $table USING lance AS ${sqlRows(0, 10)}"))
        assertNestedWriteOffloaded(created)
        checkAnswer(spark.table(table), expectedSqlRows(0, 10))

        val replaced = executedPlans {
          spark.sql(
            s"REPLACE TABLE $table USING lance AS " +
              "SELECT id as k, cast(id as double) / 4 as d FROM range(0, 6)")
        }
        assertNestedWriteOffloaded(replaced)
        checkAnswer(spark.table(table), (0 until 6).map(i => Row(i.toLong, i / 4.0)))

        // A failed write aborts the staged table, so it is never created.
        val failed = s"$catalog.default.failed"
        val e = intercept[Exception] {
          spark.sql(
            s"CREATE TABLE $failed USING lance AS " +
              s"${sqlRows(0, 100)} WHERE assert_true(id < 50) IS NULL")
        }
        assert(causedBy(e, "is not true"), e)
        val tables = spark.sql(s"SHOW TABLES IN $catalog.default").collect().map(_.getString(1))
        assert(!tables.contains("failed"), tables.mkString(", "))
    }
  }

  test("map columns round trip") {
    def maps(start: Long, n: Long): DataFrame =
      spark
        .range(start, start + n)
        .selectExpr(
          "cast(id as int) as id",
          "case when id % 4 = 0 then null " +
            "else map(concat('k', cast(id as string)), cast(id as int), 'z', null) end as m")
    withLanceTable(maps(0, 20)) {
      uri =>
        val plans = executedPlans {
          maps(20, 20).write.format("lance").mode("append").option("path", uri).save()
        }
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        checkAnswer(read(uri), maps(0, 40))
    }
  }

  /** How many null slots of each fixed-width top-level column hold a non-zero value on disk. */
  private def nonZeroNullSlots(uri: String): Map[String, Int] = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val dataset = org.lance.Dataset.open(uri, allocator)
      try {
        val scanner = dataset.newScan()
        try {
          val reader = scanner.scanBatches()
          try {
            val counts = scala.collection.mutable.Map[String, Int]()
            // A value in a row where an enclosing struct is null counts as a null slot too.
            def count(v: FieldVector, name: String, parentNull: Int => Boolean): Unit = {
              val isNull = (row: Int) => v.isNull(row) || parentNull(row)
              v match {
                case fixed: BaseFixedWidthVector if !fixed.isInstanceOf[BitVector] =>
                  val width = fixed.getTypeWidth
                  val data = fixed.getDataBuffer
                  (0 until fixed.getValueCount).foreach {
                    row =>
                      if (
                        isNull(row) &&
                        (0 until width).exists(i => data.getByte(row.toLong * width + i) != 0)
                      ) {
                        counts(name) = counts.getOrElse(name, 0) + 1
                      }
                  }
                case _ =>
              }
              val childParentNull: Int => Boolean = v match {
                case _: StructVector => isNull
                case _ => _ => false
              }
              v.getChildrenFromFields.asScala.foreach {
                child => count(child, s"$name.${child.getName}", childParentNull)
              }
            }
            while (reader.loadNextBatch()) {
              reader.getVectorSchemaRoot.getFieldVectors.asScala.foreach {
                v => count(v, v.getName, _ => false)
              }
            }
            counts.toMap
          } finally reader.close()
        } finally scanner.close()
      } finally dataset.close()
    } finally allocator.close()
  }

  test("every offloaded type keeps its values and nulls when written from a Parquet scan") {
    def nullEvery(n: Int, expr: String, name: String): String =
      s"case when id % $n = 0 then null else $expr end as $name"
    withTempPath {
      dir =>
        val parquet = s"${dir.getCanonicalPath}/input"
        spark
          .range(0, 3000, 1, 2)
          .selectExpr(
            "cast(id as int) as id",
            nullEvery(2, "cast(id % 100 as tinyint)", "i8"),
            nullEvery(3, "cast(id % 1000 as smallint)", "i16"),
            nullEvery(5, "cast(id % 100 + 1 as int)", "i32"),
            nullEvery(7, "id * 1000003", "i64"),
            nullEvery(4, "cast(id as float) / 3", "f32"),
            nullEvery(6, "cast(id as double) / 7", "f64"),
            nullEvery(8, "cast(id % 50 as decimal(7, 2))", "dec"),
            nullEvery(9, "cast(id * 1234567890123.0123456789 - 5e14 as decimal(38, 10))", "wide"),
            nullEvery(10, "date_add(date'2020-01-01', cast(id as int))", "dt"),
            nullEvery(11, "timestamp_seconds(id * 1000)", "ts"),
            nullEvery(12, "id % 3 = 0", "b"),
            nullEvery(13, "concat('s', cast(id as string))", "s"),
            nullEvery(14, "cast(concat('b', cast(id as string)) as binary)", "bin"),
            nullEvery(
              15,
              "named_struct('x', case when id % 4 = 0 then null else cast(id as int) end, " +
                "'y', cast(id as string))",
              "st"),
            nullEvery(16, "array(cast(id as int), null)", "arr"),
            nullEvery(17, "map('k', cast(id as int))", "m")
          )
          .write
          .parquet(parquet)
        val uri = s"${dir.getCanonicalPath}/t.lance"
        // Small batches, so the scan reuses its buffers and leaves old values under nulls.
        withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "256") {
          val plans = executedPlans {
            assertNoArrowLeak {
              spark.read.parquet(parquet).write.format("lance").option("path", uri).save()
            }
          }
          assertOffloaded[VeloxLanceAppendDataExec](plans)
        }
        checkAnswer(read(uri), spark.read.parquet(parquet))
        // Lance would otherwise encode those values, which grows the files.
        assert(nonZeroNullSlots(uri).isEmpty)
    }
  }

  test("a write from a native query converts no batches to rows") {
    withTempPath {
      dir =>
        val parquet = s"${dir.getCanonicalPath}/input"
        rows(0, 2000, partitions = 2).write.parquet(parquet)
        def query: DataFrame =
          spark.read.parquet(parquet).where("id % 3 <> 0").selectExpr("id", "l + 1 as l", "s", "ts")
        val uri = s"${dir.getCanonicalPath}/native.lance"
        val plans = executedPlans(query.write.format("lance").option("path", uri).save())
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        assert(rowConversions(plans).isEmpty, plans)
        checkAnswer(read(uri), query)
        // lance-spark's own writer takes rows, so the same write converts Velox's batches.
        withSQLConf(VeloxLanceConfig.ENABLE_NATIVE_WRITE.key -> "false") {
          val rowWriterPlans = executedPlans {
            query.write.format("lance").option("path", s"${dir.getCanonicalPath}/rows.lance").save()
          }
          assertFellBack(rowWriterPlans)
          assert(rowConversions(rowWriterPlans).nonEmpty, rowWriterPlans)
        }
    }
  }

  test("timestamps keep their instants in a session time zone other than UTC") {
    // Half-hourly instants across the 2024 spring-forward change in Los Angeles.
    def times(start: Long, n: Long): DataFrame =
      spark
        .range(start, start + n)
        .selectExpr(
          "cast(id as int) as id",
          "case when id % 5 = 0 then null else timestamp_seconds(1710054000 + id * 1800) end as ts")
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      withLanceTable(times(0, 10)) {
        uri =>
          val plans = executedPlans {
            times(10, 40).write.format("lance").mode("append").option("path", uri).save()
          }
          assertOffloaded[VeloxLanceAppendDataExec](plans)
          checkAnswer(
            read(uri).selectExpr("id", "unix_micros(ts)"),
            times(0, 50).selectExpr("id", "unix_micros(ts)"))
      }
    }
  }

  test("TimestampNTZ columns fall back by default and keep their local times either way") {
    def local(start: Long, n: Long): DataFrame =
      spark
        .range(start, start + n)
        .selectExpr(
          "cast(id as int) as id",
          "case when id % 5 = 0 then null " +
            "else cast(timestamp_seconds(1710054000 + id * 1800) as timestamp_ntz) end as ntz")
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      withLanceTable(local(0, 10)) {
        uri =>
          // Gluten keeps plans with TimestampNTZ on Spark unless its NTZ validation is off.
          val plans = executedPlans {
            local(10, 20).write.format("lance").mode("append").option("path", uri).save()
          }
          assertFellBack(plans)
          withSQLConf(VeloxConfig.ENABLE_TIMESTAMP_NTZ_VALIDATION.key -> "false") {
            local(30, 20).write.format("lance").mode("append").option("path", uri).save()
          }
          checkAnswer(
            read(uri).selectExpr("id", "cast(ntz as string)"),
            local(0, 50).selectExpr("id", "cast(ntz as string)"))
      }
    }
  }

  test("timestamps inside a struct that may be null fall back to lance-spark's writer") {
    // Velox would convert the timestamps of the struct's null rows too, where a CASE WHEN or a join
    // may leave any value. The row writer reads through ColumnarToRow, which skips them.
    def nullable(start: Long, n: Long): DataFrame =
      spark
        .range(start, start + n)
        .selectExpr(
          "cast(id as int) as id",
          "case when id % 3 = 0 then null " +
            "else named_struct('t', timestamp_seconds(id * 37), 'v', id) end as st")
    withLanceTable(nullable(0, 10)) {
      uri =>
        val plans = executedPlans {
          nullable(10, 50).write.format("lance").mode("append").option("path", uri).save()
        }
        assertFellBack(plans)
        checkAnswer(read(uri), nullable(0, 60))
    }
    // Structs that are never null, such as map entries, keep their timestamps offloaded.
    def neverNull(start: Long, n: Long): DataFrame =
      spark
        .range(start, start + n)
        .selectExpr(
          "cast(id as int) as id",
          "named_struct('t', timestamp_seconds(id * 37), 'v', id) as st",
          "map('k', timestamp_seconds(id * 37)) as m")
    withLanceTable(neverNull(0, 10)) {
      uri =>
        val plans = executedPlans {
          neverNull(10, 50).write.format("lance").mode("append").option("path", uri).save()
        }
        assertOffloaded[VeloxLanceAppendDataExec](plans)
        checkAnswer(read(uri), neverNull(0, 60))
    }
  }

  test("the check for timestamps in nullable structs also looks inside map keys") {
    val maybeNull = new StructType().add("t", TimestampType)
    def keyed(nullable: Boolean): StructType =
      new StructType()
        .add("m", MapType(new StructType().add("s", maybeNull, nullable), IntegerType))
    assert(LanceWriteExec.timestampInNullableStruct(keyed(nullable = true)).contains("m.key.s.t"))
    assert(LanceWriteExec.timestampInNullableStruct(keyed(nullable = false)).isEmpty)
  }

  test("a failure inside Lance fails the task and leaves the table unchanged") {
    withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "8") {
      withLanceTable(rows(0, 10)) {
        uri =>
          // Lance cannot create data files when a regular file stands where the data directory
          // was. A read-only directory would not do: CI runs as root, which can write into it.
          val data = new File(uri, "data")
          val moved = new File(uri, "data.moved")
          assert(data.isDirectory && data.renameTo(moved) && data.createNewFile())
          try {
            assertNoArrowLeak {
              val e = intercept[Exception] {
                rows(10, 200).write.format("lance").mode("append").option("path", uri).save()
              }
              assert(causedBy(e, "Lance native write failed"), e)
            }
          } finally {
            assert(data.delete() && moved.renameTo(data))
          }
          checkAnswer(read(uri), rows(0, 10))
      }
    }
  }

  /** Rows with a 4-element float vector column, every ninth vector null below id 60. */
  private def vectors(start: Long, n: Long): DataFrame = {
    val fixedSize = new MetadataBuilder().putLong("arrow.fixed-size-list.size", 4).build()
    spark
      .range(start, start + n)
      .selectExpr(
        "cast(id as int) as id",
        "case when id % 9 = 0 and id < 60 then null " +
          "else array(cast(id as float), cast(id + 0.5 as float), cast(-id as float), " +
          "cast(0 as float)) end as v"
      )
      .withColumn("v", col("v").as("v", fixedSize))
  }

  /** Reads on vanilla Spark: the Lance scan offload cannot load fixed-size list columns yet. */
  private def readVectors(uri: String): Seq[Row] = {
    var rows: Seq[Row] = Nil
    withSQLConf(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false") {
      rows = read(uri).collect().toSeq
    }
    rows
  }

  test("fixed-size list columns are written natively") {
    // Small batches, so a task switches between the copying and zero-copy paths and Lance still
    // holds earlier batches while the reader converts later ones.
    withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "8") {
      withLanceTable(vectors(0, 30)) {
        uri =>
          // Spark resolves a write to the table's vector column, so plain arrays will do. Rows
          // below 60 include null vectors, which are copied; later rows hold none, so their
          // elements move without a copy.
          def write(df: DataFrame, mode: String): Seq[SparkPlan] = executedPlans {
            df.select(col("id"), col("v").as("v", Metadata.empty))
              .write
              .format("lance")
              .mode(mode)
              .option("path", uri)
              .save()
          }
          assertOffloaded[VeloxLanceAppendDataExec](write(vectors(30, 60), "append"))
          // Also the output of a filter.
          val filtered = vectors(90, 60).where("id % 3 != 0")
          assertOffloaded[VeloxLanceAppendDataExec](write(filtered, "append"))
          checkAnswer(vectors(0, 90).union(filtered), readVectors(uri))
          assert(read(uri).schema("v").metadata.getLong("arrow.fixed-size-list.size") == 4)

          assertOffloaded[VeloxLanceOverwriteByExpressionExec](write(vectors(40, 30), "overwrite"))
          checkAnswer(vectors(40, 30), readVectors(uri))
      }
    }
  }

  test("a list of the wrong size fails the write") {
    withLanceTable(vectors(0, 10)) {
      uri =>
        val wrongSize =
          spark.range(10, 20).selectExpr("cast(id as int) as id", "array(cast(id as float)) as v")
        assertNoArrowLeak {
          val e = intercept[Exception] {
            wrongSize.write.format("lance").mode("append").option("path", uri).save()
          }
          assert(causedBy(e, "holds lists of 4 elements, but row 0 has 1 elements"), e)
        }
        checkAnswer(vectors(0, 10), readVectors(uri))
    }
  }

  test("large string types fall back to lance-spark's writer") {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/t.lance"
        def write(df: DataFrame, mode: String): Unit =
          df.write
            .format("lance")
            .mode(mode)
            .option("use_large_var_types", "true")
            .option("path", uri)
            .save()
        write(rows(0, 20), "errorifexists")
        val plans = executedPlans(write(rows(20, 20), "append"))
        assertFellBack(plans)
        checkAnswer(read(uri), rows(0, 40))
    }
  }

  test("unsigned integer columns fall back to lance-spark's writer") {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/t.lance"
        val unsigned = new ArrowType.Int(32, false)
        val id = Field.nullable("id", new ArrowType.Int(32, true))
        val allocator = new RootAllocator(Long.MaxValue)
        try {
          val schema = new Schema(Seq(id, Field.nullable("u", unsigned)).asJava)
          org.lance.Dataset.create(
            allocator,
            uri,
            schema,
            new WriteParams.Builder().build()).close()

          // lance-spark keeps the column unsigned in the commit schema, and Velox has no unsigned
          // type. Spark reads uint32 as a long, so values above Int.MaxValue are valid.
          val df = spark.range(0, 20).selectExpr("cast(id as int) as id", "id + 4000000000 as u")
          val plans = executedPlans {
            df.write.format("lance").mode("append").option("path", uri).save()
          }
          assertFellBack(plans)
          // Gluten's Lance scan does not read unsigned columns yet.
          var written: Seq[Row] = Nil
          withSQLConf(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false") {
            written = read(uri).collect().toSeq
          }
          checkAnswer(df, written)
          val dataset = org.lance.Dataset.open(uri, allocator)
          try assert(dataset.getSchema.findField("u").getType == unsigned)
          finally dataset.close()
        } finally allocator.close()
    }
  }

  test("blob columns fall back to lance-spark's writer") {
    // File format 2.1 stores blob v1 columns, and 2.2 blob v2 columns.
    for (version <- Seq("2.1", "2.2")) {
      withLanceCatalog {
        catalog =>
          val table = s"$catalog.default.t"
          spark.sql(
            s"CREATE TABLE $table (id INT, data BINARY) USING lance TBLPROPERTIES " +
              s"('data.lance.encoding' = 'blob', 'file_format_version' = '$version')")
          val plans = executedPlans(spark.sql(
            s"INSERT INTO $table SELECT cast(id as int) as id, cast(id as binary) as data " +
              "FROM range(10)"))
          assertFellBack(plans)
          checkAnswer(spark.table(table).select("id"), (0 until 10).map(Row(_)))
      }
    }
  }

  test("blob v2 columns are recognized by their Arrow extension") {
    // A blob v2 column is a struct whose fields Velox could export, so only its extension marks it.
    val extension = Map(BlobUtils.ARROW_EXTENSION_NAME_KEY -> BlobUtils.ARROW_EXTENSION_BLOB_V2)
    val blob = new Field(
      "data",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, extension.asJava),
      Seq(Field.nullable("data", ArrowType.Binary.INSTANCE)).asJava)
    val reason = LanceWriteExec.unsupportedColumn(new Schema(Seq(blob).asJava))
    assert(reason.contains("Lance blob column data is not offloaded"))
  }

  test("a sharded table falls back to lance-spark's writer") {
    withLanceCatalog {
      catalog =>
        val table = s"$catalog.default.t"
        // Lance shards a table by its partition columns and asks Spark to cluster the rows.
        spark.sql(s"CREATE TABLE $table (id INT, s STRING) USING lance PARTITIONED BY (s)")
        val plans = executedPlans(spark.sql(s"INSERT INTO $table ${sqlRows(0, 10)}"))
        assertFellBack(plans)
        checkAnswer(spark.table(table), expectedSqlRows(0, 10))
    }
  }

  test("writes stay vanilla when the native Lance write is disabled") {
    withSQLConf(VeloxLanceConfig.ENABLE_NATIVE_WRITE.key -> "false") {
      withLanceTable(rows(0, 20)) {
        uri =>
          val plans = executedPlans {
            rows(20, 20).write.format("lance").mode("append").option("path", uri).save()
          }
          assertFellBack(plans)
          checkAnswer(read(uri), rows(0, 40))
      }
    }
  }
}
