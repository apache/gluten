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
package org.apache.spark.sql.delta

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}
import org.apache.gluten.execution.{HashAggregateExecTransformer, TerminalRow}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.RefCountedArrowColumnVector

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.stats.{DeltaJobStatisticsTracker, GlutenDeltaJobStatsTracker}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.{TaskResourceRegistry, TaskResources}

import org.apache.arrow.vector.BigIntVector
import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually

import java.lang.management.ManagementFactory
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.NonFatal

class GlutenDeltaStatsLifecycleSuite extends GlutenQueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", "2")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, "true")
      .set("spark.databricks.delta.stats.collect", "true")
      .set("spark.databricks.delta.optimizeWrite.enabled", "false")
      .set("spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet", "false")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
  }

  private def statisticsThreads: Set[Long] =
    Thread.getAllStackTraces.keySet().asScala
      .filter(_.getName.startsWith("Gluten Delta Statistics Writer - "))
      .map(_.getId)
      .toSet

  private def withNativeStatistics(path: String)(write: => Unit): Unit = {
    val before = statisticsThreads
    val plans = new ConcurrentLinkedQueue[SparkPlan]()
    val completionChecks = new ConcurrentLinkedQueue[() => Unit]()
    val observer: (Path, SparkPlan) => Unit = (statsPath, plan) => {
      if (statsPath.toUri.getPath == path) {
        plans.add(plan)
        val context = TaskContext.get()
        val usage = TaskResources.getSharedUsage()
        completionChecks.add(
          () => {
            assert(context.isCompleted())
            assert(usage.current() == 0)
          })
      }
    }
    GlutenDeltaJobStatsTracker.withStatsPlanObserver(observer) {
      write
    }
    assert(!plans.isEmpty, "The write did not exercise native Delta statistics")
    plans.asScala.foreach {
      plan =>
        assert(
          plan.exists(_.isInstanceOf[HashAggregateExecTransformer]),
          s"Expected a native statistics aggregate, but got:\n${plan.treeString}")
    }
    eventually(timeout(15.seconds), interval(100.millis)) {
      completionChecks.asScala.foreach(_())
      assert(statisticsThreads.diff(before).isEmpty, "Statistics workers survived task completion")
    }
  }

  test("aborted Delta writes close native statistics before task runtimes") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        val schema = new StructType().add("id", LongType, nullable = false)
        DeltaLog.forTable(spark, path).startTransaction()
          .commit(Seq(Metadata(schemaString = schema.json)), DeltaOperations.ManualUpdate)
        spark.read.format("delta").load(path).write.format("delta").mode("overwrite").save(path)
        withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "1") {
          AbortingDeltaWrite.withInput(spark, path, schema.asNullable) {
            invalid =>
              withNativeStatistics(path) {
                val failure = intercept[Exception] {
                  invalid.write.format("delta").mode("append").save(path)
                }
                assert(
                  Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
                    .exists(_.isInstanceOf[InvariantViolationException]),
                  failure.toString)
              }
          }
        }
        assert(spark.read.format("delta").load(path).count() == 0)
        withNativeStatistics(path) {
          Seq(2L, 3L).toDF("id").write.format("delta").mode("append").save(path)
        }
        assert(spark.read.format("delta").load(path).collect().toSet == Set(Row(2L), Row(3L)))
    }
  }

  test("multi-file Delta statistics close workers after a successful task") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        withNativeStatistics(path) {
          Seq((0L, 0), (1L, 0), (2L, 0), (3L, 0)).toDF("id", "part").coalesce(1)
            .write.format("delta").partitionBy("part").option("maxRecordsPerFile", "1")
            .save(path)
        }
        val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
        assert(files.length == 4)
        val records = spark.read.json(files.map(_.stats).toSeq.toDS())
          .select("numRecords").as[Long].collect()
        assert(records.length == 4 && records.forall(_ == 1))
    }
  }

  for (failBeforeSubmission <- Seq(true, false)) {
    val timing = if (failBeforeSubmission) "before" else "during"
    test(s"newRow releases the batch when statistics fail $timing submission") {
      val before = statisticsThreads
      val workerReady = new CountDownLatch(1)
      val failWorker = new CountDownLatch(1)
      val submitting = new CountDownLatch(1)
      val closeCompleted = new CountDownLatch(1)
      val completed = new CountDownLatch(1)
      val closingThread = new AtomicReference[Thread]()
      val failures = new ConcurrentLinkedQueue[Throwable]()
      val completionChecks = new ConcurrentLinkedQueue[() => Unit]()
      val workerFailure = new IllegalStateException("injected statistics worker failure")
      val aggregate = spark.range(1).selectExpr(
        "to_json(named_struct('numRecords', count(*))) AS stats")
        .queryExecution.analyzed.collectFirst { case a: Aggregate => a }.get
      val failStatistics: String => String = _ => {
        assert(Thread.currentThread().getName.startsWith("Gluten Delta Statistics Writer - "))
        workerReady.countDown()
        assert(failWorker.await(30, TimeUnit.SECONDS), "Statistics worker was not released")
        throw workerFailure
      }
      val statsExpr = ScalaUDF(
        failStatistics,
        StringType,
        Seq(aggregate.aggregateExpressions.head))
      val tracker = GlutenDeltaJobStatsTracker(new DeltaJobStatisticsTracker(
        spark.sessionState.newHadoopConf(),
        new Path("file:/tmp/gluten-stats-worker-failure"),
        aggregate.child.output,
        statsExpr))
      val conf = spark.sessionState.conf.clone()
      conf.setConfString(GlutenConfig.BENCHMARK_TASK_STAGEID.key, "0")
      val thread = new Thread(
        () => {
          SparkSession.clearActiveSession()
          try {
            SQLConf.withExistingConf(conf) {
              TaskResources.runUnsafe {
                val context = TaskContext.get()
                val usage = TaskResources.getSharedUsage()
                completionChecks.add(
                  () => {
                    assert(context.isCompleted())
                    assert(usage.current() == 0)
                  })
                val taskTracker = tracker.newTaskInstance()
                val file = "file:/tmp/gluten-stats-worker-failure/part.parquet"
                taskTracker.newFile(file)
                // End the empty input to reach a controlled failure in the actual worker.
                val closer = new Thread(
                  () => {
                    try {
                      intercept[ExecutionException] {
                        taskTracker.closeFile(file)
                      }
                    } catch {
                      case NonFatal(e) => failures.add(e)
                    } finally {
                      closeCompleted.countDown()
                    }
                  },
                  "delta statistics failing file close"
                )
                closingThread.set(closer)
                closer.setDaemon(true)
                closer.start()
                assert(workerReady.await(30, TimeUnit.SECONDS), "Statistics worker did not start")
                if (failBeforeSubmission) {
                  failWorker.countDown()
                  assert(
                    closeCompleted.await(30, TimeUnit.SECONDS),
                    "Statistics worker did not fail")
                }
                val vector = new BigIntVector("id", ArrowBufferAllocators.contextInstance())
                val column = new RefCountedArrowColumnVector(vector)
                val inputBatch = new ColumnarBatch(Array(column), 1)
                try {
                  vector.allocateNew(1)
                  vector.set(0, 7L)
                  vector.setValueCount(1)
                  val row = new TerminalRow {
                    override def batch(): ColumnarBatch = inputBatch
                    override def withNewBatch(batch: ColumnarBatch): TerminalRow =
                      throw new UnsupportedOperationException()
                  }
                  submitting.countDown()
                  val rowFailure = intercept[ExecutionException] {
                    taskTracker.newRow(file, row)
                  }
                  assert(
                    Iterator.iterate[Throwable](rowFailure)(_.getCause).takeWhile(_ != null)
                      .exists(_ eq workerFailure))
                  assert(column.refCnt() == 1, "The rejected batch's retained reference leaked")
                  assert(column.getLong(0) == 7L, "The producer's batch was released prematurely")
                } finally {
                  inputBatch.close()
                }
                assert(column.refCnt() == 0)
              }
            }
          } catch {
            case e: InterruptedException => failures.add(e)
            case NonFatal(e) => failures.add(e)
          } finally {
            completed.countDown()
          }
        },
        "delta statistics failed worker task"
      )
      thread.setDaemon(true)
      thread.start()
      try {
        assert(submitting.await(30, TimeUnit.SECONDS), "The task did not reach newRow")
        if (!failBeforeSubmission) {
          eventually(timeout(10.seconds), interval(10.millis)) {
            assert(
              thread.getState == Thread.State.WAITING ||
                thread.getState == Thread.State.TIMED_WAITING,
              "newRow did not wait for the statistics worker")
          }
          failWorker.countDown()
        }
        assert(
          completed.await(30, TimeUnit.SECONDS),
          "newRow blocked after the statistics worker failed")
      } finally {
        // Interrupt the old blocking put as well, so the regression cannot hang the test JVM.
        failWorker.countDown()
        thread.interrupt()
        thread.join(30000)
        Option(closingThread.get()).foreach {
          closer =>
            closer.interrupt()
            closer.join(30000)
            assert(!closer.isAlive, "File close did not terminate")
        }
      }
      assert(!thread.isAlive, "Statistics cleanup did not terminate")
      failures.asScala.foreach(e => throw e)
      completionChecks.asScala.foreach(_())
      assert(statisticsThreads.diff(before).isEmpty, "Statistics workers survived task completion")
    }
  }

  test("newRow propagates a statistics worker failure while consuming input") {
    val before = statisticsThreads
    val completed = new CountDownLatch(1)
    val failures = new ConcurrentLinkedQueue[Throwable]()
    val completionChecks = new ConcurrentLinkedQueue[() => Unit]()
    val aggregate = spark.range(1).selectExpr(
      "to_json(named_struct('numRecords', count(*))) AS stats")
      .queryExecution.analyzed.collectFirst { case a: Aggregate => a }.get
    val tracker = GlutenDeltaJobStatsTracker(new DeltaJobStatisticsTracker(
      spark.sessionState.newHadoopConf(),
      new Path("file:/tmp/gluten-stats-midstream-failure"),
      aggregate.child.output,
      aggregate.aggregateExpressions.head))
    val conf = spark.sessionState.conf.clone()
    conf.setConfString(GlutenConfig.BENCHMARK_TASK_STAGEID.key, "0")
    val thread = new Thread(
      () => {
        SparkSession.clearActiveSession()
        try {
          SQLConf.withExistingConf(conf) {
            TaskResources.runUnsafe {
              val context = TaskContext.get()
              val usage = TaskResources.getSharedUsage()
              completionChecks.add(
                () => {
                  assert(context.isCompleted())
                  assert(usage.current() == 0)
                })
              def newBatch(value: Long): (ColumnarBatch, RefCountedArrowColumnVector) = {
                val vector = new BigIntVector("id", ArrowBufferAllocators.contextInstance())
                val column = new RefCountedArrowColumnVector(vector)
                vector.allocateNew(1)
                vector.set(0, value)
                vector.setValueCount(1)
                (new ColumnarBatch(Array(column), 1), column)
              }
              def terminalRow(inputBatch: ColumnarBatch): TerminalRow = new TerminalRow {
                override def batch(): ColumnarBatch = inputBatch
                override def withNewBatch(batch: ColumnarBatch): TerminalRow =
                  throw new UnsupportedOperationException()
              }
              val taskTracker = tracker.newTaskInstance()
              val file = "file:/tmp/gluten-stats-midstream-failure/part.parquet"
              taskTracker.newFile(file)
              val (firstBatch, _) = newBatch(1L)
              try {
                ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), firstBatch)
                taskTracker.newRow(file, terminalRow(firstBatch))
              } finally {
                firstBatch.close()
              }
              val (invalidBatch, invalidColumn) = newBatch(2L)
              val (rejectedBatch, rejectedColumn) = newBatch(3L)
              completionChecks.add(
                () => {
                  assert(invalidColumn.refCnt() == 0, "The worker's input batch leaked")
                  assert(rejectedColumn.refCnt() == 0, "The rejected batch leaked")
                })
              try {
                // A non-offloaded batch fails in the worker's input iterator after the first
                // native batch was consumed. Never call closeFile or send end-of-stream.
                taskTracker.newRow(file, terminalRow(invalidBatch))
                val failure = intercept[ExecutionException] {
                  taskTracker.newRow(file, terminalRow(rejectedBatch))
                }
                assert(
                  Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
                    .exists(
                      e => Option(e.getMessage).exists(_.contains("Input batch is not offloaded"))),
                  failure.toString
                )
                assert(
                  rejectedColumn.refCnt() == 1,
                  "The rejected batch's retained reference leaked")
                assert(
                  rejectedColumn.getLong(0) == 3L,
                  "The producer's batch was released prematurely")
              } finally {
                invalidBatch.close()
                rejectedBatch.close()
              }
            }
          }
        } catch {
          case e: InterruptedException => failures.add(e)
          case NonFatal(e) => failures.add(e)
        } finally {
          completed.countDown()
        }
      },
      "delta statistics midstream failure task"
    )
    thread.setDaemon(true)
    thread.start()
    try {
      assert(
        completed.await(30, TimeUnit.SECONDS),
        "newRow blocked after the statistics worker failed mid-stream")
    } finally {
      thread.interrupt()
      thread.join(30000)
    }
    assert(!thread.isAlive, "Statistics cleanup did not terminate")
    failures.asScala.foreach(e => throw e)
    completionChecks.asScala.foreach(_())
    assert(statisticsThreads.diff(before).isEmpty, "Statistics workers survived task completion")
  }

  test("canceled statistics workers can access task resources before cleanup") {
    val workerReady = new CountDownLatch(1)
    val registryAccessed = new AtomicBoolean(false)
    val resourceReleased = new AtomicBoolean(false)
    val cancellationPreserved = new AtomicBoolean(false)
    val taskThread = new AtomicReference[Thread]()
    val failures = new ConcurrentLinkedQueue[Throwable]()
    val completionChecks = new ConcurrentLinkedQueue[() => Unit]()
    val aggregate = spark.range(1).selectExpr(
      "to_json(named_struct('numRecords', count(*))) AS stats")
      .queryExecution.analyzed.collectFirst { case a: Aggregate => a }.get
    val checkRegistryOnCancel: String => String = json => {
      try {
        assert(Thread.currentThread().getName.startsWith("Gluten Delta Statistics Writer - "))
        workerReady.countDown()
        intercept[InterruptedException] {
          new CountDownLatch(1).await(30, TimeUnit.SECONDS)
        }
        // Detect the lock cycle before acquiring the monitor, so a regression fails, not hangs.
        val owner = ManagementFactory.getThreadMXBean
          .getThreadInfo(Array(taskThread.get().getId), true, false).head
        assert(
          !owner.getLockedMonitors.exists(
            _.getClassName == classOf[TaskResourceRegistry].getName),
          "Statistics cleanup is waiting for its worker while holding the task resource registry"
        )
        TaskResources.addRecycler("statistics cancellation probe", 100) {
          resourceReleased.set(true)
        }
        registryAccessed.set(true)
        json
      } catch {
        case NonFatal(e) =>
          failures.add(e)
          throw e
      } finally {
        Thread.currentThread().interrupt()
      }
    }
    val statsExpr = ScalaUDF(
      checkRegistryOnCancel,
      StringType,
      Seq(aggregate.aggregateExpressions.head))
    val tracker = GlutenDeltaJobStatsTracker(new DeltaJobStatisticsTracker(
      spark.sessionState.newHadoopConf(),
      new Path("file:/tmp/gluten-stats-cancellation"),
      aggregate.child.output,
      statsExpr))
    val conf = spark.sessionState.conf.clone()
    // The synthetic task uses stage -1, which otherwise matches the default dump stage.
    conf.setConfString(GlutenConfig.BENCHMARK_TASK_STAGEID.key, "0")
    val thread = new Thread(
      () => {
        SparkSession.clearActiveSession()
        try {
          intercept[InterruptedException] {
            SQLConf.withExistingConf(conf) {
              TaskResources.runUnsafe {
                val context = TaskContext.get()
                val usage = TaskResources.getSharedUsage()
                completionChecks.add(
                  () => {
                    assert(context.isCompleted())
                    assert(usage.current() == 0)
                  })
                val taskTracker = tracker.newTaskInstance()
                context.addTaskCompletionListener[Unit](_ => Thread.currentThread().interrupt())
                val file = "file:/tmp/gluten-stats-cancellation/part.parquet"
                taskTracker.newFile(file)
                taskTracker.closeFile(file)
              }
            }
          }
          cancellationPreserved.set(Thread.currentThread().isInterrupted)
        } catch {
          case NonFatal(e) =>
            failures.add(e)
            workerReady.countDown()
        }
      },
      "delta statistics cancellation task"
    )
    taskThread.set(thread)
    thread.setDaemon(true)
    thread.start()
    try {
      assert(workerReady.await(30, TimeUnit.SECONDS), "Statistics worker did not reach evaluation")
    } finally {
      thread.interrupt()
      thread.join(30000)
    }
    assert(!thread.isAlive, "Statistics cleanup did not terminate")
    failures.asScala.foreach(e => throw e)
    assert(registryAccessed.get(), "Statistics worker could not access its task resource registry")
    assert(resourceReleased.get(), "Worker-registered resource was not released")
    assert(cancellationPreserved.get(), "Cleanup cleared the task cancellation interrupt")
    completionChecks.asScala.foreach(_())
  }
}

private object AbortingDeltaWrite extends Eventually {
  // Local Spark tasks share this registry, not the serialized copies of their closures.
  private val writers = new ConcurrentHashMap[String, CompletableFuture[TaskContext]]()

  def withInput(spark: SparkSession, path: String, schema: StructType)(
      write: DataFrame => Unit): Unit = {
    val writer = new CompletableFuture[TaskContext]()
    writers.put(path, writer)
    try {
      val rows = spark.sparkContext.parallelize(Seq(0, 1), 2).flatMap(partitionRows(path, _))
      write(spark.createDataFrame(rows, schema))
      assert(writer.get(30, TimeUnit.SECONDS).isInterrupted(), "The valid writer was not canceled")
    } finally {
      writers.remove(path)
    }
  }

  private def partitionRows(path: String, partition: Int): Iterator[Row] = {
    val writer = writers.get(path)
    if (partition == 0) {
      Iterator(1L, 2L).map {
        id =>
          if (id == 2L) {
            // With one row per batch, the worker has accepted the first batch by this point.
            val context = TaskContext.get()
            writer.complete(context)
            eventually(timeout(30.seconds), interval(10.millis)) {
              assert(
                context.isInterrupted(),
                "Timed out waiting for the invalid partition to abort")
            }
            context.killTaskIfInterrupted()
          }
          Row(id)
      }
    } else {
      writer.get(30, TimeUnit.SECONDS)
      Iterator.single(Row(null))
    }
  }
}
