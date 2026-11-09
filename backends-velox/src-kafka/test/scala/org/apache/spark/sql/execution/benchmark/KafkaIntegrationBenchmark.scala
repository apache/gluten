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
package org.apache.spark.sql.execution.benchmark

import org.apache.gluten.execution.kafka.KafkaTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.io.File
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

/**
 * Benchmark the supported Velox Kafka read integration, fallback, checkpoint recovery and a bounded
 * Spark-producer-to-Parquet pipeline. Native reads use micro-batch execution; Kafka writes and
 * checkpoint management remain Spark operations.
 */
object KafkaIntegrationBenchmark extends SqlBasedBenchmark {
  private val scenarioNames = Seq("scan", "projection", "headers", "checkpoint", "pipeline")
  private val kafkaColumns =
    Seq("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")

  private case class Settings(
      broker: String,
      rows: Long,
      partitions: Int,
      payloadBytes: Int,
      maxOffsets: Long,
      iterations: Int,
      timeoutMs: Long,
      engines: Seq[Boolean])

  private case class ReadResult(
      queryId: UUID,
      runId: UUID,
      samples: Seq[KafkaBenchmarkSample],
      elapsedMs: Double,
      plan: String)

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("KafkaIntegrationBenchmark")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "2")
      .setIfMissing("spark.sql.adaptive.enabled", "false")
      .setIfMissing("spark.sql.streaming.stopTimeout", "10000")
    if (conf.get("spark.gluten.benchmark.kafka.engine", "both") != "spark") {
      conf
        .setIfMissing("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .setIfMissing(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .setIfMissing("spark.memory.offHeap.enabled", "true")
        .setIfMissing("spark.memory.offHeap.size", "1g")
    }
    SparkSession.builder().config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = spark.sparkContext.getConf
    val engine = conf.get("spark.gluten.benchmark.kafka.engine", "both")
    require(Set("spark", "gluten", "both").contains(engine), "Engine must be spark, gluten or both")
    val settings = Settings(
      conf.get(
        "spark.gluten.benchmark.kafka.bootstrapServers",
        sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
      conf.getLong("spark.gluten.benchmark.kafka.rows", 100000L),
      conf.getInt("spark.gluten.benchmark.kafka.partitions", 2),
      conf.getInt("spark.gluten.benchmark.kafka.payloadBytes", 128),
      conf.getLong("spark.gluten.benchmark.kafka.maxOffsetsPerTrigger", 10000L),
      conf.getInt("spark.gluten.benchmark.kafka.iterations", 3),
      conf.getLong("spark.gluten.benchmark.kafka.timeoutMs", 120000L),
      if (engine == "spark") Seq(false) else if (engine == "gluten") Seq(true) else Seq(false, true)
    )
    require(
      settings.partitions > 0 && settings.partitions <= Int.MaxValue / 2 &&
        settings.rows >= 2L * settings.partitions && settings.rows <= Long.MaxValue / 2,
      "Provide at least two rows per partition and a row count that can be doubled"
    )
    require(settings.payloadBytes >= 21, "Payload size must fit a 20-byte ID and one binary NUL")
    require(
      settings.maxOffsets > 0 && settings.iterations > 0 && settings.timeoutMs > 0,
      "Limits must be positive")
    val selected = conf.get("spark.gluten.benchmark.kafka.scenarios", "all") match {
      case "all" => scenarioNames
      case names => names.split(",").toSeq.map(_.trim)
    }
    require(
      selected.nonEmpty && selected.distinct.size == selected.size &&
        selected.forall(scenarioNames.contains),
      s"Choose distinct scenarios from ${scenarioNames.mkString(",")}, or all"
    )

    val properties = new Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.broker)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    val admin = AdminClient.create(properties)
    try {
      selected.foreach {
        case "checkpoint" => benchmarkCheckpoint(admin, settings)
        case "pipeline" => benchmarkPipeline(admin, settings)
        case scenario => benchmarkRead(admin, settings, scenario)
      }
    } finally {
      admin.close(Duration.ofSeconds(5))
    }
  }

  private def withTopic(admin: AdminClient, settings: Settings)(f: String => Unit): Unit = {
    val topic = "gluten_kafka_benchmark_" + UUID.randomUUID().toString.replace("-", "")
    admin
      .createTopics(Collections.singletonList(new NewTopic(topic, settings.partitions, 1.toShort)))
      .all()
      .get(30, TimeUnit.SECONDS)
    try {
      f(topic)
    } finally {
      admin.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS)
    }
  }

  private def produce(topic: String, settings: Settings, start: Long, rows: Long): Unit = {
    // Use Spark's producer for both engines; this is not a native Kafka write benchmark.
    withSQLConf("spark.gluten.enabled" -> "false") {
      spark
        .range(start, start + rows, 1L, settings.partitions)
        .selectExpr(
          "cast(case when id % 11 = 2 then null when id % 11 = 1 then '' " +
            "else concat(cast(id as string), chr(0), 'key') end as binary) as key",
          "cast(case when id % 11 = 0 then null when id % 11 = 1 then '' " +
            s"else concat(lpad(cast(id as string), 20, '0'), chr(0), " +
            s"repeat('x', ${settings.payloadBytes - 21})) end as binary) as value",
          s"cast(id % ${settings.partitions} as int) as partition",
          "array(named_struct('key', 'benchmark', " +
            "'value', cast(concat('id=', cast(id as string)) as binary))) as headers"
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.broker)
        .option("topic", topic)
        .save()
    }
  }

  private def readOptions(
      topic: String,
      settings: Settings,
      scenario: String): Map[String, String] = {
    val base = Map(
      "kafka.bootstrap.servers" -> settings.broker,
      "subscribe" -> topic,
      "startingOffsets" -> "earliest",
      "includeHeaders" -> (scenario == "headers").toString,
      "failOnDataLoss" -> "true"
    )
    if (scenario == "projection") {
      val offsets = (0 until settings.partitions).map(p => s""""$p":1""").mkString(",")
      base ++ Map(
        "startingOffsets" -> s"""{"$topic":{$offsets}}""",
        "minPartitions" -> (2 * settings.partitions).toString)
    } else {
      base
    }
  }

  private def project(frame: DataFrame, scenario: String): DataFrame = {
    val columns = scenario match {
      case "projection" => Seq("offset", "value", "partition")
      case "headers" => kafkaColumns :+ "headers"
      case _ => kafkaColumns
    }
    frame.selectExpr(columns: _*)
  }

  private def runRead(
      topic: String,
      settings: Settings,
      scenario: String,
      enabled: Boolean,
      checkpoint: String,
      parquet: Option[String],
      timer: Option[Benchmark.Timer] = None,
      beforeStart: () => Unit = () => (),
      firstBatchId: Long = 0L): ReadResult = {
    var result: ReadResult = null
    withSQLConf("spark.gluten.enabled" -> enabled.toString) {
      val name = "kafka_benchmark_" + UUID.randomUUID().toString.replace("-", "")
      val progress = new KafkaBenchmarkProgress(name)
      spark.streams.addListener(progress)
      var query: StreamingQuery = null
      try {
        val frame = spark.readStream
          .format("kafka")
          .options(readOptions(topic, settings, scenario))
          .option("maxOffsetsPerTrigger", settings.maxOffsets)
          .load()
        val writer = project(frame, scenario).writeStream
          .format(if (parquet.isDefined) "parquet" else "noop")
          .queryName(name)
          .option("checkpointLocation", checkpoint)
          .trigger(Trigger.AvailableNow())
        parquet.foreach(path => writer.option("path", path))
        timer.foreach(_.startTiming())
        val startNanos = System.nanoTime()
        var elapsedMs = 0.0
        try {
          beforeStart()
          query = writer.start()
          require(
            query.awaitTermination(settings.timeoutMs),
            "Kafka query exceeded completion timeout")
        } finally {
          elapsedMs = (System.nanoTime() - startNanos) / 1e6
          timer.foreach(_.stopTiming())
        }

        val plan = KafkaTestUtils.executedPlan(query)
        // Avoid loading native scan classes when only the Spark baseline is requested.
        val nativeScans = plan.collect {
          case p
              if p.getClass.getName ==
                "org.apache.gluten.execution.MicroBatchScanExecTransformer" => p
        }
        val sparkScans = plan.collect { case p: MicroBatchScanExec => p }
        try {
          KafkaBenchmarkResult.validateScanCounts(
            nativeScans.size,
            sparkScans.size,
            enabled && scenario != "headers")
        } catch {
          case e: IllegalArgumentException =>
            throw new IllegalStateException(s"Unexpected Kafka scan:\n${plan.treeString}", e)
        }
        val samples = progress.awaitSamples(settings.timeoutMs)
        KafkaBenchmarkResult.validateBatchSequence(
          samples,
          firstBatchId,
          KafkaTestUtils.executedBatchId(query))
        result = ReadResult(query.id, query.runId, samples, elapsedMs, plan.treeString)
      } finally {
        try {
          if (query != null) query.stop()
        } finally {
          spark.streams.removeListener(progress)
        }
      }
    }
    result
  }

  private def validateParquet(
      path: String,
      topic: String,
      settings: Settings,
      scenario: String,
      rows: Long): Unit = {
    withSQLConf("spark.gluten.enabled" -> "false") {
      // Read the root so Spark honors the streaming sink's committed-file metadata.
      val actual = spark.read.parquet(path)
      val expected = project(
        spark.read.format("kafka").options(readOptions(topic, settings, scenario)).load(),
        scenario)
      require(expected.count() == rows, "Kafka fixture contains missing or duplicate records")
      require(actual.count() == rows, s"Expected $rows committed Parquet rows")
      require(
        actual.exceptAll(expected).limit(1).count() == 0 &&
          expected.exceptAll(actual).limit(1).count() == 0,
        "Kafka values or metadata differ from Spark's reference read"
      )
    }
  }

  private def label(enabled: Boolean, scenario: String): String = {
    if (!enabled) "Spark"
    else if (scenario == "headers") "Gluten / Spark scan fallback" else "Gluten"
  }

  private def report(benchmark: Benchmark, name: String, result: ReadResult, rows: Long): Unit = {
    val statistics = KafkaBenchmarkResult.statistics(result.samples, rows, result.elapsedMs)
    benchmark.out.println(
      f"$name%s: ${result.samples.size}%d completed batches, $rows%d validated rows, " +
        f"wallMs=${result.elapsedMs}%.2f, rows/s=${statistics.rowsPerSecond}%.2f, " +
        f"meanBatchMs=${statistics.meanBatchMs}%.2f, p95BatchMs=${statistics.p95BatchMs}%d")
    benchmark.out.println(result.plan)
  }

  private def benchmarkRead(admin: AdminClient, settings: Settings, scenario: String): Unit = {
    val skipped = if (scenario == "projection") settings.partitions.toLong else 0L
    // Validate actual fields on a small fixture before timing the larger noop workload.
    val validationRows = math.min(settings.rows, math.max(128L, 2L * settings.partitions))
    withTopic(admin, settings) {
      topic =>
        produce(topic, settings, 0, validationRows)
        settings.engines.foreach {
          enabled =>
            withTempPath {
              dir =>
                val data = new File(dir, "data").getCanonicalPath
                runRead(
                  topic,
                  settings,
                  scenario,
                  enabled,
                  new File(dir, "checkpoint").getCanonicalPath,
                  Some(data))
                validateParquet(data, topic, settings, scenario, validationRows - skipped)
            }
        }
    }
    withTopic(admin, settings) {
      topic =>
        produce(topic, settings, 0, settings.rows)
        val rows = settings.rows - skipped
        val benchmark = new Benchmark(s"Kafka integration / $scenario read", rows, output = output)
        settings.engines.foreach {
          enabled =>
            val name = label(enabled, scenario)
            benchmark.addTimerCase(name, settings.iterations) {
              timer =>
                withTempPath {
                  checkpoint =>
                    val result = runRead(
                      topic,
                      settings,
                      scenario,
                      enabled,
                      checkpoint.getCanonicalPath,
                      None,
                      Some(timer))
                    KafkaBenchmarkResult.validatedRowCount(result.samples, rows)
                    if (timer.iteration == 0) report(benchmark, name, result, rows)
                }
            }
        }
        benchmark.run()
    }
  }

  private def benchmarkCheckpoint(admin: AdminClient, settings: Settings): Unit = {
    val benchmark =
      new Benchmark("Kafka integration / checkpoint resume", settings.rows, output = output)
    settings.engines.foreach {
      enabled =>
        val name = label(enabled, "checkpoint")
        benchmark.addTimerCase(name, settings.iterations) {
          timer =>
            withTopic(admin, settings) {
              topic =>
                withTempPath {
                  dir =>
                    val data = new File(dir, "data").getCanonicalPath
                    val checkpoint = new File(dir, "checkpoint").getCanonicalPath
                    produce(topic, settings, 0, settings.rows)
                    val first = runRead(topic, settings, "scan", enabled, checkpoint, Some(data))
                    validateParquet(data, topic, settings, "scan", settings.rows)
                    produce(topic, settings, settings.rows, settings.rows)
                    val resumed = runRead(
                      topic,
                      settings,
                      "scan",
                      enabled,
                      checkpoint,
                      Some(data),
                      Some(timer),
                      firstBatchId = first.samples.last.batchId + 1)
                    require(
                      first.queryId == resumed.queryId && first.runId != resumed.runId,
                      "Expected a new run of the same checkpointed query")
                    validateParquet(data, topic, settings, "scan", 2 * settings.rows)
                    if (timer.iteration == 0) report(benchmark, name, resumed, settings.rows)
                }
            }
        }
    }
    benchmark.run()
  }

  private def benchmarkPipeline(admin: AdminClient, settings: Settings): Unit = {
    val benchmark =
      new Benchmark("Kafka integration / producer to Parquet", settings.rows, output = output)
    settings.engines.foreach {
      enabled =>
        val name = label(enabled, "pipeline")
        benchmark.addTimerCase(name, settings.iterations) {
          timer =>
            withTopic(admin, settings) {
              topic =>
                withTempPath {
                  dir =>
                    val data = new File(dir, "data").getCanonicalPath
                    // Produce before AvailableNow snapshots offsets. Both operations are timed.
                    val result = runRead(
                      topic,
                      settings,
                      "scan",
                      enabled,
                      new File(dir, "checkpoint").getCanonicalPath,
                      Some(data),
                      Some(timer),
                      () => produce(topic, settings, 0, settings.rows))
                    validateParquet(data, topic, settings, "scan", settings.rows)
                    if (timer.iteration == 0) report(benchmark, name, result, settings.rows)
                }
            }
        }
    }
    benchmark.run()
  }
}

private[spark] case class KafkaBenchmarkSample(
    batchId: Long,
    sourceRows: Long,
    sinkRows: Long,
    durationMs: Long = -1L)

private[spark] case class KafkaBenchmarkStatistics(
    rowsPerSecond: Double,
    meanBatchMs: Double,
    p95BatchMs: Long)

private[spark] object KafkaBenchmarkResult {
  def statistics(
      samples: Seq[KafkaBenchmarkSample],
      validatedRows: Long,
      elapsedMs: Double): KafkaBenchmarkStatistics = {
    require(validatedRows > 0, "Validated row count must be positive")
    require(
      elapsedMs > 0 && !elapsedMs.isNaN && !elapsedMs.isInfinity,
      "Elapsed time must be positive and finite")
    require(samples.nonEmpty, "No completed micro-batches were observed")
    require(samples.forall(_.durationMs >= 0), "Batch durations are unavailable or negative")
    val durations = samples.map(_.durationMs).sorted
    KafkaBenchmarkStatistics(
      validatedRows * 1000.0 / elapsedMs,
      durations.map(_.toDouble).sum / durations.size,
      durations(math.ceil(durations.size * 0.95).toInt - 1))
  }

  def validateBatchSequence(
      samples: Seq[KafkaBenchmarkSample],
      expectedFirstBatchId: Long,
      expectedLastBatchId: Long): Unit = {
    require(
      expectedFirstBatchId >= 0 && expectedLastBatchId >= expectedFirstBatchId,
      "Expected batch IDs must be nonnegative and ordered")
    require(samples.nonEmpty, "No completed micro-batches were observed")
    require(
      samples.zipWithIndex.forall {
        case (sample, index) => sample.batchId == expectedFirstBatchId + index.toLong
      } && samples.last.batchId == expectedLastBatchId,
      "Micro-batch progress is incomplete or duplicated"
    )
  }

  def validatedRowCount(samples: Seq[KafkaBenchmarkSample], expectedRows: Long): Long = {
    require(expectedRows > 0, "Expected row count must be positive")
    require(samples.nonEmpty, "No completed micro-batches were observed")
    validateBatchSequence(samples, 0L, samples.last.batchId)
    require(samples.forall(_.sinkRows >= 0), "Sink row counts are unavailable")
    // Spark's V2 source metrics recognize MicroBatchScanExec, not Gluten's replacement node.
    val rows = samples.map(_.sinkRows).sum
    require(rows == expectedRows, s"Expected $expectedRows sink rows, observed $rows")
    rows
  }

  def validateScanCounts(nativeScans: Int, sparkScans: Int, nativeExpected: Boolean): Unit = {
    require(
      nativeScans == (if (nativeExpected) 1 else 0) &&
        sparkScans == (if (nativeExpected) 0 else 1),
      "Unexpected Kafka scan implementation")
  }
}

private class KafkaBenchmarkProgress(name: String) extends StreamingQueryListener {
  private val samples = new ConcurrentLinkedQueue[KafkaBenchmarkSample]()
  private val terminated = new CountDownLatch(1)
  @volatile private var runId: UUID = _

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    if (event.name == name) runId = event.runId
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    // Idle notifications do not execute a batch and may reuse its ID.
    if (progress.runId == runId && progress.durationMs.containsKey("addBatch")) {
      samples.add(
        KafkaBenchmarkSample(
          progress.batchId,
          progress.numInputRows,
          progress.sink.numOutputRows,
          progress.batchDuration))
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    if (event.runId == runId) terminated.countDown()
  }

  def awaitSamples(timeoutMs: Long): Seq[KafkaBenchmarkSample] = {
    require(
      terminated.await(timeoutMs, TimeUnit.MILLISECONDS),
      "Timed out waiting for query progress")
    samples.iterator().asScala.toVector
  }
}
