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

import org.apache.gluten.execution.{MicroBatchScanExecTransformer, OffloadKafkaScan, ProjectExecTransformerBase, RowToColumnarExecBase}
import org.apache.gluten.execution.kafka.KafkaTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress, Trigger}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration, Instant}
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

/**
 * A bounded rate -> Kafka -> console pipeline. The producer always uses Spark; the consumer
 * compares Spark, a Spark Kafka scan feeding Gluten operators, and a native Gluten Kafka scan.
 * Console rendering is included in the measured micro-batch duration.
 */
object RateMicroBatchBenchmark extends SqlBasedBenchmark {
  private def prefix: String = "spark.gluten.benchmark.rate."

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("RateMicroBatchBenchmark")
      .setIfMissing("spark.master", "local[4]")
      .setIfMissing("spark.sql.adaptive.enabled", "false")
      .setIfMissing("spark.sql.streaming.stopTimeout", "10000")
    if (conf.get(prefix + "engine", "spark") != "spark") {
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
    val broker = conf.get(
      prefix + "bootstrapServers",
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    val rowsPerSecond = conf.getLong(prefix + "rowsPerSecond", 10L)
    val partitions = conf.getInt(prefix + "partitions", 2)
    val triggerMs = conf.getLong(prefix + "triggerMs", 0L)
    val warmupMs = conf.getLong(prefix + "warmupMs", 5000L)
    val durationMs = conf.getLong(prefix + "durationMs", 10000L)
    val timeoutMs = conf.getLong(prefix + "timeoutMs", 60000L)
    val iterations = conf.getInt(prefix + "iterations", 3)
    val engine = conf.get(prefix + "engine", "spark")
    val engines = engine match {
      case "all" => Seq("spark", "hybrid", "gluten")
      case "both" => Seq("spark", "gluten")
      case "spark" | "hybrid" | "gluten" => Seq(engine)
      case _ =>
        throw new IllegalArgumentException("Engine must be spark, hybrid, gluten, both or all")
    }
    require(rowsPerSecond > 0 && partitions > 0, "Rate and partition count must be positive")
    require(triggerMs >= 0 && warmupMs >= 0, "Trigger and warm-up must be nonnegative")
    require(durationMs > triggerMs, "Measurement duration must exceed the trigger interval")
    require(timeoutMs > 0 && iterations > 0, "Timeout and iteration count must be positive")
    var nativeProjectionSignature: Option[Seq[Seq[String]]] = None

    val properties = new Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs.toString)
    properties.put(
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
      math.min(timeoutMs, 30000L).toString)
    val admin = AdminClient.create(properties)
    try {
      runBenchmark("Rate -> Kafka -> console") {
        outputLine(
          s"source=rate transport=kafka broker=$broker sink=console rowsPerSecond=$rowsPerSecond partitions=$partitions " +
            s"triggerMs=$triggerMs warmupMs=$warmupMs durationMs=$durationMs iterations=$iterations")
        outputLine("engine\titeration\trows\trows/s\tbatches\twindowMs\tmeanBatchMs\tp95BatchMs")
        engines.foreach {
          mode =>
            (1 to iterations).foreach {
              iteration =>
                withSQLConf(
                  "spark.gluten.enabled" -> (mode != "spark").toString,
                  OffloadKafkaScan.ENABLED.key -> (mode == "gluten").toString,
                  "spark.sql.json.enablePartialResults" -> "true",
                  "spark.sql.caseSensitive" -> "false"
                ) {
                  withTempPath {
                    checkpoint =>
                      val name = "rate_benchmark_" + UUID.randomUUID().toString.replace("-", "")
                      val topic = name
                      admin.createTopics(Collections.singletonList(
                        new NewTopic(topic, partitions, 1.toShort))).all()
                        .get(timeoutMs, TimeUnit.MILLISECONDS)
                      val producerSession = spark.newSession()
                      producerSession.conf.set("spark.gluten.enabled", "false")
                      val listener = new RateBenchmarkProgress(name)
                      spark.streams.addListener(listener)
                      var query: StreamingQuery = null
                      var producer: StreamingQuery = null
                      try {
                        producer = producerSession.readStream
                          .format("rate")
                          .option("rowsPerSecond", rowsPerSecond)
                          .option("numPartitions", partitions)
                          .load()
                          .selectExpr(
                            "CAST(value AS STRING) AS key",
                            "to_json(struct(timestamp, value)) AS value")
                          .writeStream
                          .format("kafka")
                          .option("kafka.bootstrap.servers", broker)
                          .option("topic", topic)
                          .option(
                            "checkpointLocation",
                            new File(checkpoint, "producer").getCanonicalPath)
                          .trigger(Trigger.ProcessingTime(triggerMs))
                          .start()
                        query = spark.readStream
                          .format("kafka")
                          .option("kafka.bootstrap.servers", broker)
                          .option("subscribe", topic)
                          .option("startingOffsets", "earliest")
                          .option("failOnDataLoss", "true")
                          .load()
                          .selectExpr(
                            "from_json(CAST(value AS STRING), " +
                              "'timestamp STRING, value BIGINT') AS record")
                          .selectExpr(
                            "CAST(record.timestamp AS TIMESTAMP) AS timestamp",
                            "record.value AS value")
                          .writeStream
                          .format("console")
                          .outputMode("append")
                          .queryName(name)
                          .option(
                            "checkpointLocation",
                            new File(checkpoint, "consumer").getCanonicalPath)
                          .trigger(Trigger.ProcessingTime(triggerMs))
                          .start()
                        if (warmupMs > 0) {
                          require(
                            !query.awaitTermination(warmupMs),
                            "Rate query terminated during warm-up")
                        }
                        val startMs = System.currentTimeMillis()
                        val startNanos = System.nanoTime()
                        require(
                          !query.awaitTermination(durationMs),
                          "Rate query terminated during measurement")
                        val elapsedMs = (System.nanoTime() - startNanos) / 1e6
                        val endMs = System.currentTimeMillis()
                        require(producer.isActive, "Rate-to-Kafka producer terminated")
                        producer.exception.foreach(e => throw e)
                        producer.stop()
                        query.stop()
                        val samples = listener.awaitSamples(timeoutMs)
                        query.exception.foreach(e => throw e)
                        val result = RateMicroBatchBenchmarkResult.summarize(
                          samples,
                          startMs,
                          endMs,
                          elapsedMs)
                        val plan = KafkaTestUtils.executedPlan(query)
                        val label = mode match {
                          case "spark" => "Spark"
                          case "hybrid" => "Gluten (Spark Kafka scan + R2C)"
                          case "gluten" => "Gluten (native Kafka scan)"
                        }
                        outputLine(s"PLAN engine=$mode iteration=$iteration\n${plan.treeString}")
                        val projections = validatePlan(mode, plan)
                        if (mode != "spark") {
                          nativeProjectionSignature.foreach {
                            expected =>
                              require(
                                projections == expected,
                                "Native downstream projections differ between runs; " +
                                  "do not compare these timings")
                          }
                          nativeProjectionSignature = Some(projections)
                        }
                        // These task-aggregated metrics describe only the last executed batch.
                        // They are not full-window totals or wall-clock latency.
                        val conversions = plan.collect { case p: RowToColumnarExecBase => p }
                        outputLine(
                          s"LAST_BATCH engine=$mode iteration=$iteration " +
                            s"batchId=${KafkaTestUtils.executedBatchId(query)} " +
                            s"r2cRows=${conversions.map(_.metrics("numInputRows").value).sum} " +
                            s"r2cTaskTimeMs=${conversions.map(_.metrics("convertTime").value).sum}")
                        outputLine(
                          f"$label%s\t$iteration%d\t${result.rows}%d\t${result.rowsPerSecond}%.2f\t" +
                            f"${result.batches}%d\t${result.elapsedMs}%.2f\t" +
                            f"${result.meanBatchMs}%.2f\t${result.p95BatchMs}%d")
                      } finally {
                        try {
                          if (query != null && query.isActive) query.stop()
                        } finally {
                          try {
                            if (producer != null) producer.stop()
                          } finally {
                            spark.streams.removeListener(listener)
                            admin.deleteTopics(Collections.singletonList(topic)).all()
                              .get(timeoutMs, TimeUnit.MILLISECONDS)
                          }
                        }
                      }
                  }
                }
            }
        }
      }
    } finally {
      admin.close(Duration.ofSeconds(5))
    }
  }

  private def validatePlan(mode: String, plan: SparkPlan): Seq[Seq[String]] = {
    val nativeScans = plan.collect { case p: MicroBatchScanExecTransformer => p }
    val sparkScans = plan.collect { case p: MicroBatchScanExec => p }
    KafkaBenchmarkResult.validateScanCounts(nativeScans.size, sparkScans.size, mode == "gluten")
    val projects = plan.collect { case p: ProjectExecTransformerBase => p }
    val conversions = plan.collect { case p: RowToColumnarExecBase => p }
    if (mode == "spark") {
      require(
        projects.isEmpty && conversions.isEmpty,
        "Spark baseline unexpectedly contains native projections or R2C")
    } else {
      require(
        projects.nonEmpty && !plan.exists(_.isInstanceOf[ProjectExec]),
        "Expected all downstream projections, including JSON decoding, to run natively")
      if (mode == "hybrid") {
        require(
          conversions.size == 1 && projects.exists {
            _.exists {
              case conversion: RowToColumnarExecBase =>
                conversion.child.exists(_.isInstanceOf[MicroBatchScanExec])
              case _ => false
            }
          },
          "Hybrid plan must contain Spark Kafka scan -> R2C -> native projection"
        )
      } else {
        require(conversions.isEmpty, "Native Kafka pipeline unexpectedly contains R2C")
      }
    }
    // SQL rendering omits query-specific expression IDs while retaining the projection expressions.
    projects.map(_.list.map(_.sql))
  }

  private def outputLine(line: String): Unit = {
    val out = output.getOrElse(System.out)
    out.write((line + System.lineSeparator()).getBytes(UTF_8))
    out.flush()
  }
}

private class RateBenchmarkProgress(name: String) extends StreamingQueryListener {
  private val progress = new ConcurrentLinkedQueue[StreamingQueryProgress]()
  private val terminated = new CountDownLatch(1)
  @volatile private var runId: UUID = _

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    if (event.name == name) runId = event.runId
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    // Idle progress events can repeat a batch ID. Only addBatch represents an executed micro-batch.
    if (event.progress.runId == runId && event.progress.durationMs.containsKey("addBatch")) {
      progress.add(event.progress)
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    if (event.runId == runId) terminated.countDown()
  }

  def awaitSamples(timeoutMs: Long): Seq[RateMicroBatchSample] = {
    require(
      terminated.await(timeoutMs, TimeUnit.MILLISECONDS),
      "Timed out waiting for rate progress")
    progress.iterator().asScala.map {
      sample =>
        RateMicroBatchSample(
          sample.batchId,
          Instant.parse(sample.timestamp).toEpochMilli,
          sample.batchDuration,
          sample.sink.numOutputRows)
    }.toVector
  }
}
