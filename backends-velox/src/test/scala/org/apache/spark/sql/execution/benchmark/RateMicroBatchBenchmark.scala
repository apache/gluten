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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress, Trigger}

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

/**
 * A bounded rate-source baseline. This benchmark does not read Kafka or prove native scan support.
 */
object RateMicroBatchBenchmark extends SqlBasedBenchmark {
  private def prefix: String = "spark.gluten.benchmark.rate."

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("RateMicroBatchBenchmark")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.adaptive.enabled", "false")
      .setIfMissing("spark.sql.streaming.stopTimeout", "10000")
    if (conf.get(prefix + "engine", "spark") != "spark") {
      conf
        .setIfMissing("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .setIfMissing("spark.memory.offHeap.enabled", "true")
        .setIfMissing("spark.memory.offHeap.size", "1g")
    }
    SparkSession.builder().config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = spark.sparkContext.getConf
    val rowsPerSecond = conf.getLong(prefix + "rowsPerSecond", 10L)
    val partitions = conf.getInt(prefix + "partitions", 2)
    val triggerMs = conf.getLong(prefix + "triggerMs", 1000L)
    val warmupMs = conf.getLong(prefix + "warmupMs", 5000L)
    val durationMs = conf.getLong(prefix + "durationMs", 10000L)
    val timeoutMs = conf.getLong(prefix + "timeoutMs", 10000L)
    val iterations = conf.getInt(prefix + "iterations", 3)
    val engine = conf.get(prefix + "engine", "spark")
    require(Set("spark", "gluten", "both").contains(engine), "Engine must be spark, gluten or both")
    require(rowsPerSecond > 0 && partitions > 0, "Rate and partition count must be positive")
    require(triggerMs > 0 && warmupMs >= 0, "Trigger must be positive; warm-up must be nonnegative")
    require(durationMs > triggerMs, "Measurement duration must exceed the trigger interval")
    require(timeoutMs > 0 && iterations > 0, "Timeout and iteration count must be positive")
    val engines = if (engine == "both") Seq(false, true) else Seq(engine == "gluten")

    runBenchmark("Rate source micro-batch scan") {
      outputLine(
        s"source=rate sink=noop rowsPerSecond=$rowsPerSecond partitions=$partitions " +
          s"triggerMs=$triggerMs warmupMs=$warmupMs durationMs=$durationMs iterations=$iterations")
      outputLine("engine\titeration\trows\trows/s\tbatches\twindowMs\tmeanBatchMs\tp95BatchMs")
      engines.foreach {
        enabled =>
          (1 to iterations).foreach {
            iteration =>
              withSQLConf("spark.gluten.enabled" -> enabled.toString) {
                withTempPath {
                  checkpoint =>
                    val name = "rate_benchmark_" + UUID.randomUUID().toString.replace("-", "")
                    val listener = new RateBenchmarkProgress(name)
                    spark.streams.addListener(listener)
                    var query: StreamingQuery = null
                    try {
                      query = spark.readStream
                        .format("rate")
                        .option("rowsPerSecond", rowsPerSecond)
                        .option("numPartitions", partitions)
                        .load()
                        .writeStream
                        .format("noop")
                        .outputMode("append")
                        .queryName(name)
                        .option("checkpointLocation", checkpoint.getCanonicalPath)
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
                      query.stop()
                      val samples = listener.awaitSamples(timeoutMs)
                      query.exception.foreach(e => throw e)
                      val result = RateMicroBatchBenchmarkResult.summarize(
                        samples,
                        startMs,
                        endMs,
                        elapsedMs)
                      val label = if (enabled) "Gluten (Spark rate source)" else "Spark"
                      outputLine(
                        f"$label%s\t$iteration%d\t${result.rows}%d\t${result.rowsPerSecond}%.2f\t" +
                          f"${result.batches}%d\t${result.elapsedMs}%.2f\t${result.meanBatchMs}%.2f\t" +
                          f"${result.p95BatchMs}%d")
                    } finally {
                      try {
                        if (query != null && query.isActive) query.stop()
                      } finally {
                        spark.streams.removeListener(listener)
                      }
                    }
                }
              }
          }
      }
    }
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
