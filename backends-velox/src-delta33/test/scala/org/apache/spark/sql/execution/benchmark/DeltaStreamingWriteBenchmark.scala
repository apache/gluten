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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.streaming.MemoryStream

import com.sun.management.OperatingSystemMXBean
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

/**
 * Local Spark 3.5 / Delta 3.3.2 streaming append benchmark, with statistics enabled.
 *
 * Run this main class using spark-submit with the backends-velox test jar, the native Gluten
 * bundle, Delta jars and the Spark test-jar classpath used by DeltaPlanningBenchmark. The workload
 * uses local[2], two input partitions and identical 24-column batches for both writers, with Gluten
 * enabled in both runs. Only native streaming writing is toggled.
 *
 * Reports Spark's triggerExecution duration and JVM process CPU separately. CPU covers source
 * enqueue through processAllAvailable, including MemoryStream encoding and local executor work; it
 * is not cluster CPU or CPU for precisely the triggerExecution interval. Input vector creation,
 * query startup, writer verification and warmup batches are excluded from reported samples.
 *
 * Set spark.gluten.benchmark.delta.streaming.warmupBatches (default 2) and measuredBatches (default
 * 5) to adjust the repetitions. No performance thresholds are asserted.
 */
object DeltaStreamingWriteBenchmark extends SqlBasedBenchmark {
  override def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[2]")
      .appName("DeltaStreamingWriteBenchmark")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "1024MB")
      .config("spark.ui.enabled", "false")
      .config("spark.default.parallelism", "2")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
  }

  private val warmupBatches = spark.sparkContext.conf
    .getInt("spark.gluten.benchmark.delta.streaming.warmupBatches", 2)
  private val measuredBatches = spark.sparkContext.conf
    .getInt("spark.gluten.benchmark.delta.streaming.measuredBatches", 5)

  private val processCpu = ManagementFactory.getOperatingSystemMXBean match {
    case bean: OperatingSystemMXBean => bean
    case other =>
      throw new IllegalStateException(s"JVM process CPU time is unavailable: ${other.getName}")
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    require(warmupBatches >= 0 && measuredBatches > 0)
    require(processCpu.getProcessCpuTime >= 0, "JVM process CPU time is unavailable")
    report(s"local[2], input partitions=2, columns=24, Gluten=true, statistics=true, " +
      s"warmup batches=$warmupBatches, measured batches=$measuredBatches")
    report("CPU interval: source enqueue through processAllAvailable, in this JVM only")
    report("mode,rows_per_batch,sample,triggerExecution_ms,local_process_cpu_ms")
    for (rowsPerBatch <- Seq(10000, 100000, 1000000); nativeWrite <- Seq(false, true)) {
      runStreamingCase(rowsPerBatch, nativeWrite)
    }
  }

  private def runStreamingCase(rowsPerBatch: Int, nativeWrite: Boolean): Unit = {
    import spark.implicits._
    val rows = (0 until rowsPerBatch).map(_.toLong).toVector
    val mode = if (nativeWrite) "native" else "spark"
    val columns = "value AS id" +: (1 until 24).map {
      i =>
        if (i % 3 == 0) s"concat('value-$i-', value) AS c$i"
        else s"value * $i AS c$i"
    }
    withSQLConf(
      "spark.gluten.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite" -> "true",
      "spark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite" ->
        nativeWrite.toString,
      "spark.databricks.delta.stats.collect" -> "true",
      "spark.databricks.delta.optimizeWrite.enabled" -> "false"
    ) {
      withTempPath {
        directory =>
          val path = directory.getCanonicalPath + "/data"
          val input = MemoryStream[Long](numPartitions = 2)(implicitly, spark.sqlContext)
          val query = input.toDF()
            .selectExpr(columns: _*)
            .writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", directory.getCanonicalPath + "/checkpoint")
            .start(path)
          val samples = scala.collection.mutable.ArrayBuffer.empty[(Long, Double)]
          try {
            query.processAllAvailable()
            // Idle progress can report batch 0 before the first data batch uses that same ID.
            var previousBatchId = -1L
            for (batch <- 0 until warmupBatches + measuredBatches) {
              val startCpu = processCpu.getProcessCpuTime
              input.addData(rows)
              query.processAllAvailable()
              val cpuMillis = (processCpu.getProcessCpuTime - startCpu) / 1000000.0
              val progress = query.recentProgress.filter {
                p => p.batchId > previousBatchId && p.numInputRows > 0
              }
              require(progress.length == 1, s"Expected one data trigger, found ${progress.length}")
              val completed = progress.head
              require(completed.numInputRows == rowsPerBatch, completed.prettyJson)
              previousBatchId = completed.batchId
              val duration = Option(completed.durationMs.get("triggerExecution"))
                .getOrElse(throw new IllegalStateException("Missing triggerExecution duration"))
                .longValue()
              if (batch >= warmupBatches) samples += ((duration, cpuMillis))
            }
          } finally {
            query.stop()
          }
          verifyWriter(path, nativeWrite, rowsPerBatch.toLong * (warmupBatches + measuredBatches))
          samples.zipWithIndex.foreach {
            case ((duration, cpuMillis), index) =>
              report(f"$mode,$rowsPerBatch,${index + 1},$duration,$cpuMillis%.3f")
          }
          val meanDuration = samples.map(_._1.toDouble).sum / samples.size
          val meanCpu = samples.map(_._2).sum / samples.size
          report(f"$mode,$rowsPerBatch,mean,$meanDuration%.3f,$meanCpu%.3f")
      }
    }
  }

  private def verifyWriter(path: String, nativeWrite: Boolean, expectedRows: Long): Unit = {
    withSQLConf("spark.gluten.enabled" -> "false") {
      val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
      require(files.nonEmpty, "The benchmark produced no data files")
      val writtenRows = files.map {
        file =>
          val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
            DeltaFileOperations.absolutePath(path, file.path),
            spark.sessionState.newHadoopConf()))
          try {
            val footer = reader.getFooter
            val rowCount = footer.getBlocks.asScala.map(_.getRowCount).sum
            if (rowCount > 0) {
              val createdBy = footer.getFileMetaData.getCreatedBy
              require(createdBy != null, s"Missing writer metadata: ${file.path}")
              require(
                createdBy.startsWith("parquet-mr") == !nativeWrite,
                s"Unexpected writer for ${file.path}: $createdBy")
            }
            rowCount
          } finally {
            reader.close()
          }
      }.sum
      require(writtenRows == expectedRows, s"Expected $expectedRows rows, found $writtenRows")
    }
  }

  private def report(line: String): Unit = {
    System.out.println(line)
    output.foreach {
      stream =>
        stream.write((line + "\n").getBytes(StandardCharsets.UTF_8))
        stream.flush()
    }
  }
}
