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

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.Utils

import io.delta.sql.DeltaSparkSessionExtension

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Focused benchmark for Delta DELETE with persistent deletion vectors.
 *
 * Usage:
 * {{{
 *   org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark [rows] [files] [iterations] [mode]
 * }}}
 *
 * Modes:
 *   - create: DELETE creates deletion vectors on a fresh table
 *   - update: DELETE updates existing deletion vectors
 *   - all: run both modes
 */
object DeltaDeleteDeletionVectorBenchmark extends BenchmarkBase {
  private val EnableNativeDmlRowIndexScan =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"

  private case class BenchmarkConf(
      rowCount: Long = 1000 * 1000,
      files: Int = 8,
      iterations: Int = 3,
      mode: String = "all")

  private case class DeleteConfs(
      glutenEnabled: Boolean,
      nativeWriteEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean)

  private case class DeleteResult(
      activeFiles: Long,
      filesWithDvs: Long,
      dvCardinality: Long,
      dvPayloadBytes: Long)

  private var sparkSession: SparkSession = _
  private var benchmarkRoot: File = _

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = parseArgs(mainArgs)
    sparkSession = createSparkSession(conf)
    benchmarkRoot = Utils.createTempDir(namePrefix = "delta-delete-dv-benchmark")

    conf.mode match {
      case "create" =>
        runDeleteBenchmark(
          name = "Delta DELETE creates deletion vectors",
          conf = conf,
          existingDv = false,
          measuredPredicate = "id % 10 = 0")
      case "update" =>
        runDeleteBenchmark(
          name = "Delta DELETE updates existing deletion vectors",
          conf = conf,
          existingDv = true,
          measuredPredicate = "id % 10 = 1")
      case "all" =>
        runDeleteBenchmark(
          name = "Delta DELETE creates deletion vectors",
          conf = conf,
          existingDv = false,
          measuredPredicate = "id % 10 = 0")
        runDeleteBenchmark(
          name = "Delta DELETE updates existing deletion vectors",
          conf = conf,
          existingDv = true,
          measuredPredicate = "id % 10 = 1")
      case other =>
        throw new IllegalArgumentException(
          s"Unknown mode '$other'. Expected create, update, or all.")
    }
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
      sparkSession = null
    }
    if (benchmarkRoot != null) {
      Utils.deleteRecursively(benchmarkRoot)
      benchmarkRoot = null
    }
  }

  private def spark: SparkSession = sparkSession

  private def parseArgs(args: Array[String]): BenchmarkConf = {
    val defaults = BenchmarkConf()
    BenchmarkConf(
      rowCount = args.headOption.map(_.toLong).getOrElse(defaults.rowCount),
      files = args.lift(1).map(_.toInt).getOrElse(defaults.files),
      iterations = args.lift(2).map(_.toInt).getOrElse(defaults.iterations),
      mode = args.lift(3).map(_.toLowerCase(Locale.ROOT)).getOrElse(defaults.mode)
    )
  }

  private def createSparkSession(conf: BenchmarkConf): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName("DeltaDeleteDeletionVectorBenchmark")
      .setIfMissing("spark.master", "local[4]")
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", conf.files.toString)
      .set("spark.sql.shuffle.partitions", conf.files.toString)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(GlutenConfig.FALLBACK_REPORTER_ENABLED.key, "false")
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, "true")
      .set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, "true")
      .set(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey,
        "true")
      .set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "false")

    SparkSession.builder.config(sparkConf).getOrCreate()
  }

  private def runDeleteBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val sparkPaths = prepareTables(s"$name-spark", conf, existingDv)
    val glutenPaths = prepareTables(s"$name-gluten", conf, existingDv)
    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase("Spark DELETE DV (Gluten disabled)", conf.iterations) {
      iteration =>
        val result = runDelete(
          sparkPaths(iteration),
          measuredPredicate,
          DeleteConfs(
            glutenEnabled = false,
            nativeWriteEnabled = false,
            nativeDmlRowIndexScanEnabled = false))
        validateDeleteResult(result, existingDv)
        printFirstIterationResult(iteration, "spark", result)
    }

    benchmark.addCase("Gluten DELETE DV (native write + DML row-index scan)", conf.iterations) {
      iteration =>
        val result = runDelete(
          glutenPaths(iteration),
          measuredPredicate,
          DeleteConfs(
            glutenEnabled = true,
            nativeWriteEnabled = true,
            nativeDmlRowIndexScanEnabled = true))
        validateDeleteResult(result, existingDv)
        printFirstIterationResult(iteration, "gluten-native", result)
    }

    benchmark.run()
  }

  private def prepareTables(
      prefix: String,
      conf: BenchmarkConf,
      existingDv: Boolean): IndexedSeq[String] = {
    (0 until conf.iterations).map {
      iteration =>
        val path = new File(
          benchmarkRoot,
          s"${sanitize(prefix)}-$iteration").getCanonicalPath
        writeTable(path, conf)
        if (existingDv) {
          val result = runDelete(
            path,
            "id % 10 = 9",
            DeleteConfs(
              glutenEnabled = false,
              nativeWriteEnabled = false,
              nativeDmlRowIndexScanEnabled = false))
          validateDeleteResult(result, existingDv = false)
        }
        path
    }
  }

  private def writeTable(path: String, conf: BenchmarkConf): Unit = {
    spark
      .range(conf.rowCount)
      .repartition(conf.files)
      .selectExpr(
        "id",
        s"cast(id % ${math.max(conf.files, 1)} as int) as part",
        "cast(id % 1000 as int) as payload")
      .write
      .format("delta")
      .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
      .mode("overwrite")
      .save(path)
  }

  private def runDelete(
      path: String,
      predicate: String,
      confs: DeleteConfs): DeleteResult = {
    withConfs(
      "spark.gluten.enabled" -> confs.glutenEnabled.toString,
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> confs.nativeWriteEnabled.toString,
      EnableNativeDmlRowIndexScan -> confs.nativeDmlRowIndexScanEnabled.toString,
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
    ) {
      spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
    }
    collectDeleteResult(path)
  }

  private def collectDeleteResult(path: String): DeleteResult = {
    val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
    val filesWithDvs = files.filter(_.deletionVector != null)
    DeleteResult(
      activeFiles = files.length,
      filesWithDvs = filesWithDvs.length,
      dvCardinality = filesWithDvs.map(_.deletionVector.cardinality).sum,
      dvPayloadBytes = filesWithDvs.map(_.deletionVector.sizeInBytes).sum
    )
  }

  private def validateDeleteResult(result: DeleteResult, existingDv: Boolean): Unit = {
    require(result.filesWithDvs > 0, s"Expected deletion vectors, got $result")
    require(result.dvCardinality > 0, s"Expected deleted-row cardinality, got $result")
    if (existingDv) {
      require(
        result.dvCardinality > result.filesWithDvs,
        s"Expected existing-DV update to retain non-trivial cardinality, got $result")
    }
  }

  private def printFirstIterationResult(
      iteration: Int,
      label: String,
      result: DeleteResult): Unit = {
    if (iteration == 0) {
      writeOutputLine(
        s"$label result: activeFiles=${result.activeFiles}, " +
          s"filesWithDvs=${result.filesWithDvs}, " +
          s"dvCardinality=${result.dvCardinality}, " +
          s"dvPayloadBytes=${result.dvPayloadBytes}")
    }
  }

  private def writeOutputLine(line: String): Unit = {
    output match {
      case Some(out) =>
        out.write((line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8))
        out.flush()
      case None =>
        println(line)
    }
  }

  private def withConfs[T](confs: (String, String)*)(f: => T): T = {
    val previous = confs.map {
      case (key, _) => key -> Try(spark.conf.get(key)).toOption
    }
    try {
      confs.foreach { case (key, value) => spark.conf.set(key, value) }
      f
    } finally {
      previous.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  private def sanitize(name: String): String =
    name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-")
}
