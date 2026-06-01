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
 *   org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark \
 *     [rows] [files] [iterations] [deleteMode] [executionMode]
 * }}}
 *
 * Delete modes: create, update, all. Execution modes: spark, gluten, all.
 */
object DeltaDeleteDeletionVectorBenchmark extends BenchmarkBase {
  private val EnableNativeDmlRowIndexScan =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"

  private case class BenchmarkConf(
      rowCount: Long = 1000 * 1000,
      files: Int = 8,
      iterations: Int = 3,
      deleteMode: String = "all",
      executionMode: String = "spark")

  private case class ExecutionMode(
      label: String,
      withGlutenPlugin: Boolean,
      deleteConfs: DeleteConfs)

  private case class DeleteConfs(
      glutenEnabled: Boolean,
      nativeWriteEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean)

  private case class DeleteResult(
      activeFiles: Long,
      filesWithDvs: Long,
      dvCardinality: Long,
      dvPayloadBytes: Long,
      finalRows: Long)

  private var sparkSession: SparkSession = _
  private var benchmarkRoot: File = _

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = parseArgs(mainArgs)
    executionModes(conf.executionMode).foreach {
      mode =>
        sparkSession = createSparkSession(conf, mode)
        benchmarkRoot = Utils.createTempDir(
          namePrefix = s"delta-delete-dv-benchmark-${mode.label}")
        try {
          conf.deleteMode match {
            case "create" =>
              runDeleteBenchmark(
                name = "Delta DELETE creates deletion vectors",
                conf = conf,
                mode = mode,
                existingDv = false,
                measuredPredicate = "id % 10 = 0",
                expectedDeletedMods = Seq(0))
            case "update" =>
              runDeleteBenchmark(
                name = "Delta DELETE updates existing deletion vectors",
                conf = conf,
                mode = mode,
                existingDv = true,
                measuredPredicate = "id % 10 = 1",
                expectedDeletedMods = Seq(9, 1)
              )
            case "all" =>
              runDeleteBenchmark(
                name = "Delta DELETE creates deletion vectors",
                conf = conf,
                mode = mode,
                existingDv = false,
                measuredPredicate = "id % 10 = 0",
                expectedDeletedMods = Seq(0))
              runDeleteBenchmark(
                name = "Delta DELETE updates existing deletion vectors",
                conf = conf,
                mode = mode,
                existingDv = true,
                measuredPredicate = "id % 10 = 1",
                expectedDeletedMods = Seq(9, 1)
              )
            case other =>
              throw new IllegalArgumentException(
                s"Unknown delete mode '$other'. Expected create, update, or all.")
          }
        } finally {
          stopSpark()
          if (benchmarkRoot != null) {
            Utils.deleteRecursively(benchmarkRoot)
            benchmarkRoot = null
          }
        }
    }
  }

  override def afterAll(): Unit = {
    stopSpark()
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
      deleteMode = args.lift(3).map(_.toLowerCase(Locale.ROOT)).getOrElse(defaults.deleteMode),
      executionMode =
        args.lift(4).map(_.toLowerCase(Locale.ROOT)).getOrElse(defaults.executionMode)
    )
  }

  private def executionModes(mode: String): Seq[ExecutionMode] = {
    val sparkOnly = ExecutionMode(
      label = "spark",
      withGlutenPlugin = false,
      deleteConfs = DeleteConfs(
        glutenEnabled = false,
        nativeWriteEnabled = false,
        nativeDmlRowIndexScanEnabled = false))
    val glutenNative = ExecutionMode(
      label = "gluten-native",
      withGlutenPlugin = true,
      deleteConfs = DeleteConfs(
        glutenEnabled = true,
        nativeWriteEnabled = true,
        nativeDmlRowIndexScanEnabled = true))
    mode match {
      case "spark" => Seq(sparkOnly)
      case "gluten" => Seq(glutenNative)
      case "all" => Seq(sparkOnly, glutenNative)
      case other =>
        throw new IllegalArgumentException(
          s"Unknown execution mode '$other'. Expected spark, gluten, or all.")
    }
  }

  private def createSparkSession(conf: BenchmarkConf, mode: ExecutionMode): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(s"DeltaDeleteDeletionVectorBenchmark-${mode.label}")
      .setIfMissing("spark.master", "local[4]")
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.default.parallelism", conf.files.toString)
      .set("spark.sql.shuffle.partitions", conf.files.toString)
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(GlutenConfig.FALLBACK_REPORTER_ENABLED.key, "false")
      .set("spark.gluten.enabled", mode.deleteConfs.glutenEnabled.toString)
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, mode.deleteConfs.nativeWriteEnabled.toString)
      .set(EnableNativeDmlRowIndexScan, mode.deleteConfs.nativeDmlRowIndexScanEnabled.toString)
      .set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, "true")
      .set(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey,
        "true")
      .set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "false")

    if (mode.withGlutenPlugin) {
      sparkConf
        .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "4g")
    }

    SparkSession.builder.config(sparkConf).getOrCreate()
  }

  private def runDeleteBenchmark(
      name: String,
      conf: BenchmarkConf,
      mode: ExecutionMode,
      existingDv: Boolean,
      measuredPredicate: String,
      expectedDeletedMods: Seq[Int]): Unit = {
    val paths = prepareTables(s"$name-${mode.label}", conf, existingDv)
    val expectedFinalRows = expectedRemainingRows(conf.rowCount, expectedDeletedMods)
    val benchmark = new Benchmark(
      name = s"$name ${mode.label} (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase(s"${mode.label} DELETE DV", conf.iterations) {
      iteration =>
        val result = runDelete(
          paths(iteration),
          measuredPredicate,
          mode.deleteConfs)
        validateDeleteResult(result, existingDv, expectedFinalRows)
        printFirstIterationResult(iteration, mode.label, result)
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
          validateDeleteResult(
            result,
            existingDv = false,
            expectedFinalRows = expectedRemainingRows(conf.rowCount, Seq(9)))
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
    val finalRows = spark.read.format("delta").load(path).count()
    DeleteResult(
      activeFiles = files.length,
      filesWithDvs = filesWithDvs.length,
      dvCardinality = filesWithDvs.map(_.deletionVector.cardinality).sum,
      dvPayloadBytes = filesWithDvs.map(_.deletionVector.sizeInBytes).sum,
      finalRows = finalRows
    )
  }

  private def validateDeleteResult(
      result: DeleteResult,
      existingDv: Boolean,
      expectedFinalRows: Long): Unit = {
    require(result.filesWithDvs > 0, s"Expected deletion vectors, got $result")
    require(result.dvCardinality > 0, s"Expected deleted-row cardinality, got $result")
    require(
      result.finalRows == expectedFinalRows,
      s"Expected $expectedFinalRows final rows, got $result")
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
          s"dvPayloadBytes=${result.dvPayloadBytes}, " +
          s"finalRows=${result.finalRows}")
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

  private def expectedRemainingRows(rowCount: Long, deletedMods: Seq[Int]): Long =
    rowCount - deletedMods.distinct.map(countRowsWithMod(rowCount, _)).sum

  private def countRowsWithMod(rowCount: Long, mod: Int): Long = {
    require(mod >= 0 && mod < 10, s"Expected modulo in [0, 10), got $mod")
    if (rowCount <= mod) {
      0L
    } else {
      ((rowCount - 1 - mod) / 10) + 1
    }
  }

  private def stopSpark(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
      sparkSession = null
    }
  }

  private def sanitize(name: String): String =
    name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-")
}
