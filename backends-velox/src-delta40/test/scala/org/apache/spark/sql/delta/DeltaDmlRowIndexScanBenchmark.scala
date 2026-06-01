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
import org.apache.gluten.extension.DeltaDeletionVectorDmlUtils
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.Utils

import io.delta.sql.DeltaSparkSessionExtension

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Focused benchmark for Delta DV DELETE target scans that must stay on Spark fallback.
 *
 * Usage:
 * {{{
 *   org.apache.spark.sql.delta.DeltaDmlRowIndexScanBenchmark \
 *     [rows] [files] [iterations] [deleteMode] [executionMode]
 * }}}
 *
 * Delete modes: create, update, all. Execution modes: spark, guarded, all.
 */
object DeltaDmlRowIndexScanBenchmark extends BenchmarkBase {
  private val EnableNativeDmlRowIndexScan =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"
  private val DmlFallbackReason = "fallback Delta DV DML row-index scan"
  private val RowIndexColumnNames =
    Set("__delta_internal_row_index", "_tmp_metadata_row_index", "row_index")
  private val FilePathColumnNames = Set("file_path", "filePath")

  private case class BenchmarkConf(
      rowCount: Long = 1000 * 1000,
      files: Int = 8,
      iterations: Int = 3,
      deleteMode: String = "all",
      executionMode: String = "spark")

  private case class ExecutionMode(
      label: String,
      withGlutenPlugin: Boolean,
      glutenEnabled: Boolean,
      nativeWriteEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean,
      expectFallbackTag: Boolean)

  private case class DeleteResult(
      deleteMs: Double,
      validationMs: Double,
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
          namePrefix = s"delta-dml-row-index-scan-${mode.label}")
        try {
          conf.deleteMode match {
            case "create" =>
              runDeleteBenchmark(
                name = "Delta DML row-index scan creates DVs",
                conf = conf,
                mode = mode,
                existingDv = false,
                measuredPredicate = "id % 10 = 0",
                expectedDeletedMods = Seq(0))
            case "update" =>
              runDeleteBenchmark(
                name = "Delta DML row-index scan updates DVs",
                conf = conf,
                mode = mode,
                existingDv = true,
                measuredPredicate = "id % 10 = 1",
                expectedDeletedMods = Seq(9, 1))
            case "all" =>
              runDeleteBenchmark(
                name = "Delta DML row-index scan creates DVs",
                conf = conf,
                mode = mode,
                existingDv = false,
                measuredPredicate = "id % 10 = 0",
                expectedDeletedMods = Seq(0))
              runDeleteBenchmark(
                name = "Delta DML row-index scan updates DVs",
                conf = conf,
                mode = mode,
                existingDv = true,
                measuredPredicate = "id % 10 = 1",
                expectedDeletedMods = Seq(9, 1))
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
      glutenEnabled = false,
      nativeWriteEnabled = false,
      nativeDmlRowIndexScanEnabled = false,
      expectFallbackTag = false)
    val guarded = ExecutionMode(
      label = "gluten-guarded-fallback",
      withGlutenPlugin = true,
      glutenEnabled = true,
      nativeWriteEnabled = false,
      nativeDmlRowIndexScanEnabled = false,
      expectFallbackTag = true
    )
    mode match {
      case "spark" => Seq(sparkOnly)
      case "guarded" => Seq(guarded)
      case "all" => Seq(sparkOnly, guarded)
      case other =>
        throw new IllegalArgumentException(
          s"Unknown execution mode '$other'. Expected spark, guarded, or all.")
    }
  }

  private def createSparkSession(conf: BenchmarkConf, mode: ExecutionMode): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(s"DeltaDmlRowIndexScanBenchmark-${mode.label}")
      .setIfMissing("spark.master", "local[4]")
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.default.parallelism", conf.files.toString)
      .set("spark.sql.shuffle.partitions", conf.files.toString)
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(GlutenConfig.FALLBACK_REPORTER_ENABLED.key, "false")
      .set("spark.gluten.enabled", mode.glutenEnabled.toString)
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, mode.nativeWriteEnabled.toString)
      .set(EnableNativeDmlRowIndexScan, mode.nativeDmlRowIndexScanEnabled.toString)
      .set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, "true")
      .set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "true")
      .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "true")
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
    validatePlanShape(s"$name-plan-${mode.label}", conf, mode, existingDv, measuredPredicate)

    val paths = prepareTables(s"$name-${mode.label}", conf, existingDv)
    val benchmark = new Benchmark(
      name = s"$name ${mode.label} (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase(s"${mode.label} validated DELETE", conf.iterations) {
      iteration =>
        val result = runDelete(paths(iteration), measuredPredicate, mode)
        validateDeleteResult(conf, result, expectedDeletedMods)
        printFirstIterationResult(iteration, mode.label, expectedDeletedMods, result)
    }

    benchmark.run()
  }

  private def validatePlanShape(
      label: String,
      conf: BenchmarkConf,
      mode: ExecutionMode,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val planPath = new File(benchmarkRoot, sanitize(label)).getCanonicalPath
    writeTable(planPath, conf.copy(rowCount = math.min(conf.rowCount, 10000L)))
    if (existingDv) {
      runDelete(planPath, "id % 10 = 9", mode.copy(expectFallbackTag = false))
    }

    var executedPlans: Seq[SparkPlan] = Seq.empty
    withDeleteConfs(mode) {
      executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
        spark.sql(s"DELETE FROM delta.`$planPath` WHERE $measuredPredicate").collect()
      }.map(_.executedPlan)
    }

    val dmlScans = executedPlans.flatMap {
      _.collect {
        case scan: FileSourceScanExec if isDmlRowIndexScan(scan) =>
          scan
      }
    }
    val fallbackReasons = dmlScans.flatMap(scan => FallbackTags.getOption(scan).map(_.reason()))
    val planText = executedPlans.map(_.treeString).mkString("\n\n")
    val dmlScanCount =
      if (dmlScans.nonEmpty) {
        dmlScans.size
      } else if (containsDmlRowIndexScanText(planText)) {
        1
      } else {
        0
      }
    require(
      dmlScanCount > 0,
      "Expected a Delta DML row-index scan in benchmark plan:\n" + planText)
    if (mode.expectFallbackTag) {
      require(
        fallbackReasons.exists(_.contains(DmlFallbackReason)),
        s"Expected guarded fallback reason '$DmlFallbackReason', got $fallbackReasons")
    }

    writeOutputLine(
      s"${mode.label} plan-shape: dmlRowIndexScans=$dmlScanCount, " +
        s"fallbackTagged=${fallbackReasons.count(_.contains(DmlFallbackReason))}, " +
        s"fallbackReasons=${fallbackReasons.mkString("[", "; ", "]")}")
  }

  private def isDmlRowIndexScan(scan: FileSourceScanExec): Boolean = {
    if (DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan)) {
      return true
    }

    val scanColumnNames = (scan.output.map(_.name) ++ scan.requiredSchema.fieldNames).toSet
    scanColumnNames.exists(RowIndexColumnNames.contains) &&
    (scanColumnNames.exists(FilePathColumnNames.contains) || scan.treeString.contains("file_path"))
  }

  private def containsDmlRowIndexScanText(planText: String): Boolean = {
    planText.contains("FileScan") &&
    planText.contains("_tmp_metadata_row_index") &&
    planText.contains("file_path")
  }

  private def prepareTables(
      prefix: String,
      conf: BenchmarkConf,
      existingDv: Boolean): IndexedSeq[String] = {
    (0 until conf.iterations).map {
      iteration =>
        val path = new File(benchmarkRoot, s"${sanitize(prefix)}-$iteration").getCanonicalPath
        writeTable(path, conf)
        if (existingDv) {
          runDelete(
            path,
            "id % 10 = 9",
            ExecutionMode(
              label = "spark-existing-dv-setup",
              withGlutenPlugin = false,
              glutenEnabled = false,
              nativeWriteEnabled = false,
              nativeDmlRowIndexScanEnabled = false,
              expectFallbackTag = false
            )
          )
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
      mode: ExecutionMode): DeleteResult = {
    val deleteStartNs = System.nanoTime()
    withDeleteConfs(mode) {
      spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
    }
    val deleteMs = nanosToMillis(System.nanoTime() - deleteStartNs)

    val validationStartNs = System.nanoTime()
    val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
    val filesWithDvs = files.filter(_.deletionVector != null)
    val finalRows = spark.read.format("delta").load(path).count()
    DeleteResult(
      deleteMs = deleteMs,
      validationMs = nanosToMillis(System.nanoTime() - validationStartNs),
      activeFiles = files.length,
      filesWithDvs = filesWithDvs.length,
      dvCardinality = filesWithDvs.map(_.deletionVector.cardinality).sum,
      dvPayloadBytes = filesWithDvs.map(_.deletionVector.sizeInBytes).sum,
      finalRows = finalRows
    )
  }

  private def withDeleteConfs[T](mode: ExecutionMode)(f: => T): T = {
    withConfs(
      "spark.gluten.enabled" -> mode.glutenEnabled.toString,
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> mode.nativeWriteEnabled.toString,
      EnableNativeDmlRowIndexScan -> mode.nativeDmlRowIndexScanEnabled.toString,
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "true",
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
    )(f)
  }

  private def validateDeleteResult(
      conf: BenchmarkConf,
      result: DeleteResult,
      expectedDeletedMods: Seq[Int]): Unit = {
    val expectedDeleted = expectedDeletedMods.map(countModulo(conf.rowCount, _)).sum
    require(result.filesWithDvs > 0, s"Expected deletion vectors, got $result")
    require(
      result.dvCardinality == expectedDeleted,
      s"Expected DV cardinality $expectedDeleted, got $result")
    require(
      result.finalRows == conf.rowCount - expectedDeleted,
      s"Expected ${conf.rowCount - expectedDeleted} final rows, got $result")
  }

  private def printFirstIterationResult(
      iteration: Int,
      label: String,
      expectedDeletedMods: Seq[Int],
      result: DeleteResult): Unit = {
    if (iteration == 0) {
      writeOutputLine(
        s"$label result: deleteMs=${formatMillis(result.deleteMs)}, " +
          s"validationMs=${formatMillis(result.validationMs)}, " +
          s"activeFiles=${result.activeFiles}, filesWithDvs=${result.filesWithDvs}, " +
          s"dvCardinality=${result.dvCardinality}, dvPayloadBytes=${result.dvPayloadBytes}, " +
          s"finalRows=${result.finalRows}, " +
          s"deletedMods=${expectedDeletedMods.mkString("[", ",", "]")}")
    }
  }

  private def countModulo(rowCount: Long, modulo: Int): Long = {
    if (rowCount <= modulo) {
      0L
    } else {
      ((rowCount - 1 - modulo) / 10) + 1
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

  private def nanosToMillis(nanos: Long): Double = nanos.toDouble / (1000 * 1000)

  private def formatMillis(value: Double): String =
    String.format(Locale.ROOT, "%.3f", Double.box(value))

  private def sanitize(name: String): String =
    name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-")

  private def stopSpark(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
      sparkSession = null
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
