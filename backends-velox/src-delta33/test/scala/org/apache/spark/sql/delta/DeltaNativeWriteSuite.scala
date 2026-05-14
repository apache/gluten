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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.config.VeloxDeltaConfig

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec, GlutenDeltaRunnableCommand}
import org.apache.spark.sql.internal.SQLConf

class DeltaNativeWriteSuite extends DeltaSQLCommandTest {

  import testImplicits._

  private lazy val isMac = sys.props
    .get("os.name")
    .exists(_.toLowerCase(java.util.Locale.ROOT).contains("mac"))

  private def withNativeWriteOffloadConf(f: => Unit): Unit = {
    val confs = Seq(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
      DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false"
    ) ++
      (if (isMac) {
         Seq(GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false")
       } else {
         Seq.empty
       })

    withSQLConf(confs: _*) {
      f
    }
  }

  private def hasGlutenDeltaWriteCommand(plan: SparkPlan): Boolean = {
    val nativeClassMatch = plan
      .collectFirst {
        case ExecutedCommandExec(_: GlutenDeltaLeafRunnableCommand) => true
        case ExecutedCommandExec(_: GlutenDeltaRunnableCommand) => true
        case _: GlutenDeltaLeafV2CommandExec => true
      }
      .getOrElse(false)

    val nativeNodeMatch = plan
      .collectFirst {
        case p if p.nodeName.startsWith("Execute GlutenDelta ") => true
        case p if p.nodeName.startsWith("GlutenDelta ") => true
      }
      .getOrElse(false)

    val nativeTreeMatch = plan.treeString.contains("GlutenDelta ")

    nativeClassMatch || nativeNodeMatch || nativeTreeMatch
  }

  private def assertContainsNativeWriteCommand(plan: SparkPlan, context: String): Unit = {
    assert(
      hasGlutenDeltaWriteCommand(plan),
      s"Expected native delta write command for $context, but got plan:\n${plan.treeString}"
    )
  }

  private def assertNoNativeWriteCommand(plan: SparkPlan, context: String): Unit = {
    assert(
      !hasGlutenDeltaWriteCommand(plan),
      s"Expected no native delta write command for $context, but got plan:\n${plan.treeString}"
    )
  }

  private def files(deltaLog: DeltaLog): Set[AddFile] = {
    deltaLog.update().allFiles.collect().toSet
  }

  private def collectOptimizeMetrics(df: DataFrame): OptimizeMetrics = {
    val metrics = df.select("metrics.*").as[OptimizeMetrics].collect()
    assert(metrics.length == 1, s"Expected one OPTIMIZE result row, got ${metrics.length}")
    metrics.head
  }

  private def assertOptimizeCommit(deltaLog: DeltaLog, context: String): Unit = {
    val latestCommit = deltaLog.history.getHistory(Some(1)).head
    assert(
      latestCommit.operation == "OPTIMIZE",
      s"Expected latest Delta operation for $context to be OPTIMIZE, got " +
        latestCommit.operation)
  }

  private def assertCompactionMetrics(
      metrics: OptimizeMetrics,
      beforeFileCount: Int,
      afterFileCount: Int,
      context: String,
      expectedPartitionsOptimized: Option[Long] = None): Unit = {
    assert(metrics.numFilesRemoved > 0, s"Expected files removed for $context")
    assert(metrics.numFilesAdded > 0, s"Expected files added for $context")
    assert(
      afterFileCount < beforeFileCount,
      s"Expected fewer active files after $context, before=$beforeFileCount after=$afterFileCount")
    assert(
      metrics.numFilesRemoved > metrics.numFilesAdded,
      s"Expected $context to compact to fewer files, removed=${metrics.numFilesRemoved} " +
        s"added=${metrics.numFilesAdded}"
    )
    assert(
      metrics.filesRemoved.totalFiles == metrics.numFilesRemoved,
      s"Removed file metrics did not match numFilesRemoved for $context")
    assert(
      metrics.filesAdded.totalFiles == metrics.numFilesAdded,
      s"Added file metrics did not match numFilesAdded for $context")
    assert(metrics.filesRemoved.totalSize > 0, s"Expected removed file size metrics for $context")
    assert(metrics.filesAdded.totalSize > 0, s"Expected added file size metrics for $context")
    assert(metrics.numBatches > 0, s"Expected at least one optimize batch for $context")
    expectedPartitionsOptimized.foreach {
      expected =>
        assert(
          metrics.partitionsOptimized == expected,
          s"Expected $expected optimized partitions for $context, got " +
            metrics.partitionsOptimized)
    }
  }

  test("native delta optimize command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          spark.range(0, 32, 1, 4).toDF("id").write.format("delta").mode("append").save(path)
          spark.range(32, 64, 1, 4).toDF("id").write.format("delta").mode("append").save(path)

          val deltaLog = DeltaLog.forTable(spark, path)
          val beforeFiles = files(deltaLog)

          val optimizeDf = sql(s"OPTIMIZE delta.`$path`")
          assertContainsNativeWriteCommand(optimizeDf.queryExecution.executedPlan, "OPTIMIZE")
          val metrics = collectOptimizeMetrics(optimizeDf)

          val afterFiles = files(deltaLog)
          assertCompactionMetrics(metrics, beforeFiles.size, afterFiles.size, "path OPTIMIZE")
          assertOptimizeCommit(deltaLog, "path OPTIMIZE")
          val result = spark.read.format("delta").load(path)
          assert(result.collect().map(_.getLong(0)).toSet == (0L until 64L).toSet)
      }
    }
  }

  test("native delta optimize table command should be offloaded") {
    withNativeWriteOffloadConf {
      withTable("delta_native_optimize_table") {
        spark
          .range(0, 32, 1, 4)
          .toDF("id")
          .write
          .format("delta")
          .mode("overwrite")
          .saveAsTable("delta_native_optimize_table")
        spark
          .range(32, 64, 1, 4)
          .toDF("id")
          .write
          .format("delta")
          .mode("append")
          .saveAsTable("delta_native_optimize_table")

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_native_optimize_table"))
        val beforeFiles = files(deltaLog)

        val optimizeDf = sql("OPTIMIZE delta_native_optimize_table")
        assertContainsNativeWriteCommand(optimizeDf.queryExecution.executedPlan, "OPTIMIZE table")
        val metrics = collectOptimizeMetrics(optimizeDf)

        val afterFiles = files(deltaLog)
        assertCompactionMetrics(metrics, beforeFiles.size, afterFiles.size, "table OPTIMIZE")
        assertOptimizeCommit(deltaLog, "table OPTIMIZE")
        val result = spark.read.table("delta_native_optimize_table")
        assert(result.collect().map(_.getLong(0)).toSet == (0L until 64L).toSet)
      }
    }
  }

  test("native delta optimize partition predicate command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          spark
            .range(0, 20, 1, 4)
            .selectExpr("id", "cast(id % 2 as int) as part")
            .write
            .format("delta")
            .partitionBy("part")
            .mode("append")
            .save(path)
          spark
            .range(20, 40, 1, 4)
            .selectExpr("id", "cast(id % 2 as int) as part")
            .write
            .format("delta")
            .partitionBy("part")
            .mode("append")
            .save(path)

          val deltaLog = DeltaLog.forTable(spark, path)
          val beforeFiles = files(deltaLog)
          val beforePart0Paths = beforeFiles
            .filter(_.partitionValues.get("part").contains("0"))
            .map(_.path)
          val beforePart1Count = beforeFiles.count(_.partitionValues.get("part").contains("1"))

          val optimizeDf = sql(s"OPTIMIZE delta.`$path` WHERE part = 1")
          assertContainsNativeWriteCommand(
            optimizeDf.queryExecution.executedPlan,
            "OPTIMIZE WHERE")
          val metrics = collectOptimizeMetrics(optimizeDf)

          val afterFiles = files(deltaLog)
          val afterPart0Paths = afterFiles
            .filter(_.partitionValues.get("part").contains("0"))
            .map(_.path)
          val afterPart1Count = afterFiles.count(_.partitionValues.get("part").contains("1"))
          assert(
            beforePart0Paths.subsetOf(afterPart0Paths),
            "OPTIMIZE WHERE part = 1 should not remove files from part = 0")
          assert(
            afterPart1Count < beforePart1Count,
            s"Expected fewer active files in part = 1, before=$beforePart1Count " +
              s"after=$afterPart1Count")
          assertCompactionMetrics(
            metrics,
            beforeFiles.size,
            afterFiles.size,
            "partition predicate OPTIMIZE",
            expectedPartitionsOptimized = Some(1L))
          assertOptimizeCommit(deltaLog, "partition predicate OPTIMIZE")
          val result = spark.read.format("delta").load(path)
          assert(result.select("id").collect().map(_.getLong(0)).toSet == (0L until 40L).toSet)
          assert(result.where("part = 0").count() == 20)
          assert(result.where("part = 1").count() == 20)
      }
    }
  }

  test("delta optimize command should not be offloaded when native write is disabled") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          spark.range(0, 10, 1, 2).toDF("id").write.format("delta").mode("append").save(path)
          spark.range(10, 20, 1, 2).toDF("id").write.format("delta").mode("append").save(path)

          withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
            val optimizeDf = sql(s"OPTIMIZE delta.`$path`")
            assertNoNativeWriteCommand(
              optimizeDf.queryExecution.executedPlan,
              "OPTIMIZE with native write disabled")
            optimizeDf.collect()
          }

          val result = spark.read.format("delta").load(path)
          assert(result.collect().map(_.getLong(0)).toSet == (0L until 20L).toSet)
      }
    }
  }
}
