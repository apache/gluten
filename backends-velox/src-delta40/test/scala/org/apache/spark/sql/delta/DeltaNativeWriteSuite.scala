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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec, GlutenDeltaRunnableCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}

import scala.jdk.CollectionConverters._

class DeltaNativeWriteSuite extends DeltaSQLCommandTest {

  import testImplicits._

  private lazy val isMac = sys.props
    .get("os.name")
    .exists(_.toLowerCase(java.util.Locale.ROOT).contains("mac"))

  private def withNativeWriteOffloadConf[T](f: => T): T = {
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
      assert(
        !spark.sessionState.conf.ansiEnabled,
        s"${SQLConf.ANSI_ENABLED.key} should be false in native write tests")
      assert(
        spark.sessionState.conf.sessionLocalTimeZone == "UTC",
        s"${SQLConf.SESSION_LOCAL_TIMEZONE.key} should be UTC in native write tests")
      assert(
        !spark.sessionState.conf
          .getConfString(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key)
          .toBoolean,
        s"${GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key} should be false in native write tests"
      )
      assert(
        !spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS),
        s"${DeltaSQLConf.DELTA_COLLECT_STATS.key} should be false in native write tests")
      if (isMac) {
        assert(
          !spark.sessionState.conf
            .getConfString(GlutenConfig.NATIVE_VALIDATION_ENABLED.key)
            .toBoolean,
          s"${GlutenConfig.NATIVE_VALIDATION_ENABLED.key} should be false on macOS"
        )
      }
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

  private def collectExecutedPlans(action: => Unit): Seq[SparkPlan] = {
    val plans = new CopyOnWriteArrayList[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plans.add(qe.executedPlan)
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)
    try {
      action
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
      var lastSize = -1
      var stableSince = System.nanoTime()
      while (System.nanoTime() < deadline) {
        val size = plans.size()
        val now = System.nanoTime()
        if (size != lastSize) {
          lastSize = size
          stableSince = now
        }
        if (size > 0 && now - stableSince >= TimeUnit.MILLISECONDS.toNanos(500)) {
          return plans.asScala.toSeq
        }
        Thread.sleep(50)
      }
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.asScala.toSeq
  }

  private def assertContainsNativeWriteCommand(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      plans.exists(hasGlutenDeltaWriteCommand),
      s"Expected native delta write command for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  private def assertNoNativeWriteCommand(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      !plans.exists(hasGlutenDeltaWriteCommand),
      s"Expected no native delta write command for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
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

  test("native delta delete command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value").write.format("delta").save(path)

          val deleteDf = sql(s"DELETE FROM delta.`$path` WHERE id = 1")
          assertContainsNativeWriteCommand(Seq(deleteDf.queryExecution.executedPlan), "DELETE")
          deleteDf.collect()

          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(2, "b"), Row(3, "c")))
      }
    }
  }

  test("native delta update command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          Seq((1, "a"), (2, "b")).toDF("id", "value").write.format("delta").save(path)

          val updateDf = sql(s"UPDATE delta.`$path` SET value = 'bb' WHERE id = 2")
          assertContainsNativeWriteCommand(Seq(updateDf.queryExecution.executedPlan), "UPDATE")
          updateDf.collect()

          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "bb")))
      }
    }
  }

  test("native delta CTAS command should be offloaded") {
    withNativeWriteOffloadConf {
      withTable("delta_native_write_ctas") {
        val ctasDf = sql(
          "CREATE TABLE delta_native_write_ctas USING delta AS " +
            "SELECT id, concat('v', cast(id as string)) AS value FROM range(1, 4)")
        assertContainsNativeWriteCommand(Seq(ctasDf.queryExecution.executedPlan), "CTAS")
        ctasDf.collect()

        val result = sql("SELECT * FROM delta_native_write_ctas ORDER BY id")
        assert(result.collect().toSeq == Seq(Row(1L, "v1"), Row(2L, "v2"), Row(3L, "v3")))
      }
    }
  }

  test("native delta RTAS command should be offloaded") {
    withNativeWriteOffloadConf {
      withTable("delta_native_write_rtas") {
        sql(
          "CREATE TABLE delta_native_write_rtas USING delta AS " +
            "SELECT id, concat('v', cast(id as string)) AS value FROM range(1, 4)")
          .collect()

        val rtasDf = sql(
          "REPLACE TABLE delta_native_write_rtas USING delta AS " +
            "SELECT id, concat('r', cast(id as string)) AS value FROM range(2, 5)")
        assertContainsNativeWriteCommand(Seq(rtasDf.queryExecution.executedPlan), "RTAS")
        rtasDf.collect()

        val result = sql("SELECT * FROM delta_native_write_rtas ORDER BY id")
        assert(result.collect().toSeq == Seq(Row(2L, "r2"), Row(3L, "r3"), Row(4L, "r4")))
      }
    }
  }

  test("native delta save command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val plans = collectExecutedPlans {
            Seq((1, "a"), (2, "b"))
              .toDF("id", "value")
              .write
              .format("delta")
              .mode("overwrite")
              .save(path)
          }

          assertContainsNativeWriteCommand(plans, "DataFrameWriter.save(overwrite)")
          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "b")))
      }
    }
  }

  test("native delta append save command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          Seq((1, "a")).toDF("id", "value").write.format("delta").mode("overwrite").save(path)

          val plans = collectExecutedPlans {
            Seq((2, "b"), (3, "c"))
              .toDF("id", "value")
              .write
              .format("delta")
              .mode("append")
              .save(path)
          }

          assertContainsNativeWriteCommand(plans, "DataFrameWriter.save(append)")
          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "b"), Row(3, "c")))
      }
    }
  }

  test("native delta partitioned save command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val plans = collectExecutedPlans {
            Seq((1, "a", 0), (2, "b", 1))
              .toDF("id", "value", "part")
              .write
              .format("delta")
              .partitionBy("part")
              .mode("overwrite")
              .save(path)
          }

          assertContainsNativeWriteCommand(plans, "partitioned DataFrameWriter.save(overwrite)")
          val result = spark.read.format("delta").load(path)
          assert(
            result.select("id", "value", "part").collect().toSet == Set(
              Row(1, "a", 0),
              Row(2, "b", 1)))
      }
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
          assertContainsNativeWriteCommand(Seq(optimizeDf.queryExecution.executedPlan), "OPTIMIZE")
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
        assertContainsNativeWriteCommand(
          Seq(optimizeDf.queryExecution.executedPlan),
          "OPTIMIZE table")
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
            Seq(optimizeDf.queryExecution.executedPlan),
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
              Seq(optimizeDf.queryExecution.executedPlan),
              "OPTIMIZE with native write disabled")
            optimizeDf.collect()
          }

          val result = spark.read.format("delta").load(path)
          assert(result.collect().map(_.getLong(0)).toSet == (0L until 20L).toSet)
      }
    }
  }

  test("delta save command should not be offloaded when native write is disabled") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val plans = withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
            collectExecutedPlans {
              Seq((1, "a"), (2, "b"))
                .toDF("id", "value")
                .write
                .format("delta")
                .mode("overwrite")
                .save(path)
            }
          }

          assertNoNativeWriteCommand(
            plans,
            "DataFrameWriter.save(overwrite) with native write disabled")
          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "b")))
      }
    }
  }
}
