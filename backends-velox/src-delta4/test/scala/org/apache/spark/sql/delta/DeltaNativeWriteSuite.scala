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
import org.apache.gluten.execution.SortExecTransformer

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Empty2Null
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.constraints.{DeltaInvariantCheckerExec, GlutenDeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.files.GlutenDeltaFileFormatWriter
import org.apache.spark.sql.delta.perf.GlutenDeltaOptimizedWriterExec
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec, GlutenDeltaRunnableCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructType}
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
      DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false",
      // Delta writes partition columns into the data files by default, which the native
      // writer does not do yet; keep partitioned tables on the native writer here.
      "spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet" -> "false"
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

  test("native delta write checks the constraints of an optimized write natively") {
    withNativeWriteOffloadConf {
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            sql(
              s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING) USING delta " +
                "PARTITIONED BY (s) TBLPROPERTIES ('delta.constraints.positive' = 'id > 0')")
            GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
            appendRows(path, (1L, "a"), (2L, "b"))
            assertConstraintsCheckedOnVeloxBatches()
            // The rebalancing shuffle runs in Velox, after the checks.
            assert(
              writerPlan.find(_.isInstanceOf[GlutenDeltaOptimizedWriterExec]).isDefined,
              writerPlan)
            def violate(): Unit = appendRows(path, (3L, "a"), (-4L, "b"))
            val nativeMessage = violationMessage(nativeWrite = true)(violate())
            assert(nativeMessage == violationMessage(nativeWrite = false)(violate()))
            assert(nativeMessage.contains(" - id : -4"))
        }
      }
    }
  }

  test("native delta write checks the constraints of replaceWhere, UPDATE and OPTIMIZE natively") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          sql(
            s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING) USING delta " +
              "PARTITIONED BY (s) TBLPROPERTIES ('delta.constraints.positive' = 'id > 0')")
          appendRows(path, (1L, "a"), (2L, "b"), (3L, "a"))

          def replaceWhere(id: Long, s: String): Unit = Seq((id, s))
            .toDF("id", "s")
            .write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", "s = 'a'")
            .save(path)
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          replaceWhere(4L, "a")
          assertConstraintsCheckedOnVeloxBatches()
          // replaceWhere adds a CHECK that the written rows match its predicate. Delta reports a
          // violation of any check of the write as DELTA_REPLACE_WHERE_MISMATCH, with the violation
          // as its cause.
          def replaceWhereMessage(nativeWrite: Boolean)(write: => Unit): String = {
            var message = ""
            withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> nativeWrite.toString) {
              val e = intercept[DeltaAnalysisException](write)
              assert(e.getCause.isInstanceOf[InvariantViolationException], e)
              message = e.getMessage
            }
            message
          }
          val outside = replaceWhereMessage(nativeWrite = true)(replaceWhere(5L, "b"))
          assert(outside == replaceWhereMessage(nativeWrite = false)(replaceWhere(5L, "b")))
          assert(outside.contains("(s = 'a')"))
          val negative = replaceWhereMessage(nativeWrite = true)(replaceWhere(-5L, "a"))
          assert(negative == replaceWhereMessage(nativeWrite = false)(replaceWhere(-5L, "a")))
          assert(negative.contains(" - id : -5"))

          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          sql(s"UPDATE delta.`$path` SET id = id + 10 WHERE s = 'b'")
          assertConstraintsCheckedOnVeloxBatches()
          def updateToNegative(): Unit = sql(s"UPDATE delta.`$path` SET id = -1 WHERE s = 'b'")
          val updateMessage = violationMessage(nativeWrite = true)(updateToNegative())
          assert(updateMessage == violationMessage(nativeWrite = false)(updateToNegative()))
          assert(updateMessage.contains(" - id : -1"))

          // Two more files in partition `a` for OPTIMIZE to compact.
          appendRows(path, (5L, "a"))
          appendRows(path, (6L, "a"))
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          sql(s"OPTIMIZE delta.`$path`")
          assertConstraintsCheckedOnVeloxBatches()
          assert(
            spark.read.format("delta").load(path).orderBy("id").collect().toSeq ==
              Seq(Row(4L, "a"), Row(5L, "a"), Row(6L, "a"), Row(12L, "b")))
      }
    }
  }

  test("native delta write checks legacy column invariants natively") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val invariant = new MetadataBuilder()
            .putString("delta.invariants", """{"expression":{"expression":"value < 3"}}""")
            .build()
          val schema = new StructType()
            .add("id", LongType)
            .add("value", IntegerType, nullable = true, invariant)
          def write(id: Long, value: Int): Unit = spark
            .createDataFrame(java.util.Arrays.asList(Row(id, value)), schema)
            .write
            .format("delta")
            .mode("append")
            .save(path)
          write(1L, 1)
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          write(2L, 2)
          assertConstraintsCheckedOnVeloxBatches()
          val nativeMessage = violationMessage(nativeWrite = true)(write(3L, 5))
          assert(nativeMessage == violationMessage(nativeWrite = false)(write(3L, 5)))
          assert(nativeMessage.contains(" - value : 5"))
      }
    }
  }

  test("native delta write finds a violation in a later batch of a task") {
    withNativeWriteOffloadConf {
      withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "4") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            sql(
              s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING) USING delta " +
                "TBLPROPERTIES ('delta.constraints.positive' = 'id > 0')")
            GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
            appendRows(path, (1L, "a"))
            assertConstraintsCheckedOnVeloxBatches()
            // One task of 12 rows in batches of 4; the bad row is the 10th.
            def violate(): Unit = spark
              .range(0, 12, 1, 1)
              .selectExpr("IF(id = 9, -id, id + 1) AS id", "CAST(id % 2 AS STRING) AS s")
              .write
              .format("delta")
              .mode("append")
              .save(path)
            val nativeMessage = violationMessage(nativeWrite = true)(violate())
            assert(nativeMessage == violationMessage(nativeWrite = false)(violate()))
            assert(nativeMessage.contains(" - id : -9"))
            assert(spark.read.format("delta").load(path).count() == 1)
        }
      }
    }
  }

  test("native delta write checks CHECK constraints on decimal, temporal, array and map columns") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          sql(
            s"CREATE TABLE delta.`$path` (id BIGINT, dec DECIMAL(10, 2), ntz TIMESTAMP_NTZ, " +
              "d DATE, arr ARRAY<INT>, m MAP<STRING, INT>) USING delta TBLPROPERTIES (" +
              "'delta.constraints.c_dec' = 'dec > 0', " +
              "'delta.constraints.c_ntz' = \"ntz > TIMESTAMP_NTZ'2000-01-01 00:00:00'\", " +
              "'delta.constraints.c_d' = 'd <= current_date()', " +
              "'delta.constraints.c_arr' = 'size(arr) > 0', " +
              "'delta.constraints.c_m' = 'cardinality(m) > 0')")
          def write(dec: String, ntz: String, d: String, arr: String, m: String): Unit = spark
            .sql(
              s"SELECT 1L AS id, CAST($dec AS DECIMAL(10, 2)) AS dec, TIMESTAMP_NTZ'$ntz' AS ntz, " +
                s"DATE'$d' AS d, CAST($arr AS ARRAY<INT>) AS arr, CAST($m AS MAP<STRING, INT>) AS m")
            .write
            .format("delta")
            .mode("append")
            .save(path)
          val good = Seq("1.5", "2026-01-01 00:00:00", "2026-01-01", "array(1)", "map('k', 1)")
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          write(good(0), good(1), good(2), good(3), good(4))
          assertConstraintsCheckedOnVeloxBatches()
          // An array or map value prints as its object. A map's hash code differs per run.
          def withoutHashCodes(message: String): String = message.replaceAll("@[0-9a-f]+", "@")
          Seq(
            0 -> "-1.5" -> "c_dec",
            1 -> "1990-01-01 00:00:00" -> "c_ntz",
            2 -> "2999-01-01" -> "c_d",
            3 -> "array()" -> "c_arr",
            4 -> "map()" -> "c_m").foreach {
            case ((column, value), constraint) =>
              val values = good.updated(column, value)
              def violate(): Unit = write(values(0), values(1), values(2), values(3), values(4))
              val nativeMessage = withoutHashCodes(violationMessage(nativeWrite = true)(violate()))
              assert(
                nativeMessage == withoutHashCodes(violationMessage(nativeWrite = false)(violate())))
              assert(nativeMessage.contains(s"CHECK constraint $constraint "), nativeMessage)
          }
          assert(spark.read.format("delta").load(path).count() == 1)
      }
    }
  }

  private def appendRows(path: String, rows: (java.lang.Long, String)*): Unit = {
    rows.toDF("id", "s").write.format("delta").mode("append").save(path)
  }

  /** The plan of the last write by the Velox writer. */
  private def writerPlan: SparkPlan = GlutenDeltaFileFormatWriter.getExecutedPlanForTesting
    .getOrElse(fail("Expected a write by the Velox writer"))

  test("native delta write checks top-level NOT NULL constraints natively") {
    for (optimizeWrite <- Seq(false, true)) {
      withNativeWriteOffloadConf {
        withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> optimizeWrite.toString) {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              sql(s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING) USING delta")
              GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
              appendRows(path, (1L, "a"))
              assert(
                writerPlan.find(_.isInstanceOf[GlutenDeltaInvariantCheckerExec]).isDefined,
                writerPlan)
              assert(writerPlan.find(_.isInstanceOf[DeltaInvariantCheckerExec]).isEmpty, writerPlan)
              assert(!writerPlan.treeString.contains("ColumnarToRow"), writerPlan)

              val e = intercept[InvariantViolationException] {
                appendRows(path, (2L, "b"), (null, "c"))
              }
              assert(e.getMessage.contains("NOT NULL constraint violated for column: id"))
              // An optimized write of several input partitions shuffles the checked batches
              // natively, which keeps only the text of the violation.
              val shuffled = intercept[InvariantViolationException] {
                spark
                  .range(0, 1000, 1, 4)
                  .repartition(4)
                  .selectExpr("IF(id = 777, CAST(NULL AS BIGINT), id) AS id", "'x' AS s")
                  .write
                  .format("delta")
                  .mode("append")
                  .save(path)
              }
              assert(shuffled.getMessage.contains("NOT NULL constraint violated for column: id"))
              assert(spark.read.format("delta").load(path).collect().toSeq == Seq(Row(1L, "a")))
          }
        }
      }
    }
  }

  /** The message of the violation that `write` raises, with the native write on or off. */
  private def violationMessage(nativeWrite: Boolean)(write: => Unit): String = {
    var message = ""
    withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> nativeWrite.toString) {
      message = intercept[InvariantViolationException](write).getMessage
    }
    message
  }

  /** Creates a table with a NOT NULL field in a nullable struct and a generated date partition. */
  private def createGeneratedColumnTable(path: String, changeDataFeed: Boolean = false): Unit = {
    // Inside this package, DeltaTable is Delta's internal object.
    val builder = io.delta.tables.DeltaTable
      .create(spark)
      .location(path)
      .addColumn("id", "BIGINT")
      .addColumn(
        io.delta.tables.DeltaTable
          .columnBuilder(spark, "meta")
          .dataType(new StructType().add("tracking", StringType, nullable = false))
          .build())
      .addColumn("ts", "TIMESTAMP")
      .addColumn(
        io.delta.tables.DeltaTable
          .columnBuilder(spark, "d")
          .dataType("STRING")
          .generatedAlwaysAs("date_format(ts, 'yyyy-MM-dd')")
          .build())
      .partitionedBy("d")
    if (changeDataFeed) {
      builder.property("delta.enableChangeDataFeed", "true").execute()
    } else {
      builder.execute()
    }
  }

  /** Asserts that the last write by the Velox writer checked its constraints on Velox batches. */
  private def assertConstraintsCheckedOnVeloxBatches(): Unit = {
    assert(writerPlan.find(_.isInstanceOf[GlutenDeltaInvariantCheckerExec]).isDefined, writerPlan)
    assert(writerPlan.find(_.isInstanceOf[DeltaInvariantCheckerExec]).isEmpty, writerPlan)
    // Delta's Empty2Null projection of the string partition columns runs natively too.
    assert(
      writerPlan.find {
        case p: ProjectExec => p.projectList.exists(_.exists(_.isInstanceOf[Empty2Null]))
        case _ => false
      }.isEmpty,
      writerPlan)
    // No batch is converted to rows. (A local relation's rows are converted to batches first.)
    assert(!writerPlan.treeString.contains("ColumnarToRow"), writerPlan)
  }

  test("native delta write checks CHECK constraints on the Velox batches") {
    for (partitioned <- Seq(false, true)) {
      withNativeWriteOffloadConf {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            sql(
              s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING) USING delta " +
                (if (partitioned) "PARTITIONED BY (s) " else "") +
                "TBLPROPERTIES ('delta.constraints.positive' = 'id > 0')")
            GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
            appendRows(path, (1L, "a"))
            assertConstraintsCheckedOnVeloxBatches()

            // On a partitioned table the writer's native sort pulls the checked batches, which
            // keeps only the text of an exception. The write still fails with Delta's own error.
            def violate(): Unit = appendRows(path, (2L, "b"), (-3L, "c"))
            val nativeMessage = violationMessage(nativeWrite = true)(violate())
            assert(nativeMessage == violationMessage(nativeWrite = false)(violate()))
            assert(nativeMessage.contains("CHECK constraint positive (id > 0) violated by row"))
            assert(nativeMessage.contains(" - id : -3"))
            // A task of a later input partition finds the violation while it checks whether it
            // has rows to write.
            def violateInLastPartition(): Unit = spark
              .range(0, 1000, 1, 4)
              .selectExpr("IF(id = 999, -id, id + 1) AS id", "CAST(id % 3 AS STRING) AS s")
              .write
              .format("delta")
              .mode("append")
              .save(path)
            val lastPartitionMessage =
              violationMessage(nativeWrite = true)(violateInLastPartition())
            assert(lastPartitionMessage == violationMessage(nativeWrite =
              false)(violateInLastPartition()))
            assert(lastPartitionMessage.contains(" - id : -999"))
            assert(spark.read.format("delta").load(path).collect().toSeq == Seq(Row(1L, "a")))
        }
      }
    }
  }

  test("native delta write turns an empty partition string into NULL before the checks") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          sql(
            s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING NOT NULL) USING delta " +
              "PARTITIONED BY (s) TBLPROPERTIES ('delta.constraints.positive' = 'id > 0')")
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          appendRows(path, (1L, "a"))
          assertConstraintsCheckedOnVeloxBatches()

          // Delta writes an empty partition string as NULL, which breaks NOT NULL on `s`.
          def emptyPartition(): Unit = appendRows(path, (3L, ""))
          val nativeMessage = violationMessage(nativeWrite = true)(emptyPartition())
          assert(nativeMessage == violationMessage(nativeWrite = false)(emptyPartition()))
          assert(nativeMessage.contains("NOT NULL constraint violated for column: s"))
          assert(spark.read.format("delta").load(path).collect().toSeq == Seq(Row(1L, "a")))
      }
    }
  }

  test("native delta write checks nested NOT NULL and generated column constraints natively") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          createGeneratedColumnTable(path)
          // Writing the generated column adds a CHECK that it matches its expression.
          def append(id: Long, tracking: String, ts: String, d: String): Unit = {
            Seq((id, tracking, ts, d))
              .toDF("id", "tracking", "ts", "d")
              .selectExpr(
                "id",
                "named_struct('tracking', tracking) AS meta",
                "CAST(ts AS TIMESTAMP) AS ts",
                "d")
              .write
              .format("delta")
              .mode("append")
              .save(path)
          }
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          append(1L, "t1", "2026-04-01 10:00:00", "2026-04-01")
          assertConstraintsCheckedOnVeloxBatches()

          def nullTracking(): Unit = append(2L, null, "2026-04-01 11:00:00", "2026-04-01")
          val notNullMessage = violationMessage(nativeWrite = true)(nullTracking())
          assert(notNullMessage == violationMessage(nativeWrite = false)(nullTracking()))
          assert(notNullMessage.contains("NOT NULL constraint violated for column: meta.tracking"))

          // A NULL struct breaks the NOT NULL constraint of its field too, as in Delta.
          def nullMeta(): Unit = Seq((4L, "2026-04-01 13:00:00", "2026-04-01"))
            .toDF("id", "ts", "d")
            .selectExpr(
              "id",
              "CAST(NULL AS STRUCT<tracking: STRING>) AS meta",
              "CAST(ts AS TIMESTAMP) AS ts",
              "d")
            .write
            .format("delta")
            .mode("append")
            .save(path)
          val nullMetaMessage = violationMessage(nativeWrite = true)(nullMeta())
          assert(nullMetaMessage == violationMessage(nativeWrite = false)(nullMeta()))
          assert(
            nullMetaMessage.contains("NOT NULL constraint violated for column: meta.tracking"))

          def wrongDate(): Unit = append(3L, "t3", "2026-04-01 12:00:00", "2026-04-02")
          val generatedMessage = violationMessage(nativeWrite = true)(wrongDate())
          assert(generatedMessage == violationMessage(nativeWrite = false)(wrongDate()))
          assert(generatedMessage.contains("CHECK constraint Generated Column"))
          assert(generatedMessage.contains(" - d : 2026-04-02"))

          val rows = spark.read.format("delta").load(path).selectExpr("id", "meta.tracking", "d")
          assert(rows.collect().toSeq == Seq(Row(1L, "t1", "2026-04-01")))
      }
    }
  }

  test("native delta DELETE with change data feed checks generated columns natively") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          createGeneratedColumnTable(path, changeDataFeed = true)
          spark
            .range(0, 100, 1, 1)
            .selectExpr(
              "id",
              "named_struct('tracking', CAST(id AS STRING)) AS meta",
              "CAST('2026-04-01 10:00:00' AS TIMESTAMP) AS ts")
            .write
            .format("delta")
            .mode("append")
            .save(path)
          // The DELETE rewrites the file with the generated column, so Delta adds its CHECK.
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          sql(s"DELETE FROM delta.`$path` WHERE id % 10 = 0")
          assertConstraintsCheckedOnVeloxBatches()
          assert(spark.read.format("delta").load(path).count() == 90)
          val deleted = spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "2")
            .load(path)
            .where("_change_type = 'delete'")
          assert(deleted.count() == 10)
      }
    }
  }

  test("native partitioned delta write with presorted input") {
    // A string partition column goes through the project that turns empty strings into nulls.
    val rows =
      Seq((1L, "a", 1), (2L, "b", 2), (3L, "a", 1), (4L, "c", 3), (5L, "", 4), (6L, null, 4))
    for {
      partitionColumn <- Seq("s", "p")
      presorted <- Seq(false, true)
    } {
      val expected = rows.map {
        case (id, "", p) if partitionColumn == "s" => Row(id, null, p)
        case row => Row.fromTuple(row)
      }
      withNativeWriteOffloadConf {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
            val df = rows.toDF("id", "s", "p")
            val input = if (presorted) df.sortWithinPartitions(partitionColumn) else df
            input.write.format("delta").partitionBy(partitionColumn).save(path)
            assert(!writerPlan.treeString.contains("ColumnarToRow"), writerPlan)
            // The writer sorts unsorted input; presorted input keeps the user's sort only.
            assert(writerPlan.collect { case s: SortExecTransformer => s }.size == 1, writerPlan)
            assert(
              spark.read.format("delta").load(path).select("id", "s", "p").orderBy("id")
                .collect().toSeq == expected)
        }
      }
    }
  }

  test("native delta write of input sorted before the empty-to-null project") {
    def check(rows: Seq[(Long, String, Int)], partitionBy: Seq[String], writerSorts: Boolean)(
        sortBy: Column*): Unit = {
      withNativeWriteOffloadConf {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            // One task and one row per batch, so a partition that comes back after another
            // partition gets a second file.
            withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "1") {
              GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
              spark.sparkContext
                .parallelize(rows, 1)
                .toDF("id", "s", "p")
                .sortWithinPartitions(sortBy: _*)
                .write
                .format("delta")
                .partitionBy(partitionBy: _*)
                .save(path)
              assert(!writerPlan.treeString.contains("ColumnarToRow"), writerPlan)
            }
            val partitions = files(DeltaLog.forTable(spark, path)).toSeq.map(_.partitionValues)
            assert(partitions.size == 2 && partitions.distinct.size == 2, partitions)
            val sorts = writerPlan.collect { case s: SortExecTransformer => s }
            assert(sorts.size == (if (writerSorts) 2 else 1), writerPlan)
        }
      }
    }
    // Empty2Null turns these into (null, 1), (null, 2), (null, 1), (null, 2).
    val twoColumns = Seq((1L, null, 1), (2L, null, 2), (3L, "", 1), (4L, "", 2))
    check(twoColumns, Seq("s", "p"), writerSorts = true)($"s", $"p")
    val oneColumn = Seq((1L, "", 1), (2L, "a", 1), (3L, null, 1))
    // These become null, "a", null.
    check(oneColumn, Seq("s"), writerSorts = true)($"s".asc_nulls_last)
    check(oneColumn, Seq("s"), writerSorts = true)($"s".desc_nulls_first)
    // These stay grouped: null, null, "a" and "a", null, null.
    check(oneColumn, Seq("s"), writerSorts = false)($"s")
    check(oneColumn, Seq("s"), writerSorts = false)($"s".desc_nulls_last)
    // The same with s after another partition column.
    check(oneColumn, Seq("p", "s"), writerSorts = true)($"p", $"s".asc_nulls_last)
    check(oneColumn, Seq("p", "s"), writerSorts = false)($"p", $"s")
  }

  test("native delta write checks NOT NULL constraints on a partitioned table") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          sql(
            s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING NOT NULL) USING delta " +
              "PARTITIONED BY (s)")
          appendRows(path, (1L, "a"))
          // The writer's native sort pulls the checked batches, and keeps only the text of the
          // violation. The write still fails with Delta's exception.
          val dataViolation = intercept[InvariantViolationException] {
            appendRows(path, (2L, "a"), (null, "b"))
          }
          assert(dataViolation.getMessage.contains("NOT NULL constraint violated for column: id"))
          // Delta turns an empty partition string into a null before checking it.
          val partitionViolation = intercept[InvariantViolationException] {
            appendRows(path, (3L, ""))
          }
          assert(
            partitionViolation.getMessage.contains("NOT NULL constraint violated for column: s"))
          assert(spark.read.format("delta").load(path).collect().toSeq == Seq(Row(1L, "a")))
          // With this conf off, Delta checks the empty string itself, which is not null.
          withSQLConf(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL.key -> "false") {
            appendRows(path, (4L, ""))
          }
      }
    }
  }
}
