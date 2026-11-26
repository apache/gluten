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
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.constraints.{DeltaInvariantCheckerExec, GlutenDeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.files.GlutenDeltaFileFormatWriter
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec, GlutenDeltaRunnableCommand}
import org.apache.spark.sql.internal.SQLConf

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile

import scala.collection.JavaConverters._

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

  private def footers(path: String): Seq[(AddFile, ParquetMetadata)] = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    files(deltaLog).toSeq.map {
      file =>
        val reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(DeltaFileOperations.absolutePath(path, file.path), hadoopConf))
        try {
          file -> reader.getFooter
        } finally {
          reader.close()
        }
    }
  }

  // Vanilla Spark writes with parquet-mr; the Velox writer does not.
  private def writtenByVelox(footer: ParquetMetadata): Boolean =
    !footer.getFileMetaData.getCreatedBy.startsWith("parquet-mr")

  private def appendTenRows(path: String): Unit = {
    spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) AS value")
      .coalesce(1)
      .write
      .format("delta")
      .mode("append")
      .save(path)
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

  test("a table without column mapping is written natively") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          sql(s"CREATE TABLE delta.`$path` (id BIGINT, value STRING) USING delta")
          appendTenRows(path)

          val written = footers(path)
          assert(written.nonEmpty)
          written.foreach {
            case (file, footer) =>
              assert(writtenByVelox(footer), s"${file.path}: ${footer.getFileMetaData}")
          }
      }
    }
  }

  for (mode <- Seq("name", "id")) {
    test(s"$mode column mapping falls back and writes Parquet field IDs") {
      withNativeWriteOffloadConf {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            sql(
              s"CREATE TABLE delta.`$path` (id BIGINT, value STRING) USING delta " +
                s"TBLPROPERTIES ('delta.columnMapping.mode' = '$mode')")
            appendTenRows(path)

            val written = footers(path)
            assert(written.nonEmpty)
            written.foreach {
              case (file, footer) =>
                assert(!writtenByVelox(footer), s"${file.path}: ${footer.getFileMetaData}")
                assert(
                  footer.getFileMetaData.getSchema.getFields.asScala.forall(_.getId != null),
                  s"${file.path}: ${footer.getFileMetaData.getSchema}")
            }
            // Spark's reader matches columns by field ID under id mapping, and fails on files
            // without them.
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              assert(
                spark.read.format("delta").load(path).collect().toSet ==
                  (0 until 10).map(i => Row(i.toLong, i.toString)).toSet)
            }
        }
      }
    }
  }

  test("native delta write keeps the case of nested field names") {
    withNativeWriteOffloadConf {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            spark
              .range(3)
              .selectExpr(
                "id",
                "named_struct('userId', id, 'homeAddr', named_struct('zipCode', id * 10)) AS Info",
                "array(named_struct('itemId', id)) AS itemList",
                "map(CAST(id AS STRING), named_struct('itemCount', id)) AS itemsByKey"
              )
              .write
              .format("delta")
              .save(path)

            val nestedNames = Seq("userId", "homeAddr", "zipCode", "itemId", "itemCount")
            val written = footers(path).map {
              case (file, footer) =>
                (file.path, footer.getFileMetaData.getCreatedBy, footer.getFileMetaData.getSchema)
            }
            assert(written.nonEmpty)
            written.foreach {
              case (file, createdBy, schema) =>
                assert(!createdBy.startsWith("parquet-mr"), s"$file was not written by Velox")
                val names = parquetFieldNames(schema)
                assert(nestedNames.forall(names.contains), names)
                assert(!nestedNames.map(_.toLowerCase).exists(names.contains), names)
            }
            // A case-sensitive reader finds the nested fields by their original names.
            withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
              val rows = spark.read
                .format("delta")
                .load(path)
                .orderBy("id")
                .selectExpr(
                  "Info.userId",
                  "Info.homeAddr.zipCode",
                  "itemList[0].itemId",
                  "itemsByKey")
              assert(
                rows.collect().toSeq ==
                  (0L until 3L).map(i => Row(i, i * 10, i, Map(i.toString -> Row(i)))))
            }
        }
      }
    }
  }

  private def parquetFieldNames(t: org.apache.parquet.schema.Type): Seq[String] =
    t.getName +: (if (t.isPrimitive) Nil
                  else t.asGroupType.getFields.asScala.toSeq.flatMap(parquetFieldNames))

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

  test("native delta write keeps DeltaInvariantCheckerExec for CHECK constraints") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          sql(
            s"CREATE TABLE delta.`$path` (id BIGINT NOT NULL, s STRING) USING delta " +
              "TBLPROPERTIES ('delta.constraints.positive' = 'id > 0')")
          GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
          appendRows(path, (1L, "a"))
          assert(writerPlan.find(_.isInstanceOf[DeltaInvariantCheckerExec]).isDefined, writerPlan)
          assert(
            writerPlan.find(_.isInstanceOf[GlutenDeltaInvariantCheckerExec]).isEmpty,
            writerPlan)
      }
    }
  }

  test("native partitioned delta write with concurrent writers or presorted input") {
    // A string partition column goes through the project that turns empty strings into nulls.
    val rows =
      Seq((1L, "a", 1), (2L, "b", 2), (3L, "a", 1), (4L, "c", 3), (5L, "", 4), (6L, null, 4))
    for {
      maxWriters <- Seq("0", "2")
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
            withSQLConf(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS.key -> maxWriters) {
              GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
              val df = rows.toDF("id", "s", "p")
              val input = if (presorted) df.sortWithinPartitions(partitionColumn) else df
              input.write.format("delta").partitionBy(partitionColumn).save(path)
              assert(!writerPlan.treeString.contains("ColumnarToRow"), writerPlan)
              // The writer sorts unsorted input; presorted input keeps the user's sort only.
              assert(writerPlan.collect { case s: SortExecTransformer => s }.size == 1, writerPlan)
            }
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
