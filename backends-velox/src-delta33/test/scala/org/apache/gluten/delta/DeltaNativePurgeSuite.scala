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
package org.apache.gluten.delta

import org.apache.gluten.config.VeloxDeltaConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.delta.{DeletionVectorsTableFeature, DeltaLog}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.DeltaReorgTableCommand
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.{CommandExecutionMode, CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{AlterTableExec, GlutenDeltaLeafV2CommandExec, GlutenDeltaRunnableCommand}

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

/**
 * REORG TABLE ... APPLY (PURGE), and the deletion vector purge of ALTER TABLE ... DROP FEATURE
 * deletionVectors, rewrite files with the Velox writer. Vanilla Delta sets up each table, so the
 * files a purge adds show which writer it used.
 */
class DeltaNativePurgeSuite extends DeltaSQLCommandTest {

  // Delta 4.4 writes partition columns into the data files by default, which the Velox writer
  // does not; Delta 3.3 ignores this setting.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet", "false")

  private val numRows = 20

  private val expectedRows =
    (0 until numRows).filter(_ % 3 != 0).map(i => Row(i.toLong, i.toString, i % 2)).toSet

  /** Creates a table whose files all have deletion vectors, with vanilla Delta. */
  private def createTableWithDVs(
      path: String,
      partitioned: Boolean,
      properties: (String, String)*): Unit = {
    val partitionedBy = if (partitioned) "PARTITIONED BY (part)" else ""
    val tblProperties = (("delta.enableDeletionVectors" -> "true") +: properties)
      .map { case (k, v) => s"'$k' = '$v'" }
      .mkString(", ")
    withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
      sql(
        s"CREATE TABLE delta.`$path` (id BIGINT, value STRING, part INT) USING delta " +
          s"$partitionedBy TBLPROPERTIES ($tblProperties)")
      spark
        .range(0, numRows, 1, 2)
        .selectExpr("id", "CAST(id AS STRING) AS value", "CAST(id % 2 AS INT) AS part")
        .write
        .format("delta")
        .mode("append")
        .save(path)
      sql(s"DELETE FROM delta.`$path` WHERE id % 3 = 0")
    }
    val files = allFiles(path)
    assert(files.nonEmpty && files.forall(_.deletionVector != null), files)
  }

  private def allFiles(path: String): Seq[AddFile] =
    DeltaLog.forTable(spark, path).update().allFiles.collect().toSeq

  private def rows(path: String): Set[Row] =
    spark.read.format("delta").load(path).collect().toSet

  // Vanilla Delta writes with parquet-mr; the Velox writer does not.
  private def writtenByVelox(path: String, file: AddFile): Boolean = {
    val hadoopConf = DeltaLog.forTable(spark, path).newDeltaHadoopConf()
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(DeltaFileOperations.absolutePath(path, file.path), hadoopConf))
    try {
      !reader.getFooter.getFileMetaData.getCreatedBy.startsWith("parquet-mr")
    } finally {
      reader.close()
    }
  }

  /** Checks that no file has a deletion vector and that the purge's files came from `velox`. */
  private def assertPurged(path: String, before: Seq[AddFile], velox: Boolean): Unit = {
    val after = allFiles(path)
    assert(after.forall(_.deletionVector == null), after)
    val added = after.filterNot(file => before.exists(_.path == file.path))
    assert(added.nonEmpty, after)
    added.foreach(file => assert(writtenByVelox(path, file) == velox, file.path))
  }

  /** The physical plan of the command that `sql` ran. */
  private def commandPlan(df: DataFrame): SparkPlan = df.queryExecution.executedPlan match {
    case result: CommandResultExec => result.commandPhysicalPlan
    case plan => plan
  }

  /** The physical plan of a command, without running it. */
  private def planned(sqlText: String): SparkPlan = spark.sessionState
    .executePlan(spark.sessionState.sqlParser.parsePlan(sqlText), CommandExecutionMode.SKIP)
    .executedPlan

  private def isOffloaded(plan: SparkPlan): Boolean = plan match {
    case ExecutedCommandExec(_: GlutenDeltaRunnableCommand) => true
    case _: GlutenDeltaLeafV2CommandExec => true
    case _ => false
  }

  test("REORG PURGE rewrites the files with deletion vectors natively") {
    withTempDir {
      nativeDir =>
        withTempDir {
          vanillaDir =>
            val path = nativeDir.getCanonicalPath
            createTableWithDVs(path, partitioned = false)
            val before = allFiles(path)

            val reorg = sql(s"REORG TABLE delta.`$path` APPLY (PURGE)")
            assert(isOffloaded(commandPlan(reorg)), commandPlan(reorg))
            assertPurged(path, before, velox = true)
            val commit = DeltaLog.forTable(spark, path).history.getHistory(Some(1)).head
            assert(commit.operation == "REORG", commit)
            assert(
              commit.operationMetrics.flatMap(_.get("numDeletionVectorsRemoved")) ==
                Some(before.size.toString),
              commit)

            val vanillaPath = vanillaDir.getCanonicalPath
            createTableWithDVs(vanillaPath, partitioned = false)
            val vanillaBefore = allFiles(vanillaPath)
            withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
              val vanillaReorg = sql(s"REORG TABLE delta.`$vanillaPath` APPLY (PURGE)")
              assert(!isOffloaded(commandPlan(vanillaReorg)), commandPlan(vanillaReorg))
            }
            assertPurged(vanillaPath, vanillaBefore, velox = false)

            assert(rows(path) == rows(vanillaPath))
            assert(rows(path) == expectedRows)
        }
    }
  }

  test("REORG PURGE of a partitioned table rewrites the selected partitions natively") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTableWithDVs(path, partitioned = true)
        val before = allFiles(path)

        val reorg = sql(s"REORG TABLE delta.`$path` WHERE part = 1 APPLY (PURGE)")
        assert(isOffloaded(commandPlan(reorg)), commandPlan(reorg))
        val (kept, added) = allFiles(path).partition(file => before.exists(_.path == file.path))
        assert(
          kept.map(_.path).toSet ==
            before.filter(_.partitionValues("part") == "0").map(_.path).toSet)
        assert(kept.forall(_.deletionVector != null), kept)
        assert(added.nonEmpty && added.forall(_.partitionValues("part") == "1"), added)
        assert(added.forall(_.deletionVector == null), added)
        added.foreach(file => assert(writtenByVelox(path, file), file.path))
        assert(rows(path) == expectedRows)

        sql(s"REORG TABLE delta.`$path` APPLY (PURGE)")
        assertPurged(path, before, velox = true)
        assert(rows(path) == expectedRows)
    }
  }

  test("REORG PURGE of a column-mapped table keeps Delta's writer") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTableWithDVs(path, partitioned = false, "delta.columnMapping.mode" -> "name")
        val before = allFiles(path)

        val reorg = sql(s"REORG TABLE delta.`$path` APPLY (PURGE)")
        assert(!isOffloaded(commandPlan(reorg)), commandPlan(reorg))
        assertPurged(path, before, velox = false)
        assert(rows(path) == expectedRows)
    }
  }

  test("REORG PURGE of a clustered table keeps Delta's path") {
    withTable("reorg_clustered") {
      sql("CREATE TABLE reorg_clustered (id BIGINT, value STRING) USING delta CLUSTER BY (id)")
      val plan = planned("REORG TABLE reorg_clustered APPLY (PURGE)")
      assert(!isOffloaded(plan), plan)
    }
  }

  test("REORG UPGRADE UNIFORM keeps Delta's path") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTableWithDVs(path, partitioned = false)

        val plan = planned(
          s"REORG TABLE delta.`$path` APPLY (UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = 2))")
        assert(
          plan match {
            case ExecutedCommandExec(_: DeltaReorgTableCommand) => true
            case _ => false
          },
          plan)
    }
  }

  test("DROP FEATURE deletionVectors purges the deletion vectors natively") {
    assume(DeletionVectorsTableFeature.isRemovable, "Delta 3.3 cannot drop deletion vectors")
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTableWithDVs(path, partitioned = false)
        val before = allFiles(path)
        val otherFeature = planned(s"ALTER TABLE delta.`$path` DROP FEATURE v2Checkpoint")
        assert(otherFeature.isInstanceOf[AlterTableExec], otherFeature)

        val drop = sql(s"ALTER TABLE delta.`$path` DROP FEATURE deletionVectors")
        assert(isOffloaded(commandPlan(drop)), commandPlan(drop))
        assertPurged(path, before, velox = true)
        val protocol = DeltaLog.forTable(spark, path).update().protocol
        assert(!protocol.isFeatureSupported(DeletionVectorsTableFeature), protocol)
        assert(rows(path) == expectedRows)
    }
  }
}
