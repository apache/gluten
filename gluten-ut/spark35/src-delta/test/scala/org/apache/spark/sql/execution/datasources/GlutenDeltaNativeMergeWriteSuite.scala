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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenQueryTest}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction, TransactionExecutionObserver}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener

import io.delta.tables.DeltaTable
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class GlutenDeltaNativeMergeWriteSuite extends GlutenQueryTest with SharedSparkSession {

  private val mergeWriteKey = "spark.gluten.sql.columnar.backend.velox.delta.enableNativeMergeWrite"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite", "true")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.optimizeWrite.enabled", "false")
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
  }

  private def capturePlans(f: => Unit): Seq[SparkPlan] = {
    val plans = new ConcurrentLinkedQueue[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit =
        plans.add(qe.executedPlan)
      override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      f
      spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.asScala.toSeq
  }

  // This source also compiles for other backends, which do not provide the command class.
  private def hasNativeMerge(plans: Seq[SparkPlan]): Boolean = plans.exists(_.exists {
    case ExecutedCommandExec(command) =>
      command.getClass.getSimpleName == "GlutenDeltaMergeIntoCommand"
    case _ => false
  })

  private def dataFiles(path: String): Map[String, AddFile] =
    DeltaLog.forTable(spark, path).update().allFiles.collect().map(f => f.path -> f).toMap

  private def createdBy(path: String, file: AddFile): String = {
    val filePath = DeltaFileOperations.absolutePath(path, file.path)
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(filePath, spark.sessionState.newHadoopConf()))
    try {
      reader.getFileMetaData.getCreatedBy
    } finally {
      reader.close()
    }
  }

  private def readDelta(path: String): DataFrame = spark.read.format("delta").load(path)

  /**
   * Creates a target and an identical twin without Gluten, runs `merge` on both (the twin again
   * without Gluten), then checks how the target's MERGE was planned, which writer produced the
   * files it added, and that both tables read back the same.
   */
  private def checkMerge(
      createTable: String => Unit,
      merge: String => Unit,
      nativePlan: Boolean,
      nativeFiles: Boolean,
      read: String => DataFrame = readDelta): Unit = {
    withTempDir {
      targetDir =>
        withTempDir {
          twinDir =>
            val target = targetDir.getCanonicalPath
            val twin = twinDir.getCanonicalPath
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              createTable(target)
              createTable(twin)
              merge(twin)
            }
            val before = dataFiles(target).keySet
            val plans = capturePlans(merge(target))
            assert(
              hasNativeMerge(plans) == nativePlan,
              s"Expected native MERGE plan = $nativePlan:\n${plans.map(_.treeString).mkString}")
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val added = dataFiles(target).filter { case (path, _) => !before.contains(path) }
              assert(added.nonEmpty, "MERGE added no data files")
              added.values.foreach {
                file =>
                  val writer = createdBy(target, file)
                  assert(
                    !writer.startsWith("parquet-mr") == nativeFiles,
                    s"Expected native file = $nativeFiles, ${file.path} was written by $writer")
              }
              checkAnswer(read(target), read(twin).collect().toSeq)
            }
        }
    }
  }

  /** Returns a function that creates a Delta table at a path and inserts `rows` into it. */
  private def table(
      schema: String,
      rows: String,
      properties: String = "",
      partitionBy: String = ""): String => Unit = {
    path =>
      val partitioning = if (partitionBy.isEmpty) "" else s"PARTITIONED BY ($partitionBy)"
      val tblProperties = if (properties.isEmpty) "" else s"TBLPROPERTIES ($properties)"
      sql(s"CREATE TABLE delta.`$path` ($schema) USING delta $partitioning $tblProperties")
      sql(s"INSERT INTO delta.`$path` $rows")
  }

  private def withSource(query: String)(f: => Unit): Unit = {
    withTempView("src") {
      sql(query).createOrReplaceTempView("src")
      f
    }
  }

  private val idValueTable = table("id INT, v STRING", "SELECT id, concat('v', id) FROM range(10)")

  private val updateDeleteInsert: String => Unit =
    path => sql(s"""
                   |MERGE INTO delta.`$path` t USING src s ON t.id = s.id
                   |WHEN MATCHED AND s.op = 'delete' THEN DELETE
                   |WHEN MATCHED THEN UPDATE SET t.v = s.v
                   |WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
                   |""".stripMargin)

  private val idValueOpSource =
    "SELECT * FROM VALUES (1, 'x', 'delete'), (2, 'u2', 'update'), (3, 'u3', 'update'), " +
      "(10, 'n10', 'insert'), (11, 'n11', 'insert') AS s(id, v, op)"

  test("MERGE with update, delete and insert writes native files") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withSource(idValueOpSource) {
      checkMerge(idValueTable, updateDeleteInsert, nativePlan = true, nativeFiles = true)
    }
  }

  test("insert-only MERGE writes native files") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withSource(idValueOpSource) {
      checkMerge(
        idValueTable,
        path => sql(s"""
                       |MERGE INTO delta.`$path` t USING src s ON t.id = s.id
                       |WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
                       |""".stripMargin),
        nativePlan = true,
        nativeFiles = true
      )
    }
  }

  for (repartition <- Seq(true, false)) {
    test(s"MERGE into a partitioned table writes native files: repartition=$repartition") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withSQLConf(
        "spark.databricks.delta.merge.repartitionBeforeWrite.enabled" -> repartition.toString) {
        withSource(
          "SELECT * FROM VALUES (1, 'x', 'delete', 'a'), (2, 'u2', 'update', 'b'), " +
            "(10, 'n10', 'insert', NULL), (11, 'n11', 'insert', 'c') AS s(id, v, op, p)") {
          checkMerge(
            table(
              "id INT, v STRING, p STRING",
              "SELECT id, concat('v', id), CASE id % 3 WHEN 0 THEN 'a' WHEN 1 THEN 'b' END " +
                "FROM range(10)",
              partitionBy = "p"
            ),
            path => sql(s"""
                           |MERGE INTO delta.`$path` t USING src s ON t.id = s.id
                           |WHEN MATCHED AND s.op = 'delete' THEN DELETE
                           |WHEN MATCHED THEN UPDATE SET t.v = s.v, t.p = s.p
                           |WHEN NOT MATCHED THEN INSERT (id, v, p) VALUES (s.id, s.v, s.p)
                           |""".stripMargin),
            nativePlan = true,
            nativeFiles = true
          )
        }
      }
    }
  }

  test("MERGE with change data feed writes native files and the same changes") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withSource(idValueOpSource) {
      checkMerge(
        table(
          "id INT, v STRING",
          "SELECT id, concat('v', id) FROM range(10)",
          "'delta.enableChangeDataFeed' = 'true'"),
        path => sql(s"""
                       |MERGE INTO delta.`$path` t USING src s ON t.id = s.id
                       |WHEN MATCHED THEN UPDATE SET t.v = s.v
                       |WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
                       |WHEN NOT MATCHED BY SOURCE AND t.id > 7 THEN DELETE
                       |""".stripMargin),
        nativePlan = true,
        nativeFiles = true,
        read = path =>
          spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "2")
            .load(path)
            .drop("_commit_timestamp")
      )
    }
  }

  test("MERGE twice with deletion vectors writes native files") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withSource(idValueOpSource) {
      checkMerge(
        table(
          "id INT, v STRING",
          "SELECT id, concat('v', id) FROM range(10)",
          "'delta.enableDeletionVectors' = 'true'"),
        path => {
          updateDeleteInsert(path)
          assert(dataFiles(path).values.exists(_.deletionVector != null))
          sql(s"MERGE INTO delta.`$path` t USING src s ON t.id = s.id + 1 " +
            "WHEN MATCHED THEN UPDATE SET t.v = concat(t.v, '!')")
        },
        nativePlan = true,
        nativeFiles = true
      )
    }
  }

  test("DeltaTable merge API writes native files") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withSource("SELECT * FROM VALUES (2, 'u2'), (10, 'n10') AS s(id, v)") {
      checkMerge(
        idValueTable,
        path =>
          DeltaTable
            .forPath(spark, path)
            .as("t")
            .merge(spark.table("src").as("s"), "t.id = s.id")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute(),
        nativePlan = true,
        nativeFiles = true
      )
    }
  }

  // Tables or settings the native writer does not reproduce faithfully yet.
  private val unsupportedTables: Seq[(String, String => Unit, Seq[(String, String)])] = Seq(
    ("native MERGE write disabled", idValueTable, Seq(mergeWriteKey -> "false")),
    (
      "column mapping in name mode",
      table(
        "id INT, v STRING",
        "SELECT id, concat('v', id) FROM range(10)",
        "'delta.columnMapping.mode' = 'name'"),
      Seq.empty),
    (
      "DOUBLE partition column",
      table(
        "id INT, v STRING, p DOUBLE",
        "SELECT id, concat('v', id), CAST(id % 2 AS DOUBLE) FROM range(10)",
        partitionBy = "p"),
      Seq.empty),
    (
      "nested field name with upper-case letters",
      table(
        "id INT, v STRING, s STRUCT<Value: INT>",
        "SELECT id, concat('v', id), named_struct('Value', id) FROM range(10)"),
      Seq.empty),
    (
      "statistics collection disabled",
      idValueTable,
      Seq("spark.databricks.delta.stats.collect" -> "false"))
  )

  for ((name, create, confs) <- unsupportedTables) {
    test(s"MERGE keeps Delta's writer: $name") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withSource(idValueOpSource) {
        checkMerge(
          create,
          path => withSQLConf(confs: _*)(updateDeleteInsert(path)),
          nativePlan = false,
          nativeFiles = false)
      }
    }
  }

  test("MERGE keeps Delta's writer under another transaction observer") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    val observer = new TransactionExecutionObserver {
      override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = f
      override def preparingCommit[T](f: => T): T = f
      override def beginDoCommit(): Unit = ()
      override def beginBackfill(): Unit = ()
      override def beginPostCommit(): Unit = ()
      override def transactionCommitted(): Unit = ()
      override def transactionAborted(): Unit = ()
      override def createChild(): TransactionExecutionObserver = this
    }
    withSource(idValueOpSource) {
      checkMerge(
        idValueTable,
        path => TransactionExecutionObserver.withObserver(observer)(updateDeleteInsert(path)),
        nativePlan = true,
        nativeFiles = false)
    }
  }
}
