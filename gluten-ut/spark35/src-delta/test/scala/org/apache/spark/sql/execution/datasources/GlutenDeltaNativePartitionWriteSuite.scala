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
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.SparkVersionUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.crypto.{DecryptionPropertiesFactory, EncryptionPropertiesFactory, FileDecryptionProperties, FileEncryptionProperties}
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.json4s.{JInt, JNothing}
import org.json4s.jackson.JsonMethods.parse

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class GlutenDeltaNativePartitionWriteSuite extends GlutenQueryTest with SharedSparkSession {

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
      .set("spark.sql.maxConcurrentOutputFileWriters", "0")
      // Delta 4.4+ writes partition columns into the data files by default, which makes the write
      // fall back to vanilla Delta. Older Delta ignores this unknown table property default.
      .set("spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet", "false")
  }

  test("native Delta write preserves analysis of non-streaming queries") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    checkAnswer(spark.sql("SELECT 1 AS id"), Seq(Row(1)))
  }

  // Small batches cross partition boundaries; a large batch requires splitting a single stripe.
  for ((batchSize, limit, collectStats) <- Seq((4, 4, false), (16, 4, true), (4, 0, true))) {
    test(s"native Delta partitioned files: batch=$batchSize, limit=$limit, stats=$collectStats") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      val schema = new StructType().add("id", LongType).add("part", IntegerType)
      val expected = Seq((null, 3), (Int.box(0), 4), (Int.box(1), 9)).flatMap {
        case (part, count) =>
          (0 until count).map(i => Row(if (i % 3 == 0) null else Long.box(i / 2), part))
      }
      withSQLConf(
        GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> batchSize.toString,
        "spark.databricks.delta.stats.collect" -> collectStats.toString,
        "spark.sql.jsonGenerator.ignoreNullFields" -> "true"
      ) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            val plans = new ConcurrentLinkedQueue[SparkPlan]()
            val listener = new QueryExecutionListener {
              override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit =
                plans.add(qe.executedPlan)
              override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = {}
            }
            spark.listenerManager.register(listener)
            try {
              spark.createDataFrame(spark.sparkContext.parallelize(expected, 1), schema)
                .write.format("delta").partitionBy("part")
                .option("maxRecordsPerFile", limit.toString).save(path)
              spark.sparkContext.listenerBus.waitUntilEmpty(10000)
            } finally {
              spark.listenerManager.unregister(listener)
            }
            // This source also compiles for other backends, which do not provide these classes.
            assert(
              plans.asScala.exists(_.exists {
                case ExecutedCommandExec(command) =>
                  Set("GlutenDeltaLeafRunnableCommand", "GlutenDeltaRunnableCommand")
                    .contains(command.getClass.getSimpleName)
                case plan => plan.getClass.getSimpleName == "GlutenDeltaLeafV2CommandExec"
              }),
              s"Expected a native Delta write:\n${plans.asScala.map(_.treeString).mkString}"
            )

            // Read the physical files with Spark to independently check layout and per-file stats.
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
              val fileSizes = files.toSeq.map {
                file =>
                  val filePath = DeltaFileOperations.absolutePath(path, file.path).toString
                  val reader = ParquetFileReader.open(
                    HadoopInputFile.fromPath(
                      DeltaFileOperations.absolutePath(path, file.path),
                      spark.sessionState.newHadoopConf()))
                  try {
                    // Vanilla Spark writes with parquet-mr; the Velox writer does not.
                    assert(
                      !reader.getFooter.getFileMetaData.getCreatedBy.startsWith("parquet-mr"),
                      s"${file.path} was not written by the native writer")
                  } finally {
                    reader.close()
                  }
                  val rows = spark.read.parquet(filePath).select("id").collect().toSeq
                  if (collectStats) {
                    val stats = parse(file.stats)
                    val values = rows.filterNot(_.isNullAt(0)).map(_.getLong(0))
                    assert((stats \ "numRecords") == JInt(rows.size), file.stats)
                    assert(
                      (stats \ "nullCount" \ "id") == JInt(rows.size - values.size),
                      file.stats)
                    assert(
                      (stats \ "minValues" \ "id") ==
                        (if (values.isEmpty) JNothing else JInt(values.min)),
                      file.stats)
                    assert(
                      (stats \ "maxValues" \ "id") ==
                        (if (values.isEmpty) JNothing else JInt(values.max)),
                      file.stats)
                  } else {
                    assert(file.stats == null || file.stats.isEmpty)
                  }
                  file.partitionValues("part") -> rows.size
              }.groupBy(_._1).map {
                case (part, sizes) => part -> sizes.map(_._2).sorted
              }
              // Exact sizes detect premature rotation as well as files exceeding the limit.
              assert(fileSizes == Map(
                (null, Seq(3)),
                "0" -> Seq(4),
                "1" -> (if (limit == 0) Seq(9) else Seq(1, 4, 4))))
              checkAnswer(spark.read.format("delta").load(path).select("id", "part"), expected)
            }
        }
      }
    }
  }

  for (
    partitioned <- Seq(false, true);
    collectStats <- Seq(false, true);
    nativeWrite <- Seq(false, true);
    nativeStreamingWrite <- Seq(false, true)
  ) {
    test(s"Delta streaming appends: partitioned=$partitioned, stats=$collectStats, " +
      s"nativeWrite=$nativeWrite, nativeStreamingWrite=$nativeStreamingWrite") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      import testImplicits._
      // Spark 4.x also compiles this suite, but native streaming writes only support Spark 3.5.
      val expectNativeWrite = nativeWrite && nativeStreamingWrite &&
        SparkVersionUtil.compareMajorMinorVersion((3, 5)) == 0
      withSQLConf(
        "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite" -> nativeWrite.toString,
        "spark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite" ->
          nativeStreamingWrite.toString,
        "spark.databricks.delta.stats.collect" -> collectStats.toString,
        "spark.sql.jsonGenerator.ignoreNullFields" -> "true"
      ) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath + "/data"
            val input = MemoryStream[(Option[Long], Option[String])]
            val writer = input.toDF().toDF("id", "part")
              .filter($"id".isNull || $"id" >= 0L)
              .writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", dir.getCanonicalPath + "/checkpoint")
            val query = (if (partitioned) writer.partitionBy("part") else writer).start(path)
            try {
              input.addData((Some(1L), Some("a")), (None, Some("a")))
              query.processAllAvailable()
              input.addData((Some(2L), None), (Some(3L), Some("b")))
              query.processAllAvailable()
              val deltaLog = DeltaLog.forTable(spark, path)
              val versionBeforeEmptyBatch = deltaLog.update().version
              // Advance the source offset with an empty output batch.
              input.addData((Some(-1L), Some("discarded")))
              query.processAllAvailable()
              assert(deltaLog.update().version > versionBeforeEmptyBatch)
              assert(query.recentProgress.nonEmpty)
              assert(query.recentProgress.forall(_.sink.description == s"DeltaSink[$path]"))
            } finally {
              query.stop()
            }
            // Read with Spark so native reader behavior cannot hide a write error.
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val hadoopConf = spark.sessionState.newHadoopConf()
              val dataPath = new Path(path)
              val fileSystem = dataPath.getFileSystem(hadoopConf)
              val physicalFiles = fileSystem.listFiles(dataPath, true)
              var emptyFiles = 0
              // Statistics can exclude empty files from the log, so inspect physical files too.
              while (physicalFiles.hasNext) {
                val file = physicalFiles.next()
                if (
                  file.getPath.getName.endsWith(".parquet") &&
                  !file.getPath.toString.contains("/_delta_log/")
                ) {
                  assert(file.getLen > 0, s"Zero-byte Parquet file: ${file.getPath}")
                  val reader = ParquetFileReader.open(
                    HadoopInputFile.fromPath(file.getPath, hadoopConf))
                  try {
                    // Check the physical writer, not only a plan wrapper that could fall back.
                    val createdBy = reader.getFooter.getFileMetaData.getCreatedBy
                    val numRows = reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum
                    assert(createdBy != null, s"Missing writer metadata for ${file.getPath}")
                    // Empty tasks use Spark's writer to produce a valid schema-only Parquet file.
                    assert(
                      createdBy.startsWith("parquet-mr") == (!expectNativeWrite || numRows == 0),
                      s"Unexpected writer for ${file.getPath}: $createdBy")
                    if (numRows == 0) emptyFiles += 1
                  } finally {
                    reader.close()
                  }
                }
              }
              if (!partitioned) assert(emptyFiles > 0, "Missing schema-only empty-batch file")
              val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
              assert(files.nonEmpty)
              files.foreach {
                file =>
                  val filePath = DeltaFileOperations.absolutePath(path, file.path)
                  assert(file.size > 0, s"Committed zero-byte file: ${file.path}")
                  assert(fileSystem.getFileStatus(filePath).getLen == file.size, file.path)
                  if (collectStats) {
                    val rows = spark.read.parquet(filePath.toString).select("id").collect().toSeq
                    val values = rows.filterNot(_.isNullAt(0)).map(_.getLong(0))
                    val stats = parse(file.stats)
                    assert((stats \ "numRecords") == JInt(rows.size), file.stats)
                    assert(
                      (stats \ "nullCount" \ "id") == JInt(rows.size - values.size),
                      file.stats)
                    assert(
                      (stats \ "minValues" \ "id") ==
                        (if (values.isEmpty) JNothing else JInt(values.min)),
                      file.stats)
                    assert(
                      (stats \ "maxValues" \ "id") ==
                        (if (values.isEmpty) JNothing else JInt(values.max)),
                      file.stats)
                  } else {
                    assert(file.stats == null || file.stats.isEmpty)
                  }
              }
              checkAnswer(
                spark.read.format("delta").load(path).select("id", "part"),
                Seq(Row(1L, "a"), Row(null, "a"), Row(2L, null), Row(3L, "b")))
            }
        }
      }
    }
  }

  for (
    (option, sessionLimit, partitioned, expectedNative) <- Seq(
      (None, 1, false, false),
      (Some("1"), 0, false, false),
      (Some("0"), 1, false, true),
      (Some("-1"), 1, false, true),
      (Some("1"), 0, true, true));
    collectStats <- Seq(false, true)
  ) {
    test(s"Delta streaming safeguards: file limit option=$option, session=$sessionLimit, " +
      s"partitioned=$partitioned, stats=$collectStats") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      import testImplicits._
      withSQLConf(
        "spark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite" -> "true",
        "spark.sql.files.maxRecordsPerFile" -> sessionLimit.toString,
        "spark.databricks.delta.stats.collect" -> collectStats.toString
      ) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath + "/data"
            val input = MemoryStream[Long](numPartitions = 1)(implicitly, spark.sqlContext)
            val writer = input.toDF().selectExpr("value AS id", "0 AS part")
              .writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", dir.getCanonicalPath + "/checkpoint")
            option.foreach(value => writer.option("MaXrEcOrDsPeRfIlE", value))
            val query = (if (partitioned) writer.partitionBy("part") else writer).start(path)
            try {
              input.addData(1L, 2L)
              query.processAllAvailable()
            } finally {
              query.stop()
            }
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
              val expectNative = expectedNative &&
                SparkVersionUtil.compareMajorMinorVersion((3, 5)) == 0
              val rowCounts = files.map {
                file =>
                  assert(file.size > 0, s"Committed zero-byte file: ${file.path}")
                  val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                    DeltaFileOperations.absolutePath(path, file.path),
                    spark.sessionState.newHadoopConf()))
                  try {
                    assert(reader.getFooter.getFileMetaData.getCreatedBy
                      .startsWith("parquet-mr") == !expectNative)
                    reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum
                  } finally {
                    reader.close()
                  }
              }.sorted.toSeq
              val limit = option.map(_.toLong).getOrElse(sessionLimit.toLong)
              assert(rowCounts == (if (limit > 0) Seq(1L, 1L) else Seq(2L)))
              checkAnswer(
                spark.read.format("delta").load(path).select("id"),
                Seq(Row(1L), Row(2L)))
            }
        }
      }
    }
  }

  for (setting <- Seq("session", "hadoop", "table", "table-after-start")) {
    test(s"Delta streaming safeguards: Parquet encryption from $setting") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      import testImplicits._
      val cryptoKey = "parquet.crypto.factory.class"
      val factory = classOf[GlutenDeltaStreamingEncryptionFactory].getName
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val previousFactory = Option(hadoopConf.get(cryptoKey))
      withSQLConf(
        "spark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite" -> "true",
        "spark.databricks.delta.stats.collect" -> "false") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath + "/data"
            // Create metadata only, so no plaintext data files predate encryption.
            spark.sql(s"CREATE TABLE delta.`$path` (id BIGINT) USING delta")
            if (setting == "table") {
              spark.sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('$cryptoKey'='$factory')")
            }
            if (setting == "hadoop") hadoopConf.set(cryptoKey, factory)
            try {
              val settings = if (setting == "session") Seq(cryptoKey -> factory) else Seq.empty
              withSQLConf(settings: _*) {
                val input = MemoryStream[Long](numPartitions = 1)(implicitly, spark.sqlContext)
                val query = input.toDF().toDF("id").writeStream
                  .format("delta").outputMode("append")
                  .option("checkpointLocation", dir.getCanonicalPath + "/checkpoint").start(path)
                try {
                  query.processAllAvailable()
                  if (setting == "table-after-start") {
                    spark.sql(
                      s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('$cryptoKey'='$factory')")
                  }
                  input.addData(1L, 2L)
                  query.processAllAvailable()
                } finally {
                  query.stop()
                }
              }
            } finally {
              previousFactory match {
                case Some(value) => hadoopConf.set(cryptoKey, value)
                case None => hadoopConf.unset(cryptoKey)
              }
            }
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false", cryptoKey -> factory) {
              val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
              assert(files.nonEmpty)
              files.foreach {
                file =>
                  val filePath = DeltaFileOperations.absolutePath(path, file.path)
                  val stream = filePath.getFileSystem(spark.sessionState.newHadoopConf())
                    .open(filePath)
                  try {
                    val magic = new Array[Byte](4)
                    stream.readFully(magic)
                    assert(
                      new String(magic, StandardCharsets.US_ASCII) == "PARE",
                      s"Expected encrypted Parquet file: ${file.path}")
                  } finally {
                    stream.close()
                  }
              }
              checkAnswer(spark.read.format("delta").load(path), Seq(Row(1L), Row(2L)))
            }
        }
      }
    }
  }

  private val dateWriteCases =
    (for {
      mode <- Seq("EXCEPTION", "LEGACY", "CORRECTED")
      collectStats <- Seq(false, true)
      nativeStreaming <- Seq(false, true)
    } yield ("date", mode, collectStats, nativeStreaming)) ++
      Seq("struct", "array", "map", "partition").map((_, "EXCEPTION", false, true))

  for ((shape, mode, collectStats, nativeStreaming) <- dateWriteCases) {
    test(s"Delta streaming dates: shape=$shape, mode=$mode, stats=$collectStats, " +
      s"nativeStreaming=$nativeStreaming") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      import testImplicits._
      withSQLConf(
        "spark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite" ->
          nativeStreaming.toString,
        "spark.databricks.delta.stats.collect" -> collectStats.toString,
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> mode
      ) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath + "/data"
            val checkpoint = dir.getCanonicalPath + "/checkpoint"
            val input = MemoryStream[Long](numPartitions = 1)(implicitly, spark.sqlContext)
            val date = "DATE '1500-01-01'"
            val expression = shape match {
              case "struct" => s"named_struct('nested', $date)"
              case "array" => s"array($date)"
              case "map" => s"map('nested', $date)"
              case _ => date
            }
            val writer = input.toDF().selectExpr("value AS id", s"$expression AS d")
              .writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", checkpoint)
            val query = (if (shape == "partition") writer.partitionBy("d") else writer)
              .start(path)
            val commitPath = new Path(checkpoint + "/commits/0")
            val fs = commitPath.getFileSystem(spark.sessionState.newHadoopConf())
            try {
              input.addData(1L)
              if (mode == "EXCEPTION" && shape != "partition") {
                val error = intercept[StreamingQueryException](query.processAllAvailable())
                val causes = Iterator.iterate(error: Throwable)(_.getCause).takeWhile(_ != null)
                assert(
                  causes.exists(
                    t =>
                      Option(t.getMessage)
                        .exists(_.contains("WRITE_ANCIENT_DATETIME"))),
                  error.toString)
                assert(
                  DeltaLog.forTable(spark, path).update().version == -1L,
                  "The rejected batch committed a Delta version")
                assert(fs.exists(new Path(checkpoint + "/offsets/0")))
                assert(!fs.exists(commitPath), "The rejected batch advanced Spark's checkpoint")
              } else {
                query.processAllAvailable()
                assert(DeltaLog.forTable(spark, path).update().version == 0L)
                assert(fs.exists(commitPath))
              }
            } finally {
              query.stop()
            }
            if (mode != "EXCEPTION" || shape == "partition") {
              // Velox uses proleptic Gregorian days; Spark's legacy footer overrides this read mode.
              withSQLConf(
                GlutenConfig.GLUTEN_ENABLED.key -> "false",
                SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> "CORRECTED") {
                val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
                assert(files.nonEmpty)
                files.foreach {
                  file =>
                    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                      DeltaFileOperations.absolutePath(path, file.path),
                      spark.sessionState.newHadoopConf()))
                    try {
                      val metadata = reader.getFooter.getFileMetaData
                      val expectNative = nativeStreaming && mode != "LEGACY" &&
                        SparkVersionUtil.compareMajorMinorVersion((3, 5)) == 0
                      assert(metadata.getCreatedBy.startsWith("parquet-mr") == !expectNative)
                      if (shape != "partition") {
                        assert(metadata.getKeyValueMetaData
                          .containsKey("org.apache.spark.legacyDateTime") == (mode == "LEGACY"))
                        val columnIO = new ColumnIOFactory().getColumnIO(metadata.getSchema)
                        val records = columnIO.getRecordReader(
                          reader.readNextRowGroup(),
                          new GroupRecordConverter(metadata.getSchema))
                        val physicalDay = records.read().getInteger("d", 0)
                        assert(physicalDay == (if (mode == "LEGACY") -171655 else -171664))
                      }
                    } finally {
                      reader.close()
                    }
                }
                checkAnswer(
                  spark.read.format("delta").load(path)
                    .selectExpr("id", "CAST(d AS STRING)"),
                  Seq(Row(1L, "1500-01-01")))
              }
            }
        }
      }
    }
  }

  for (collectStats <- Seq(false, true)) {
    test(s"Delta streaming safeguards: replay committed epoch, stats=$collectStats") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withSQLConf(
        "spark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite" -> "true",
        "spark.databricks.delta.stats.collect" -> collectStats.toString) {
        withTempDir {
          dir =>
            val source = new java.io.File(dir, "source")
            assert(source.mkdir())
            def addInput(name: String, ids: Seq[Long]): Unit = {
              val json = ids.map(id => s"""{"id":$id}""").mkString("\n")
              val staged = new java.io.File(dir, name).toPath
              Files.write(staged, json.getBytes(StandardCharsets.UTF_8))
              Files.move(
                staged,
                new java.io.File(source, name).toPath,
                StandardCopyOption.ATOMIC_MOVE)
            }
            val path = dir.getCanonicalPath + "/data"
            val checkpoint = dir.getCanonicalPath + "/checkpoint"
            def startQuery() = spark.readStream.schema(new StructType().add("id", LongType))
              .json(source.getCanonicalPath).coalesce(1)
              .writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", checkpoint).start(path)
            addInput("first.json", Seq(1L, 2L))
            val first = startQuery()
            try {
              first.processAllAvailable()
            } finally {
              first.stop()
            }
            val log = DeltaLog.forTable(spark, path)
            val committed = log.update()
            val filesBeforeReplay = committed.allFiles.collect().toSet
            assert(committed.transactions(first.id.toString) == 0L)
            val commitPath = new Path(checkpoint + "/commits/0")
            val fs = commitPath.getFileSystem(spark.sessionState.newHadoopConf())
            assert(fs.exists(new Path(checkpoint + "/offsets/0")))
            // Recreate the durable state after Delta commits but before Spark records completion.
            // Keep the source offset and Delta SetTransaction; remove only Spark's commit marker.
            assert(fs.delete(commitPath, false))
            val replay = startQuery()
            try {
              assert(replay.id == first.id)
              assert(replay.runId != first.runId)
              replay.processAllAvailable()
              assert(fs.exists(commitPath), "Spark did not finish replaying epoch 0")
              val replayed = log.update()
              assert(replayed.version == committed.version)
              assert(replayed.allFiles.collect().toSet == filesBeforeReplay)
              withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
                checkAnswer(spark.read.format("delta").load(path), Seq(Row(1L), Row(2L)))
              }
              addInput("second.json", Seq(3L, 4L))
              replay.processAllAvailable()
              val next = log.update()
              assert(next.version == committed.version + 1)
              assert(next.transactions(replay.id.toString) == 1L)
              assert(next.allFiles.collect().toSet.diff(filesBeforeReplay).nonEmpty)
            } finally {
              replay.stop()
            }
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              checkAnswer(
                spark.read.format("delta").load(path),
                Seq(Row(1L), Row(2L), Row(3L), Row(4L)))
              log.update().allFiles.collect().foreach {
                file =>
                  val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                    DeltaFileOperations.absolutePath(path, file.path),
                    spark.sessionState.newHadoopConf()))
                  try {
                    val expectNative = SparkVersionUtil.compareMajorMinorVersion((3, 5)) == 0
                    assert(reader.getFooter.getFileMetaData.getCreatedBy
                      .startsWith("parquet-mr") == !expectNative)
                  } finally {
                    reader.close()
                  }
              }
            }
        }
      }
    }
  }
}

// Fixed test-only key: real encrypted files let the tests detect a plaintext native write.
class GlutenDeltaStreamingEncryptionFactory
  extends EncryptionPropertiesFactory with DecryptionPropertiesFactory {
  override def getFileEncryptionProperties(
      conf: Configuration,
      path: Path,
      context: WriteContext): FileEncryptionProperties =
    FileEncryptionProperties.builder(Array.fill[Byte](16)(1)).build()

  override def getFileDecryptionProperties(
      conf: Configuration,
      path: Path): FileDecryptionProperties =
    FileDecryptionProperties.builder().withFooterKey(Array.fill[Byte](16)(1)).build()
}
