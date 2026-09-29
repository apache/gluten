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

import org.apache.gluten.columnarbatch.VeloxColumnarBatches
import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}
import org.apache.gluten.execution.{HashAggregateExecTransformer, TerminalRow, VeloxColumnarToRowExec}
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.GlutenDeltaFileFormatWriter.GlutenDynamicPartitionDataSingleWriter
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.GlutenDeltaJobStatsTracker
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{DeltaFileOperations, JsonUtils}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory, WriteJobDescription}
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec, GlutenDeltaRunnableCommand}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources
import org.apache.spark.util.SerializableConfiguration

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, TaskAttemptID}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.collection.mutable

class DeltaNativeWriteLayoutSuite extends DeltaSQLCommandTest {

  test("partition stripes account only their own rows across input batches") {
    withRecordingWriter(maxRecords = 4) {
      (writer, factory) =>
        writeBatch(writer, Seq(10L -> 0, 11L -> 0, 12L -> 0, 20L -> 1))
        writeBatch(writer, Seq(21L -> 1, 22L -> 1, 23L -> 1))
        writer.commit()
        assert(factory.rowsByPartition == Map(
          "part=0" -> Seq(Seq(10L, 11L, 12L)),
          "part=1" -> Seq(Seq(20L, 21L, 22L, 23L))))
    }
  }

  test("partition stripes fill a partial file and split at exact file boundaries") {
    withRecordingWriter(maxRecords = 4) {
      (writer, factory) =>
        writeBatch(writer, Seq(0L -> 0, 1L -> 0))
        writeBatch(writer, (2L to 10L).map(_ -> 0) ++ Seq(20L -> 1))
        writeBatch(writer, Seq(21L -> 1, 22L -> 1, 23L -> 1, 24L -> 1))
        writer.commit()
        assert(factory.rowsByPartition == Map(
          "part=0" -> Seq(Seq(0L, 1L, 2L, 3L), Seq(4L, 5L, 6L, 7L), Seq(8L, 9L, 10L)),
          "part=1" -> Seq(Seq(20L, 21L, 22L, 23L), Seq(24L))))
    }
  }

  test("empty batches create no files and unlimited stripes share the current file") {
    withRecordingWriter(maxRecords = 0) {
      (writer, factory) =>
        writeBatch(writer, Seq.empty)
        assert(factory.writtenRows.isEmpty)
        writeBatch(writer, (0L until 5L).map(_ -> 0))
        writeBatch(writer, Seq.empty)
        writeBatch(writer, (5L until 8L).map(_ -> 0))
        writer.commit()
        assert(factory.rowsByPartition == Map("part=0" -> Seq((0L until 8L).toSeq)))
    }
  }

  for (failure <- Seq("open", "write")) {
    test(s"partition stripe cleanup after output $failure failure") {
      withRecordingWriter(maxRecords = 4, failure = Some(failure)) {
        (writer, factory) =>
          val error = intercept[IllegalStateException] {
            // The middle partition fails, leaving the final stripe unconsumed.
            writeBatch(writer, Seq(10L -> 0, 20L -> 1, 30L -> 2))
          }
          assert(error.getMessage == s"injected $failure failure")
          writer.abort()
          assert(factory.closeCounts.values.forall(_ == 1))
          assert(factory.closeCounts.keySet == factory.writtenRows.keySet)
      }
    }
  }

  private class Carrier(val value: ColumnarBatch) extends TerminalRow {
    override def batch(): ColumnarBatch = value
    override def withNewBatch(batch: ColumnarBatch): TerminalRow = new Carrier(batch)
  }

  private def writeBatch(
      writer: GlutenDynamicPartitionDataSingleWriter,
      rows: Seq[(Long, Int)]): Unit = {
    val schema = new StructType().add("id", LongType).add("part", IntegerType)
    val vectors = ArrowWritableColumnVector.allocateColumns(rows.size, schema)
    rows.zipWithIndex.foreach {
      case ((id, part), index) =>
        vectors(0).putLong(index, id)
        vectors(1).putInt(index, part)
    }
    val batch = VeloxColumnarBatches.ensureVeloxBatch(
      new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), rows.size))
    try {
      writer.write(new Carrier(batch))
    } finally {
      batch.close()
    }
  }

  private class RecordingWriterFactory(failure: Option[String]) extends OutputWriterFactory {
    val writtenRows = mutable.LinkedHashMap.empty[String, mutable.ArrayBuffer[Long]]
    val closeCounts = mutable.Map.empty[String, Int]

    def rowsByPartition: Map[String, Seq[Seq[Long]]] = writtenRows.toSeq
      .groupBy { case (path, _) => new Path(path).getParent.getName }
      .map { case (part, files) => part -> files.map(_._2.toSeq) }

    override def getFileExtension(context: TaskAttemptContext): String = ".recording"

    override def newInstance(
        outputPath: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
      if (failure.contains("open") && outputPath.contains("part=1/")) {
        throw new IllegalStateException("injected open failure")
      }
      val rows = mutable.ArrayBuffer.empty[Long]
      writtenRows(outputPath) = rows
      new OutputWriter {
        private val converter = new VeloxColumnarToRowExec.Converter(new SQLMetric("convertTime"))
        override def write(row: InternalRow): Unit = {
          if (failure.contains("write") && outputPath.contains("part=1/")) {
            throw new IllegalStateException("injected write failure")
          }
          val batch = row.asInstanceOf[TerminalRow].batch()
          converter.toRowIterator(batch).foreach(value => rows += value.getLong(0))
        }
        override def close(): Unit = {
          closeCounts(outputPath) = closeCounts.getOrElse(outputPath, 0) + 1
          converter.close()
        }
        override def path(): String = outputPath
      }
    }
  }

  private def withRecordingWriter(
      maxRecords: Long,
      failure: Option[String] = None)(
      f: (GlutenDynamicPartitionDataSingleWriter, RecordingWriterFactory) => Unit): Unit = {
    // Initialize the plugin before entering a task resource scope on the test thread.
    val conf = spark.sessionState.newHadoopConf()
    withTempDir {
      dir =>
        TaskResources.runUnsafe {
          val root = dir.getCanonicalPath
          val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
          val factory = new RecordingWriterFactory(failure)
          val id = AttributeReference("id", LongType)()
          val part = AttributeReference("part", IntegerType)()
          val description = new WriteJobDescription(
            uuid = "native-layout-test",
            serializableHadoopConf = new SerializableConfiguration(conf),
            outputWriterFactory = factory,
            allColumns = Seq(id, part),
            dataColumns = Seq(id),
            partitionColumns = Seq(part),
            bucketSpec = None,
            path = root,
            customPartitionLocations = Map.empty,
            maxRecordsPerFile = maxRecords,
            timeZoneId = "UTC",
            statsTrackers = Seq.empty
          )
          val committer = new FileCommitProtocol {
            override def setupJob(context: JobContext): Unit = {}
            override def commitJob(context: JobContext, commits: Seq[TaskCommitMessage]): Unit = {}
            override def abortJob(context: JobContext): Unit = {}
            override def setupTask(context: TaskAttemptContext): Unit = {}
            override def commitTask(context: TaskAttemptContext): TaskCommitMessage =
              new TaskCommitMessage(null)
            override def abortTask(context: TaskAttemptContext): Unit = {}
            override def newTaskTempFile(
                context: TaskAttemptContext,
                partition: Option[String],
                extension: String): String =
              new Path(new Path(root, partition.getOrElse("")), s"data$extension").toString
            override def newTaskTempFileAbsPath(
                context: TaskAttemptContext,
                directory: String,
                extension: String): String =
              new Path(directory, s"data$extension").toString
          }
          val writer = new GlutenDynamicPartitionDataSingleWriter(description, context, committer)
          try {
            f(writer, factory)
          } finally {
            try {
              writer.abort()
            } finally {
              writer.close()
            }
          }
        }
    }
  }

  private val dataSchema = new StructType()
    .add("id", LongType)
    .add("value", LongType)
    .add("label", StringType)
    .add("all_null", LongType)
    .add("part", IntegerType)

  // Repeated rows make set-based comparisons insufficient. Partitions and batches have unequal
  // sizes, and nullable fields exercise nullCount and absent min/max values independently.
  private val inputRows = Seq((0, 5), (1, 11), (2, 4)).flatMap {
    case (part, count) =>
      (0 until count).map {
        index =>
          val value = index / 2
          Row(
            value.toLong,
            if (value % 3 == 0) null else Long.box(value.toLong - 4L),
            if (value % 2 == 0) null else s"v$value",
            null,
            part)
      }
  }

  for {
    collectStats <- Seq(false, true)
    optimized <- Seq(false, true)
    maxRecords <- Seq(4L, 0L)
  } {
    test(
      s"partitioned native layout: stats=$collectStats, optimized=$optimized, limit=$maxRecords") {
      withSQLConf(
        VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "true",
        DeltaSQLConf.DELTA_COLLECT_STATS.key -> collectStats.toString,
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> optimized.toString,
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
        "spark.sql.jsonGenerator.ignoreNullFields" -> "true",
        "spark.sql.maxConcurrentOutputFileWriters" -> "0"
      ) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            val data = spark.createDataFrame(
              spark.sparkContext.parallelize(inputRows, if (optimized) 2 else 1),
              dataSchema)
            val statsPlans = new ConcurrentLinkedQueue[SparkPlan]()
            val writePlans = new ConcurrentLinkedQueue[SparkPlan]()
            val listener = new QueryExecutionListener {
              override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit =
                writePlans.add(qe.executedPlan)
              override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = {}
            }
            spark.listenerManager.register(listener)
            try {
              GlutenDeltaJobStatsTracker.withStatsPlanObserver {
                (statsPath, plan) => if (statsPath.toUri.getPath == path) statsPlans.add(plan)
              } {
                data.write
                  .format("delta")
                  .partitionBy("part")
                  .option("maxRecordsPerFile", maxRecords.toString)
                  .save(path)
              }
              spark.sparkContext.listenerBus.waitUntilEmpty(10000)
            } finally {
              spark.listenerManager.unregister(listener)
            }
            assert(
              writePlans.asScala.exists(isNativeWrite),
              s"Expected a native Delta write:\n${writePlans.asScala.map(_.treeString).mkString}")
            if (collectStats) {
              assert(!statsPlans.isEmpty, "The native statistics tracker was not used")
              statsPlans.asScala.foreach {
                plan =>
                  assert(
                    plan.exists(_.isInstanceOf[HashAggregateExecTransformer]),
                    s"Expected native statistics aggregation:\n${plan.treeString}")
              }
            } else {
              assert(statsPlans.isEmpty, "Statistics should be disabled")
            }
            verifyFiles(path, collectStats, maxRecords)
        }
      }
    }
  }

  private def isNativeWrite(plan: SparkPlan): Boolean = plan.exists {
    case ExecutedCommandExec(_: GlutenDeltaLeafRunnableCommand) => true
    case ExecutedCommandExec(_: GlutenDeltaRunnableCommand) => true
    case _: GlutenDeltaLeafV2CommandExec => true
    case _ => false
  }

  private def verifyFiles(path: String, collectStats: Boolean, maxRecords: Long): Unit = {
    // Use Spark's reader as an independent oracle, including for the physical files: reading a
    // Delta table alone can hide missing partition columns or incorrect per-file statistics.
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      val actual = spark.read.format("delta").load(path)
        .select(dataSchema.fieldNames.map(col): _*).collect().toSeq
      assert(rowCounts(actual) == rowCounts(inputRows))
      val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
      assert(files.nonEmpty)
      val filePaths = files.map(file => DeltaFileOperations.absolutePath(path, file.path))
      val physical = spark.read.parquet(filePaths.map(_.toString): _*)
        .select(input_file_name(), col("id"), col("value"), col("label"), col("all_null"))
        .collect()
        .groupBy(row => new Path(new URI(row.getString(0))).getName)
        .map { case (name, rows) => name -> rows.map(row => Row.fromSeq(row.toSeq.tail)).toSeq }
      var totalRows = 0L
      files.zip(filePaths).foreach {
        case (file, filePath) =>
          val reader = ParquetFileReader.open(
            HadoopInputFile.fromPath(filePath, spark.sessionState.newHadoopConf()))
          val footerRows =
            try {
              reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum
            } finally {
              reader.close()
            }
          val rows = physical(filePath.getName)
          assert(footerRows == rows.size, s"Footer and physical rows disagree for $filePath")
          assert(footerRows > 0, s"Unexpected empty data file: $filePath")
          if (maxRecords > 0) {
            assert(footerRows <= maxRecords, s"$filePath has $footerRows rows, limit $maxRecords")
          }
          totalRows += footerRows
          if (collectStats) verifyStats(file, rows)
          else assert(file.stats == null || file.stats.isEmpty)
      }
      assert(totalRows == inputRows.size)
      val history = DeltaLog.forTable(spark, path).history.getHistory(Some(1)).head
      assert(
        history.operationMetrics.flatMap(_.get("numOutputRows")).contains(inputRows.size.toString))
      val physicalRows = files.zip(filePaths).toSeq.flatMap {
        case (file, filePath) =>
          val partition = file.partitionValues("part").toInt
          physical(filePath.getName).map(row => Row.fromSeq(row.toSeq :+ partition))
      }
      assert(rowCounts(physicalRows) == rowCounts(inputRows))
    }
  }

  private def verifyStats(file: AddFile, rows: Seq[Row]): Unit = {
    assert(file.stats != null && file.stats.nonEmpty, s"Missing statistics: ${file.path}")
    val stats = JsonUtils.fromJson[Map[String, Any]](file.stats)
    assert(asLong(stats("numRecords")) == rows.size)
    val columns = dataSchema.fieldNames.dropRight(1)
    val expectedNulls = columns.zipWithIndex.map {
      case (name, index) => name -> rows.count(_.isNullAt(index)).toLong
    }.toMap
    val actualNulls = stats("nullCount").asInstanceOf[Map[String, Any]]
      .map { case (name, count) => name -> asLong(count) }
    assert(actualNulls == expectedNulls, file.stats)
    val nonNulls = columns.zipWithIndex.map {
      case (name, index) => name -> rows.filterNot(_.isNullAt(index)).map(_.get(index))
    }.filter(_._2.nonEmpty)
    val expectedMin = nonNulls.map {
      case (name, values) => name -> extrema(values, minimum = true)
    }.toMap
    val expectedMax = nonNulls.map {
      case (name, values) => name -> extrema(values, minimum = false)
    }.toMap
    Seq("minValues" -> expectedMin, "maxValues" -> expectedMax).foreach {
      case (key, expected) =>
        val actual = stats(key).asInstanceOf[Map[String, Any]].map {
          case (name, number: Number) => name -> number.longValue()
          case (name, value) => name -> value
        }
        assert(actual == expected, s"$key mismatch for ${file.path}: ${file.stats}")
    }
  }

  private def extrema(values: Seq[Any], minimum: Boolean): Any = values.head match {
    case _: Number =>
      val numbers = values.map(asLong)
      if (minimum) numbers.min else numbers.max
    case _: String =>
      val strings = values.map(_.asInstanceOf[String])
      if (minimum) strings.min else strings.max
    case other => throw new IllegalArgumentException(s"Unexpected statistics value: $other")
  }

  private def asLong(value: Any): Long = value.asInstanceOf[Number].longValue()

  private def rowCounts(rows: Seq[Row]): Map[Row, Int] =
    rows.groupBy(identity).map { case (row, copies) => row -> copies.size }
}
