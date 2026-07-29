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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.clickhouse.RuntimeSettings
import org.apache.gluten.vectorized.NativeExpressionEvaluator

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, HadoopMapReduceCommitProtocol}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.lang.reflect.Field

import scala.collection.mutable
import scala.language.implicitConversions

trait CHColumnarWrite[T <: FileCommitProtocol] {

  def description: WriteJobDescription
  def jobTrackerID: String
  def committer: T
  def doSetupNativeTask(): Unit

  def setupTask(): Unit = {
    committer.setupTask(getTaskAttemptContext)
    doSetupNativeTask()
  }

  def abortTask(): Unit = {
    committer.abortTask(getTaskAttemptContext)
  }
  def commitTask(writeResults: Seq[InternalRow]): Option[WriteTaskResult]

  lazy val basicWriteJobStatsTracker: WriteTaskStatsTracker = description.statsTrackers
    .find(_.isInstanceOf[BasicWriteJobStatsTracker])
    .map(_.newTaskInstance())
    .get

  private lazy val (taskAttemptContext: TaskAttemptContext, jobId: String) = {
    // Copied from `SparkHadoopWriterUtils.createJobID` to be compatible with multi-version
    def createJobID(jobTrackerID: String, id: Int): JobID = {
      if (id < 0) {
        throw new IllegalArgumentException("Job number is negative")
      }
      new JobID(jobTrackerID, id)
    }

    val sparkStageId: Int = TaskContext.get().stageId()
    val sparkPartitionId: Int = TaskContext.get().partitionId()
    val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue
    val jobID = createJobID(jobTrackerID, sparkStageId)
    val taskId = new TaskID(jobID, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the configuration object
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobID.toString)
    hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    (new TaskAttemptContextImpl(hadoopConf, taskAttemptId), jobID.toString)
  }

  def getTaskAttemptContext: TaskAttemptContext = this.taskAttemptContext

  def getJobId: String = this.jobId
}

object CreateFileNameSpec {
  def apply(taskContext: TaskAttemptContext, description: WriteJobDescription): FileNameSpec = {
    val fileCounter = 0
    val suffix = f"-c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskContext)
    FileNameSpec("", suffix)
  }
}

// More details in local_engine::FileNameGenerator in NormalFileWriter.cpp
object FileNamePlaceHolder {
  val ID = "{id}"
  val BUCKET = "{bucket}"
}

/** [[HadoopMapReduceAdapter]] for [[HadoopMapReduceCommitProtocol]]. */
case class HadoopMapReduceAdapter(sparkCommitter: HadoopMapReduceCommitProtocol) {
  private lazy val committer: OutputCommitter = {
    val field: Field = classOf[HadoopMapReduceCommitProtocol].getDeclaredField("committer")
    field.setAccessible(true)
    field.get(sparkCommitter).asInstanceOf[OutputCommitter]
  }
  private lazy val GetFilename = {
    val m = classOf[HadoopMapReduceCommitProtocol]
      .getDeclaredMethod("getFilename", classOf[TaskAttemptContext], classOf[FileNameSpec])
    m.setAccessible(true)
    m
  }

  private def newTaskAttemptTempPath(defaultPath: String): String = {
    assert(committer != null)
    val stagingDir: Path = committer match {
      // For FileOutputCommitter it has its own staging path called "work path".
      case f: FileOutputCommitter =>
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(defaultPath))
      case _ =>
        new Path(defaultPath)
    }
    stagingDir.toString
  }

  private def getFilename(taskContext: TaskAttemptContext, spec: FileNameSpec): String = {
    GetFilename.invoke(sparkCommitter, taskContext, spec).asInstanceOf[String]
  }

  def getTaskAttemptTempPathAndFilePattern(
      taskContext: TaskAttemptContext,
      description: WriteJobDescription): (String, String) = {
    val stageDir = newTaskAttemptTempPath(description.path)

    if (description.bucketSpec.isDefined) {
      val filePart = getFilename(taskContext, FileNameSpec("", ""))
      val fileSuffix = CreateFileNameSpec(taskContext, description).suffix
      (stageDir, s"${filePart}_${FileNamePlaceHolder.BUCKET}$fileSuffix")
    } else {
      val filename = getFilename(taskContext, CreateFileNameSpec(taskContext, description))
      (stageDir, filename)
    }
  }
}

case class NativeFileWriteResult(filename: String, partition_id: String, record_count: Long) {
  lazy val relativePath: String = if (CHColumnarWrite.validatedPartitionID(partition_id)) {
    s"$partition_id/$filename"
  } else {
    filename
  }
}

object NativeFileWriteResult {
  implicit def apply(row: InternalRow): NativeFileWriteResult = {
    NativeFileWriteResult(row.getString(0), row.getString(1), row.getLong(2))
  }

  /**
   * {{{
   * val schema =
   *   StructType(
   *     StructField("filename", StringType, false) ::
   *     StructField("partition_id", StringType, false) ::
   *     StructField("record_count", LongType, false) :: <= overlap with vanilla =>
   *     min...
   *     max...
   *     null_count...)
   * }}}
   */
  private val inputBasedSchema: Seq[AttributeReference] = Seq(
    AttributeReference("filename", StringType, nullable = false)(),
    AttributeReference("partittion_id", StringType, nullable = false)(),
    AttributeReference("record_count", LongType, nullable = false)()
  )

  def nativeStatsSchema(vanilla: Seq[AttributeReference]): Seq[AttributeReference] = {
    inputBasedSchema.take(2) ++ vanilla
  }
}

case class NativeStatCompute(rows: Seq[InternalRow]) {
  def apply[T](stats: Seq[T => Unit], extra: Option[InternalRow => Unit] = None)(implicit
      creator: InternalRow => T): Unit = {
    rows.foreach {
      row =>
        val stat = creator(row)
        stats.foreach(agg => agg(stat))
        extra.foreach(_(row))
    }
  }
}

case class NativeBasicWriteTaskStatsTracker(
    writeDir: String,
    basicWriteJobStatsTracker: WriteTaskStatsTracker)
  extends (NativeFileWriteResult => Unit) {
  private var numWrittenRows: Long = 0
  override def apply(stat: NativeFileWriteResult): Unit = {
    val absolutePath = s"$writeDir/${stat.relativePath}"
    if (CHColumnarWrite.validatedPartitionID(stat.partition_id)) {
      basicWriteJobStatsTracker.newPartition(new GenericInternalRow(Array[Any](stat.partition_id)))
    }
    basicWriteJobStatsTracker.newFile(absolutePath)
    basicWriteJobStatsTracker.closeFile(absolutePath)
    numWrittenRows += stat.record_count
  }
  private def finalStats: BasicWriteTaskStats = basicWriteJobStatsTracker
    .getFinalStats(0)
    .asInstanceOf[BasicWriteTaskStats]

  def result: BasicWriteTaskStats = finalStats.copy(numRows = numWrittenRows)
}

case class FileCommitInfo(description: WriteJobDescription)
  extends (NativeFileWriteResult => Unit) {
  private val partitions: mutable.Set[String] = mutable.Set[String]()
  private val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()

  def apply(stat: NativeFileWriteResult): Unit = {
    val tmpAbsolutePath = s"${description.path}/${stat.relativePath}"
    if (CHColumnarWrite.validatedPartitionID(stat.partition_id)) {
      partitions += stat.partition_id
      val customOutputPath =
        description.customPartitionLocations.get(
          PartitioningUtils.parsePathFragment(stat.partition_id))
      if (customOutputPath.isDefined) {
        addedAbsPathFiles(tmpAbsolutePath) = customOutputPath.get + "/" + stat.filename
      }
    }
  }

  def result: (Set[String], Map[String, String]) = {
    (partitions.toSet, addedAbsPathFiles.toMap)
  }
}

case class HadoopMapReduceCommitProtocolWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: HadoopMapReduceCommitProtocol)
  extends CHColumnarWrite[HadoopMapReduceCommitProtocol]
  with Logging {

  private var stageDir: String = _

  private lazy val adapter: HadoopMapReduceAdapter = HadoopMapReduceAdapter(committer)

  /**
   * This function is used in [[CHColumnarWriteFilesRDD]] to inject the staging write path before
   * initializing the native plan and collect native write files metrics for each backend.
   */
  override def doSetupNativeTask(): Unit = {
    val (writePath, writeFilePattern) =
      adapter.getTaskAttemptTempPathAndFilePattern(getTaskAttemptContext, description)
    stageDir = writePath
    logDebug(s"Native staging write path: $stageDir and file pattern: $writeFilePattern")

    val settings =
      Map(
        RuntimeSettings.TASK_WRITE_TMP_DIR.key -> stageDir,
        RuntimeSettings.TASK_WRITE_FILENAME_PATTERN.key -> writeFilePattern)
    NativeExpressionEvaluator.updateQueryRuntimeSettings(settings)
  }

  def doCollectNativeResult(stats: Seq[InternalRow]): Option[WriteTaskResult] = {
    // Write an empty iterator
    if (stats.isEmpty) {
      None
    } else {
      val commitInfo = FileCommitInfo(description)
      val basicNativeStat = NativeBasicWriteTaskStatsTracker(stageDir, basicWriteJobStatsTracker)
      val basicNativeStats = Seq(commitInfo, basicNativeStat)
      NativeStatCompute(stats)(basicNativeStats)
      val (partitions, addedAbsPathFiles) = commitInfo.result
      val updatedPartitions = partitions
      Some(
        WriteTaskResult(
          new TaskCommitMessage(addedAbsPathFiles -> updatedPartitions),
          ExecutedWriteSummary(
            updatedPartitions = updatedPartitions,
            stats = Seq(basicNativeStat.result))
        ))
    }
  }

  override def commitTask(writeResults: Seq[InternalRow]): Option[WriteTaskResult] = {
    doCollectNativeResult(writeResults).map(
      nativeWriteTaskResult => {
        val (_, taskCommitTime) = Utils.timeTakenMs {
          committer.commitTask(getTaskAttemptContext)
        }

        // Just for update task commit time
        description.statsTrackers.foreach {
          stats => stats.newTaskInstance().getFinalStats(taskCommitTime)
        }
        nativeWriteTaskResult
      })
  }
}

object CHColumnarWrite {
  def apply(
      jobTrackerID: String,
      description: WriteJobDescription,
      committer: FileCommitProtocol): CHColumnarWrite[FileCommitProtocol] = committer match {
    case h: HadoopMapReduceCommitProtocol =>
      HadoopMapReduceCommitProtocolWrite(jobTrackerID, description, h)
        .asInstanceOf[CHColumnarWrite[FileCommitProtocol]]
    case other => CHDeltaColumnarWrite(jobTrackerID, description, other)
  }

  private val EMPTY_PARTITION_ID = "__NO_PARTITION_ID__"

  def validatedPartitionID(partitionID: String): Boolean = partitionID != EMPTY_PARTITION_ID
}
