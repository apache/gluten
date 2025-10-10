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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.{Partition, SparkException, TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, SparkHadoopWriterUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.FileAlreadyExistsException

import java.util.Date

import scala.collection.mutable

// Bolt write files metrics start
//
// Follows the code in bolt `HiveDataSink::close()`
// The json can be as following:
// {
//     "inMemoryDataSizeInBytes":0,
//     "containsNumberedFileNames":true,
//     "onDiskDataSizeInBytes":307,
//     "fileWriteInfos":[
//         {
//             "fileSize":307,
//             "writeFileName":
//                "Gluten_Stage_1_TID_2_0_2_d1db3b31-4f99-41cb-a4e7-3b8607506168.parquet",
//             "targetFileName":
//                "Gluten_Stage_1_TID_2_0_2_d1db3b31-4f99-41cb-a4e7-3b8607506168.parquet"
//         }
//     ],
//     "writePath":"file:/home/gluten/spark-warehouse/inserttable/part1=1/part2=1",
//     "rowCount":1,
//     "targetPath":"file:/home/gluten/spark-warehouse/inserttable/part1=1/part2=1",
//     "updateMode":"NEW",
//     "name":"part1=1/part2=1"
// }
case class BoltWriteFilesInfo(writeFileName: String, targetFileName: String, fileSize: Long)

case class BoltWriteFilesMetrics(
    name: String,
    updateMode: String,
    writePath: String,
    targetPath: String,
    fileWriteInfos: Seq[BoltWriteFilesInfo],
    rowCount: Long,
    inMemoryDataSizeInBytes: Long,
    onDiskDataSizeInBytes: Long,
    containsNumberedFileNames: Boolean)

// Bolt write files metrics end

/**
 * This RDD is used to make sure we have injected staging write path before initializing the native
 * plan, and support Spark file commit protocol.
 */
class BoltColumnarWriteFilesRDD(
    var prev: RDD[ColumnarBatch],
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    jobTrackerID: String)
  extends RDD[WriterCommitMessage](prev) {

  private def collectNativeWriteFilesMetrics(cb: ColumnarBatch): Option[WriteTaskResult] = {
    // Currently, the cb contains three columns: row, fragments, and context.
    // The first row in the row column contains the number of written numRows.
    // The fragments column contains detailed information about the file writes.
    val loadedCb = ColumnarBatches.load(ArrowBufferAllocators.contextInstance, cb)
    try {
      assert(loadedCb.numCols() == 3)
      val numWrittenRows = loadedCb.column(0).getLong(0)

      var updatedPartitions = Set.empty[String]
      val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()
      var numBytes = 0L
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      for (i <- 0 until loadedCb.numRows() - 1) {
        val fragments = loadedCb.column(1).getUTF8String(i + 1)
        val metrics = objectMapper
          .readValue(fragments.toString.getBytes("UTF-8"), classOf[BoltWriteFilesMetrics])
        logDebug(s"Bolt write files metrics: $metrics")

        val fileWriteInfos = metrics.fileWriteInfos
        assert(fileWriteInfos.length == 1)
        val fileWriteInfo = fileWriteInfos.head
        numBytes += fileWriteInfo.fileSize
        val targetFileName = fileWriteInfo.targetFileName
        val outputPath = description.path

        // part1=1/part2=1
        val partitionFragment = metrics.name
        // Write a partitioned table
        if (partitionFragment != "") {
          updatedPartitions += partitionFragment
          val tmpOutputPath = outputPath + "/" + partitionFragment + "/" + targetFileName
          val customOutputPath = description.customPartitionLocations.get(
            PartitioningUtils.parsePathFragment(partitionFragment))
          if (customOutputPath.isDefined) {
            addedAbsPathFiles(tmpOutputPath) = customOutputPath.get + "/" + targetFileName
          }
        }
      }

      val numFiles = loadedCb.numRows() - 1
      val partitionsInternalRows = updatedPartitions.map {
        part =>
          val parts = new Array[Any](1)
          parts(0) = part
          new GenericInternalRow(parts)
      }.toSeq
      val stats = BasicWriteTaskStats(
        partitions = partitionsInternalRows,
        numFiles = numFiles,
        numBytes = numBytes,
        numRows = numWrittenRows)
      val summary =
        ExecutedWriteSummary(updatedPartitions = updatedPartitions, stats = Seq(stats))

      // Write an empty iterator
      if (numFiles == 0) {
        None
      } else {
        Some(
          WriteTaskResult(
            new TaskCommitMessage(addedAbsPathFiles.toMap -> updatedPartitions),
            summary))
      }
    } finally {
      loadedCb.close()
    }
  }

  private def reportTaskMetrics(writeTaskResult: WriteTaskResult): Unit = {
    val stats = writeTaskResult.summary.stats.head.asInstanceOf[BasicWriteTaskStats]
    val (numBytes, numWrittenRows) = (stats.numBytes, stats.numRows)
    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    // We should update it after calling `commitTask` to overwrite the metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach {
      outputMetrics =>
        outputMetrics.setBytesWritten(numBytes)
        outputMetrics.setRecordsWritten(numWrittenRows)
    }
  }

  private def writeFilesForEmptyIterator(
      commitProtocol: SparkWriteFilesCommitProtocol): WriteTaskResult = {
    val taskAttemptContext = commitProtocol.taskAttemptContext

    val dataWriter =
      if (commitProtocol.sparkPartitionId != 0) {
        // In case of empty job, leave first partition to save meta for file format like parquet.
        new EmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty) {
        new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else {
        new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
      }

    // We have done `setupTask` outside
    dataWriter.writeWithIterator(Iterator.empty)
    dataWriter.commit()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[WriterCommitMessage] = {
    val commitProtocol = new SparkWriteFilesCommitProtocol(jobTrackerID, description, committer)

    commitProtocol.setupTask()
    val writePath = commitProtocol.newTaskAttemptTempPath()
    val suffix = description.outputWriterFactory.getFileExtension(commitProtocol.taskAttemptContext)
    val fileNameSpec = FileNameSpec("", suffix)
    val fileName = commitProtocol.getFilename(fileNameSpec)
    logDebug(s"Bolt staging write path: $writePath")
    var writeTaskResult: WriteTaskResult = null
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath, fileName)

        // Initialize the native plan
        val iter = firstParent[ColumnarBatch].iterator(split, context)
        assert(iter.hasNext)
        val resultColumnarBatch = iter.next()
        assert(resultColumnarBatch != null)
        val nativeWriteTaskResult = collectNativeWriteFilesMetrics(resultColumnarBatch)
        if (nativeWriteTaskResult.isEmpty) {
          // If we are writing an empty iterator, then bolt would do nothing.
          // Here we fallback to use vanilla Spark write files to generate an empty file for
          // metadata only.
          writeTaskResult = writeFilesForEmptyIterator(commitProtocol)
          // We have done commit task inside `writeFilesForEmptyIterator`.
        } else {
          writeTaskResult = nativeWriteTaskResult.get
          commitProtocol.commitTask()
        }
      })(
        catchBlock = {
          // If there is an error, abort the task
          commitProtocol.abortTask(writePath)
          logError(s"Job ${commitProtocol.getJobId} aborted.")
        }
      )
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw new SparkException(
          s"Task failed while writing rows to staging path: $writePath, " +
            s"output path: ${description.path}",
          t)
    }

    assert(writeTaskResult != null)
    reportTaskMetrics(writeTaskResult)
    Iterator.single(writeTaskResult)
  }

  override protected def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}

case class BoltColumnarWriteFilesExec private (
    override val left: SparkPlan,
    override val right: SparkPlan,
    t: WriteFilesExecTransformer,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends ColumnarWriteFilesExec(left, right) {

  override protected def doValidateInternal(): ValidationResult = {
    t.doValidateInternal()
  }

  override def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    assert(child.supportsColumnar)

    val rdd = child.executeColumnar()
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    if (rdd.partitions.length == 0) {
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      writeFilesForEmptyRDD(description, committer, jobTrackerID)
    } else {
      new BoltColumnarWriteFilesRDD(rdd, description, committer, jobTrackerID)
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan =
    copy(newLeft, newRight, t, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)
}
