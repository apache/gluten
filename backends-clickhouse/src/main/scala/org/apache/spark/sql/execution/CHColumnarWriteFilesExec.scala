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

import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.memory.CHThreadGroup

import org.apache.spark.{Partition, SparkException, TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util.Date

/**
 * This RDD is used to make sure we have injected staging write path before initializing the native
 * plan, and support Spark file commit protocol.
 */
class CHColumnarWriteFilesRDD(
    var prev: RDD[ColumnarBatch],
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    jobTrackerID: String)
  extends RDD[WriterCommitMessage](prev) {

  private def reportTaskMetrics(writeTaskResult: WriteTaskResult): Unit = {
    writeTaskResult.summary.stats.find(_.isInstanceOf[BasicWriteTaskStats]).foreach {
      s =>
        val stats = s.asInstanceOf[BasicWriteTaskStats]
        // Reports bytesWritten and recordsWritten to the Spark output metrics.
        // We should update it after calling `commitTask` to overwrite the metrics.
        Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach {
          outputMetrics =>
            outputMetrics.setBytesWritten(stats.numBytes)
            outputMetrics.setRecordsWritten(stats.numRows)
        }
    }
  }

  private def writeFilesForEmptyIterator(
      taskAttemptContext: TaskAttemptContext,
      sparkPartitionId: Int
  ): WriteTaskResult = {

    val dataWriter =
      if (sparkPartitionId != 0) {
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
    CHThreadGroup.registerNewThreadGroup()

    val commitProtocol = CHColumnarWrite(jobTrackerID, description, committer)
    commitProtocol.setupTask()

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {

        // Initialize the native plan
        val iter = firstParent[ColumnarBatch].iterator(split, context)
        assert(iter.hasNext)
        val resultColumnarBatch = iter.next()
        assert(resultColumnarBatch != null)

        /**
         * we assume the number of records is less than 10,000.So the memory overhead is acceptable.
         * otherwise, we need to access ColumnarBatch row by row, which is not efficient.
         */
        val writeResults = CHExecUtil.c2r(resultColumnarBatch).map(_.copy()).toSeq
        // TODO: task commit time
        // TODO: get the schema from result ColumnarBatch and verify it.
        assert(!iter.hasNext)

        val writeTaskResult = commitProtocol
          .commitTask(writeResults)
          .orElse {
            // If we are writing an empty iterator, then gluten backend would do nothing.
            // Here we fallback to use vanilla Spark write files to generate an empty file for
            // metadata only.
            Some(
              writeFilesForEmptyIterator(commitProtocol.getTaskAttemptContext, context.partitionId))
            // We have done commit task inside `writeFilesForEmptyIterator`.
          }
          .get
        reportTaskMetrics(writeTaskResult)
        Iterator.single(writeTaskResult)
      })(
        catchBlock = {
          // If there is an error, abort the task
          commitProtocol.abortTask()
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
          s"Task failed while writing rows to output path: ${description.path}",
          t)
    }
  }

  override protected def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}

case class CHColumnarWriteFilesExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    t: WriteFilesExecTransformer,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec
) extends ColumnarWriteFilesExec(left, right) {

  override protected def doValidateInternal(): ValidationResult = {
    t.doValidateInternal()
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan =
    copy(newLeft, newRight, t, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)

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
      new CHColumnarWriteFilesRDD(rdd, description, committer, jobTrackerID)
    }
  }
}
