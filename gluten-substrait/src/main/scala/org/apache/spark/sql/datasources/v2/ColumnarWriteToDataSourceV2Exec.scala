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
package org.apache.spark.sql.datasources.v2

import org.apache.gluten.connector.write.ColumnarBatchDataWriterFactory

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.v2.StreamWriterCommitProgress
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

case class DataWritingColumnarBatchSparkTaskResult(
    numRows: Long,
    writerCommitMessage: WriterCommitMessage)

trait WritingColumnarBatchSparkTask[W <: DataWriter[ColumnarBatch]]
  extends Logging
  with Serializable {

  protected def write(writer: W, row: ColumnarBatch): Unit

  def run(
      factory: ColumnarBatchDataWriterFactory,
      context: TaskContext,
      iter: Iterator[ColumnarBatch],
      customMetrics: Map[String, SQLMetric]): DataWritingColumnarBatchSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = factory.createWriter(partId, taskId).asInstanceOf[W]

    var count = 0
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
        val batch = iter.next()
        // Count is here.
        count += batch.numRows()
        write(dataWriter, batch)
      }

      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
      logInfo(s"Writer for partition ${context.partitionId()} is committing.")

      val msg = dataWriter.commit()
      // Native write's metrics should be updated again after commit.
      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)

      logInfo(
        s"Committed partition $partId (task $taskId, attempt $attemptId, " +
          s"stage $stageId.$stageAttempt)")

      DataWritingColumnarBatchSparkTaskResult(count, msg)

    })(
      catchBlock = {
        // If there is an error, abort this writer
        logError(
          s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
        dataWriter.abort()
        logError(
          s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
      },
      finallyBlock = {
        dataWriter.close()
      }
    )
  }
}

object DataWritingColumnarBatchSparkTask
  extends WritingColumnarBatchSparkTask[DataWriter[ColumnarBatch]] {

  override protected def write(writer: DataWriter[ColumnarBatch], batch: ColumnarBatch): Unit = {
    writer.write(batch)
  }
}

object StreamWriterCommitProgressUtil {
  def getStreamWriterCommitProgress(numOutputRows: Long): StreamWriterCommitProgress = {
    StreamWriterCommitProgress(numOutputRows)
  }
}
