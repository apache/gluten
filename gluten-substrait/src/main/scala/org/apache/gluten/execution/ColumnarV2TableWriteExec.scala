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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarMicroBatchWriterFactory, ColumnarStreamingDataWriterFactory}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.extension.columnar.transition.Convention.RowType

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriterCommitMessage}
import org.apache.spark.sql.datasources.v2.{DataWritingColumnarBatchSparkTask, DataWritingColumnarBatchSparkTaskResult, StreamWriterCommitProgressUtil, WritingColumnarBatchSparkTask}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator

trait ColumnarV2TableWriteExec extends V2TableWriteExec with ValidatablePlan {

  def refreshCache: () => Unit

  def write: Write

  def batchWrite: BatchWrite = write.toBatch

  def withNewQuery(newQuery: SparkPlan): SparkPlan = withNewChildInternal(newQuery)

  protected def createBatchWriterFactory(schema: StructType): ColumnarBatchDataWriterFactory

  protected def createStreamingWriterFactory(schema: StructType): ColumnarStreamingDataWriterFactory

  override protected def run(): Seq[InternalRow] = {
    writeColumnarBatchWithV2(batchWrite)
    refreshCache()
    Nil
  }

  override def batchType(): Convention.BatchType = Convention.BatchType.None

  override def rowType0(): Convention.RowType = RowType.VanillaRowType

  override def requiredChildConvention(): Seq[ConventionReq] = Seq(
    ConventionReq.ofBatch(
      ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType)))

  private def writingTaskBatch: WritingColumnarBatchSparkTask[_] = DataWritingColumnarBatchSparkTask

  private def writeColumnarBatchWithV2(batchWrite: BatchWrite): Unit = {
    val rdd: RDD[ColumnarBatch] = {
      val tempRdd = query.executeColumnar()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        tempRdd
      }
    }
    // introduce a local var to avoid serializing the whole class
    val task = writingTaskBatch
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(
      s"Start processing data source write support: $batchWrite. " +
        s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics
    val factory = batchWrite match {
      case m: MicroBatchWrite =>
        val epochIdField = m.getClass.getDeclaredField("epochId")
        epochIdField.setAccessible(true)
        val epochId = epochIdField.getLong(m)
        new ColumnarMicroBatchWriterFactory(epochId, createStreamingWriterFactory(query.schema))
      case _ =>
        createBatchWriterFactory(query.schema)
    }
    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) =>
          task.run(factory, context, iter, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingColumnarBatchSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(
        StreamWriterCommitProgressUtil.getStreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("_LEGACY_ERROR_TEMP_2070", cause = cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        throw cause
    }
  }

  override val customMetrics: Map[String, SQLMetric] = {
    write
      .supportedCustomMetrics()
      .map {
        customMetric =>
          customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
      }
      .toMap ++ BackendsApiManager.getMetricsApiInstance.genBatchWriteMetrics(sparkContext)
  }
}
