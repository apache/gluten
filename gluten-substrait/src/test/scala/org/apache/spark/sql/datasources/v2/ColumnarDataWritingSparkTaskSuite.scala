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

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class ColumnarDataWritingSparkTaskSuite extends AnyFunSuite {

  test("columnar write task honors Spark output commit coordinator") {
    val stageId = 101
    val stageAttempt = 0
    val partitionId = 0
    val coordinator = new RecordingOutputCommitCoordinator

    withSparkEnv(coordinator) {
      val factory = new RecordingColumnarBatchDataWriterFactory

      val firstResult = DataWritingColumnarBatchSparkTask.run(
        factory,
        taskContext(stageId, stageAttempt, partitionId, taskAttemptId = 100L, attemptNumber = 0),
        Iterator.empty,
        useCommitCoordinator = true,
        Map.empty)

      assert(firstResult.writerCommitMessage == RecordingWriterCommitMessage("writer-0"))
      assert(factory.writers.head.commits == 1)
      assert(factory.writers.head.aborts == 0)
      assert(coordinator.requests == Seq((stageId, stageAttempt, partitionId, 0)))

      // The commit-denied error type is not stable across the Spark versions this
      // module compiles against: 3.3-4.0 throw CommitDeniedException (extends
      // Exception), 4.1 throws SparkException. Assert on the message instead.
      val deniedError = intercept[Exception] {
        DataWritingColumnarBatchSparkTask.run(
          factory,
          taskContext(stageId, stageAttempt, partitionId, taskAttemptId = 101L, attemptNumber = 1),
          Iterator.empty,
          useCommitCoordinator = true,
          Map.empty)
      }

      assert(deniedError.getMessage.contains("Commit denied"))
      assert(factory.writers(1).commits == 0)
      assert(factory.writers(1).aborts == 1)
      assert(factory.writers(1).closed == 1)
      assert(coordinator.requests == Seq(
        (stageId, stageAttempt, partitionId, 0),
        (stageId, stageAttempt, partitionId, 1)))
    }
  }

  test("columnar write task counts output rows as Long") {
    val factory = new RecordingColumnarBatchDataWriterFactory

    val result = DataWritingColumnarBatchSparkTask.run(
      factory,
      taskContext(102, 0, partitionId = 0, taskAttemptId = 200L, attemptNumber = 0),
      Iterator(batchWithRows(Int.MaxValue), batchWithRows(1)),
      useCommitCoordinator = false,
      Map.empty
    )

    assert(result.numRows == Int.MaxValue.toLong + 1L)
    assert(factory.writers.head.commits == 1)
    assert(factory.writers.head.aborts == 0)
    assert(factory.writers.head.closed == 1)
  }

  test("columnar write task aborts and closes writer when commit fails") {
    val factory = new RecordingColumnarBatchDataWriterFactory(
      index => new RecordingColumnarBatchWriter(s"writer-$index", failOnCommit = true))

    val commitError = intercept[RuntimeException] {
      DataWritingColumnarBatchSparkTask.run(
        factory,
        taskContext(103, 0, partitionId = 0, taskAttemptId = 300L, attemptNumber = 0),
        Iterator.empty,
        useCommitCoordinator = false,
        Map.empty)
    }

    assert(commitError.getMessage.contains("commit failed"))
    assert(factory.writers.head.commits == 1)
    assert(factory.writers.head.aborts == 1)
    assert(factory.writers.head.closed == 1)
  }

  test("columnar write task does not abort committed writer after post-commit metrics failure") {
    val factory = new RecordingColumnarBatchDataWriterFactory(
      index => new RecordingColumnarBatchWriter(s"writer-$index", failMetricsAfterCommit = true))

    val metricsError = intercept[RuntimeException] {
      DataWritingColumnarBatchSparkTask.run(
        factory,
        taskContext(104, 0, partitionId = 0, taskAttemptId = 400L, attemptNumber = 0),
        Iterator.empty,
        useCommitCoordinator = false,
        Map.empty)
    }

    assert(metricsError.getMessage.contains("metrics failed"))
    assert(factory.writers.head.commits == 1)
    assert(factory.writers.head.aborts == 0)
    assert(factory.writers.head.closed == 1)
  }

  test("columnar write task closes denied speculative attempt when abort fails") {
    val stageId = 105
    val stageAttempt = 0
    val partitionId = 0
    val coordinator = new RecordingOutputCommitCoordinator

    withSparkEnv(coordinator) {
      val factory = new RecordingColumnarBatchDataWriterFactory(
        index => new RecordingColumnarBatchWriter(s"writer-$index", failOnAbort = true))

      val deniedError = intercept[Exception] {
        DataWritingColumnarBatchSparkTask.run(
          factory,
          taskContext(stageId, stageAttempt, partitionId, taskAttemptId = 500L, attemptNumber = 1),
          Iterator.empty,
          useCommitCoordinator = true,
          Map.empty)
      }

      assert(deniedError.getMessage.contains("Commit denied"))
      assert(factory.writers.head.commits == 0)
      assert(factory.writers.head.aborts == 1)
      assert(factory.writers.head.closed == 1)
      assert(coordinator.requests == Seq((stageId, stageAttempt, partitionId, 1)))
    }
  }

  // Mocked rather than constructed: the TaskContextImpl and SparkEnv constructor
  // signatures differ across the Spark versions this module compiles against.
  private def taskContext(
      stageId: Int,
      stageAttempt: Int,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int): TaskContext = {
    val context = mock(classOf[TaskContext])
    when(context.stageId()).thenReturn(stageId)
    when(context.stageAttemptNumber()).thenReturn(stageAttempt)
    when(context.partitionId()).thenReturn(partitionId)
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    when(context.attemptNumber()).thenReturn(attemptNumber)
    context
  }

  private def withSparkEnv[T](coordinator: OutputCommitCoordinator)(body: => T): T = {
    val previous = SparkEnv.get
    val env = mock(classOf[SparkEnv])
    when(env.outputCommitCoordinator).thenReturn(coordinator)
    SparkEnv.set(env)
    try {
      body
    } finally {
      SparkEnv.set(previous)
    }
  }

  private def batchWithRows(numRows: Int): ColumnarBatch = {
    new ColumnarBatch(Array.empty[org.apache.spark.sql.vectorized.ColumnVector], numRows)
  }

}

private class RecordingOutputCommitCoordinator
  extends OutputCommitCoordinator(new SparkConf(), true) {
  val requests = scala.collection.mutable.ArrayBuffer.empty[(Int, Int, Int, Int)]

  override def canCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = {
    requests += ((stage, stageAttempt, partition, attemptNumber))
    attemptNumber == 0
  }
}

private class RecordingColumnarBatchDataWriterFactory(
    writerBuilder: Int => RecordingColumnarBatchWriter =
      index => new RecordingColumnarBatchWriter(s"writer-$index"))
  extends ColumnarBatchDataWriterFactory {
  val writers = scala.collection.mutable.ArrayBuffer.empty[RecordingColumnarBatchWriter]

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    val writer = writerBuilder(writers.size)
    writers += writer
    writer
  }
}

private class RecordingColumnarBatchWriter(
    id: String,
    failOnCommit: Boolean = false,
    failOnAbort: Boolean = false,
    failMetricsAfterCommit: Boolean = false)
  extends DataWriter[ColumnarBatch] {
  var commits = 0
  var aborts = 0
  var closed = 0

  override def write(record: ColumnarBatch): Unit = {}

  override def commit(): WriterCommitMessage = {
    commits += 1
    if (failOnCommit) {
      throw new RuntimeException(s"commit failed for $id")
    }
    RecordingWriterCommitMessage(id)
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    if (failMetricsAfterCommit && commits > 0) {
      throw new RuntimeException(s"metrics failed for $id")
    }
    Array.empty
  }

  override def abort(): Unit = {
    aborts += 1
    if (failOnAbort) {
      throw new RuntimeException(s"abort failed for $id")
    }
  }

  override def close(): Unit = {
    closed += 1
  }
}

private case class RecordingWriterCommitMessage(id: String) extends WriterCommitMessage
