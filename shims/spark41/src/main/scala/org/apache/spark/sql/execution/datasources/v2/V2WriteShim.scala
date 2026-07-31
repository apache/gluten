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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.write.{BatchWrite, MergeSummaryImpl, Write, WriterCommitMessage}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

object V2WriteShim extends AdaptiveSparkPlanHelper {
  def commit(
      batchWrite: BatchWrite,
      messages: Array[WriterCommitMessage],
      query: SparkPlan): Unit = {
    collectFirst(query) { case mergeRows: MergeRowsExec => mergeRows } match {
      case Some(mergeRows) =>
        val metrics = mergeRows.metrics
        batchWrite.commit(
          messages,
          MergeSummaryImpl(
            metrics.get("numTargetRowsCopied").map(_.value).getOrElse(-1L),
            metrics.get("numTargetRowsDeleted").map(_.value).getOrElse(-1L),
            metrics.get("numTargetRowsUpdated").map(_.value).getOrElse(-1L),
            metrics.get("numTargetRowsInserted").map(_.value).getOrElse(-1L),
            metrics.get("numTargetRowsMatchedUpdated").map(_.value).getOrElse(-1L),
            metrics.get("numTargetRowsMatchedDeleted").map(_.value).getOrElse(-1L),
            metrics.get("numTargetRowsNotMatchedBySourceUpdated").map(_.value).getOrElse(-1L),
            metrics.get("numTargetRowsNotMatchedBySourceDeleted").map(_.value).getOrElse(-1L)
          )
        )
      case None => batchWrite.commit(messages)
    }
  }

  def postDriverMetrics(
      write: Write,
      metrics: Map[String, SQLMetric],
      sparkContext: SparkContext): Unit = {
    val driverMetrics = write.reportDriverMetrics().map {
      customTaskMetric =>
        val metric = metrics(customTaskMetric.name())
        metric.set(customTaskMetric.value())
        metric
    }
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, driverMetrics.toSeq)
  }
}
