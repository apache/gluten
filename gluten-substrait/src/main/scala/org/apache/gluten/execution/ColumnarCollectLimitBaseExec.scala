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
import org.apache.gluten.config.ShuffleWriterType
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{LimitExec, ShuffledColumnarBatchRDD, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.metric.SQLColumnarShuffleReadMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class ColumnarCollectLimitBaseExec(
    limit: Int,
    childPlan: SparkPlan,
    offset: Int = 0
) extends LimitExec
  with ValidatablePlan {

  override def rowType(): Convention.RowType = Convention.RowType.None

  override def batchType(): Convention.BatchType =
    BackendsApiManager.getSettings.primaryBatchType

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)

  private lazy val readMetrics =
    SQLColumnarShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  private lazy val shuffleWriterType: ShuffleWriterType =
    BackendsApiManager.getSparkPlanExecApiInstance
      .getShuffleWriterType(outputPartitioning, child.output)

  @transient private lazy val serializer: Serializer =
    BackendsApiManager.getSparkPlanExecApiInstance
      .createColumnarBatchSerializer(child.schema, metrics, shuffleWriterType)

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance
      .genColumnarShuffleExchangeMetrics(sparkContext, shuffleWriterType) ++
      readMetrics ++ writeMetrics

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()

    if (childRDD.getNumPartitions == 0) {
      return sparkContext.parallelize(Seq.empty[ColumnarBatch], 1)
    }

    val processedRDD =
      if (childRDD.getNumPartitions == 1) childRDD
      else shuffleLimitedPartitions(childRDD)

    processedRDD.mapPartitions(
      partition => {
        if (limit > 0) {
          val adjusted = math.max(0, limit - offset)
          collectWithOffsetAndLimit(partition, offset, adjusted)
        } else {
          collectWithOffsetAndLimit(partition, offset, -1)
        }
      })
  }

  private def shuffleLimitedPartitions(childRDD: RDD[ColumnarBatch]): RDD[ColumnarBatch] = {
    val applyLocalLimit = (offset == 0 && limit >= 0)
    val locallyLimited = if (applyLocalLimit) {
      childRDD.mapPartitions {
        collectWithOffsetAndLimit(_, 0, limit)
      }
    } else {
      childRDD
    }
    new ShuffledColumnarBatchRDD(
      BackendsApiManager.getSparkPlanExecApiInstance.genShuffleDependency(
        locallyLimited,
        child.output,
        child.output,
        SinglePartition,
        serializer,
        writeMetrics,
        metrics,
        shuffleWriterType
      ),
      readMetrics
    )
  }

  protected def collectWithOffsetAndLimit(
      inputIter: Iterator[ColumnarBatch],
      offset: Int,
      limit: Int): Iterator[ColumnarBatch]

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = SinglePartition

  override protected def doValidateInternal(): ValidationResult = {
    ValidationResult.failed("Columnar shuffle not enabled or child does not support columnar.")
  }

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

}
