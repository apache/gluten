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

import org.apache.gluten.metrics.IMetrics

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

class WholeStageTransformerPartitionSuite extends SharedSparkSession {
  test("align whole-stage and input-wrapper partitions by index") {
    val wholeStageRDD = createWholeStageRDD(nativePartitionCount = 3, inputPartitionCount = 3)

    val partitions = wholeStageRDD.partitions.map(_.asInstanceOf[FirstZippedPartitionsPartition])
    assert(partitions.map(_.index).sameElements(Array(0, 1, 2)))
    assert(partitions.map(_.inputPartition.index).sameElements(Array(0, 1, 2)))
    assert(
      partitions
        .map(_.inputColumnarRDDPartitions.map(_.index))
        .sameElements(Array(Seq(0), Seq(1), Seq(2))))
  }

  test("fail when an input wrapper has fewer partitions than the whole stage") {
    val wholeStageRDD = createWholeStageRDD(nativePartitionCount = 3, inputPartitionCount = 2)
    val error = intercept[IllegalArgumentException](wholeStageRDD.partitions)
    assert(error.getMessage.contains("Whole-stage partition count 3"))
    assert(error.getMessage.contains("input RDD partition count 2"))
  }

  test("fail when an input wrapper has more partitions than the whole stage") {
    val wholeStageRDD = createWholeStageRDD(nativePartitionCount = 2, inputPartitionCount = 3)
    val error = intercept[IllegalArgumentException](wholeStageRDD.partitions)
    assert(error.getMessage.contains("Whole-stage partition count 2"))
    assert(error.getMessage.contains("input RDD partition count 3"))
  }

  private def createWholeStageRDD(
      nativePartitionCount: Int,
      inputPartitionCount: Int): GlutenWholeStageColumnarRDD = {
    val nativePartitions =
      (0 until nativePartitionCount).map(index => GlutenPartition(index, Array.emptyByteArray))
    val inputRDDs =
      new ColumnarInputRDDsWrapper(Seq(new PartitionOnlyRDD(sparkContext, inputPartitionCount)))

    new GlutenWholeStageColumnarRDD(
      sparkContext,
      nativePartitions,
      inputRDDs,
      SQLMetrics.createTimingMetric(sparkContext, "pipeline time"),
      (_: InputMetricsWrapper) => (),
      (_: IMetrics) => ())
  }

  private class PartitionOnlyRDD(sc: SparkContext, partitionCount: Int)
    extends RDD[ColumnarBatch](sc, Nil) {

    override protected def getPartitions: Array[Partition] =
      Array.tabulate(partitionCount)(TestPartition)

    override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] =
      throw new UnsupportedOperationException("Partition-only test RDD must not be executed")
  }

  private case class TestPartition(index: Int) extends Partition
}
