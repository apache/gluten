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
package org.apache.iceberg.spark.source

import org.apache.gluten.execution.SparkDataSourceRDDPartition

import org.apache.spark.Partition
import org.apache.spark.broadcast.Broadcast

import org.apache.iceberg.{FileScanTask, ScanTask, ScanTaskGroup, Table}
import org.apache.iceberg.types.Types
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

import scala.collection.JavaConverters._

class GlutenIcebergSourceUtilSuite extends AnyFunSuite {

  private def makeSparkInputPartition(length: Long): SparkInputPartition = {
    val task = mock(classOf[FileScanTask])
    when(task.isFileScanTask).thenReturn(true)
    when(task.asFileScanTask()).thenReturn(task)
    when(task.length()).thenReturn(length)

    val taskGroup = new ScanTaskGroup[ScanTask] {
      override def tasks(): java.util.Collection[ScanTask] =
        Collections.singletonList(task)

      override def sizeBytes(): Long = length

      override def estimatedRowsCount(): Long = 0L

      override def filesCount(): Int = 1
    }

    val constructor = classOf[SparkInputPartition].getDeclaredConstructor(
      classOf[Types.StructType],
      classOf[ScanTaskGroup[_]],
      classOf[Broadcast[Table]],
      classOf[String],
      classOf[String],
      java.lang.Boolean.TYPE,
      classOf[Array[String]],
      java.lang.Boolean.TYPE
    )
    constructor.setAccessible(true)
    constructor.newInstance(
      Types.StructType.of(),
      taskGroup,
      null,
      null,
      null,
      Boolean.box(false),
      Array.empty[String],
      Boolean.box(false)
    )
  }

  private def makePartitions(
      inputPartitions: Seq[SparkInputPartition],
      numPartitions: Int): Seq[Partition] = {
    val numGroups = inputPartitions.size / numPartitions +
      (if (inputPartitions.size % numPartitions == 0) 0 else 1)
    inputPartitions.grouped(numGroups).toSeq.zipWithIndex.map {
      case (partitions, idx) => new SparkDataSourceRDDPartition(idx, partitions)
    }
  }

  private def partitionLengths(partition: Partition): Seq[Long] = {
    partition
      .asInstanceOf[SparkDataSourceRDDPartition]
      .inputPartitions
      .flatMap(
        _.asInstanceOf[SparkInputPartition]
          .taskGroup[ScanTask]()
          .tasks()
          .asScala)
      .map(_.asFileScanTask().length())
  }

  private def partitionFileNums(partition: Partition): Int = {
    partition
      .asInstanceOf[SparkDataSourceRDDPartition]
      .inputPartitions
      .map(
        _.asInstanceOf[SparkInputPartition]
          .taskGroup[ScanTask]()
          .tasks()
          .size)
      .sum
  }

  test("large files are distributed evenly by size") {
    val inputPartitions = Seq(100L, 90L, 80L, 70L).map(makeSparkInputPartition)
    val initialPartitions = makePartitions(inputPartitions, 2)

    val result = GlutenIcebergSourceUtil.regeneratePartitions(initialPartitions, 0.0)

    assert(result.size === 2)

    val sizes = result.map(partitionLengths(_).sum)
    assert(sizes.forall(_ === 170))
  }

  test("small files are distributed evenly by number of files") {
    val inputPartitions = Seq.fill(10)(10L).map(makeSparkInputPartition)
    val initialPartitions = makePartitions(inputPartitions, 5)

    val result = GlutenIcebergSourceUtil.regeneratePartitions(initialPartitions, 1.0)

    assert(result.size === 5)
    val counts = result.map(partitionLengths(_).size)
    assert(counts.forall(_ === 2))
  }

  test("small files should not be placed into one partition") {
    val inputPartitions = Seq(10L, 20L, 30L, 40L, 100L).map(makeSparkInputPartition)
    val initialPartitions = makePartitions(inputPartitions, 2)

    val result = GlutenIcebergSourceUtil.regeneratePartitions(initialPartitions, 0.5)

    assert(result.size === 2)
    assert(result.forall(partition => partitionLengths(partition).exists(_ <= 40)))
  }

  test("mixed small and large files should be evenly distributed") {
    val inputPartitions =
      Seq(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L).map(makeSparkInputPartition)
    val initialPartitions = makePartitions(inputPartitions, 3)

    val result = GlutenIcebergSourceUtil.regeneratePartitions(initialPartitions, 0.5)

    assert(result.size === 3)
    assert(result.forall(partition => partitionFileNums(partition) >= 3))
  }

  test("zero length files") {
    val inputPartitions = Seq(0L, 0L).map(makeSparkInputPartition)
    val initialPartitions = makePartitions(inputPartitions, 2)

    val result = GlutenIcebergSourceUtil.regeneratePartitions(initialPartitions, 0.0)

    assert(result.size === 2)
    assert(result.count(partitionLengths(_).nonEmpty) === 2)
  }

  test("empty inputs") {
    val result = GlutenIcebergSourceUtil.regeneratePartitions(Seq.empty, 0.5)
    assert(result.size === 0)
  }
}
