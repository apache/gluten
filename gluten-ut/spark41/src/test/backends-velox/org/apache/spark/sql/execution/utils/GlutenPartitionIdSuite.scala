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
package org.apache.spark.sql.execution.utils

import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.execution.VeloxColumnarToRowExec
import org.apache.gluten.memory.SimpleMemoryUsageRecorder
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowColumnVectors

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenQueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.{TaskResource, TaskResourceRegistry, TaskResources}

import org.apache.arrow.vector.IntVector

import scala.collection.immutable.TreeMap
import scala.collection.mutable

class GlutenPartitionIdSuite extends GlutenQueryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  private class TrackingConverter extends VeloxColumnarToRowExec.Converter(
      new SQLMetric("convertTime")) {
    var closeCalls: Int = 0

    override def close(): Unit = {
      closeCalls += 1
      super.close()
    }
  }

  private def inputBatch(): ColumnarBatch = {
    val root = ArrowColumnVectors.allocateRoot(new StructType().add("value", IntegerType))
    val vector = root.getVector(0).asInstanceOf[IntVector]
    val values = Seq(2, 1, 2, 1)
    vector.allocateNew(values.size)
    values.zipWithIndex.foreach { case (value, index) => vector.set(index, value) }
    root.setRowCount(values.size)
    val batch = VeloxColumnarBatches.toVeloxBatch(
      ColumnarBatches.offload(
        ArrowBufferAllocators.contextInstance(),
        ArrowColumnVectors.toBatch(root)))
    TaskResources.addRecycler("partition ID test input", 100) {
      ColumnarBatches.forceClose(batch)
    }
    batch
  }

  private def partitioner: KeyGroupedPartitionIdPartitioner = {
    val ordering = RowOrdering.createNaturalAscendingOrdering(Seq(IntegerType))
    val ids = TreeMap.empty[InternalRow, Int](ordering)
      .updated(InternalRow(1), 0)
      .updated(InternalRow(2), 1)
    new KeyGroupedPartitionIdPartitioner(ids, Seq(IntegerType), None)
  }

  private def iteratorRecyclerCount(): Int = {
    // Inspect registrations directly: heap/GC assertions would make this regression flaky.
    val getRegistry = TaskResources.getClass.getDeclaredMethod("getTaskResourceRegistry")
    getRegistry.setAccessible(true)
    val registry = getRegistry.invoke(TaskResources)
    val resourcesField = classOf[TaskResourceRegistry].getDeclaredField("resources")
    resourcesField.setAccessible(true)
    resourcesField
      .get(registry)
      .asInstanceOf[mutable.Map[String, TaskResource]]
      .values
      .count(_.resourceName() == "Iterators#IteratorRecycler")
  }

  test("partition ID conversion does not retain one iterator recycler per batch") {
    var converter: TrackingConverter = null
    val usage = TaskResources.runUnsafe {
      val batch = inputBatch()
      converter = new TrackingConverter
      val reader = new VeloxColumnarToRowExec.Converter(new SQLMetric("convertTime"))
      TaskResources.addRecycler("partition ID test reader", 100) {
        reader.close()
      }
      val batches = Iterator(new ColumnarBatch(Array.empty[ColumnVector], 3)) ++
        Iterator.fill(128)(batch) ++
        Iterator(new ColumnarBatch(Array.empty[ColumnVector], 0))
      val output = ExecUtil.computeAndAddPartitionId(batches, partitioner, row => row, converter)
      val recyclerCount = iteratorRecyclerCount()
      for (_ <- 0 until 128) {
        assert(output.hasNext)
        val (key, result) = output.next()
        assert(key == 0)
        val rows = reader.toRowIterator(result).map(row => (row.getInt(0), row.getInt(1))).toList
        assert(rows == List((1, 2), (0, 1), (1, 2), (0, 1)))
        assert(iteratorRecyclerCount() == recyclerCount)
        assert(converter.closeCalls == 0)
      }
      assert(!output.hasNext)
      assert(converter.closeCalls == 1)
      assert(!output.hasNext)
      assert(converter.closeCalls == 1)
      TaskResources.getSharedUsage()
    }
    assert(converter.closeCalls == 1)
    assert(usage.current() == 0)
  }

  Seq(false, true).foreach {
    failTask =>
      test(s"partition ID conversion closes on early task completion, failure=$failTask") {
        var converter: TrackingConverter = null
        var usage: SimpleMemoryUsageRecorder = null
        val failure = new IllegalStateException("stop partition ID conversion")
        val runTask = () =>
          TaskResources.runUnsafe {
            usage = TaskResources.getSharedUsage()
            val batch = inputBatch()
            converter = new TrackingConverter
            val output = ExecUtil.computeAndAddPartitionId(
              Iterator.fill(128)(batch),
              partitioner,
              row => row,
              converter)
            assert(output.hasNext)
            assert(output.next()._2.numRows() == 4)
            assert(converter.closeCalls == 0)
            if (failTask) {
              throw failure
            }
          }
        if (failTask) {
          assert(intercept[IllegalStateException](runTask()) eq failure)
        } else {
          runTask()
        }
        assert(converter.closeCalls == 1)
        assert(usage.current() == 0)
        assert(!TaskResources.inSparkTask())
      }
  }
}
