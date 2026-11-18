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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.catalyst.plans.logical.{Range => LogicalRange}
import org.apache.spark.sql.test.SharedSparkSession

import scala.collection.mutable.ArrayBuffer

class ColumnarRangeExecSuite extends SparkFunSuite with SharedSparkSession {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "1g")
    .set("spark.gluten.sql.columnar.backend.lib", "bolt")

  private def readBatches(
      start: Long,
      end: Long,
      step: Long,
      maxBatches: Int): (Vector[Vector[Long]], Boolean) = {
    val plan = ColumnarRangeExec(LogicalRange(start, end, step, Some(1)))
    val partitions = plan.executeColumnar().mapPartitions {
      batches =>
        val values = ArrayBuffer.empty[Vector[Long]]
        // Bound consumption so broken range advancement cannot make the test hang.
        while (values.size < maxBatches && batches.hasNext) {
          val batch = batches.next()
          values += Vector.tabulate(batch.numRows())(row => batch.column(0).getLong(row))
        }
        // The iterator closes its Arrow payloads on hasNext and task completion.
        Iterator.single((values.toVector, !batches.hasNext))
    }.collect()
    assert(partitions.length == 1)
    partitions.head
  }

  test("range preserves values and terminates with a Long.MaxValue step") {
    val (batches, exhausted) =
      readBatches(Long.MinValue, Long.MaxValue, Long.MaxValue, maxBatches = 2)
    assert(batches == Vector(Vector(Long.MinValue, -1L, Long.MaxValue - 1L)))
    assert(exhausted)
  }

  test("range preserves values and terminates with a large negative step") {
    val (batches, exhausted) =
      readBatches(Long.MaxValue, Long.MinValue, -Long.MaxValue, maxBatches = 2)
    assert(batches == Vector(Vector(Long.MaxValue, 0L, -Long.MaxValue)))
    assert(exhausted)
  }

  test("range advances safely with a Long.MinValue step") {
    val (batches, exhausted) =
      readBatches(Long.MaxValue, Long.MinValue, Long.MinValue, maxBatches = 2)
    assert(batches == Vector(Vector(Long.MaxValue, -1L)))
    assert(exhausted)
  }

  test("range bounds a remaining count above Int.MaxValue before conversion") {
    val (batches, exhausted) = readBatches(0L, 1L << 32, 1L, maxBatches = 1)
    assert(batches == Vector((0L until 1000L).toVector))
    assert(!exhausted)
  }
}
