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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, GenericInternalRow, Literal}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types.LongType

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for the lazy-split wrapper's stats!=null branch: batches whose [lowerBound, upperBound]
 * does not cover the literal must be pruned; batches whose range covers it must be returned.
 *
 * Pure JVM. Uses GenericInternalRow with the vanilla 5-slot-per-col schema (lower, upper,
 * nullCount, count, sizeInBytes).
 */
class ColumnarCachedBatchBuildFilterPruneSuite extends AnyFunSuite {

  // Build a CachedColumnarBatch with BIGINT 1-col stats [lower, upper].
  private def batchWithStats(numRows: Int, lower: Long, upper: Long): CachedColumnarBatch = {
    val stats = new GenericInternalRow(Array[Any](lower, upper, 0, numRows, numRows * 8L))
    CachedColumnarBatch(
      numRows = numRows,
      sizeInBytes = numRows * 8L,
      bytes = Array.fill[Byte](numRows * 4)(0),
      stats = stats)
  }

  test("EqualTo literal in [lower, upper] keeps the batch") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(50L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val batches: Iterator[CachedBatch] = Iterator(batchWithStats(10, 0L, 100L))
    val result = filter(0, batches).toList

    assert(result.length === 1, "batch with [0, 100] covers literal 50, must be kept")
    assert(result.head.numRows === 10)
  }

  test("EqualTo literal outside [lower, upper] prunes the batch") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(999L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val batches: Iterator[CachedBatch] = Iterator(batchWithStats(10, 0L, 100L))
    val result = filter(0, batches).toList

    assert(result.length === 0, "batch with [0, 100] cannot contain 999, must be pruned")
  }

  test("mixed null/non-null stats: null through, non-null pruned by predicate") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(50L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val nullStats = CachedColumnarBatch(
      numRows = 7,
      sizeInBytes = 28L,
      bytes = Array[Byte](9, 9),
      stats = null)
    val keptBatch = batchWithStats(10, 0L, 100L) // covers 50, kept
    val prunedBatch = batchWithStats(5, 200L, 300L) // does not cover 50, pruned

    val batches: Iterator[CachedBatch] = Iterator(nullStats, prunedBatch, keptBatch)
    val result = filter(0, batches).toList

    // Expected order: nullStats first (direct), then keptBatch.
    // prunedBatch dropped by parent.
    assert(
      result.length === 2,
      s"expected 2 (nullStats + keptBatch), got ${result.length}")
    assert(
      result.map(_.numRows) === Seq(7, 10),
      "order: stats=null pass-through first, then stats-covers-literal kept")
  }
}
