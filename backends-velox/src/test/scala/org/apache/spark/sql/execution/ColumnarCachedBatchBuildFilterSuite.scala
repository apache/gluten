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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for the buildFilter stats=null guard (lazy-split iterator wrapper). Without the wrapper,
 * vanilla `SimpleMetricsCachedBatchSerializer.buildFilter` NPEs on `partitionFilter.eval(null)`
 * (non-trivial predicates, both codegen and interpreted paths). A naive null-literal occupier row
 * would fail differently and silently drop every batch.
 *
 * Pure JVM, no native lib. Drives buildFilter on a synthetic Iterator[CachedBatch] of
 * CachedColumnarBatch with stats=null under a non-trivial predicate.
 */
class ColumnarCachedBatchBuildFilterSuite extends AnyFunSuite {

  test("buildFilter must direct stats=null batches through under EqualTo predicate") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", IntegerType, nullable = false)()
    val predicate = EqualTo(attr, Literal(5))
    val cachedAttributes = Seq(attr)

    val filter = serializer.buildFilter(Seq(predicate), cachedAttributes)

    // Three v1 binary batches: stats=null. A correct wrapper directs
    // them all through (no pruning, since we have no stats to prune on).
    val batches: Iterator[CachedBatch] = Iterator(
      CachedColumnarBatch(
        numRows = 3,
        sizeInBytes = 12L,
        bytes = Array[Byte](1, 2, 3),
        stats = null),
      CachedColumnarBatch(
        numRows = 5,
        sizeInBytes = 20L,
        bytes = Array[Byte](4, 5),
        stats = null),
      CachedColumnarBatch(
        numRows = 7,
        sizeInBytes = 28L,
        bytes = Array[Byte](6, 7, 8),
        stats = null)
    )

    // GREEN expectation: all 3 batches returned (stats=null -> direct through).
    // Without the wrapper: NPE thrown at partitionFilter.eval(null)
    //   OR silent drop (if naive occupier-row placeholder was used).
    val result = filter(0, batches).toList

    assert(
      result.length === 3,
      "v1 binary (stats=null) batches must all be returned (lazy-split wrapper); " +
        s"got ${result.length}")
    assert(
      result.map(_.numRows) === Seq(3, 5, 7),
      "batch order must be preserved through wrapper")
  }
}
