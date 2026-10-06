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
package org.apache.spark.shuffle

import org.apache.gluten.execution.CPUStageMode
import org.apache.gluten.vectorized.{ColumnarBatchSerializerInstance, NativePartitioning}

import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.test.SharedSparkSession

/**
 * White-box regression test for the Celeborn-to-local shuffle fallback ClassCastException.
 *
 * When [[org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleManager]] falls back to the local
 * [[org.apache.spark.shuffle.sort.ColumnarShuffleManager]] (Celeborn service unavailable or a
 * fallback policy is applied), the read side dispatches to Gluten's local
 * [[ColumnarShuffleReader]]. The dependency is still a [[ColumnarShuffleDependency]], but its
 * serializer was bound at plan build time to the Celeborn serializer, which is a plain
 * `SerializerInstance` and not a [[ColumnarBatchSerializerInstance]].
 *
 * Before the fix, `read()` matched on the dependency type and hard-cast the serializer with
 * `asInstanceOf[ColumnarBatchSerializerInstance]`, throwing `ClassCastException` before any block
 * was fetched. After the fix, `read()` matches on the serializer instance type and routes a
 * non-columnar serializer through the per-stream `deserializeStream` path.
 *
 * The cast happens before the block-fetch iterator is constructed, so an empty `blocksByAddress` is
 * enough to distinguish the two behaviors without a real Celeborn cluster, network, or on-disk
 * shuffle data.
 */
class ColumnarShuffleReaderSuite extends SharedSparkSession {

  private def newColumnarDep(
      serializer: Serializer): ColumnarShuffleDependency[Int, Int, Int] = {
    val rdd = spark.sparkContext.parallelize(Seq((0, 0)), 1)
    new ColumnarShuffleDependency[Int, Int, Int](
      rdd,
      new HashPartitioner(1),
      serializer = serializer,
      nativePartitioning = new NativePartitioning("hash", 1),
      metrics = Map.empty[String, SQLMetric]
    )
  }

  private def readWith(serializer: Serializer): Seq[Product2[Int, Int]] = {
    val dep = newColumnarDep(serializer)
    val handle = new BaseShuffleHandle(dep.shuffleId, dep)
    val reader = new ColumnarShuffleReader[Int, Int](
      handle,
      // Empty input: the serializer cast happens before any block is fetched, so no real
      // shuffle data is needed to exercise the dispatch decision.
      Iterator.empty,
      TaskContext.empty(),
      new TempShuffleReadMetrics(),
      CPUStageMode
    )
    reader.read().toSeq
  }

  test(
    "read() tolerates a non-ColumnarBatchSerializerInstance on a ColumnarShuffleDependency " +
      "(Celeborn -> local fallback) instead of throwing ClassCastException") {
    // A plain row-based serializer stands in for the Celeborn serializer left on the
    // dependency after CelebornShuffleManager falls back to the local ColumnarShuffleManager.
    val nonColumnarSerializer = new JavaSerializer(spark.sparkContext.getConf)
    assert(!nonColumnarSerializer.newInstance().isInstanceOf[ColumnarBatchSerializerInstance])

    // Must not throw ClassCastException; empty input yields no rows.
    val rows = readWith(nonColumnarSerializer)
    assert(rows.isEmpty)
  }
}
