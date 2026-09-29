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

import org.apache.spark.{broadcast, SparkContext}
import org.apache.spark.sql.execution.SerializedHashTableBroadcastRelation
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * RDD for handling serialized broadcast hash tables built on the driver. This RDD deserializes the
 * hash table on each executor.
 */
case class VeloxSerializedBroadcastRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[BuildSideRelation],
    broadcastContext: BroadcastHashJoinContext)
  extends BroadcastBuildSideRDD(sc, broadcasted) {

  override def genBroadcastBuildSideIterator(): Iterator[ColumnarBatch] = {
    val serialized = broadcasted.value match {
      case relation: SerializedHashTableBroadcastRelation => relation.getSerializedHashTable
      case other =>
        throw new IllegalStateException(
          s"VeloxSerializedBroadcastRDD expects SerializedHashTableBroadcastRelation, " +
            s"but got: ${other.getClass.getName}")
    }
    VeloxBroadcastBuildSideCache.deserializeOnExecutor(
      serialized,
      broadcastContext.buildHashTableId,
      broadcastContext.deserializeHashTableTimeMetric
    )

    // Return empty iterator as hash table is already built
    Iterator.empty
  }

  /**
   * Get bloom filter metrics from the serialized hash table. This is called from the driver to get
   * metrics that were computed during hash table build.
   */
  def getBloomFilterMetrics: (Long, Long) = {
    val serialized = broadcasted.value match {
      case relation: SerializedHashTableBroadcastRelation => relation.getSerializedHashTable
      case other =>
        throw new IllegalStateException(
          s"VeloxSerializedBroadcastRDD expects SerializedHashTableBroadcastRelation, " +
            s"but got: ${other.getClass.getName}")
    }
    (serialized.bloomFilterBlocksByteSize, serialized.hashProbeDynamicFiltersProduced)
  }
}
