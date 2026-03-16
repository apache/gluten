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

import org.apache.gluten.iterator.Iterators

import org.apache.spark.{broadcast, SparkContext}
import org.apache.spark.sql.execution.ColumnarBuildSideRelation
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class VeloxBroadcastBuildSideRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[BuildSideRelation],
    broadcastContext: BroadcastHashJoinContext,
    isBNL: Boolean = false)
  extends BroadcastBuildSideRDD(sc, broadcasted) {

  override def genBroadcastBuildSideIterator(): Iterator[ColumnarBatch] = {
    val offload = broadcasted.value.asReadOnlyCopy() match {
      case columnar: ColumnarBuildSideRelation =>
        columnar.offload
      case unsafe: UnsafeColumnarBuildSideRelation =>
        unsafe.isOffload
    }
    val output = if (isBNL || !offload) {
      val relation = broadcasted.value.asReadOnlyCopy()
      Iterators
        .wrap(relation.deserialized)
        .recyclePayload(batch => batch.close())
        .create()
    } else {
      VeloxBroadcastBuildSideCache.getOrBuildBroadcastHashTable(broadcasted, broadcastContext)
      Iterator.empty
    }

    output
  }
}
