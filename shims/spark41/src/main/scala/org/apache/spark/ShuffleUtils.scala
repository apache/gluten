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
package org.apache.spark

import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleHandle}
import org.apache.spark.storage.{BlockId, BlockManagerId}

object ShuffleUtils {
  def getReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Tuple2[Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])], Boolean] = {
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    if (baseShuffleHandle.dependency.isShuffleMergeFinalizedMarked) {
      val res = SparkEnv.get.mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
        handle.shuffleId,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition)
      (res.iter.map(b => (b._1, b._2.toSeq)), res.enableBatchFetch)
    } else {
      val address = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
        handle.shuffleId,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition)
      (address.map(b => (b._1, b._2.toSeq)), true)
    }
  }
}
