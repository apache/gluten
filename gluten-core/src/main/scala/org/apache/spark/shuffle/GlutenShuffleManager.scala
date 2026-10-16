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

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.annotation.Experimental

/**
 * Shuffle manager that routes shuffle API calls to different shuffle managers registered by
 * different backends.
 *
 * A SPIP may cause refactoring of this class in the future:
 * https://issues.apache.org/jira/browse/SPARK-45792
 *
 * Experimental: This is not expected to be used in production yet. Use backend shuffle manager
 * (e.g., ColumnarShuffleManager or other RSS shuffle manager provided in Gluten's code
 * base)instead.
 *
 * TODO: Spark 4.3 requires `spark.shuffle.manager` to be a `BlockingShuffleManager` (a
 * `private[spark]` subtype of `ShuffleManager` that doesn't exist before 4.3) and rejects any other
 * implementation at startup, so this class can't be used as the shuffle manager on Spark 4.3 yet.
 */
@Experimental
class GlutenShuffleManager(conf: SparkConf, isDriver: Boolean) extends ShuffleManager {
  private val routerBuilder = ShuffleManagerRegistry.get().newRouterBuilder(conf, isDriver)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    routerBuilder.getOrBuild().registerShuffle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    routerBuilder.getOrBuild().getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    routerBuilder
      .getOrBuild()
      .getReader(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    routerBuilder.getOrBuild().unregisterShuffle(shuffleId)
  }

  // Not marked `override`: Spark 4.3 moved `shuffleBlockResolver` to `BlockingShuffleManager`.
  def shuffleBlockResolver: ShuffleBlockResolver = {
    routerBuilder.getOrBuild().shuffleBlockResolver
  }

  override def stop(): Unit = {
    routerBuilder.getOrBuild().stop()
  }
}
