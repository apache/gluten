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

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleWriterFactory

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

class BoltCelebornColumnarShuffleWriterFactory extends CelebornShuffleWriterFactory {

  override def createShuffleWriterInstance[K, V](
      shuffleId: Int,
      handle: CelebornShuffleHandle[K, V, V],
      context: TaskContext,
      celebornConf: CelebornConf,
      client: ShuffleClient,
      writeMetrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    new BoltCelebornColumnarShuffleWriter[K, V](
      shuffleId,
      handle,
      context,
      celebornConf,
      client,
      writeMetrics)
  }
}
