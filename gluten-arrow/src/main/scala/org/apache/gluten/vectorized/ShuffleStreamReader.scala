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
package org.apache.gluten.vectorized

import org.apache.spark.storage.BlockId

import java.io.InputStream

case class ShuffleStreamReader(streams: Iterator[(BlockId, InputStream)]) {
  private val jniStreams = streams.map {
    case (blockId, in) =>
      JniByteInputStreams.create(in)
  }

  // Called from native side to get the next stream. The native caller should make sure
  // the streams are properly closed.
  def nextStream(): JniByteInputStream = {
    if (jniStreams.hasNext) {
      jniStreams.next
    } else {
      null
    }
  }
}
