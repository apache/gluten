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

import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.execution.metric.SQLMetric

abstract class SettableColumnarBatchSerializer(
    val readBatchNumRows: SQLMetric,
    val numOutputRows: SQLMetric,
    val decompressTime: SQLMetric,
    val deserializeTime: SQLMetric,
    val deserializerCreatTime: SQLMetric,
    val deserializerDestroyTime: SQLMetric,
    val mergeTime: SQLMetric,
    val totalReadTime: SQLMetric,
    val shuffleWallTime: SQLMetric)
  extends Serializer
  with Serializable {

  protected var numPartitions = -1
  protected var partitionShortName = ""

  // if true, return raw stream instead of columnar batch
  // if returnRawStream is False, then it cannot be modified anymore
  private var returnRawStream = Option.empty[Boolean]

  def setNumPartitions(numPartitions: Int): Unit = {
    this.numPartitions = numPartitions
  }

  def setPartitionShortName(shortName: String): Unit = {
    this.partitionShortName = shortName
  }
}
