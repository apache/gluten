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

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

/**
 * A variant of spark's [[org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition]]
 * that holds the several input partitions Gluten groups into one split, and reports their combined
 * preferred locations.
 */
class SparkDataSourceRDDPartition(val index: Int, val inputPartitions: Seq[InputPartition])
  extends Partition
  with Serializable
  with InputPartition {

  override def preferredLocations(): Array[String] = {
    inputPartitions.flatMap(_.preferredLocations()).toArray
  }
}
