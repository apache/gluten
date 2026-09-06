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
package org.apache.gluten.execution.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BlockStripes, OutputWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

trait GlutenFormatWriterInjects {
  def createOutputWriter(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: java.util.Map[String, String]): OutputWriter

  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType]

  def getWriterWrappedSparkPlan(plan: SparkPlan): SparkPlan

  def nativeConf(
      options: Map[String, String],
      compressionCodec: String): java.util.Map[String, String]

  def formatName: String
}

trait GlutenRowSplitter {
  def splitBlockByPartitionAndBucket(
      batch: ColumnarBatch,
      partitionColIndice: Array[Int],
      hasBucket: Boolean,
      reservePartitionColumns: Boolean = false): BlockStripes
}

object GlutenFormatFactory {
  private var instances: Map[String, GlutenFormatWriterInjects] = _
  private var postRuleFactory: SparkSession => Rule[SparkPlan] = _
  private var rowSplitterInstance: GlutenRowSplitter = _

  def register(items: GlutenFormatWriterInjects*): Unit = {
    instances = items.map(item => (item.formatName, item)).toMap
  }

  def isRegistered(name: String): Boolean = instances.contains(name)

  def apply(name: String): GlutenFormatWriterInjects = {
    instances.getOrElse(
      name,
      throw new IllegalStateException(s"GlutenFormatWriterInjects for $name is not initialized"))
  }

  def injectPostRuleFactory(factory: SparkSession => Rule[SparkPlan]): Unit = {
    postRuleFactory = factory
  }

  def register(rowSplitter: GlutenRowSplitter): Unit = {
    rowSplitterInstance = rowSplitter
  }

  def rowSplitter: GlutenRowSplitter = {
    if (rowSplitterInstance == null) {
      throw new IllegalStateException("GlutenRowSplitter is not initialized")
    }
    rowSplitterInstance
  }
}
