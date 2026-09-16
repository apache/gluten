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
package org.apache.gluten.backendsapi

import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}
import org.apache.spark.util.collection.BitSet

import com.google.protobuf.{Any, Message}

import java.util

trait TransformerApi {

  /** Generate Seq[Partition] for FileSourceScanExecTransformer. */
  def genPartitionSeq(
      relation: HadoopFsRelation,
      requiredSchema: StructType,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean,
      filterExprs: Seq[Expression] = Seq.empty): Seq[Partition]

  /**
   * Post-process native config, For example, for ClickHouse backend, sync 'spark.executor.cores' to
   * 'spark.gluten.sql.columnar.backend.ch.runtime_settings.max_threads'
   */
  def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {}

  def getSupportExpressionClassName: util.Set[String] = {
    util.Collections.emptySet()
  }

  def getPlanOutput(plan: SparkPlan): Seq[Attribute] = {
    plan.output
  }

  def createCheckOverflowExprNode(
      context: SubstraitContext,
      substraitExprName: String,
      childNode: ExpressionNode,
      childResultType: DataType,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode

  def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String

  def packPBMessage(message: Message): Any

  /** Packs Iceberg column initial defaults into a backend-specific read extension. */
  def packIcebergReadExtension(
      fieldIds: util.Map[String, Integer],
      initialDefaults: util.Map[String, String]): Any = {
    throw new UnsupportedOperationException("Iceberg initial-default reads are not supported")
  }

  /** This method is only used for CH backend tests */
  def invalidateSQLExecutionResource(executionId: String): Unit = {}

  def genWriteParameters(write: WriteFilesExecTransformer): Any

  /** use Hadoop Path class to encode the file path */
  def encodeFilePathIfNeed(filePath: String): String = filePath
}
