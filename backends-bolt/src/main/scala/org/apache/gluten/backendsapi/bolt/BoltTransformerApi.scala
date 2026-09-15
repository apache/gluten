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
package org.apache.gluten.backendsapi.bolt

import org.apache.gluten.backendsapi.{BackendsApiManager, TransformerApi}
import org.apache.gluten.config.BoltConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.proto.ConfigMap
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.utils.PartitionsUtil
import org.apache.gluten.vectorized.PlanEvaluatorJniWrapper

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.execution.HiveFileFormat
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources
import org.apache.spark.util.collection.BitSet

import com.google.protobuf.{Any, Message}

import java.util.{Map => JMap}

class BoltTransformerApi extends TransformerApi with Logging {

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
      filterExprs: Seq[Expression] = Seq.empty): Seq[Partition] = {
    PartitionsUtil(
      relation,
      requiredSchema,
      selectedPartitions,
      output,
      bucketedScan,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)
      .genPartitionSeq()
  }

  override def postProcessNativeConfig(
      nativeConfMap: JMap[String, String],
      backendPrefix: String): Unit = {
    BoltConfig.postProcessNativeBackendConf(nativeConfMap)
  }

  override def createCheckOverflowExprNode(
      context: SubstraitContext,
      substraitExprName: String,
      childNode: ExpressionNode,
      childResultType: DataType,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode = {
    if (childResultType.equals(dataType)) {
      childNode
    } else {
      val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
      ExpressionBuilder.makeCast(typeNode, childNode, nullOnOverflow)
    }
  }

  override def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String = {
    TaskResources.runUnsafe {
      val jniWrapper = PlanEvaluatorJniWrapper.create(
        Runtimes.contextInstance(
          BackendsApiManager.getBackendName,
          "BoltTransformerApi#getNativePlanString"))
      jniWrapper.nativePlanString(substraitPlan, details)
    }
  }

  override def packPBMessage(message: Message): Any = Any.pack(message, "")

  override def genWriteParameters(write: WriteFilesExecTransformer): Any = {
    write.fileFormat match {
      case _ @(_: ParquetFileFormat | _: HiveFileFormat) =>
        // Only Parquet is supported. It's safe to set a fixed "parquet" here
        // because others already fell back by WriteFilesExecTransformer's validation.
        val shortName = "parquet"
        val nativeConf =
          GlutenFormatFactory(shortName)
            .nativeConf(
              write.caseInsensitiveOptions,
              WriteFilesExecTransformer.getCompressionCodec(write.caseInsensitiveOptions))
        packPBMessage(
          ConfigMap
            .newBuilder()
            .putAllConfigs(nativeConf)
            .putConfigs("format", shortName)
            .build())
      case _ =>
        throw new GlutenException("Unsupported file write format: " + write.fileFormat)
    }
  }
}
