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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.TransformerApi
import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.execution.{CHHashAggregateExecTransformer, WriteFilesExecTransformer}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{BooleanLiteralNode, ExpressionBuilder, ExpressionNode}
import org.apache.gluten.utils.{CHPartitionsUtil, ExpressionDocUtil}

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.delta.MergeTreeFileFormat
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v1.Write
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.internal.SparkConfigUtil
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.BitSet

import com.google.common.collect.Lists
import com.google.protobuf.{Any, Message}
import org.apache.hadoop.fs.Path

import java.util

class CHTransformerApi extends TransformerApi with Logging {

  def genPartitionSeq(
      relation: HadoopFsRelation,
      requiredSchema: StructType,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean,
      filterExprs: Seq[Expression]): Seq[Partition] = {
    relation.location match {
      case index: TahoeFileIndex
          if relation.fileFormat
            .isInstanceOf[DeltaMergeTreeFileFormat] =>
        // Generate NativeMergeTreePartition for MergeTree
        ClickHouseTableV2
          .partsPartitions(
            index.deltaLog,
            relation,
            selectedPartitions,
            output,
            bucketedScan,
            optionalBucketSet,
            optionalNumCoalescedBuckets,
            disableBucketedScan,
            filterExprs
          )
      case _ =>
        // Generate FilePartition for Parquet
        CHPartitionsUtil(
          relation,
          selectedPartitions,
          bucketedScan,
          optionalBucketSet,
          optionalNumCoalescedBuckets).genPartitionSeq()
    }
  }

  override def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {

    require(backendPrefix == CHConfig.CONF_PREFIX)
    if (nativeConfMap.getOrDefault(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, "false").toBoolean) {
      val offHeapSize: Long =
        SparkConfigUtil.get(nativeConfMap, GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES)
      if (offHeapSize > 0) {
        // Only set default max_bytes_before_external_group_by for CH when it is not set explicitly.
        val groupBySpillKey = CHConfig.runtimeSettings("max_bytes_before_external_group_by")
        if (!nativeConfMap.containsKey(groupBySpillKey)) {
          val groupBySpillValue = offHeapSize * 0.5
          nativeConfMap.put(groupBySpillKey, groupBySpillValue.toLong.toString)
        }

        val maxMemoryUsageKey = CHConfig.runtimeSettings("max_memory_usage")
        if (!nativeConfMap.containsKey(maxMemoryUsageKey)) {
          val maxMemoryUsageValue = offHeapSize
          nativeConfMap.put(maxMemoryUsageKey, maxMemoryUsageValue.toString)
        }

        // Only set default max_bytes_before_external_join for CH when join_algorithm is grace_hash
        val joinAlgorithmKey = CHConfig.runtimeSettings("join_algorithm")
        if (
          nativeConfMap.containsKey(joinAlgorithmKey) &&
          nativeConfMap.get(joinAlgorithmKey) == "grace_hash"
        ) {
          val joinSpillKey = CHConfig.runtimeSettings("max_bytes_in_join")
          if (!nativeConfMap.containsKey(joinSpillKey)) {
            val joinSpillValue = offHeapSize * 0.7
            nativeConfMap.put(joinSpillKey, joinSpillValue.toLong.toString)
          }
        }
      }
    }

    val injectConfig: (String, String) => Unit = (srcKey, dstKey) => {
      if (nativeConfMap.containsKey(srcKey) && !nativeConfMap.containsKey(dstKey)) {
        nativeConfMap.put(dstKey, nativeConfMap.get(srcKey))
      }
    }

    val hdfsConfigPrefix = CHConfig.runtimeConfig("hdfs")
    injectConfig("spark.hadoop.input.connect.timeout", s"$hdfsConfigPrefix.input_connect_timeout")
    injectConfig("spark.hadoop.input.read.timeout", s"$hdfsConfigPrefix.input_read_timeout")
    injectConfig("spark.hadoop.input.write.timeout", s"$hdfsConfigPrefix.input_write_timeout")
    injectConfig(
      "spark.hadoop.dfs.client.log.severity",
      s"$hdfsConfigPrefix.dfs_client_log_severity")

    // Respect spark config spark.sql.orc.compression.codec for CH backend
    // TODO: consider compression or orc.compression in table options.
    val orcCompressionKey = CHConfig.runtimeSettings("output_format_orc_compression_method")
    if (!nativeConfMap.containsKey(orcCompressionKey)) {
      if (nativeConfMap.containsKey("spark.sql.orc.compression.codec")) {
        val compression = nativeConfMap.get("spark.sql.orc.compression.codec").toLowerCase()
        compression match {
          case "none" => nativeConfMap.put(orcCompressionKey, "none")
          case "uncompressed" => nativeConfMap.put(orcCompressionKey, "none")
          case "snappy" => nativeConfMap.put(orcCompressionKey, "snappy")
          case "zlib" => nativeConfMap.put(orcCompressionKey, "zlib")
          case "zstd" => nativeConfMap.put(orcCompressionKey, "zstd")
          case "lz4" => nativeConfMap.put(orcCompressionKey, "lz4")
          case _ =>
            throw new UnsupportedOperationException(s"Not supported ORC compression: $compression")
        }
      } else {
        nativeConfMap.put(orcCompressionKey, "snappy")
      }
    }

    if (nativeConfMap.containsKey(CHConfig.ENABLE_GLUTEN_LOCAL_FILE_CACHE.key)) {
      // We can't use gluten_cache.local.enabled
      // because FileCacheSettings doesn't contain this field.
      nativeConfMap.put(
        CHConfig.runtimeConfig("enable.gluten_cache.local"),
        nativeConfMap.get(CHConfig.ENABLE_GLUTEN_LOCAL_FILE_CACHE.key))
      nativeConfMap.remove(CHConfig.ENABLE_GLUTEN_LOCAL_FILE_CACHE.key)
    }
  }

  override def getSupportExpressionClassName: util.Set[String] = {
    ExpressionDocUtil.supportExpression()
  }

  override def getPlanOutput(plan: SparkPlan): Seq[Attribute] = {
    plan match {
      case hash: HashAggregateExec =>
        // when grouping expression has alias,
        // output name will be different from grouping expressions,
        // so using output attribute instead of grouping expression
        val groupingExpressions = hash.output.splitAt(hash.groupingExpressions.size)._1
        val aggResultAttributes = CHHashAggregateExecTransformer
          .getCHAggregateResultExpressions(
            groupingExpressions,
            hash.aggregateExpressions,
            hash.resultExpressions
          )
          .map(_.toAttribute)
        if (aggResultAttributes.size == hash.output.size) {
          aggResultAttributes
        } else {
          var output = Seq.empty[Attribute]
          for (i <- hash.output.indices) {
            if (i < groupingExpressions.size) {
              output = output :+ aggResultAttributes(i)
            } else {
              output = output :+ hash.output(i)
            }
          }
          output
        }
      case _ =>
        plan.output
    }

  }

  override def createCheckOverflowExprNode(
      context: SubstraitContext,
      substraitExprName: String,
      childNode: ExpressionNode,
      childResultType: DataType,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode = {
    val functionId = context.registerFunction(
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(dataType, BooleanType),
        ConverterUtils.FunctionConfig.OPT))

    // Just make a fake toType value, because native engine cannot accept datatype itself.
    val toTypeNodes =
      ExpressionBuilder.makeDecimalLiteral(new Decimal().set(0, dataType.precision, dataType.scale))
    val expressionNodes =
      Lists.newArrayList(childNode, new BooleanLiteralNode(nullOnOverflow), toTypeNodes)
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }

  override def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String = {
    throw new UnsupportedOperationException("CH backend does not support this method")
  }

  override def packPBMessage(message: Message): Any = Any.pack(message)

  override def invalidateSQLExecutionResource(executionId: String): Unit = {
    GlutenDriverEndpoint.invalidateResourceRelation(executionId)
  }

  override def genWriteParameters(writeExec: WriteFilesExecTransformer): Any = {
    val fileFormatStr = writeExec.fileFormat match {
      case register: DataSourceRegister =>
        register.shortName
      case _ => "UnknownFileFormat"
    }
    val childOutput = writeExec.child.output

    val partitionIndexes =
      writeExec.partitionColumns.map(p => childOutput.indexWhere(_.exprId == p.exprId))
    require(partitionIndexes.forall(_ >= 0))

    val common = Write.Common
      .newBuilder()
      .setFormat(s"$fileFormatStr")
      .setJobTaskAttemptId("") // we cannot get job and task id at the driver side)
    partitionIndexes.foreach {
      idx =>
        require(idx >= 0)
        common.addPartitionColIndex(idx)
    }

    val write = Write.newBuilder().setCommon(common.build())

    writeExec.fileFormat match {
      case d: MergeTreeFileFormat =>
        write.setMergetree(MergeTreeFileFormat.createWrite(d.metadata))
      case _: ParquetFileFormat =>
        write.setParquet(Write.ParquetWrite.newBuilder().build())
      case _: OrcFileFormat =>
        write.setOrc(Write.OrcWrite.newBuilder().build())
    }
    packPBMessage(write.build())
  }

  /** use Hadoop Path class to encode the file path */
  override def encodeFilePathIfNeed(filePath: String): String =
    new Path(filePath).toUri.toASCIIString
}
