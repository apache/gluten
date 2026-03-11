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
package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.IcebergWriteJniWrapper
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.proto.{IcebergNestedField, IcebergPartitionField, IcebergPartitionSpec}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil

import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.ArrowSchema
import org.apache.iceberg.{PartitionSpec, SortOrder}
import org.apache.iceberg.transforms.IcebergTransformUtil

import java.util.stream.Collectors

case class IcebergDataWriteFactory(
    schema: StructType,
    format: Integer,
    directory: String,
    codec: String,
    partitionSpec: PartitionSpec,
    sortOrder: SortOrder,
    field: IcebergNestedField,
    queryId: String)
  extends ColumnarBatchDataWriterFactory
  with ColumnarStreamingDataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    createWriter(partitionId, taskId, 0)
  }

  /**
   * Returns a data writer to do the actual writing work. Note that, Spark will reuse the same data
   * object instance when sending data to the data writer, for better performance. Data writers are
   * responsible for defensive copies if necessary, e.g. copy the data before buffer it in a list.
   * <p> If this method fails (by throwing an exception), the corresponding Spark write task would
   * fail and get retried until hitting the maximum retry times.
   */
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    val fields = partitionSpec
      .fields()
      .stream()
      .map[IcebergPartitionField](f => IcebergTransformUtil.convertPartitionField(f, partitionSpec))
      .collect(Collectors.toList[IcebergPartitionField])
    val specProto = IcebergPartitionSpec
      .newBuilder()
      .setSpecId(partitionSpec.specId())
      .addAllFields(fields)
      .build()
    val operationId = queryId + "-" + epochId
    val (writerHandle, jniWrapper) =
      getJniWrapper(
        schema,
        format,
        directory,
        codec,
        partitionId,
        taskId,
        operationId,
        specProto,
        field)
    IcebergColumnarBatchDataWriter(writerHandle, jniWrapper, format, partitionSpec, sortOrder)
  }

  private def getJniWrapper(
      localSchema: StructType,
      format: Int,
      directory: String,
      codec: String,
      partitionId: Int,
      taskId: Long,
      operationId: String,
      partitionSpec: IcebergPartitionSpec,
      field: IcebergNestedField): (Long, IcebergWriteJniWrapper) = {
    val schema = SparkArrowUtil.toArrowSchema(localSchema, SQLConf.get.sessionLocalTimeZone)
    val arrowAlloc = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(arrowAlloc)
    ArrowAbiUtil.exportSchema(arrowAlloc, schema, cSchema)
    val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "IcebergWrite#write")
    val jniWrapper = new IcebergWriteJniWrapper(runtime)
    val writer =
      jniWrapper.init(
        cSchema.memoryAddress(),
        format,
        directory,
        codec,
        partitionId,
        taskId,
        operationId,
        partitionSpec.toByteArray,
        field.toByteArray)
    cSchema.close()
    (writer, jniWrapper)
  }
}
