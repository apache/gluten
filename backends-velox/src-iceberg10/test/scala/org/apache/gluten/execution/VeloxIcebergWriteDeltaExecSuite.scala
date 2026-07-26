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

import org.apache.gluten.proto.IcebergNativeWriteMode

import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import org.apache.iceberg.{FileFormat, FileMetadata, PartitionSpec}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class VeloxIcebergWriteDeltaExecSuite extends AnyFunSuite {

  private val dataSchema = StructType(
    Seq(
      StructField("id", IntegerType),
      StructField("value", StringType)))
  private val deleteSchema = StructType(
    Seq(
      StructField("_file", StringType),
      StructField("_pos", LongType)))
  private val inputSchema = StructType(
    Seq(
      StructField("__row_operation", IntegerType),
      StructField("value", StringType),
      StructField("_file", StringType),
      StructField("id", IntegerType),
      StructField("_pos", LongType)
    ))

  test("build merge descriptor with resolved channels and existing deletion vector") {
    val existingDeletionVector = FileMetadata
      .deleteFileBuilder(PartitionSpec.unpartitioned())
      .ofPositionDeletes()
      .withPath("file:/table/old.puffin")
      .withFormat(FileFormat.PUFFIN)
      .withRecordCount(3)
      .withFileSizeInBytes(40)
      .withReferencedDataFile("file:/table/data.parquet")
      .withContentOffset(8)
      .withContentSizeInBytes(12)
      .build()

    val descriptor = VeloxIcebergWriteDeltaExec.buildNativeWriteInfo(
      RowLevelOperation.Command.MERGE,
      inputSchema,
      dataSchema,
      deleteSchema,
      Seq(existingDeletionVector))

    assert(descriptor.getWriteMode == IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_MERGE)
    assert(descriptor.getOperationColumnIndex == 0)
    assert(descriptor.getDataColumnIndicesList.asScala.toSeq == Seq(3, 1))
    assert(descriptor.getFilePathColumnIndex == 2)
    assert(descriptor.getRowPositionColumnIndex == 4)
    assert(descriptor.getExistingDeletionVectorsCount == 1)

    val existing = descriptor.getExistingDeletionVectors(0)
    assert(existing.getReferencedDataFile == "file:/table/data.parquet")
    assert(existing.getPuffinPath == "file:/table/old.puffin")
    assert(existing.getContentOffset == 8)
    assert(existing.getContentLength == 12)
    assert(existing.getRecordCount == 3)
  }

  test("build delete descriptor without data channels") {
    val descriptor = VeloxIcebergWriteDeltaExec.buildNativeWriteInfo(
      RowLevelOperation.Command.DELETE,
      inputSchema,
      dataSchema,
      deleteSchema,
      Seq.empty)

    assert(
      descriptor.getWriteMode ==
        IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR)
    assert(descriptor.getDataColumnIndicesCount == 0)
  }

}
