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

import io.substrait.proto.ReadRel
import org.scalatest.funsuite.AnyFunSuite

class IcebergScanTransformerSuite extends AnyFunSuite {
  private type DeleteFile =
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile

  test("validates complete Puffin deletion-vector metadata") {
    assert(
      IcebergScanTransformer.validateDeleteFile(deletionVector(), supportsEqualityDelete = false)
        .isEmpty)
  }

  test("rejects incomplete Puffin deletion-vector metadata") {
    val missingContentSize = deletionVector().toBuilder.clearContentSizeInBytes().build()

    assert(
      IcebergScanTransformer
        .validateDeleteFile(missingContentSize, supportsEqualityDelete = false)
        .contains("Puffin deletion vector has incomplete metadata"))
  }

  test("falls back for unsupported equality deletes") {
    val parquetEquality =
      ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile
        .newBuilder()
        .setFileContent(
          ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FileContent.EQUALITY_DELETES)
        .setFilePath("file:/table/delete.parquet")
        .setFileSize(128)
        .setRecordCount(2)
        .setParquet(ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.newBuilder())
        .build()
    assert(
      IcebergScanTransformer
        .validateDeleteFile(parquetEquality, supportsEqualityDelete = false)
        .contains("Contains equality delete files"))
    assert(
      IcebergScanTransformer
        .validateDeleteFile(parquetEquality, supportsEqualityDelete = true)
        .isEmpty)
  }

  private def deletionVector(): DeleteFile =
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile
      .newBuilder()
      .setFileContent(
        ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FileContent.POSITION_DELETES)
      .setFilePath("file:/table/delete.puffin")
      .setFileSize(256)
      .setRecordCount(3)
      .setPuffin(ReadRel.LocalFiles.FileOrFiles.PuffinReadOptions.newBuilder())
      .setContentOffset(64)
      .setContentSizeInBytes(32)
      .setReferencedDataFile("file:/table/data.parquet")
      .setDataSequenceNumber(7)
      .build()
}
