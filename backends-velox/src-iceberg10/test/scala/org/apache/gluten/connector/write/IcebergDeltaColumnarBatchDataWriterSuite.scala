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

import org.apache.gluten.execution.IcebergWriteJniWrapper

import org.apache.spark.broadcast.Broadcast

import org.apache.iceberg.{PartitionSpec, SortOrder}
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.spark.source.SparkPositionDeltaWriteUtil
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util

class IcebergDeltaColumnarBatchDataWriterSuite extends AnyFunSuite {

  test("abort and close release native writer once") {
    val jniWrapper = mock(classOf[IcebergWriteJniWrapper])
    val writer = newWriter(jniWrapper, mock(classOf[FileIO]))

    writer.abort()
    writer.abort()
    writer.close()
    writer.close()

    verify(jniWrapper, times(1)).abort(7L)
    verify(jniWrapper, times(1)).close(7L)
  }

  test("commit conversion failure deletes native output") {
    val jniWrapper = mock(classOf[IcebergWriteJniWrapper])
    val fileIO = mock(classOf[FileIO])
    val nativeResult =
      """{
        |  "path":"file:/table/orphan.puffin",
        |  "fileSizeInBytes":50,
        |  "metrics":{"recordCount":4},
        |  "partitionSpecJson":0,
        |  "fileFormat":"PUFFIN",
        |  "referencedDataFile":"file:/table/missing.parquet",
        |  "content":"POSITION_DELETES",
        |  "contentOffset":16,
        |  "contentSizeInBytes":20
        |}""".stripMargin
    when(jniWrapper.commit(7L)).thenReturn(Array(nativeResult))
    val writer = newWriter(jniWrapper, fileIO)

    val error = intercept[IllegalArgumentException](writer.commit())

    assert(error.getMessage.contains("unplanned data file"))
    verify(fileIO).deleteFile("file:/table/orphan.puffin")
    writer.abort()
    verify(jniWrapper, times(0)).abort(7L)
  }

  private def newWriter(
      jniWrapper: IcebergWriteJniWrapper,
      fileIO: FileIO): IcebergDeltaColumnarBatchDataWriter = {
    val referenced = mock(classOf[Broadcast[_]])
      .asInstanceOf[Broadcast[
        util.Map[String, SparkPositionDeltaWriteUtil.ReferencedDataFile]]]
    when(referenced.value)
      .thenReturn(util.Collections.emptyMap[
        String,
        SparkPositionDeltaWriteUtil.ReferencedDataFile]())
    IcebergDeltaColumnarBatchDataWriter(
      7L,
      jniWrapper,
      1,
      util.Collections.singletonMap(Int.box(0), PartitionSpec.unpartitioned()),
      SortOrder.unsorted(),
      fileIO,
      referenced)
  }
}
