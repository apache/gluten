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
package org.apache.iceberg.spark.source

import org.apache.gluten.connector.write.PartitionDataJson

import org.apache.iceberg._
import org.apache.iceberg.io.{FileIO, InputFile, OutputFile}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

import scala.collection.mutable.ArrayBuffer

class SparkPositionDeltaWriteUtilSuite extends AnyFunSuite {

  test("convert mixed native data and deletion-vector results") {
    val schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()))
    val spec = PartitionSpec.builderFor(schema).identity("id").build()
    val partition = new PartitionDataJson(Array[AnyRef](Int.box(7)))
    val dataFile = DataFiles
      .builder(spec)
      .withPath("file:/table/id=7/old.parquet")
      .withFormat(FileFormat.PARQUET)
      .withPartition(partition)
      .withRecordCount(10)
      .withFileSizeInBytes(100)
      .build()
    val oldDv = FileMetadata
      .deleteFileBuilder(spec)
      .ofPositionDeletes()
      .withPath("file:/table/id=7/old.puffin")
      .withFormat(FileFormat.PUFFIN)
      .withPartition(partition)
      .withRecordCount(2)
      .withFileSizeInBytes(40)
      .withReferencedDataFile(dataFile.location())
      .withContentOffset(8)
      .withContentSizeInBytes(12)
      .build()
    val referenced = new SparkPositionDeltaWriteUtil.ReferencedDataFile(dataFile, spec, oldDv)

    val dataResult =
      s"""{
         |  "path":"file:/table/id=7/new.parquet",
         |  "fileSizeInBytes":120,
         |  "metrics":{"recordCount":3},
         |  "partitionSpecJson":${spec.specId()},
         |  "partitionDataJson":"{\\"partitionValues\\":[7]}",
         |  "fileFormat":"PARQUET",
         |  "content":"DATA"
         |}""".stripMargin
    val dvResult =
      s"""{
         |  "path":"file:/table/id=7/new.puffin",
         |  "fileSizeInBytes":50,
         |  "metrics":{"recordCount":4},
         |  "partitionSpecJson":${spec.specId()},
         |  "fileFormat":"PUFFIN",
         |  "referencedDataFile":"${dataFile.location()}",
         |  "content":"POSITION_DELETES",
         |  "contentOffset":16,
         |  "contentSizeInBytes":20
         |}""".stripMargin

    val message = SparkPositionDeltaWriteUtil.toDeltaTaskCommit(
      Collections.singletonMap(Int.box(spec.specId()), spec),
      SortOrder.unsorted(),
      FileFormat.PARQUET,
      Array(dataResult, dvResult),
      Collections.singletonMap(dataFile.location().toString, referenced)
    )
    val commit = message.asInstanceOf[SparkPositionDeltaWrite.DeltaTaskCommit]

    assert(commit.dataFiles().length == 1)
    assert(commit.dataFiles()(0).recordCount() == 3)
    assert(commit.dataFiles()(0).partition().get(0, classOf[Integer]) == Int.box(7))

    assert(commit.deleteFiles().length == 1)
    val newDv = commit.deleteFiles()(0)
    assert(newDv.format() == FileFormat.PUFFIN)
    assert(newDv.referencedDataFile() == dataFile.location().toString)
    assert(newDv.recordCount() == 4)
    assert(newDv.contentOffset() == 16)
    assert(newDv.contentSizeInBytes() == 20)
    assert(newDv.partition().get(0, classOf[Integer]) == Int.box(7))

    assert(commit.rewrittenDeleteFiles().sameElements(Array(oldDv)))
    assert(commit.referencedDataFiles().sameElements(Array(dataFile.location())))
  }

  test("reject deletion vector for an unplanned data file") {
    val spec = PartitionSpec.unpartitioned()
    val dvResult =
      """{
        |  "path":"file:/table/new.puffin",
        |  "fileSizeInBytes":50,
        |  "metrics":{"recordCount":4},
        |  "partitionSpecJson":0,
        |  "fileFormat":"PUFFIN",
        |  "referencedDataFile":"file:/table/missing.parquet",
        |  "content":"POSITION_DELETES",
        |  "contentOffset":16,
        |  "contentSizeInBytes":20
        |}""".stripMargin

    val error = intercept[IllegalArgumentException] {
      SparkPositionDeltaWriteUtil.toDeltaTaskCommit(
        Collections.singletonMap(Int.box(spec.specId()), spec),
        SortOrder.unsorted(),
        FileFormat.PARQUET,
        Array(dvResult),
        Collections.emptyMap[String, SparkPositionDeltaWriteUtil.ReferencedDataFile]()
      )
    }

    assert(error.getMessage.contains("unplanned data file"))
  }

  test("reject incomplete deletion-vector metadata") {
    val spec = PartitionSpec.unpartitioned()
    val dataFile = DataFiles
      .builder(spec)
      .withPath("file:/table/data.parquet")
      .withFormat(FileFormat.PARQUET)
      .withRecordCount(10)
      .withFileSizeInBytes(100)
      .build()
    val referenced = new SparkPositionDeltaWriteUtil.ReferencedDataFile(dataFile, spec, null)
    val dvResult =
      s"""{
         |  "path":"file:/table/new.puffin",
         |  "fileSizeInBytes":50,
         |  "metrics":{"recordCount":4},
         |  "partitionSpecJson":0,
         |  "fileFormat":"PUFFIN",
         |  "referencedDataFile":"${dataFile.location()}",
         |  "content":"POSITION_DELETES",
         |  "contentOffset":16
         |}""".stripMargin

    val error = intercept[IllegalArgumentException] {
      SparkPositionDeltaWriteUtil.toDeltaTaskCommit(
        Collections.singletonMap(Int.box(spec.specId()), spec),
        SortOrder.unsorted(),
        FileFormat.PARQUET,
        Array(dvResult),
        Collections.singletonMap(dataFile.location().toString, referenced)
      )
    }

    assert(error.getMessage.contains("content size"))
  }

  test("delete native files after commit conversion failure") {
    val deleted = ArrayBuffer.empty[String]
    val fileIO = new FileIO {
      override def newInputFile(path: String): InputFile = null
      override def newOutputFile(path: String): OutputFile = null
      override def deleteFile(path: String): Unit = deleted += path
    }
    val cause = new IllegalArgumentException("conversion failed")
    val valid =
      """{
        |  "path":"file:/table/orphan.puffin",
        |  "fileSizeInBytes":50
        |}""".stripMargin

    SparkPositionDeltaWriteUtil.deleteNativeFiles(
      fileIO,
      Array(valid, valid, "not-json"),
      cause)

    assert(deleted.toSeq == Seq("file:/table/orphan.puffin"))
    assert(cause.getSuppressed.length == 1)
  }
}
