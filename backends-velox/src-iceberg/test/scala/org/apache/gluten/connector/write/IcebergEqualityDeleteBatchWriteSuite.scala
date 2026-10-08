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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.write.WriterCommitMessage

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{DataFiles, FileFormat, FileMetadata, IsolationLevel, Metrics, PartitionSpec, RowDelta, Schema, Table, TableProperties}
import org.apache.iceberg.exceptions.{CommitStateUnknownException, ValidationException}
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.types.{Conversions, Types}
import org.mockito.Mockito.{doThrow, mock, never, verify, when, RETURNS_SELF}

import java.nio.file.Files
import java.util.Collections

import scala.collection.JavaConverters._

class IcebergEqualityDeleteBatchWriteSuite extends SparkFunSuite {
  private val schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()))

  private def withTestTable(f: Table => Unit): Unit = {
    val directory = Files.createTempDirectory("gluten-delete-commit").toFile
    try {
      val table = new HadoopTables(new Configuration()).create(
        schema,
        PartitionSpec.unpartitioned(),
        Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"),
        directory.toURI.toString)
      append(table, 1)
      f(table)
    } finally FileUtils.deleteDirectory(directory)
  }

  private def append(table: Table, id: Int): Unit = {
    val bound = Conversions.toByteBuffer(Types.IntegerType.get(), Int.box(id))
    val metrics = new Metrics(
      1L,
      null,
      null,
      null,
      null,
      Collections.singletonMap(Int.box(1), bound),
      Collections.singletonMap(Int.box(1), bound))
    table.newAppend().appendFile(DataFiles.builder(table.spec())
      .withPath(s"${table.location()}/data-$id-${java.util.UUID.randomUUID()}.parquet").withFormat(
        FileFormat.PARQUET)
      .withFileSizeInBytes(1L).withMetrics(metrics).build()).commit()
  }

  private def messages(table: Table): Array[WriterCommitMessage] = {
    val path = s"${table.location()}/delete.parquet"
    table.io().newOutputFile(path).create().close()
    Array(IcebergEqualityDeleteCommitMessage(Array(FileMetadata.deleteFileBuilder(table.spec())
      .ofEqualityDeletes(1).withPath(path).withFormat(FileFormat.PARQUET)
      .withFileSizeInBytes(0).withRecordCount(1).build())))
  }

  private def writer(table: Table, filter: Expression): IcebergEqualityDeleteBatchWrite =
    new IcebergEqualityDeleteBatchWrite(
      table,
      table.currentSnapshot().snapshotId(),
      filter,
      true,
      "test-app")

  test("conflicting concurrent inserts reject the delete and its uncommitted files are removed") {
    withTestTable {
      table =>
        val write = writer(table, Expressions.equal("id", Int.box(2)))
        val commit = messages(table)
        append(table, 2)
        intercept[ValidationException](write.commit(commit))
        write.abort(commit)
        assert(!table.io().newInputFile(s"${table.location()}/delete.parquet").exists())
        assert(table.currentSnapshot().summary().get("total-equality-deletes") == "0")
    }
  }

  test("unrelated concurrent inserts allow the equality delete to commit") {
    withTestTable {
      table =>
        val write = writer(table, Expressions.equal("id", Int.box(1)))
        val commit = messages(table)
        append(table, 2)
        write.commit(commit)
        assert(table.currentSnapshot().summary().get("total-equality-deletes") == "1")
        assert(table.currentSnapshot().summary().get("spark.app.id") == "test-app")
        assert(table.io().newInputFile(s"${table.location()}/delete.parquet").exists())
    }
  }

  test("concurrent deletes reject a stale equality delete") {
    withTestTable {
      table =>
        val write = writer(table, Expressions.alwaysTrue())
        val commit = messages(table)
        val concurrent = FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
          .withPath(s"${table.location()}/concurrent.parquet").withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(0).withRecordCount(1).build()
        table.newRowDelta().addDeletes(concurrent).commit()
        intercept[ValidationException](write.commit(commit))
        write.abort(commit)
        assert(!table.io().newInputFile(s"${table.location()}/delete.parquet").exists())
    }
  }

  test("abort before commit cleans completed task files and tolerates missing task messages") {
    withTestTable {
      table =>
        val write = writer(table, Expressions.alwaysTrue())
        write.abort(messages(table) ++ Array[WriterCommitMessage](null))
        assert(!table.io().newInputFile(s"${table.location()}/delete.parquet").exists())
    }
  }

  test("an unknown commit outcome retains files that may already be referenced by a snapshot") {
    val table = mock(classOf[Table])
    val delta = mock(classOf[RowDelta], RETURNS_SELF)
    val io = mock(classOf[FileIO])
    when(table.newRowDelta()).thenReturn(delta)
    when(table.io()).thenReturn(io)
    doThrow(new CommitStateUnknownException(new RuntimeException("lost commit response")))
      .when(delta).commit()
    val file = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned()).ofEqualityDeletes(1)
      .withPath("delete.parquet").withFormat(FileFormat.PARQUET)
      .withFileSizeInBytes(0).withRecordCount(1).build()
    val write =
      new IcebergEqualityDeleteBatchWrite(table, 1L, Expressions.alwaysTrue(), true, "test")
    val commit = Array[WriterCommitMessage](IcebergEqualityDeleteCommitMessage(Array(file)))
    intercept[CommitStateUnknownException](write.commit(commit))
    write.abort(commit)
    verify(io, never()).deleteFile("delete.parquet")
  }
  test("snapshot isolation bounds equality deletes to data visible at the read snapshot") {
    withTestTable {
      table =>
        val snapshot = table.currentSnapshot()
        val write = new IcebergEqualityDeleteBatchWrite(
          table,
          snapshot.snapshotId(),
          Expressions.equal("id", Int.box(1)),
          true,
          "test-app",
          isolationLevel = IsolationLevel.SNAPSHOT,
          deleteSequenceNumber = Some(snapshot.sequenceNumber() + 1)
        )
        append(table, 1)
        write.commit(messages(table))
        val scan = table.newScan().planFiles()
        try {
          val tasks = scan.iterator().asScala.toSeq
          assert(tasks.size == 2)
          assert(tasks.count(!_.deletes().isEmpty) == 1)
          assert(tasks.filter(
            !_.deletes().isEmpty).head.file().dataSequenceNumber() == snapshot.sequenceNumber())
        } finally scan.close()
    }
  }

  test("abort removes both replacement data files and equality-delete files") {
    withTestTable {
      table =>
        val path = s"${table.location()}/replacement.parquet"
        table.io().newOutputFile(path).create().close()
        val data = DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(0).withRecordCount(1).build()
        val deletes =
          messages(table).head.asInstanceOf[IcebergEqualityDeleteCommitMessage].deleteFiles
        val commit = IcebergRowDeltaCommitMessage(Array(data), deletes, Array.empty[String])
        writer(table, Expressions.alwaysTrue()).abort(Array(commit, null))
        assert(!table.io().newInputFile(path).exists())
        assert(!table.io().newInputFile(deletes.head.path().toString).exists())
    }
  }

}
