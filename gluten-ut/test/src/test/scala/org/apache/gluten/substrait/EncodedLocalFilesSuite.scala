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
package org.apache.gluten.substrait

import org.apache.gluten.substrait.rel.EncodedLocalFiles

import org.apache.spark.SparkFunSuite

import io.substrait.proto.ReadRel
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.ref.WeakReference
import java.util.{ArrayList, Arrays, Collections}

class EncodedLocalFilesSuite extends SparkFunSuite {
  test("streamed encoding releases file messages and preserves the LocalFiles wire format") {
    def file(path: String, start: Long): ReadRel.LocalFiles.FileOrFiles =
      ReadRel.LocalFiles.FileOrFiles.newBuilder()
        .setUriFile(path).setStart(start).setLength(42).setPartitionIndex(3)
        .setIceberg(ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.newBuilder()
          .setParquet(ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.getDefaultInstance))
        .addPartitionColumns(ReadRel.LocalFiles.FileOrFiles.partitionColumn.newBuilder()
          .setKey("p").setValue("10"))
        .build()

    def append(builder: EncodedLocalFiles.Builder, path: String, start: Long)
        : WeakReference[ReadRel.LocalFiles.FileOrFiles] = {
      val message = file(path, start)
      builder.addFile(message)
      new WeakReference(message)
    }

    val builder = new EncodedLocalFiles.Builder()
    val references = Seq(
      append(builder, "file:///a.parquet", 17),
      append(builder, "file:///b.parquet", 59))
    val split = builder.build(Collections.singletonList("host-a"))
    eventually(timeout(Span(10, Seconds))) {
      System.gc()
      assert(references.forall(_.get() == null), "Encoded work retained Java protobuf messages")
    }
    val expected = ReadRel.LocalFiles.newBuilder()
      .addItems(file("file:///a.parquet", 17))
      .addItems(file("file:///b.parquet", 59))
      .build()
    assert(split.toProtobufBytes().sameElements(expected.toByteArray))
    assert(split.toProtobuf == expected)
    builder.addFile(file("file:///c.parquet", 101))
    assert(builder.build(Collections.emptyList()).toProtobuf.getItemsCount == 3)
    assert(split.toProtobuf == expected)
    assert(new EncodedLocalFiles.Builder().build(Collections.emptyList()).toProtobuf ==
      ReadRel.LocalFiles.getDefaultInstance)
  }

  test("encoded local files survive task serialization without protobuf reconstruction") {
    val files = ReadRel.LocalFiles.newBuilder().addItems(
      ReadRel.LocalFiles.FileOrFiles.newBuilder()
        .setUriFile("file:///a.parquet").setStart(17).setLength(42).setPartitionIndex(3)).build()
    val bytes = files.toByteArray
    val locations = new ArrayList[String](Arrays.asList("host-a"))
    val split = new EncodedLocalFiles(bytes, locations)
    locations.clear()
    assert(split.preferredLocations() == Arrays.asList("host-a"))
    assert(split.toProtobufBytes() eq bytes)
    val buffer = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(buffer)
    try out.writeObject(split)
    finally out.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray))
    val restored =
      try in.readObject().asInstanceOf[EncodedLocalFiles]
      finally in.close()
    assert(restored.toProtobufBytes().sameElements(bytes))
    assert(restored.toProtobuf == files)
    assert(restored.preferredLocations() == split.preferredLocations())
    intercept[UnsupportedOperationException] {
      restored.preferredLocations().add("host-b")
    }
  }
}
