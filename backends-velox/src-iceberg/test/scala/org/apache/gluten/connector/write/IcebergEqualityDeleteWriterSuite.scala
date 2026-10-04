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

import org.apache.iceberg.{FileContent, FileFormat, Metrics, MetricsConfig, PartitionSpec, Schema}
import org.apache.iceberg.types.{Conversions, Types}

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

class IcebergEqualityDeleteWriterSuite extends SparkFunSuite {
  private val schema = new Schema(
    Types.NestedField.optional(11, "id", Types.LongType.get()),
    Types.NestedField.optional(29, "part", Types.IntegerType.get()))

  private def message(partition: String = "null"): String =
    s"""{
       |  "path": "/tmp/equality-delete.parquet",
       |  "fileSizeInBytes": 1234,
       |  "content": "EQUALITY_DELETES",
       |  "equalityFieldIds": [11],
       |  "metrics": {"recordCount": 3, "nullValueCounts": {"11": 1}},
       |  "partitionDataJson": $partition
       |}""".stripMargin

  test("native metadata produces a delete file with equality IDs and metrics") {
    val result = IcebergEqualityDeleteCommitMessage.fromJson(
      Array(message()),
      PartitionSpec.unpartitioned(),
      FileFormat.PARQUET,
      Seq(11))
    assert(result.deleteFiles.length == 1)
    val file = result.deleteFiles.head
    assert(file.content() == FileContent.EQUALITY_DELETES)
    assert(file.equalityFieldIds().asScala.map(_.intValue()).toSeq == Seq(11))
    assert(file.path().toString == "/tmp/equality-delete.parquet")
    assert(file.fileSizeInBytes() == 1234)
    assert(file.recordCount() == 3)
    assert(file.nullValueCounts().get(11) == 1L)
    assert(file.format() == FileFormat.PARQUET)
  }

  test("partition values and spec ID are preserved") {
    val spec = PartitionSpec.builderFor(schema).withSpecId(7).identity("part").build()
    val json = message("\"{\\\"partitionValues\\\":[10]}\"")
    val file = IcebergEqualityDeleteCommitMessage
      .fromJson(Array(json), spec, FileFormat.PARQUET, Seq(11))
      .deleteFiles.head
    assert(file.specId() == 7)
    assert(file.partition().get(0, classOf[Integer]) == 10)
  }

  test("reject data-file metadata and mismatched or invalid equality IDs") {
    Seq(
      message().replace("EQUALITY_DELETES", "DATA"),
      message().replace("[11]", "[29]"),
      message().replace("[11]", "null")
    ).foreach {
      json =>
        intercept[IllegalArgumentException] {
          IcebergEqualityDeleteCommitMessage.fromJson(
            Array(json),
            PartitionSpec.unpartitioned(),
            FileFormat.PARQUET,
            Seq(11))
        }
    }
    Seq(Seq.empty[Int], Seq(0), Seq(-1), Seq(11, 11)).foreach {
      ids =>
        intercept[IllegalArgumentException] {
          IcebergEqualityDeleteCommitMessage.fromJson(
            Array.empty,
            PartitionSpec.unpartitioned(),
            FileFormat.PARQUET,
            ids)
        }
    }
  }

  test("empty input produces no delete files") {
    assert(IcebergEqualityDeleteCommitMessage
      .fromJson(Array.empty, PartitionSpec.unpartitioned(), FileFormat.PARQUET, Seq(11))
      .deleteFiles.isEmpty)
  }

  test("native metrics honor none, counts, full and truncation with sparse field IDs") {
    val metricsSchema = new Schema(
      Types.NestedField.optional(11, "hidden", Types.IntegerType.get()),
      Types.NestedField.optional(29, "counted", Types.StringType.get()),
      Types.NestedField.optional(37, "text", Types.StringType.get()),
      Types.NestedField.optional(43, "binary", Types.BinaryType.get()),
      Types.NestedField.optional(59, "full", Types.StringType.get()),
      Types.NestedField.optional(61, "number", Types.LongType.get())
    )
    val config = MetricsConfig.fromProperties(Map(
      "write.metadata.metrics.default" -> "truncate(2)",
      "write.metadata.metrics.column.hidden" -> "none",
      "write.metadata.metrics.column.counted" -> "counts",
      "write.metadata.metrics.column.full" -> "full"
    ).asJava)
    val text = "a\uD83D\uDE00z"
    val values = Seq[(Int, Any)](
      11 -> 9,
      29 -> "counts",
      37 -> text,
      43 -> ByteBuffer.wrap(Array(0xff.toByte, 0xff.toByte, 1.toByte)),
      59 -> "untruncated",
      61 -> 123L)
    val bounds = values.map {
      case (id, value) =>
        Integer.valueOf(id) -> Conversions.toByteBuffer(metricsSchema.findType(id), value)
    }.toMap.asJava
    val counts =
      values.map { case (id, _) => Integer.valueOf(id) -> java.lang.Long.valueOf(3) }.toMap.asJava
    val metrics = new Metrics(3L, counts, counts, counts, null, bounds, bounds)
    val result = IcebergEqualityDeleteMetrics(metricsSchema, config)(metrics)
    assert(result.recordCount() == 3L)
    Seq(result.columnSizes(), result.valueCounts(), result.nullValueCounts()).foreach {
      map =>
        assert(!map.containsKey(11))
        assert(map.get(29) == 3L)
    }
    assert(result.nanValueCounts() == null)
    assert(!result.lowerBounds().containsKey(29))
    assert(!result.upperBounds().containsKey(29))
    def stringBound(map: java.util.Map[Integer, ByteBuffer], id: Int): String =
      Conversions.fromByteBuffer[CharSequence](metricsSchema.findType(id), map.get(id)).toString
    assert(stringBound(result.lowerBounds(), 37) == "a\uD83D\uDE00")
    assert(stringBound(result.upperBounds(), 37) == "a\uD83D\uDE01")
    assert(result.lowerBounds().get(43) == ByteBuffer.wrap(Array(0xff.toByte, 0xff.toByte)))
    assert(!result.upperBounds().containsKey(43))
    assert(stringBound(result.lowerBounds(), 59) == "untruncated")
    assert(result.upperBounds().get(61) == bounds.get(61))
    assert(stringBound(metrics.lowerBounds(), 37) == text)
    assert(metrics.upperBounds().get(43).remaining() == 3)
  }

  test("native metrics tolerate absent bounds and omit overflowing Unicode upper bounds") {
    val metricsSchema = new Schema(Types.NestedField.optional(29, "text", Types.StringType.get()))
    val config =
      MetricsConfig.fromProperties(Map("write.metadata.metrics.default" -> "truncate(1)").asJava)
    val max = new String(Character.toChars(Character.MAX_CODE_POINT))
    val upper = Map[Integer, ByteBuffer](Integer.valueOf(29) -> Conversions.toByteBuffer(
      Types.StringType.get(),
      max + "x")).asJava
    val result = IcebergEqualityDeleteMetrics(metricsSchema, config)(
      new Metrics(1L, null, null, null, null, null, upper))
    assert(result.columnSizes() == null)
    assert(result.lowerBounds() == null)
    assert(result.upperBounds().isEmpty)
  }

}
