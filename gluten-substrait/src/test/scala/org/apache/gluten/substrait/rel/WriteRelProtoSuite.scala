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
package org.apache.gluten.substrait.rel

import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext

import com.google.protobuf.Descriptors.Descriptor
import io.substrait.proto.WriteRel
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

/**
 * Pins the wire tags of the vendored `WriteRel` after its rebase onto upstream Substrait v0.98.0. A
 * round trip through the generated classes cannot catch a renumber or an enum-value rename, because
 * producer and consumer share one schema, so these assert on the descriptors instead. See
 * docs/developers/SubstraitModifications.md for the numbering convention.
 */
class WriteRelProtoSuite extends AnyFunSuite {

  private def assertFieldNumbers(descriptor: Descriptor, expected: (String, Int)*): Unit =
    expected.foreach {
      case (name, number) =>
        val field = descriptor.findFieldByName(name)
        assert(field != null, s"${descriptor.getName} has no field named $name")
        assert(field.getNumber === number, s"${descriptor.getName} field $name changed its number")
    }

  test("makeWriteRel targets a named table with an explicit schema and bucket spec") {
    val context = new SubstraitContext
    val bucketSpec = WriteRel.BucketSpec
      .newBuilder()
      .setNumBuckets(4)
      .addBucketColumnNames("c0")
      .build()
    val rel = RelBuilder.makeWriteRel(
      null,
      Collections.singletonList(TypeBuilder.makeI32(false)),
      Collections.singletonList("c0"),
      Collections.emptyList(),
      null,
      bucketSpec,
      context,
      0L)
    val writeRel = rel.toProtobuf.getWrite

    assert(writeRel.hasNamedTable)
    assert(writeRel.hasTableSchema)
    assert(!writeRel.hasInput)
    assert(writeRel.hasBucketSpec)
    assert(writeRel.getBucketSpec.getNumBuckets === 4)
    assert(writeRel.getBucketSpec.getBucketColumnNamesList.contains("c0"))
    // The producer never sets these 0.98 fields; they must stay at their proto defaults.
    assert(writeRel.getOp === WriteRel.WriteOp.WRITE_OP_UNSPECIFIED)
    assert(writeRel.getOutput === WriteRel.OutputMode.OUTPUT_MODE_UNSPECIFIED)
    assert(!writeRel.hasCommon)
  }

  test("WriteRel field numbers and OutputMode value match upstream Substrait v0.98.0") {
    assertFieldNumbers(
      WriteRel.getDescriptor,
      "named_table" -> 1,
      "extension_table" -> 2,
      "table_schema" -> 3,
      "op" -> 4,
      "input" -> 5,
      "output" -> 6,
      "common" -> 7,
      "create_mode" -> 8,
      "advanced_extension" -> 9,
      // Gluten-local graft, relocated off upstream's field 7 to the 1000+ range.
      "bucket_spec" -> 1000
    )
    assertFieldNumbers(
      WriteRel.BucketSpec.getDescriptor,
      "num_buckets" -> 1,
      "bucket_column_names" -> 2,
      "sort_column_names" -> 3)

    // Upstream renamed the modified-rows value; tag 2 must carry the new name and not the old one.
    val modifiedRecords = WriteRel.OutputMode.getDescriptor.findValueByNumber(2)
    assert(modifiedRecords != null, "WriteRel.OutputMode has no value numbered 2")
    assert(modifiedRecords.getName === "OUTPUT_MODE_MODIFIED_RECORDS")
    assert(
      WriteRel.OutputMode.getDescriptor.findValueByName("OUTPUT_MODE_MODIFIED_TUPLES") === null,
      "the pre-0.98 OUTPUT_MODE_MODIFIED_TUPLES name must be gone"
    )
  }
}
