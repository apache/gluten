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

import com.google.protobuf.Descriptors.Descriptor
import io.substrait.proto.ReadRel
import org.scalatest.funsuite.AnyFunSuite

/**
 * Pins the wire tags of the vendored `ReadRel.read_type` oneof to upstream Substrait v0.98.0,
 * including the official `iceberg_table = 9`.
 *
 * Producer and consumer share one schema, so a renumber round-trips cleanly through the generated
 * classes and cannot be caught by exercising them; these assert on the descriptors instead.
 */
class ReadRelProtoSuite extends AnyFunSuite {

  private def assertFieldNumbers(descriptor: Descriptor, expected: (String, Int)*): Unit =
    expected.foreach {
      case (name, number) =>
        val field = descriptor.findFieldByName(name)
        assert(field != null, s"${descriptor.getName} has no field named $name")
        assert(field.getNumber === number, s"${descriptor.getName} field $name changed its number")
    }

  test("ReadRel.read_type field numbers match upstream v0.98.0 verbatim") {
    assertFieldNumbers(
      ReadRel.getDescriptor,
      "virtual_table" -> 5,
      "local_files" -> 6,
      "named_table" -> 7,
      "extension_table" -> 8,
      // Official Substrait 0.98 addition.
      "iceberg_table" -> 9
    )
  }

  test("ReadRel.IcebergTable structure matches the vendored upstream layout") {
    assertFieldNumbers(ReadRel.IcebergTable.getDescriptor, "direct" -> 1)
    assertFieldNumbers(
      ReadRel.IcebergTable.MetadataFileRead.getDescriptor,
      "metadata_uri" -> 1,
      "snapshot_id" -> 2,
      "snapshot_timestamp" -> 3)
  }
}
