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

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import io.substrait.proto.{Expression, ReadRel}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Pins the wire tags of the vendored `ReadRel.VirtualTable` after its rebase onto upstream
 * Substrait v0.98.0. A round trip through the generated classes cannot catch a renumber or a field
 * rename, because producer and consumer share one schema, so these assert on the descriptors
 * instead. There is no JVM producer for virtual tables - the only producer and consumer live in
 * cpp/velox/substrait - which is precisely why nothing else in the build would notice a regression
 * here.
 */
class VirtualTableProtoSuite extends AnyFunSuite {

  private def field(name: String, descriptor: Descriptor): FieldDescriptor = {
    val f = descriptor.findFieldByName(name)
    assert(f != null, s"${descriptor.getName} has no field named $name")
    f
  }

  test("VirtualTable carries expressions on tag 2 and no longer declares values") {
    val descriptor = ReadRel.VirtualTable.getDescriptor

    val expressions = field("expressions", descriptor)
    assert(expressions.getNumber === 2, "VirtualTable.expressions changed its number")
    assert(expressions.isRepeated, "VirtualTable.expressions must stay repeated")
    assert(
      expressions.getMessageType.getFullName === "substrait.Expression.Nested.Struct",
      "VirtualTable.expressions must hold Expression.Nested.Struct, not Expression.Literal.Struct"
    )

    // Upstream reserved tag 1 and the `values` name; a rebase must not reintroduce either.
    assert(
      descriptor.findFieldByName("values") === null,
      "the pre-0.98 VirtualTable.values field must be gone")
    assert(
      descriptor.findFieldByNumber(1) === null,
      "VirtualTable tag 1 is reserved upstream and must stay unused")
  }

  test("virtual_table stays on read_type tag 5") {
    val virtualTable = field("virtual_table", ReadRel.getDescriptor)
    assert(virtualTable.getNumber === 5, "ReadRel.virtual_table changed its number")
    assert(
      virtualTable.getContainingOneof != null &&
        virtualTable.getContainingOneof.getName === "read_type",
      "ReadRel.virtual_table must stay in the read_type oneof"
    )
  }

  test("Nested.Struct fields are Expressions, so each value needs a literal wrapper") {
    val fields = field("fields", Expression.Nested.Struct.getDescriptor)
    assert(fields.getNumber === 1)
    assert(fields.isRepeated)
    assert(
      fields.getMessageType.getFullName === "substrait.Expression",
      "Nested.Struct.fields must hold Expression - this is what forces the .literal() unwrap in " +
        "SubstraitToVeloxPlan"
    )

    // The literal wrapper the C++ producer and consumer rely on.
    val literal = field("literal", Expression.getDescriptor)
    assert(literal.getNumber === 1)
    assert(literal.getJavaType === FieldDescriptor.JavaType.MESSAGE)
    assert(literal.getMessageType.getFullName === "substrait.Expression.Literal")
  }
}
