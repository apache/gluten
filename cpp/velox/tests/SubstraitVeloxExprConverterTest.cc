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

#include "substrait/SubstraitToVeloxExpr.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace gluten {

// Regression test for a SIGSEGV in
// SubstraitVeloxExprConverter::toVeloxExpr(Expression::FieldReference, ...).
// The direct-reference loop descends one nested struct_field at a time with
// `inputColumnType = asRowType(childAt(idx))`. When the field path traverses a
// non-struct child -- e.g. a field nested under an array, as produced by Delta's
// nested-array UPDATE rewrite ("nested data support - ... updating array type")
// -- asRowType() returns null and the next iteration dereferenced that null
// RowType, crashing the whole forked JVM. A SIGSEGV is not catchable, so plan
// validation could not fall back. The converter must instead throw a
// VeloxUserError, which SubstraitToVeloxPlanValidator catches to fall back to
// vanilla execution.
TEST(SubstraitVeloxExprConverterTest, nestedFieldReferenceIntoNonStructThrows) {
  // Schema with a single array column.
  RowTypePtr inputType = ROW({"arr"}, {ARRAY(INTEGER())});

  // Reference column 0 (the array), then descend one more level via a child
  // struct_field -- i.e. into the array's element, which is not a struct/row.
  ::substrait::Expression::FieldReference fieldReference;
  auto* structField = fieldReference.mutable_direct_reference()->mutable_struct_field();
  structField->set_field(0);
  structField->mutable_child()->mutable_struct_field()->set_field(0);

  VELOX_ASSERT_THROW(
      SubstraitVeloxExprConverter::toVeloxExpr(fieldReference, inputType),
      "Nested field reference into a non-struct type");
}

// A field-reference index past the end of the row type must be rejected cleanly.
// The out-of-range check is defense-in-depth: Spark's analyzer keeps field indices
// in bounds, but a malformed Substrait plan might have a negative index or an index
// exceeding the row size. Without the check, a negative index casts to a huge uint32_t
// and causes undefined behavior; an out-of-bounds index would throw from Velox's
// RowType::childAt but with a less clear error message.
TEST(SubstraitVeloxExprConverterTest, fieldReferenceIndexOutOfRangeThrows) {
  RowTypePtr inputType = ROW({"a", "b"}, {INTEGER(), INTEGER()});

  ::substrait::Expression::FieldReference fieldReference;
  fieldReference.mutable_direct_reference()->mutable_struct_field()->set_field(5);

  VELOX_ASSERT_USER_THROW(SubstraitVeloxExprConverter::toVeloxExpr(fieldReference, inputType), "out of range");
}

} // namespace gluten
