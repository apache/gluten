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

#include "utils/ParquetFieldIds.h"

#include <gtest/gtest.h>

#include "IcebergNestedField.pb.h"
#include "utils/Exception.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace gluten;

namespace {

using FieldIds = std::vector<parquet::ParquetFieldId>;

parquet::ParquetFieldId id(int32_t fieldId, FieldIds children = {}) {
  return parquet::ParquetFieldId{fieldId, std::move(children)};
}

std::string validationError(const FieldIds& fieldIds, const RowTypePtr& rowType, bool allowMissingIds = false) {
  try {
    validateParquetFieldIds(fieldIds, *rowType, allowMissingIds);
  } catch (const GlutenException& e) {
    return e.what();
  }
  return "";
}

void expectValid(const FieldIds& fieldIds, const RowTypePtr& rowType, bool allowMissingIds = false) {
  EXPECT_EQ(validationError(fieldIds, rowType, allowMissingIds), "");
}

void expectError(
    const FieldIds& fieldIds,
    const RowTypePtr& rowType,
    const std::vector<std::string>& fragments,
    bool allowMissingIds = false) {
  const auto error = validationError(fieldIds, rowType, allowMissingIds);
  ASSERT_NE(error, "");
  for (const auto& fragment : fragments) {
    EXPECT_NE(error.find(fragment), std::string::npos) << error;
  }
}

} // namespace

TEST(ParquetFieldIdsTest, flatSchema) {
  expectValid({id(1), id(2)}, ROW({"a", "b"}, {INTEGER(), VARCHAR()}));
}

TEST(ParquetFieldIdsTest, nestedTypes) {
  auto rowType =
      ROW({"s", "a", "m", "items"},
          {ROW({"x", "y"}, {INTEGER(), INTEGER()}),
           ARRAY(INTEGER()),
           MAP(VARCHAR(), INTEGER()),
           ARRAY(ROW({"sku", "qty"}, {VARCHAR(), INTEGER()}))});
  expectValid(
      {id(1, {id(2), id(3)}), id(4, {id(5)}), id(6, {id(7), id(8)}), id(9, {id(10, {id(11), id(12)})})}, rowType);
}

TEST(ParquetFieldIdsTest, nestedArraysAndMaps) {
  auto rowType = ROW({"a", "b"}, {ARRAY(ARRAY(INTEGER())), ARRAY(MAP(VARCHAR(), ARRAY(INTEGER())))});
  expectValid({id(1, {id(2, {id(3)})}), id(4, {id(5, {id(6), id(7, {id(8)})})})}, rowType);
}

// Iceberg allows 0, and velox::parquet::Writer writes it to the footer like any other ID.
TEST(ParquetFieldIdsTest, zeroIsAnId) {
  auto rowType = ROW({"a", "b"}, {INTEGER(), INTEGER()});
  expectValid({id(0), id(1)}, rowType);
  expectError({id(0), id(0)}, rowType, {"Parquet field ID 0 of 'b' is also the ID of 'a'"});
}

TEST(ParquetFieldIdsTest, missingIds) {
  auto rowType = ROW({"a", "s"}, {INTEGER(), ROW({"x"}, {INTEGER()})});
  expectError({id(1), id(2, {id(-1)})}, rowType, {"'s.x' is missing"});
  // Missing IDs are not compared with each other.
  expectValid({id(-1), id(2, {id(-1)})}, rowType, /*allowMissingIds=*/true);
}

// Velox does not check below an array or a map whose element, key or value is not a struct.
TEST(ParquetFieldIdsTest, shortTreeBelowArrayOrMap) {
  expectError({id(1, {id(2)})}, ROW({"a"}, {ARRAY(ARRAY(INTEGER()))}), {"'a.element'", "expected 1 children", "got 0"});
  expectError(
      {id(1, {id(2, {id(3)})})},
      ROW({"a"}, {ARRAY(MAP(VARCHAR(), INTEGER()))}),
      {"'a.element'", "expected 2 children", "got 1"});
}

// Velox ignores extra children.
TEST(ParquetFieldIdsTest, extraChildren) {
  expectError({id(1), id(2)}, ROW({"a"}, {INTEGER()}), {"'<root>'", "expected 1 children", "got 2"});
  expectError({id(1, {id(2)})}, ROW({"a"}, {INTEGER()}), {"'a'", "expected 0 children", "got 1"});
  expectError({id(1, {id(2), id(3)})}, ROW({"s"}, {ROW({"x"}, {INTEGER()})}), {"'s'", "expected 1 children", "got 2"});
}

TEST(ParquetFieldIdsTest, tooFewColumns) {
  expectError({id(1)}, ROW({"a", "b"}, {INTEGER(), INTEGER()}), {"'<root>'", "expected 2 children", "got 1"});
}

TEST(ParquetFieldIdsTest, duplicateIdAcrossSiblingStructs) {
  auto rowType = ROW({"s1", "s2"}, {ROW({"x"}, {INTEGER()}), ROW({"y"}, {INTEGER()})});
  expectError({id(1, {id(5)}), id(2, {id(5)})}, rowType, {"ID 5 of 's2.y' is also the ID of 's1.x'"});
}

TEST(ParquetFieldIdsTest, errorNamesTheDottedPath) {
  auto rowType = ROW({"payload"}, {ROW({"items"}, {ARRAY(ROW({"sku"}, {VARCHAR()}))})});
  expectError({id(1, {id(2, {id(3)})})}, rowType, {"'payload.items.element'"});
}

TEST(ParquetFieldIdsTest, emptySchema) {
  expectValid({}, ROW({}, {}));
}

TEST(ParquetFieldIdsTest, fromIcebergProto) {
  // As IcebergNestedFieldVisitor builds it: a root without an ID around the columns.
  IcebergNestedField root;
  auto* a = root.add_children();
  a->set_id(0);
  auto* items = root.add_children();
  items->set_id(1);
  auto* element = items->add_children();
  element->set_id(2);
  element->add_children()->set_id(3);

  auto fieldIds = parquetFieldIdsFromProto(root);
  ASSERT_EQ(fieldIds.size(), 2);
  EXPECT_EQ(fieldIds[0].fieldId, 0);
  EXPECT_TRUE(fieldIds[0].children.empty());
  EXPECT_EQ(fieldIds[1].fieldId, 1);
  ASSERT_EQ(fieldIds[1].children.size(), 1);
  EXPECT_EQ(fieldIds[1].children[0].fieldId, 2);
  ASSERT_EQ(fieldIds[1].children[0].children.size(), 1);
  EXPECT_EQ(fieldIds[1].children[0].children[0].fieldId, 3);
  expectValid(fieldIds, ROW({"a", "items"}, {INTEGER(), ARRAY(ROW({"sku"}, {VARCHAR()}))}));
}
