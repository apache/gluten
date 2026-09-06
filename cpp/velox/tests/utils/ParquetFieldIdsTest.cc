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

#include "ParquetFieldIds.pb.h"
#include "utils/Exception.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace gluten;

namespace {

// Builders keep the tests readable; every one of them mirrors what the JVM walker
// is specified to emit.
FieldIdNode prim(const std::string& name, std::optional<int32_t> id) {
  return FieldIdNode{name, FieldIdNode::Kind::kPrimitive, id, {}};
}

FieldIdNode strct(const std::string& name, std::optional<int32_t> id, std::vector<FieldIdNode> children) {
  return FieldIdNode{name, FieldIdNode::Kind::kStruct, id, std::move(children)};
}

FieldIdNode arr(const std::string& name, std::optional<int32_t> id, FieldIdNode element) {
  return FieldIdNode{name, FieldIdNode::Kind::kArray, id, {std::move(element)}};
}

FieldIdNode map(const std::string& name, std::optional<int32_t> id, FieldIdNode key, FieldIdNode value) {
  return FieldIdNode{name, FieldIdNode::Kind::kMap, id, {std::move(key), std::move(value)}};
}

std::string resolveError(const std::vector<FieldIdNode>& nodes, const RowTypePtr& rowType, bool checkNames = true) {
  try {
    resolveParquetFieldIds(nodes, rowType, checkNames);
  } catch (const GlutenException& e) {
    return e.what();
  }
  return "";
}

} // namespace

TEST(ParquetFieldIdsTest, flatSchema) {
  auto rowType = ROW({"a", "b"}, {INTEGER(), VARCHAR()});
  auto ids = resolveParquetFieldIds({prim("a", 1), prim("b", 2)}, rowType, true);
  ASSERT_EQ(ids.size(), 2);
  EXPECT_EQ(ids[0].fieldId, 1);
  EXPECT_EQ(ids[1].fieldId, 2);
  EXPECT_TRUE(ids[0].children.empty());
}

TEST(ParquetFieldIdsTest, nestedStruct) {
  auto rowType = ROW({"s"}, {ROW({"x", "y"}, {INTEGER(), INTEGER()})});
  auto ids = resolveParquetFieldIds({strct("s", 1, {prim("x", 2), prim("y", 3)})}, rowType, true);
  ASSERT_EQ(ids.size(), 1);
  EXPECT_EQ(ids[0].fieldId, 1);
  ASSERT_EQ(ids[0].children.size(), 2);
  EXPECT_EQ(ids[0].children[0].fieldId, 2);
  EXPECT_EQ(ids[0].children[1].fieldId, 3);
}

TEST(ParquetFieldIdsTest, arrayHasExactlyOneChild) {
  auto rowType = ROW({"a"}, {ARRAY(INTEGER())});
  auto ids = resolveParquetFieldIds({arr("a", 1, prim("", 2))}, rowType, true);
  ASSERT_EQ(ids.size(), 1);
  ASSERT_EQ(ids[0].children.size(), 1);
  EXPECT_EQ(ids[0].children[0].fieldId, 2);
}

TEST(ParquetFieldIdsTest, mapHasKeyThenValue) {
  auto rowType = ROW({"m"}, {MAP(VARCHAR(), INTEGER())});
  auto ids = resolveParquetFieldIds({map("m", 1, prim("", 2), prim("", 3))}, rowType, true);
  ASSERT_EQ(ids[0].children.size(), 2);
  EXPECT_EQ(ids[0].children[0].fieldId, 2);
  EXPECT_EQ(ids[0].children[1].fieldId, 3);
}

TEST(ParquetFieldIdsTest, arrayOfStruct) {
  auto rowType = ROW({"items"}, {ARRAY(ROW({"sku", "qty"}, {VARCHAR(), INTEGER()}))});
  auto ids = resolveParquetFieldIds({arr("items", 5, strct("", 6, {prim("sku", 7), prim("qty", 8)}))}, rowType, true);
  ASSERT_EQ(ids[0].children.size(), 1);
  ASSERT_EQ(ids[0].children[0].children.size(), 2);
  EXPECT_EQ(ids[0].children[0].children[1].fieldId, 8);
}

// The two cases velox's own validateSchemaRecursive never reaches: it recurses into
// an array element only when that element is a ROW, so a short tree under a nested
// array or a map reaches an unguarded .at(0) instead of a diagnostic.
TEST(ParquetFieldIdsTest, nestedArrayShortTreeIsRejected) {
  auto rowType = ROW({"a"}, {ARRAY(ARRAY(INTEGER()))});
  auto bad = arr("a", 1, FieldIdNode{"", FieldIdNode::Kind::kArray, 2, {}}); // inner array missing its element
  auto err = resolveError({bad}, rowType);
  EXPECT_NE(err.find("a.element"), std::string::npos) << err;
  EXPECT_NE(err.find("exactly one child"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, arrayOfMapShortTreeIsRejected) {
  auto rowType = ROW({"a"}, {ARRAY(MAP(VARCHAR(), INTEGER()))});
  auto bad = arr("a", 1, FieldIdNode{"", FieldIdNode::Kind::kMap, 2, {prim("", 3)}}); // map with one child
  auto err = resolveError({bad}, rowType);
  EXPECT_NE(err.find("a.element"), std::string::npos) << err;
  EXPECT_NE(err.find("exactly two children"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, nestedArrayHappyPath) {
  auto rowType = ROW({"a"}, {ARRAY(ARRAY(INTEGER()))});
  auto ids = resolveParquetFieldIds({arr("a", 1, arr("", 2, prim("", 3)))}, rowType, true);
  EXPECT_EQ(ids[0].children[0].children[0].fieldId, 3);
}

TEST(ParquetFieldIdsTest, tooFewTopLevelNodesIsRejected) {
  auto rowType = ROW({"a", "b"}, {INTEGER(), INTEGER()});
  auto err = resolveError({prim("a", 1)}, rowType);
  EXPECT_NE(err.find("one child per struct field"), std::string::npos) << err;
}

// velox guards with '<=', so it silently accepts and ignores extra children.
TEST(ParquetFieldIdsTest, tooManyNodesIsRejected) {
  auto rowType = ROW({"a"}, {INTEGER()});
  auto err = resolveError({prim("a", 1), prim("b", 2)}, rowType);
  EXPECT_NE(err.find("got 2"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, nameMismatchIsRejectedWhenCheckingNames) {
  auto rowType = ROW({"a", "b"}, {INTEGER(), INTEGER()});
  auto err = resolveError({prim("a", 1), prim("wrong", 2)}, rowType);
  EXPECT_NE(err.find("'wrong'"), std::string::npos) << err;
  EXPECT_NE(err.find("'b'"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, nameMismatchIsToleratedWhenNotCheckingNames) {
  // The Iceberg wire format carries no names.
  auto rowType = ROW({"a", "b"}, {INTEGER(), INTEGER()});
  auto ids = resolveParquetFieldIds({prim("", 1), prim("", 2)}, rowType, false);
  EXPECT_EQ(ids[1].fieldId, 2);
}

TEST(ParquetFieldIdsTest, kindMismatchIsRejected) {
  auto rowType = ROW({"a"}, {ARRAY(INTEGER())});
  auto err = resolveError({prim("a", 1)}, rowType);
  EXPECT_NE(err.find("PRIMITIVE"), std::string::npos) << err;
  EXPECT_NE(err.find("ARRAY"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, absentIdLowersToSentinelNotZero) {
  // A Delta row-tracking column legitimately has no field id. It must reach velox
  // as a strictly negative value: fieldIdMetadata() emits metadata for id >= 0, so
  // 0 would be written to the footer as a real field id.
  auto rowType = ROW({"a", "internal"}, {INTEGER(), INTEGER()});
  auto ids = resolveParquetFieldIds({prim("a", 1), prim("internal", std::nullopt)}, rowType, true);
  EXPECT_EQ(ids[0].fieldId, 1);
  EXPECT_EQ(ids[1].fieldId, kNoParquetFieldId);
  EXPECT_LT(ids[1].fieldId, 0);
}

TEST(ParquetFieldIdsTest, zeroIdIsRejected) {
  auto rowType = ROW({"a"}, {INTEGER()});
  auto err = resolveError({prim("a", 0)}, rowType);
  EXPECT_NE(err.find("positive"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, negativeIdIsRejected) {
  auto rowType = ROW({"a"}, {INTEGER()});
  auto err = resolveError({prim("a", -1)}, rowType);
  EXPECT_NE(err.find("positive"), std::string::npos) << err;
}

// Global, not per-level. velox checks neither.
TEST(ParquetFieldIdsTest, duplicateIdAcrossSiblingStructsIsRejected) {
  auto rowType = ROW({"s1", "s2"}, {ROW({"x"}, {INTEGER()}), ROW({"y"}, {INTEGER()})});
  auto err = resolveError({strct("s1", 1, {prim("x", 5)}), strct("s2", 2, {prim("y", 5)})}, rowType);
  EXPECT_NE(err.find("Duplicate"), std::string::npos) << err;
  EXPECT_NE(err.find("s1.x"), std::string::npos) << err;
  EXPECT_NE(err.find("s2.y"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, errorNamesTheDottedPath) {
  auto rowType = ROW({"payload"}, {ROW({"items"}, {ARRAY(ROW({"sku"}, {VARCHAR()}))})});
  auto bad = strct("payload", 1, {arr("items", 2, strct("", 3, {}))}); // struct element missing 'sku'
  auto err = resolveError({bad}, rowType);
  EXPECT_NE(err.find("payload.items.element"), std::string::npos) << err;
}

TEST(ParquetFieldIdsTest, emptySchemaResolvesToEmptyVector) {
  auto rowType = ROW({}, {});
  auto ids = resolveParquetFieldIds({}, rowType, true);
  EXPECT_TRUE(ids.empty());
}

TEST(ParquetFieldIdsTest, decodesFromProtoWithFieldPresence) {
  ParquetFieldIdSchema schema;
  auto* a = schema.add_fields();
  a->set_name("a");
  a->set_kind(ParquetFieldIdNode::PRIMITIVE);
  a->set_id(7);
  auto* b = schema.add_fields();
  b->set_name("b");
  b->set_kind(ParquetFieldIdNode::PRIMITIVE);
  // no id set -> absence, which must survive decoding as absence

  auto nodes = fromProto(schema);
  ASSERT_EQ(nodes.size(), 2);
  ASSERT_TRUE(nodes[0].id.has_value());
  EXPECT_EQ(nodes[0].id.value(), 7);
  EXPECT_FALSE(nodes[1].id.has_value());
}

TEST(ParquetFieldIdsTest, decodesNestedProto) {
  ParquetFieldIdSchema schema;
  auto* items = schema.add_fields();
  items->set_name("items");
  items->set_kind(ParquetFieldIdNode::ARRAY);
  items->set_id(1);
  auto* element = items->add_children();
  element->set_kind(ParquetFieldIdNode::STRUCT);
  element->set_id(2);
  auto* sku = element->add_children();
  sku->set_name("sku");
  sku->set_kind(ParquetFieldIdNode::PRIMITIVE);
  sku->set_id(3);

  auto rowType = ROW({"items"}, {ARRAY(ROW({"sku"}, {VARCHAR()}))});
  auto ids = resolveParquetFieldIds(fromProto(schema), rowType, true);
  EXPECT_EQ(ids[0].children[0].children[0].fieldId, 3);
}
