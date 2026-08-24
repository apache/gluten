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

#include "compute/iceberg/IcebergPlanConverter.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

namespace gluten {
namespace {

TEST(IcebergPlanConverterTest, projectsRenamedAndReorderedNestedFields) {
  const IcebergFieldIdInfo field{
      "profile",
      1,
      {
          {"display_name", 2, {}},
          {"address", 3, {{"city", 4, {}}, {"zip", 5, {}}}},
      }};
  const auto requestedType = ROW({"address", "display_name"}, {ROW({"zip"}, {VARCHAR()}), VARCHAR()});

  const auto actual = IcebergPlanConverter::toParquetFieldId(field, requestedType, false);

  ASSERT_EQ(actual.fieldId, 1);
  ASSERT_EQ(actual.children.size(), 2);
  EXPECT_EQ(actual.children[0].fieldId, 3);
  ASSERT_EQ(actual.children[0].children.size(), 1);
  EXPECT_EQ(actual.children[0].children[0].fieldId, 5);
  EXPECT_EQ(actual.children[1].fieldId, 2);
}

TEST(IcebergPlanConverterTest, projectsCollectionElementFields) {
  const IcebergFieldIdInfo field{"items", 1, {{"element", 2, {{"label", 3, {}}, {"amount", 4, {}}}}}};
  const auto requestedType = ARRAY(ROW({"amount"}, {BIGINT()}));

  const auto actual = IcebergPlanConverter::toParquetFieldId(field, requestedType, false);

  ASSERT_EQ(actual.children.size(), 1);
  EXPECT_EQ(actual.children[0].fieldId, 2);
  ASSERT_EQ(actual.children[0].children.size(), 1);
  EXPECT_EQ(actual.children[0].children[0].fieldId, 4);
}

TEST(IcebergPlanConverterTest, projectsMapValueFields) {
  const IcebergFieldIdInfo field{"attributes", 1, {{"key", 2, {}}, {"value", 3, {{"label", 4, {}}, {"rank", 5, {}}}}}};
  const auto requestedType = MAP(VARCHAR(), ROW({"rank"}, {INTEGER()}));

  const auto actual = IcebergPlanConverter::toParquetFieldId(field, requestedType, false);

  ASSERT_EQ(actual.children.size(), 2);
  EXPECT_EQ(actual.children[0].fieldId, 2);
  EXPECT_EQ(actual.children[1].fieldId, 3);
  ASSERT_EQ(actual.children[1].children.size(), 1);
  EXPECT_EQ(actual.children[1].children[0].fieldId, 5);
}

TEST(IcebergPlanConverterTest, acceptsLegacyRootOnlyFieldId) {
  const IcebergFieldIdInfo field{"profile", 1, {}};

  const auto actual = IcebergPlanConverter::toParquetFieldId(field, ROW({"name"}, {VARCHAR()}), false);

  EXPECT_EQ(actual.fieldId, 1);
  EXPECT_TRUE(actual.children.empty());
}

} // namespace
} // namespace gluten
