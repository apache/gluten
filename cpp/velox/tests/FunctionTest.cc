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

#include "FilePathGenerator.h"
#include "JsonToProtoConverter.h"

#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "substrait/SubstraitParser.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/TypeUtils.h"
#include "substrait/VariantToVectorConverter.h"
#include "substrait/VeloxToSubstraitType.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace gluten {

class FunctionTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  std::shared_ptr<gluten::SubstraitToVeloxPlanConverter> planConverter_ =
      std::make_shared<gluten::SubstraitToVeloxPlanConverter>(pool(), veloxCfg_.get(), std::vector<std::shared_ptr<ResultIterator>>());
};

TEST_F(FunctionTest, makeNames) {
  std::string prefix = "n";
  int size = 0;
  std::vector<std::string> names = SubstraitParser::makeNames(prefix, size);
  ASSERT_EQ(names.size(), size);

  size = 5;
  names = SubstraitParser::makeNames(prefix, size);
  ASSERT_EQ(names.size(), size);
  for (int i = 0; i < size; i++) {
    std::string expected = "n_" + std::to_string(i);
    ASSERT_EQ(names[i], expected);
  }
}

TEST_F(FunctionTest, makeNodeName) {
  std::string nodeName = SubstraitParser::makeNodeName(1, 0);
  ASSERT_EQ(nodeName, "n1_0");
}

TEST_F(FunctionTest, getIdxFromNodeName) {
  std::string nodeName = "n1_0";
  int index = SubstraitParser::getIdxFromNodeName(nodeName);
  ASSERT_EQ(index, 0);
}

TEST_F(FunctionTest, getNameBeforeDelimiter) {
  std::string functionSpec = "lte:fp64_fp64";
  auto funcName = SubstraitParser::getNameBeforeDelimiter(functionSpec);
  ASSERT_EQ(funcName, "lte");

  functionSpec = "lte:";
  funcName = SubstraitParser::getNameBeforeDelimiter(functionSpec);
  ASSERT_EQ(funcName, "lte");

  functionSpec = "lte";
  funcName = SubstraitParser::getNameBeforeDelimiter(functionSpec);
  ASSERT_EQ(funcName, "lte");
}

TEST_F(FunctionTest, constructFunctionMap) {
  std::string planPath = FilePathGenerator::getDataFilePath("q1_first_stage.json");
  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);
  planConverter_->constructFunctionMap(substraitPlan);

  auto functionMap = planConverter_->getFunctionMap();
  ASSERT_EQ(functionMap.size(), 9);

  std::string function = planConverter_->findFuncSpec(1);
  ASSERT_EQ(function, "lte:fp64_fp64");

  function = planConverter_->findFuncSpec(2);
  ASSERT_EQ(function, "and:bool_bool");

  function = planConverter_->findFuncSpec(3);
  ASSERT_EQ(function, "subtract:opt_fp64_fp64");

  function = planConverter_->findFuncSpec(4);
  ASSERT_EQ(function, "multiply:opt_fp64_fp64");

  function = planConverter_->findFuncSpec(5);
  ASSERT_EQ(function, "add:opt_fp64_fp64");

  function = planConverter_->findFuncSpec(6);
  ASSERT_EQ(function, "sum:opt_fp64");

  function = planConverter_->findFuncSpec(7);
  ASSERT_EQ(function, "count:opt_fp64");

  function = planConverter_->findFuncSpec(8);
  ASSERT_EQ(function, "count:opt_i32");

  function = planConverter_->findFuncSpec(9);
  ASSERT_EQ(function, "is_not_null:fp64");
}

TEST_F(FunctionTest, setVectorFromVariants) {
  auto resultVec = setVectorFromVariants(BOOLEAN(), {variant(false), variant(true)}, pool_.get());
  ASSERT_EQ(false, resultVec->asFlatVector<bool>()->valueAt(0));
  ASSERT_EQ(true, resultVec->asFlatVector<bool>()->valueAt(1));

  auto min8 = std::numeric_limits<int8_t>::min();
  auto max8 = std::numeric_limits<int8_t>::max();
  resultVec = setVectorFromVariants(TINYINT(), {variant(min8), variant(max8)}, pool_.get());
  EXPECT_EQ(min8, resultVec->asFlatVector<int8_t>()->valueAt(0));
  EXPECT_EQ(max8, resultVec->asFlatVector<int8_t>()->valueAt(1));

  auto min16 = std::numeric_limits<int16_t>::min();
  auto max16 = std::numeric_limits<int16_t>::max();
  resultVec = setVectorFromVariants(SMALLINT(), {variant(min16), variant(max16)}, pool_.get());
  EXPECT_EQ(min16, resultVec->asFlatVector<int16_t>()->valueAt(0));
  EXPECT_EQ(max16, resultVec->asFlatVector<int16_t>()->valueAt(1));

  auto min32 = std::numeric_limits<int32_t>::min();
  auto max32 = std::numeric_limits<int32_t>::max();
  resultVec = setVectorFromVariants(INTEGER(), {variant(min32), variant(max32)}, pool_.get());
  EXPECT_EQ(min32, resultVec->asFlatVector<int32_t>()->valueAt(0));
  EXPECT_EQ(max32, resultVec->asFlatVector<int32_t>()->valueAt(1));

  auto min64 = std::numeric_limits<int64_t>::min();
  auto max64 = std::numeric_limits<int64_t>::max();
  resultVec = setVectorFromVariants(BIGINT(), {variant(min64), variant(max64)}, pool_.get());
  EXPECT_EQ(min64, resultVec->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(max64, resultVec->asFlatVector<int64_t>()->valueAt(1));

  // Floats are harder to compare because of low-precision. Just making sure
  // they don't throw.
  EXPECT_NO_THROW(setVectorFromVariants(
      REAL(), {variant(static_cast<float>(0.99L)), variant(static_cast<float>(-1.99L))}, pool_.get()));

  resultVec = setVectorFromVariants(
      DOUBLE(), {variant(static_cast<double>(0.99L)), variant(static_cast<double>(-1.99L))}, pool_.get());
  ASSERT_EQ(static_cast<double>(0.99L), resultVec->asFlatVector<double>()->valueAt(0));
  ASSERT_EQ(static_cast<double>(-1.99L), resultVec->asFlatVector<double>()->valueAt(1));

  resultVec = setVectorFromVariants(VARCHAR(), {variant(""), variant("asdf")}, pool_.get());
  ASSERT_EQ("", resultVec->asFlatVector<StringView>()->valueAt(0).str());
  ASSERT_EQ("asdf", resultVec->asFlatVector<StringView>()->valueAt(1).str());

  ASSERT_ANY_THROW(setVectorFromVariants(VARBINARY(), {variant(""), variant("asdf")}, pool_.get()));

  resultVec =
      setVectorFromVariants(TIMESTAMP(), {variant(Timestamp(9020, 0)), variant(Timestamp(8875, 0))}, pool_.get());
  ASSERT_EQ("1970-01-01T02:30:20.000000000", resultVec->asFlatVector<Timestamp>()->valueAt(0).toString());
  ASSERT_EQ("1970-01-01T02:27:55.000000000", resultVec->asFlatVector<Timestamp>()->valueAt(1).toString());

  resultVec = setVectorFromVariants(DATE(), {variant(9020), variant(8875)}, pool_.get());
  ASSERT_EQ("1994-09-12", DATE()->toString(resultVec->asFlatVector<int32_t>()->valueAt(0)));
  ASSERT_EQ("1994-04-20", DATE()->toString(resultVec->asFlatVector<int32_t>()->valueAt(1)));

  resultVec = setVectorFromVariants(INTERVAL_DAY_TIME(), {variant(9020LL), variant(8875LL)}, pool_.get());
  ASSERT_TRUE(resultVec->type()->isIntervalDayTime());
  ASSERT_EQ(9020, resultVec->asFlatVector<int64_t>()->valueAt(0));
  ASSERT_EQ(8875, resultVec->asFlatVector<int64_t>()->valueAt(1));
}

TEST_F(FunctionTest, getFunctionType) {
  std::vector<std::string> types = SubstraitParser::getSubFunctionTypes("sum:opt_i32");
  ASSERT_EQ("i32", types[0]);

  types = SubstraitParser::getSubFunctionTypes("sum:i32");
  ASSERT_EQ("i32", types[0]);

  types = SubstraitParser::getSubFunctionTypes("sum:opt_str_str");
  ASSERT_EQ(2, types.size());
  ASSERT_EQ("str", types[0]);
  ASSERT_EQ("str", types[1]);
}

TEST_F(FunctionTest, sigToTypes) {
  std::vector<TypePtr> types = SubstraitParser::sigToTypes("sum:opt_i32");
  ASSERT_EQ(types[0]->kind(), TypeKind::INTEGER);

  types = SubstraitParser::sigToTypes("and:opt_bool_bool");
  ASSERT_EQ(2, types.size());
  ASSERT_EQ(types[0]->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(types[1]->kind(), TypeKind::BOOLEAN);

  types = SubstraitParser::sigToTypes("sum:dec<12,9>");
  ASSERT_EQ(getDecimalPrecisionScale(*types[0]).first, 12);
  ASSERT_EQ(getDecimalPrecisionScale(*types[0]).second, 9);

  types = SubstraitParser::sigToTypes("sum:struct<i32,str,dec<18,6>,bool>");
  ASSERT_EQ(types[0]->kind(), TypeKind::ROW);
  ASSERT_EQ(types[0]->childAt(0)->kind(), TypeKind::INTEGER);
  ASSERT_EQ(types[0]->childAt(1)->kind(), TypeKind::VARCHAR);
  ASSERT_TRUE(types[0]->childAt(2)->isDecimal());
  ASSERT_EQ(types[0]->childAt(3)->kind(), TypeKind::BOOLEAN);
}

} // namespace gluten
