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

#include <algorithm>

#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "substrait/SubstraitToVeloxPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace gluten {

class Substrait2VeloxValuesNodeConversionTest : public OperatorTestBase {};

// SELECT * FROM tmp
TEST_F(Substrait2VeloxValuesNodeConversionTest, valuesNode) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);
  auto veloxCfg = std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  std::shared_ptr<SubstraitToVeloxPlanConverter> planConverter_ = std::make_shared<SubstraitToVeloxPlanConverter>(
      pool_.get(),
      veloxCfg.get(),
      std::vector<std::shared_ptr<ResultIterator>>{},
      VeloxConnectorIds{},
      std::nullopt,
      std::nullopt,
      false);
  auto veloxPlan = planConverter_->toVeloxPlan(substraitPlan);

  RowVectorPtr expectedData = makeRowVector(
      {makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775, 4077358421272316858}),
       makeFlatVector<int32_t>({581869302, -708632711, -133711905}),
       makeFlatVector<double>({0.90579193414549275, 0.96886777112423139, 0.63235925003444637}),
       makeFlatVector<bool>({true, false, false}),
       makeFlatVector<int32_t>(3, nullptr, nullEvery(1))

      });

  createDuckDbTable({expectedData});
  assertQuery(veloxPlan, "SELECT * FROM tmp");
}

TEST_F(Substrait2VeloxValuesNodeConversionTest, zeroColumnOneRowValuesNode) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable_emptySchema.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);
  auto veloxCfg = std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  auto planConverter = std::make_shared<SubstraitToVeloxPlanConverter>(
      pool_.get(),
      veloxCfg.get(),
      std::vector<std::shared_ptr<ResultIterator>>{},
      VeloxConnectorIds{},
      std::nullopt,
      std::nullopt,
      false);
  auto veloxPlan = planConverter->toVeloxPlan(substraitPlan);

  auto valuesNode = std::dynamic_pointer_cast<const core::ValuesNode>(veloxPlan);
  ASSERT_NE(valuesNode, nullptr);
  ASSERT_TRUE(valuesNode->outputType()->equivalent(*ROW({})));
  ASSERT_EQ(valuesNode->values().size(), 1);
  ASSERT_EQ(valuesNode->values().front()->childrenSize(), 0);
  ASSERT_EQ(valuesNode->values().front()->size(), 1);
  ASSERT_EQ(planConverter->splitInfos().at(valuesNode->id())->leafType, SplitInfo::LeafType::TRIVIAL_LEAF);

  CursorParameters params;
  params.planNode = veloxPlan;
  auto [cursor, results] = readCursor(params);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results.front()->childrenSize(), 0);
  ASSERT_EQ(results.front()->size(), 1);
}

TEST_F(Substrait2VeloxValuesNodeConversionTest, virtualTableDoesNotConsumeTableScanSplit) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);
  const auto virtualRead = substraitPlan.relations(0).root().input().read();

  auto* setRel = substraitPlan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_set();
  setRel->set_op(::substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL);
  setRel->add_inputs()->mutable_read()->CopyFrom(virtualRead);
  auto* tableScanRead = setRel->add_inputs()->mutable_read();
  tableScanRead->CopyFrom(virtualRead);
  tableScanRead->clear_virtual_table();

  auto veloxCfg = std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  VeloxConnectorIds connectorIds;
  connectorIds.hive = "test-hive";
  auto planConverter = std::make_shared<SubstraitToVeloxPlanConverter>(
      pool_.get(),
      veloxCfg.get(),
      std::vector<std::shared_ptr<ResultIterator>>{},
      std::move(connectorIds),
      std::nullopt,
      std::nullopt,
      false);
  auto tableScanSplit = std::make_shared<SplitInfo>();
  tableScanSplit->leafType = SplitInfo::LeafType::TABLE_SCAN;
  planConverter->setSplitInfos({tableScanSplit});

  auto veloxPlan = planConverter->toVeloxPlan(substraitPlan);
  ASSERT_NE(std::dynamic_pointer_cast<const core::LocalPartitionNode>(veloxPlan), nullptr);

  const auto& splitInfos = planConverter->splitInfos();
  ASSERT_EQ(splitInfos.size(), 2);
  ASSERT_EQ(
      std::count_if(
          splitInfos.begin(),
          splitInfos.end(),
          [](const auto& entry) { return entry.second->leafType == SplitInfo::LeafType::TRIVIAL_LEAF; }),
      1);
  ASSERT_EQ(
      std::count_if(
          splitInfos.begin(), splitInfos.end(), [&](const auto& entry) { return entry.second == tableScanSplit; }),
      1);
}

TEST_F(Substrait2VeloxValuesNodeConversionTest, rejectsEmptyVirtualTableInAllModes) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable_emptySchema.json");
  for (const bool validationMode : {false, true}) {
    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(planPath, substraitPlan);
    substraitPlan.mutable_relations(0)
        ->mutable_root()
        ->mutable_input()
        ->mutable_read()
        ->mutable_virtual_table()
        ->clear_expressions();

    auto veloxCfg =
        std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
    auto planConverter = std::make_shared<SubstraitToVeloxPlanConverter>(
        pool_.get(),
        veloxCfg.get(),
        std::vector<std::shared_ptr<ResultIterator>>{},
        VeloxConnectorIds{},
        std::nullopt,
        std::nullopt,
        validationMode);

    VELOX_ASSERT_THROW(planConverter->toVeloxPlan(substraitPlan), "Virtual table must contain at least one row group.");
  }
}

TEST_F(Substrait2VeloxValuesNodeConversionTest, supportsZeroRowVirtualTableInAllModes) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable.json");
  for (const bool validationMode : {false, true}) {
    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(planPath, substraitPlan);
    substraitPlan.mutable_relations(0)
        ->mutable_root()
        ->mutable_input()
        ->mutable_read()
        ->mutable_virtual_table()
        ->mutable_expressions(0)
        ->clear_fields();

    auto veloxCfg =
        std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
    auto planConverter = std::make_shared<SubstraitToVeloxPlanConverter>(
        pool_.get(),
        veloxCfg.get(),
        std::vector<std::shared_ptr<ResultIterator>>{},
        VeloxConnectorIds{},
        std::nullopt,
        std::nullopt,
        validationMode);

    auto values =
        std::dynamic_pointer_cast<const facebook::velox::core::ValuesNode>(planConverter->toVeloxPlan(substraitPlan));
    ASSERT_NE(values, nullptr);
    ASSERT_EQ(values->values().size(), 1);
    ASSERT_EQ(values->values().front()->size(), 0);
    if (!validationMode) {
      CursorParameters params;
      params.planNode = values;
      auto [cursor, results] = readCursor(params);
      ASSERT_TRUE(results.empty());
    }
  }
}

TEST_F(Substrait2VeloxValuesNodeConversionTest, rejectsNonLiteralVirtualTableFieldsInAllModes) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable.json");
  for (const bool validationMode : {false, true}) {
    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(planPath, substraitPlan);
    substraitPlan.mutable_relations(0)
        ->mutable_root()
        ->mutable_input()
        ->mutable_read()
        ->mutable_virtual_table()
        ->mutable_expressions(0)
        ->mutable_fields(0)
        ->Clear();

    auto veloxCfg =
        std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
    auto planConverter = std::make_shared<SubstraitToVeloxPlanConverter>(
        pool_.get(),
        veloxCfg.get(),
        std::vector<std::shared_ptr<ResultIterator>>{},
        VeloxConnectorIds{},
        std::nullopt,
        std::nullopt,
        validationMode);

    VELOX_ASSERT_THROW(planConverter->toVeloxPlan(substraitPlan), "ReadRel.VirtualTable expressions must be literals.");
  }
}

TEST_F(Substrait2VeloxValuesNodeConversionTest, rejectsMalformedEmptySchemaValueInAllModes) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable_emptySchema.json");
  for (const bool validationMode : {false, true}) {
    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(planPath, substraitPlan);
    substraitPlan.mutable_relations(0)
        ->mutable_root()
        ->mutable_input()
        ->mutable_read()
        ->mutable_virtual_table()
        ->mutable_expressions(0)
        ->add_fields()
        ->mutable_literal()
        ->set_i32(1);

    auto veloxCfg =
        std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
    auto planConverter = std::make_shared<SubstraitToVeloxPlanConverter>(
        pool_.get(),
        veloxCfg.get(),
        std::vector<std::shared_ptr<ResultIterator>>{},
        VeloxConnectorIds{},
        std::nullopt,
        std::nullopt,
        validationMode);

    VELOX_ASSERT_THROW(
        planConverter->toVeloxPlan(substraitPlan),
        "ReadRel.VirtualTable field count must be a multiple of the column count.");
  }
}

} // namespace gluten
