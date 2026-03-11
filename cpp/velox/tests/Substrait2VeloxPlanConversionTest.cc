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

#include "JsonToProtoConverter.h"

#include <filesystem>
#include "compute/VeloxPlanConverter.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"

#include "FilePathGenerator.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

namespace gluten {

class Substrait2VeloxPlanConversionTest : public exec::test::HiveConnectorTestBase {
 protected:
  std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>> makeSplits(
      std::shared_ptr<const core::PlanNode> planNode) {
    const auto& splitInfos = planConverter_->splitInfos();
    auto leafPlanNodeIds = planNode->leafPlanNodeIds();
    // Only one leaf node is expected here.
    EXPECT_EQ(1, leafPlanNodeIds.size());
    const auto& splitInfo = splitInfos.at(*leafPlanNodeIds.begin());

    const auto& paths = splitInfo->paths;
    const auto& starts = splitInfo->starts;
    const auto& lengths = splitInfo->lengths;
    const auto fileFormat = splitInfo->format;

    std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>> splits;
    splits.reserve(paths.size());

    for (int i = 0; i < paths.size(); i++) {
      auto path = fmt::format("{}{}", tmpDir_->getPath(), paths[i]);
      auto start = starts[i];
      auto length = lengths[i];
      auto split = facebook::velox::exec::test::HiveConnectorSplitBuilder(path)
                       .fileFormat(fileFormat)
                       .start(start)
                       .length(length)
                       .build();
      splits.emplace_back(split);
    }
    return splits;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{exec::test::TempDirectoryPath::create()};
  std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  std::shared_ptr<VeloxPlanConverter> planConverter_ =
      std::make_shared<VeloxPlanConverter>(pool(), veloxCfg_.get(), std::vector<std::shared_ptr<ResultIterator>>());
};

// This test will firstly generate mock TPC-H lineitem ORC file. Then, Velox's
// computing will be tested based on the generated ORC file.
// Input: Json file of the Substrait plan for the below modified TPC-H Q6 query:
//
//  SELECT sum(l_extendedprice * l_discount) AS revenue
//  FROM lineitem
//  WHERE
//    l_shipdate_new >= 8766 AND l_shipdate_new < 9131 AND
//    l_discount BETWEEN .06 - 0.01 AND .06 + 0.01 AND
//    l_quantity < 24
//
//  Tested Velox operators: TableScan (Filter Pushdown), Project, Aggregate.
TEST_F(Substrait2VeloxPlanConversionTest, q6) {
  FLAGS_velox_exception_user_stacktrace_enabled = true;
  FLAGS_velox_exception_system_stacktrace_enabled = true;
  std::unordered_map<std::string, std::string> hiveConfig{
      {"hive.orc.use-column-names", "true"}, {"hive.parquet.use-column-names", "true"}};
  std::shared_ptr<const facebook::velox::config::ConfigBase> config{
      std::make_shared<facebook::velox::config::ConfigBase>(std::move(hiveConfig))};
  resetHiveConnector(config);

  // Generate the used ORC file.
  auto type =
      ROW({"l_orderkey",
           "l_partkey",
           "l_suppkey",
           "l_linenumber",
           "l_quantity",
           "l_extendedprice",
           "l_discount",
           "l_tax",
           "l_returnflag",
           "l_linestatus",
           "l_shipdate",
           "l_commitdate",
           "l_receiptdate",
           "l_shipinstruct",
           "l_shipmode",
           "l_comment"},
          {BIGINT(),
           BIGINT(),
           BIGINT(),
           INTEGER(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           VARCHAR(),
           VARCHAR(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           VARCHAR(),
           VARCHAR(),
           VARCHAR()});
  std::vector<VectorPtr> vectors;
  // TPC-H lineitem table has 16 columns.
  int colNum = 16;
  vectors.reserve(colNum);
  std::vector<int64_t> lOrderkeyData = {
      4636438147,
      2012485446,
      1635327427,
      8374290148,
      2972204230,
      8001568994,
      989963396,
      2142695974,
      6354246853,
      4141748419};
  vectors.emplace_back(makeFlatVector<int64_t>(lOrderkeyData));
  std::vector<int64_t> lPartkeyData = {
      263222018, 255918298, 143549509, 96877642, 201976875, 196938305, 100260625, 273511608, 112999357, 299103530};
  vectors.emplace_back(makeFlatVector<int64_t>(lPartkeyData));
  std::vector<int64_t> lSuppkeyData = {
      2102019, 13998315, 12989528, 4717643, 9976902, 12618306, 11940632, 871626, 1639379, 3423588};
  vectors.emplace_back(makeFlatVector<int64_t>(lSuppkeyData));
  std::vector<int32_t> lLinenumberData = {4, 6, 1, 5, 1, 2, 1, 5, 2, 6};
  vectors.emplace_back(makeFlatVector<int32_t>(lLinenumberData));
  std::vector<double> lQuantityData = {6.0, 1.0, 19.0, 4.0, 6.0, 12.0, 23.0, 11.0, 16.0, 19.0};
  vectors.emplace_back(makeFlatVector<double>(lQuantityData));
  std::vector<double> lExtendedpriceData = {
      30586.05, 7821.0, 1551.33, 30681.2, 1941.78, 66673.0, 6322.44, 41754.18, 8704.26, 63780.36};
  vectors.emplace_back(makeFlatVector<double>(lExtendedpriceData));
  std::vector<double> lDiscountData = {0.05, 0.06, 0.01, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06, 0.07};
  vectors.emplace_back(makeFlatVector<double>(lDiscountData));
  std::vector<double> lTaxData = {0.02, 0.03, 0.01, 0.0, 0.01, 0.01, 0.03, 0.07, 0.01, 0.04};
  vectors.emplace_back(makeFlatVector<double>(lTaxData));
  std::vector<std::string> lReturnflagData = {"N", "A", "A", "R", "A", "N", "A", "A", "N", "R"};
  vectors.emplace_back(makeFlatVector<std::string>(lReturnflagData));
  std::vector<std::string> lLinestatusData = {"O", "F", "F", "F", "F", "O", "F", "F", "O", "F"};
  vectors.emplace_back(makeFlatVector<std::string>(lLinestatusData));
  std::vector<double> lShipdateNewData = {
      8953.666666666666,
      8773.666666666666,
      9034.666666666666,
      8558.666666666666,
      9072.666666666666,
      8864.666666666666,
      9004.666666666666,
      8778.666666666666,
      9013.666666666666,
      8832.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lShipdateNewData));
  std::vector<double> lCommitdateNewData = {
      10447.666666666666,
      8953.666666666666,
      8325.666666666666,
      8527.666666666666,
      8438.666666666666,
      10049.666666666666,
      9036.666666666666,
      8666.666666666666,
      9519.666666666666,
      9138.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lCommitdateNewData));
  std::vector<double> lReceiptdateNewData = {
      10456.666666666666,
      8979.666666666666,
      8299.666666666666,
      8474.666666666666,
      8525.666666666666,
      9996.666666666666,
      9103.666666666666,
      8726.666666666666,
      9593.666666666666,
      9178.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lReceiptdateNewData));
  std::vector<std::string> lShipinstructData = {
      "COLLECT COD",
      "NONE",
      "TAKE BACK RETURN",
      "NONE",
      "TAKE BACK RETURN",
      "NONE",
      "DELIVER IN PERSON",
      "DELIVER IN PERSON",
      "TAKE BACK RETURN",
      "NONE"};
  vectors.emplace_back(makeFlatVector<std::string>(lShipinstructData));
  std::vector<std::string> lShipmodeData = {
      "FOB", "REG AIR", "MAIL", "FOB", "RAIL", "SHIP", "REG AIR", "REG AIR", "TRUCK", "AIR"};
  vectors.emplace_back(makeFlatVector<std::string>(lShipmodeData));
  std::vector<std::string> lCommentData = {
      " the furiously final foxes. quickly final p",
      "thely ironic",
      "ate furiously. even, pending pinto bean",
      "ackages af",
      "odolites. slyl",
      "ng the regular requests sleep above",
      "lets above the slyly ironic theodolites sl",
      "lyly regular excuses affi",
      "lly unusual theodolites grow slyly above",
      " the quickly ironic pains lose car"};
  vectors.emplace_back(makeFlatVector<std::string>(lCommentData));

  // Write data into an DWRF file.
  writeToFile(tmpDir_->getPath() + "/mock_lineitem.dwrf", {makeRowVector(type->names(), vectors)});

  // Find and deserialize Substrait plan json file.
  std::string subPlanPath = FilePathGenerator::getDataFilePath("q6_first_stage.json");
  std::string splitPath = FilePathGenerator::getDataFilePath("q6_first_stage_split.json");

  // Read q6_first_stage.json and resume the Substrait plan.
  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);
  ::substrait::ReadRel_LocalFiles split;
  JsonToProtoConverter::readFromFile(splitPath, split);

  // Convert to Velox PlanNode.
  auto planNode = planConverter_->toVeloxPlan(substraitPlan, std::vector<::substrait::ReadRel_LocalFiles>{split});
  auto expectedResult = makeRowVector({
      makeFlatVector<double>(1, [](auto /*row*/) { return 13613.1921; }),
  });

  exec::test::AssertQueryBuilder(planNode).splits(makeSplits(planNode)).assertResults(expectedResult);
}

TEST_F(Substrait2VeloxPlanConversionTest, ifthenTest) {
  std::string subPlanPath = FilePathGenerator::getDataFilePath("if_then.json");
  std::string splitPath = FilePathGenerator::getDataFilePath("if_then_split.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);
  ::substrait::ReadRel_LocalFiles split;
  JsonToProtoConverter::readFromFile(splitPath, split);

  // Convert to Velox PlanNode.
  auto planNode = planConverter_->toVeloxPlan(substraitPlan, std::vector<::substrait::ReadRel_LocalFiles>{split});
  ASSERT_EQ(
      "-- Project[1][expressions: ] -> \n  -- TableScan[0][table: hive_table, remaining filter: (and(and(and(and(isnotnull(\"hd_vehicle_count\"),or(equalto(\"hd_buy_potential\",>10000),equalto(\"hd_buy_potential\",unknown))),greaterthan(\"hd_vehicle_count\",0)),if(greaterthan(\"hd_vehicle_count\",0),greaterthan(divide(cast(\"hd_dep_count\" as DOUBLE),cast(\"hd_vehicle_count\" as DOUBLE)),1.2))),isnotnull(\"hd_demo_sk\"))), data columns: ROW<hd_demo_sk:BIGINT,hd_buy_potential:VARCHAR,hd_dep_count:BIGINT,hd_vehicle_count:BIGINT>] -> n0_0:BIGINT, n0_1:VARCHAR, n0_2:BIGINT, n0_3:BIGINT\n",
      planNode->toString(true, true));
}

TEST_F(Substrait2VeloxPlanConversionTest, filterUpper) {
  std::string subPlanPath = FilePathGenerator::getDataFilePath("filter_upper.json");
  std::string splitPath = FilePathGenerator::getDataFilePath("filter_upper_split.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);
  ::substrait::ReadRel_LocalFiles split;
  JsonToProtoConverter::readFromFile(splitPath, split);

  // Convert to Velox PlanNode.
  auto planNode = planConverter_->toVeloxPlan(substraitPlan, std::vector<::substrait::ReadRel_LocalFiles>{split});
  ASSERT_EQ(
      "-- Project[1][expressions: ] -> \n  -- TableScan[0][table: hive_table, remaining filter: (and(isnotnull(\"key\"),lessthan(\"key\",3))), data columns: ROW<key:INTEGER>] -> n0_0:INTEGER\n",
      planNode->toString(true, true));
}

} // namespace gluten
