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
#include <limits>
#include "compute/VeloxPlanConverter.h"
#include "operators/functions/RegistrationAllFunctions.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/VeloxToSubstraitType.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"

#include "FilePathGenerator.h"
#include "compute/VeloxBackend.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

namespace gluten {

class Substrait2VeloxPlanConversionTest : public exec::test::HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    registerAllFunctions();
    parquet::registerParquetReaderFactory();
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
    parquet::unregisterParquetReaderFactory();
  }

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
      facebook::velox::exec::test::HiveConnectorSplitBuilder splitBuilder(path);
      splitBuilder.fileFormat(fileFormat).start(start).length(length);
      if (splitInfo->columnMappingMode.has_value()) {
        splitBuilder.columnMappingMode(*splitInfo->columnMappingMode);
      }
      for (const auto& [name, value] : splitInfo->metadataColumns.at(i)) {
        splitBuilder.infoColumn(name, value);
      }
      auto split = splitBuilder.build();
      splits.emplace_back(split);
    }
    return splits;
  }

  ::substrait::ReadRel makeRead(
      const RowTypePtr& type,
      const std::vector<::substrait::NamedStruct::ColumnType>& columnTypes = {}) {
    google::protobuf::Arena arena;
    VeloxToSubstraitTypeConvertor typeConverter;
    ::substrait::ReadRel read;
    read.mutable_common()->mutable_direct();
    read.mutable_base_schema()->CopyFrom(typeConverter.toSubstraitNamedStruct(arena, type));
    for (auto columnType : columnTypes) {
      read.mutable_base_schema()->add_column_types(columnType);
    }
    return read;
  }

  ::substrait::ReadRel_LocalFiles makeParquetFiles(
      const std::vector<std::string>& paths,
      const RowTypePtr& tableSchema = nullptr) {
    ::substrait::ReadRel_LocalFiles files;
    google::protobuf::Arena arena;
    VeloxToSubstraitTypeConvertor typeConverter;
    for (const auto& path : paths) {
      auto* file = files.add_items();
      file->set_uri_file(path);
      file->set_length(std::numeric_limits<uint64_t>::max());
      file->mutable_parquet();
      auto* metadata = file->add_metadata_columns();
      metadata->set_key("file_name");
      metadata->set_value(std::filesystem::path(path).filename().string());
      if (tableSchema) {
        file->mutable_schema()->CopyFrom(typeConverter.toSubstraitNamedStruct(arena, tableSchema));
      }
    }
    return files;
  }

  std::shared_ptr<const core::TableScanNode> convertRead(
      const ::substrait::ReadRel& read,
      const ::substrait::ReadRel_LocalFiles& files,
      const std::vector<std::string>& functions = {}) {
    ::substrait::Plan plan;
    plan.add_relations()->mutable_rel()->mutable_read()->CopyFrom(read);
    for (int i = 0; i < functions.size(); ++i) {
      auto* function = plan.add_extensions()->mutable_extension_function();
      function->set_function_anchor(i);
      function->set_name(functions[i]);
    }
    planConverter_ = std::make_shared<VeloxPlanConverter>(
        pool(),
        veloxCfg_.get(),
        std::vector<std::shared_ptr<ResultIterator>>{},
        VeloxConnectorIds{.hive = facebook::velox::exec::test::kHiveConnectorId});
    auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(planConverter_->toVeloxPlan(plan, {files}));
    VELOX_CHECK_NOT_NULL(scan);
    return scan;
  }

  void writeParquet(const std::string& name, const RowVectorPtr& rows) {
    const auto path = tmpDir_->getPath() + name;
    auto file = std::make_unique<LocalWriteFile>(path, false, true);
    auto sink = std::make_unique<dwio::common::WriteFileSink>(std::move(file), path);
    dwio::common::WriterOptions options;
    options.memoryPool = rootPool_.get();
    parquet::Writer writer(std::move(sink), options, asRowType(rows->type()));
    writer.write(rows);
    writer.close();
  }

  void setParquetUseColumnNames(bool enabled) {
    resetHiveConnector(std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{{"hive.parquet.use-column-names", enabled ? "true" : "false"}}));
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{exec::test::TempDirectoryPath::create()};
  std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  std::shared_ptr<VeloxPlanConverter> planConverter_ = std::make_shared<VeloxPlanConverter>(
      pool(),
      veloxCfg_.get(),
      std::vector<std::shared_ptr<ResultIterator>>{},
      VeloxConnectorIds{.hive = facebook::velox::exec::test::kHiveConnectorId});
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
      "-- Project[1][expressions: ] -> \n  -- TableScan[0][table: hive_table, remaining filter: (and(and(and(and(isnotnull(\"hd_vehicle_count\"),or(equalto(\"hd_buy_potential\",>10000),equalto(\"hd_buy_potential\",unknown))),greaterthan(\"hd_vehicle_count\",0)),if(greaterthan(\"hd_vehicle_count\",0),greaterthan(divide(spark_legacy_cast(\"hd_dep_count\"),spark_legacy_cast(\"hd_vehicle_count\")),1.2))),isnotnull(\"hd_demo_sk\"))), data columns: ROW<hd_demo_sk:BIGINT,hd_buy_potential:VARCHAR,hd_dep_count:BIGINT,hd_vehicle_count:BIGINT>] -> n0_0:BIGINT, n0_1:VARCHAR, n0_2:BIGINT, n0_3:BIGINT\n",
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

TEST_F(Substrait2VeloxPlanConversionTest, derivedFileSchemaUsesRegularSlots) {
  auto read = makeRead(
      ROW({"file_name", "p", "file_name", "row_index", "keep"}, {BIGINT(), INTEGER(), VARCHAR(), BIGINT(), BOOLEAN()}),
      {::substrait::NamedStruct::NORMAL_COL,
       ::substrait::NamedStruct::PARTITION_COL,
       ::substrait::NamedStruct::METADATA_COL,
       ::substrait::NamedStruct::ROWINDEX_COL,
       ::substrait::NamedStruct::NORMAL_COL});
  read.mutable_filter()->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(4);
  auto scan = convertRead(read, makeParquetFiles({"/unused.parquet"}));
  auto table = std::dynamic_pointer_cast<const HiveTableHandle>(scan->tableHandle());
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->dataColumns()->toString(), "ROW<file_name:BIGINT,keep:BOOLEAN>");
  EXPECT_EQ(scan->outputType()->size(), 5);
  EXPECT_EQ(scan->assignments().size(), 5);
  const std::vector<ColumnType> roles{
      ColumnType::kRegular,
      ColumnType::kPartitionKey,
      ColumnType::kSynthesized,
      ColumnType::kRowIndex,
      ColumnType::kRegular};
  for (int i = 0; i < roles.size(); ++i) {
    auto column =
        std::dynamic_pointer_cast<const HiveColumnHandle>(scan->assignments().at(scan->outputType()->nameOf(i)));
    ASSERT_NE(column, nullptr);
    EXPECT_EQ(column->columnType(), roles[i]);
  }
  EXPECT_EQ(scan->outputType()->childAt(0)->kind(), TypeKind::BIGINT);
  EXPECT_EQ(scan->outputType()->childAt(2)->kind(), TypeKind::VARCHAR);
  auto filter = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(table->remainingFilter());
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(filter->name(), "keep");
}

TEST_F(Substrait2VeloxPlanConversionTest, emptyAndUntaggedFileSchemas) {
  const auto type = ROW({"file_name", "row_index"}, {VARCHAR(), BIGINT()});
  auto scan = convertRead(
      makeRead(type, {::substrait::NamedStruct::METADATA_COL, ::substrait::NamedStruct::ROWINDEX_COL}),
      makeParquetFiles({"/unused.parquet"}));
  auto table = std::dynamic_pointer_cast<const HiveTableHandle>(scan->tableHandle());
  ASSERT_NE(table, nullptr);
  ASSERT_NE(table->dataColumns(), nullptr);
  EXPECT_EQ(table->dataColumns()->size(), 0);
  EXPECT_EQ(scan->outputType()->size(), 2);
  EXPECT_EQ(scan->assignments().size(), 2);

  scan = convertRead(makeRead(type), makeParquetFiles({"/unused.parquet"}));
  table = std::dynamic_pointer_cast<const HiveTableHandle>(scan->tableHandle());
  ASSERT_NE(table, nullptr);
  EXPECT_TRUE(table->dataColumns()->equivalent(*type));
}

TEST_F(Substrait2VeloxPlanConversionTest, explicitFileSchemaPreservesPhysicalFields) {
  auto read = makeRead(
      ROW({"FILE_NAME", "P", "ROW_INDEX", "VALUE"}, {VARCHAR(), INTEGER(), BIGINT(), BIGINT()}),
      {::substrait::NamedStruct::METADATA_COL,
       ::substrait::NamedStruct::PARTITION_COL,
       ::substrait::NamedStruct::ROWINDEX_COL,
       ::substrait::NamedStruct::NORMAL_COL});
  auto tableSchema = ROW({"FILE_NAME", "VALUE", "ROW_INDEX", "P"}, {BIGINT(), BIGINT(), VARCHAR(), INTEGER()});
  auto scan = convertRead(read, makeParquetFiles({"/unused.parquet"}, tableSchema));
  auto table = std::dynamic_pointer_cast<const HiveTableHandle>(scan->tableHandle());
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->dataColumns()->toString(), "ROW<file_name:BIGINT,value:BIGINT,row_index:VARCHAR>");
  EXPECT_EQ(scan->outputType()->size(), 4);
  EXPECT_EQ(scan->outputType()->childAt(0)->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(scan->outputType()->childAt(2)->kind(), TypeKind::BIGINT);
}

TEST_F(Substrait2VeloxPlanConversionTest, parquetMetadataOnlyPreservesRows) {
  setParquetUseColumnNames(true);
  writeParquet("/first.parquet", makeRowVector({"file_name"}, {makeFlatVector<int64_t>({10, 20})}));
  writeParquet("/second.parquet", makeRowVector({"file_name"}, {makeFlatVector<int64_t>({30})}));
  const auto files = makeParquetFiles({"/first.parquet", "/second.parquet"});
  auto scan = convertRead(makeRead(ROW({"file_name"}, {VARCHAR()}), {::substrait::NamedStruct::METADATA_COL}), files);
  auto expected = makeRowVector({makeFlatVector<std::string>({"first.parquet", "first.parquet", "second.parquet"})});
  exec::test::AssertQueryBuilder(scan).splits(makeSplits(scan)).assertResults(expected);

  // The same incompatible type is still an error when requested as physical data.
  scan = convertRead(makeRead(ROW({"file_name"}, {VARCHAR()})), files);
  VELOX_ASSERT_THROW(
      exec::test::AssertQueryBuilder(scan).splits(makeSplits(scan)).assertResults(expected),
      "Converted type BIGINT is not allowed for requested type VARCHAR");
}

TEST_F(Substrait2VeloxPlanConversionTest, parquetMetadataAndRegularFilters) {
  setParquetUseColumnNames(true);
  auto rows =
      makeRowVector({"file_name", "keep"}, {makeFlatVector<int64_t>({10, 20}), makeFlatVector<bool>({true, false})});
  writeParquet("/first.parquet", rows);
  writeParquet("/second.parquet", rows);
  const auto files = makeParquetFiles({"/first.parquet", "/second.parquet"});
  for (const std::string filename : {"first.parquet", "missing.parquet"}) {
    auto read = makeRead(
        ROW({"file_name", "keep"}, {VARCHAR(), BOOLEAN()}),
        {::substrait::NamedStruct::METADATA_COL, ::substrait::NamedStruct::NORMAL_COL});
    auto* conjunction = read.mutable_filter()->mutable_scalar_function();
    conjunction->set_function_reference(1);
    conjunction->mutable_output_type()->mutable_bool_()->set_nullability(
        ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    auto* equal = conjunction->add_arguments()->mutable_value()->mutable_scalar_function();
    equal->set_function_reference(0);
    equal->mutable_output_type()->CopyFrom(conjunction->output_type());
    equal->add_arguments()
        ->mutable_value()
        ->mutable_selection()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(0);
    equal->add_arguments()->mutable_value()->mutable_literal()->set_string(filename);
    conjunction->add_arguments()
        ->mutable_value()
        ->mutable_selection()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(1);
    auto scan = convertRead(read, files, {"equal:opt_str_str", "and:opt_bool_bool"});
    const int size = filename == "first.parquet" ? 1 : 0;
    auto expected = makeRowVector({
        makeFlatVector<std::string>(size, [&](auto /*row*/) { return filename; }),
        makeFlatVector<bool>(size, [](auto /*row*/) { return true; }),
    });
    exec::test::AssertQueryBuilder(scan).splits(makeSplits(scan)).assertResults(expected);
  }
}

TEST_F(Substrait2VeloxPlanConversionTest, parquetMetadataWithExplicitPositionalSchema) {
  setParquetUseColumnNames(false);
  auto rows =
      makeRowVector({"file_name", "keep"}, {makeFlatVector<int64_t>({10, 20}), makeFlatVector<bool>({true, false})});
  writeParquet("/first.parquet", rows);
  const auto files = makeParquetFiles({"/first.parquet"}, asRowType(rows->type()));
  auto scan = convertRead(
      makeRead(
          ROW({"file_name", "keep"}, {VARCHAR(), BOOLEAN()}),
          {::substrait::NamedStruct::METADATA_COL, ::substrait::NamedStruct::NORMAL_COL}),
      files);
  auto expected = makeRowVector({
      makeFlatVector<std::string>({"first.parquet", "first.parquet"}),
      makeFlatVector<bool>({true, false}),
  });
  exec::test::AssertQueryBuilder(scan).splits(makeSplits(scan)).assertResults(expected);
}

TEST_F(Substrait2VeloxPlanConversionTest, expandSelectionMustBeTopLevelField) {
  const auto makeExpandRel = [](bool nestedSelection) {
    ::substrait::Rel rel;
    auto* expand = rel.mutable_expand();
    expand->mutable_common()->mutable_direct();

    auto* read = expand->mutable_input()->mutable_read();
    read->mutable_common()->mutable_direct();
    auto* schema = read->mutable_base_schema();
    for (const auto* name : {"nested", "mask", "value"}) {
      schema->add_names(name);
    }

    auto* nestedType = schema->mutable_struct_()->add_types()->mutable_struct_();
    nestedType->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    nestedType->add_names("");
    nestedType->add_names("");
    nestedType->add_types()->mutable_i64()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    nestedType->add_types()->mutable_bool_()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(
        ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(
        ::substrait::Type_Nullability_NULLABILITY_NULLABLE);

    auto* field = expand->add_fields()
                      ->mutable_switching_field()
                      ->add_duplicates()
                      ->mutable_selection()
                      ->mutable_direct_reference()
                      ->mutable_struct_field();
    field->set_field(nestedSelection ? 0 : 1);
    if (nestedSelection) {
      field->mutable_child()->mutable_struct_field()->set_field(1);
    }
    return rel;
  };

  const auto makeConverter = [&] {
    return std::make_shared<SubstraitToVeloxPlanConverter>(
        pool(),
        veloxCfg_.get(),
        std::vector<std::shared_ptr<ResultIterator>>{},
        VeloxConnectorIds{.hive = facebook::velox::exec::test::kHiveConnectorId},
        std::nullopt,
        std::nullopt,
        /*validationMode=*/true);
  };

  auto plan = makeConverter()->toVeloxPlan(makeExpandRel(/*nestedSelection=*/false));
  auto expand = std::dynamic_pointer_cast<const core::ExpandNode>(plan);
  ASSERT_NE(expand, nullptr);
  ASSERT_EQ(expand->projections().size(), 1);
  ASSERT_EQ(expand->projections().front().size(), 1);
  auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expand->projections().front().front());
  ASSERT_NE(field, nullptr);
  EXPECT_TRUE(field->isInputColumn());
  EXPECT_EQ(field->name(), "n0_1");

  VELOX_ASSERT_USER_THROW(
      makeConverter()->toVeloxPlan(makeExpandRel(/*nestedSelection=*/true)),
      "Expand Operator only supports a top-level field or literal.");
}

TEST_F(Substrait2VeloxPlanConversionTest, aggregateMaskMustBeTopLevelField) {
  const auto makeAggregateRel = [](bool nestedMask) {
    ::substrait::Rel rel;
    auto* aggregate = rel.mutable_aggregate();
    aggregate->mutable_common()->mutable_direct();

    auto* read = aggregate->mutable_input()->mutable_read();
    read->mutable_common()->mutable_direct();
    auto* schema = read->mutable_base_schema();
    for (const auto* name : {"nested", "mask", "value"}) {
      schema->add_names(name);
    }

    auto* nestedType = schema->mutable_struct_()->add_types()->mutable_struct_();
    nestedType->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    nestedType->add_names("");
    nestedType->add_names("");
    nestedType->add_types()->mutable_i64()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    nestedType->add_types()->mutable_bool_()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(
        ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(
        ::substrait::Type_Nullability_NULLABILITY_NULLABLE);

    auto* measure = aggregate->add_measures();
    auto* maskField =
        measure->mutable_filter()->mutable_selection()->mutable_direct_reference()->mutable_struct_field();
    maskField->set_field(nestedMask ? 0 : 1);
    if (nestedMask) {
      maskField->mutable_child()->mutable_struct_field()->set_field(1);
    }

    auto* function = measure->mutable_measure();
    function->set_function_reference(1);
    function->set_phase(::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
    function->set_invocation(::substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
    function->add_arguments()
        ->mutable_value()
        ->mutable_selection()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(2);
    function->mutable_output_type()->mutable_i64()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    return rel;
  };

  const auto makeConverter = [&] {
    auto converter = std::make_shared<SubstraitToVeloxPlanConverter>(
        pool(),
        veloxCfg_.get(),
        std::vector<std::shared_ptr<ResultIterator>>{},
        VeloxConnectorIds{.hive = facebook::velox::exec::test::kHiveConnectorId},
        std::nullopt,
        std::nullopt,
        /*validationMode=*/true);
    converter->constructFunctionMap(std::unordered_map<uint64_t, std::string>{{1, "sum:opt_i64"}});
    return converter;
  };

  auto plan = makeConverter()->toVeloxPlan(makeAggregateRel(/*nestedMask=*/false));
  auto aggregation = std::dynamic_pointer_cast<const core::AggregationNode>(plan);
  ASSERT_NE(aggregation, nullptr);
  ASSERT_EQ(aggregation->aggregates().size(), 1);
  ASSERT_NE(aggregation->aggregates().front().mask, nullptr);
  EXPECT_TRUE(aggregation->aggregates().front().mask->isInputColumn());
  EXPECT_EQ(aggregation->aggregates().front().mask->name(), "n0_1");

  // A nested selection converts to a DereferenceTypedExpr, which cannot be an
  // AggregationNode mask. Reject it instead of silently dropping the filter.
  VELOX_ASSERT_USER_THROW(
      makeConverter()->toVeloxPlan(makeAggregateRel(/*nestedMask=*/true)),
      "Aggregation Operator only supports a top-level field mask.");
}

} // namespace gluten
