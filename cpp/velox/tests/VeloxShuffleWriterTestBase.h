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

#pragma once

#include <arrow/result.h>

#include <gtest/gtest.h>

#include <compute/VeloxBackend.h>
#include "../utils/VeloxArrowUtils.h"
#include "config/GlutenConfig.h"
#include "config/VeloxConfig.h"
#include "memory/VeloxColumnarBatch.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "utils/LocalRssClient.h"
#include "utils/TestAllocationListener.h"
#include "velox/type/Type.h"

#include "velox/vector/tests/VectorTestUtils.h"

namespace gluten {

namespace {
std::string makeString(uint32_t length) {
  static const std::string kLargeStringOf128Bytes =
      "thisisalaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaargestringlengthmorethan16bytes";
  std::string res{};
  auto repeats = length / kLargeStringOf128Bytes.length();
  while (repeats--) {
    res.append(kLargeStringOf128Bytes);
  }
  if (auto remains = length % kLargeStringOf128Bytes.length()) {
    res.append(kLargeStringOf128Bytes.substr(0, remains));
  }
  return res;
}
} // namespace

class VeloxShuffleWriterTestBase : public facebook::velox::test::VectorTestBase {
 public:
  virtual ~VeloxShuffleWriterTestBase() = default;

  static void setUpVeloxBackend() {
    auto listener = std::make_unique<TestAllocationListener>();
    listener_ = listener.get();

    std::unordered_map<std::string, std::string> conf{
        {kMemoryReservationBlockSize, "1"}, {kDebugModeEnabled, "true"}, {kGpuAsyncShuffleReaderThreads, "2"}};

    VeloxBackend::create(std::move(listener), conf);
  }

  static void tearDownVeloxBackend() {
    VeloxBackend::get()->tearDown();
  }

 protected:
  void setUpTestData() {
    GLUTEN_THROW_NOT_OK(setLocalDirsAndDataFile());

    // Set up test data.
    children1_ = {
        makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4, std::nullopt, 5, 6, std::nullopt, 7}),
        makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2, 2, std::nullopt, std::nullopt, 3, -3}),
        makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 5, 6, 7, 8, std::nullopt}),
        makeNullableFlatVector<int64_t>(
            {std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt}),
        makeNullableFlatVector<float>(
            {-0.1234567,
             0.1234567,
             std::nullopt,
             std::nullopt,
             -0.142857,
             std::nullopt,
             0.142857,
             0.285714,
             0.428617,
             std::nullopt}),
        makeNullableFlatVector<bool>(
            {std::nullopt, true, false, std::nullopt, true, true, false, true, std::nullopt, std::nullopt}),
        makeFlatVector<facebook::velox::StringView>(
            {"a",
             "bobbobbobooooooooooooooooooooooooooooob1",
             "alice2",
             "bob3",
             "Alice4",
             "Bob5",
             "AlicE6",
             "boB7",
             "ALICE8",
             "BOB9"}),
        makeNullableFlatVector<facebook::velox::StringView>(
            {std::nullopt,
             "bob_1",
             std::nullopt,
             std::nullopt,
             "Alice_4",
             "Bob_5",
             std::nullopt,
             "alicE_7",
             std::nullopt,
             "boB_9"}),
        facebook::velox::BaseVector::create(facebook::velox::UNKNOWN(), 10, pool())};

    children2_ = {
        makeNullableFlatVector<int8_t>({std::nullopt, std::nullopt}),
        makeFlatVector<int8_t>({1, -1}),
        makeNullableFlatVector<int32_t>({100, std::nullopt}),
        makeFlatVector<int64_t>({1, 1}),
        makeFlatVector<float>({0.142857, -0.142857}),
        makeFlatVector<bool>({true, false}),
        makeFlatVector<facebook::velox::StringView>(
            {"bob",
             "alicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealice"}),
        makeNullableFlatVector<facebook::velox::StringView>({std::nullopt, std::nullopt}),
        facebook::velox::BaseVector::create(facebook::velox::UNKNOWN(), 2, pool())};

    childrenNoNull_ = {
        makeFlatVector<int8_t>({0, 1}),
        makeFlatVector<int8_t>({0, -1}),
        makeFlatVector<int32_t>({0, 100}),
        makeFlatVector<int64_t>({0, 1}),
        makeFlatVector<float>({0, 0.142857}),
        makeFlatVector<bool>({false, true}),
        makeFlatVector<facebook::velox::StringView>({"", "alice"}),
        makeFlatVector<facebook::velox::StringView>({"alice", ""}),
    };

    largeString1_ = makeString(1024);
    int32_t numRows = 1024;
    childrenLargeBinary1_ = {
        makeFlatVector<int8_t>(std::vector<int8_t>(numRows, 0)),
        makeFlatVector<int8_t>(std::vector<int8_t>(numRows, 0)),
        makeFlatVector<int32_t>(std::vector<int32_t>(numRows, 0)),
        makeFlatVector<int64_t>(std::vector<int64_t>(numRows, 0)),
        makeFlatVector<float>(std::vector<float>(numRows, 0)),
        makeFlatVector<bool>(std::vector<bool>(numRows, true)),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(numRows, largeString1_.c_str())),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(numRows, std::nullopt)),
    };

    largeString2_ = makeString(4096);
    numRows = 2048;
    auto vectorToSpill = childrenLargeBinary2_ = {
        makeFlatVector<int8_t>(std::vector<int8_t>(numRows, 0)),
        makeFlatVector<int8_t>(std::vector<int8_t>(numRows, 0)),
        makeFlatVector<int32_t>(std::vector<int32_t>(numRows, 0)),
        makeFlatVector<int64_t>(std::vector<int64_t>(numRows, 0)),
        makeFlatVector<float>(std::vector<float>(numRows, 0)),
        makeFlatVector<bool>(std::vector<bool>(numRows, true)),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(numRows, largeString2_.c_str())),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(numRows, std::nullopt)),
    };

    childrenComplex_ = {
        makeNullableFlatVector<int32_t>({std::nullopt, 1}),
        makeRowVector({
            makeFlatVector<int32_t>({1, 3}),
            makeNullableFlatVector<facebook::velox::StringView>({std::nullopt, "de"}),
        }),
        makeNullableFlatVector<facebook::velox::StringView>({std::nullopt, "10 I'm not inline string"}),
        makeArrayVector<int64_t>({
            {1, 2, 3, 4, 5},
            {1, 2, 3},
        }),
        makeMapVector<int32_t, facebook::velox::StringView>(
            {{{1, "str1000"}, {2, "str2000"}}, {{3, "str3000"}, {4, "str4000"}}}),
    };

    inputVector1_ = makeRowVector(children1_);
    inputVector2_ = makeRowVector(children2_);
    inputVectorNoNull_ = makeRowVector(childrenNoNull_);
    inputVectorLargeBinary1_ = makeRowVector(childrenLargeBinary1_);
    inputVectorLargeBinary2_ = makeRowVector(childrenLargeBinary2_);
    inputVectorComplex_ = makeRowVector(childrenComplex_);
  }

  arrow::Status splitRowVector(
      VeloxShuffleWriter& shuffleWriter,
      facebook::velox::RowVectorPtr vector,
      int64_t memLimit = ShuffleWriter::kMinMemLimit) {
    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
    return shuffleWriter.write(cb, memLimit);
  }

  // Create multiple local dirs and join with comma.
  arrow::Status setLocalDirsAndDataFile() {
    static const std::string kTestLocalDirsPrefix = "columnar-shuffle-test-";

    // Create first tmp dir and create data file.
    // To prevent tmpDirs from being deleted in the dtor, we need to store them.
    tmpDirs_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(tmpDirs_.back(), arrow::internal::TemporaryDir::Make(kTestLocalDirsPrefix))
    ARROW_ASSIGN_OR_RAISE(dataFile_, createTempShuffleFile(tmpDirs_.back()->path().ToString()));
    localDirs_.push_back(tmpDirs_.back()->path().ToString());

    // Create second tmp dir.
    tmpDirs_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(tmpDirs_.back(), arrow::internal::TemporaryDir::Make(kTestLocalDirsPrefix))
    localDirs_.push_back(tmpDirs_.back()->path().ToString());
    return arrow::Status::OK();
  }

  inline static TestAllocationListener* listener_{nullptr};

  std::vector<std::unique_ptr<arrow::internal::TemporaryDir>> tmpDirs_;
  std::string dataFile_;
  std::vector<std::string> localDirs_;

  std::vector<facebook::velox::VectorPtr> children1_;
  std::vector<facebook::velox::VectorPtr> children2_;
  std::vector<facebook::velox::VectorPtr> childrenNoNull_;
  std::vector<facebook::velox::VectorPtr> childrenLargeBinary1_;
  std::vector<facebook::velox::VectorPtr> childrenLargeBinary2_;
  std::vector<facebook::velox::VectorPtr> childrenComplex_;

  facebook::velox::RowVectorPtr inputVector1_;
  facebook::velox::RowVectorPtr inputVector2_;
  facebook::velox::RowVectorPtr inputVectorNoNull_;
  std::string largeString1_;
  std::string largeString2_;
  facebook::velox::RowVectorPtr inputVectorLargeBinary1_;
  facebook::velox::RowVectorPtr inputVectorLargeBinary2_;
  facebook::velox::RowVectorPtr inputVectorComplex_;
};

} // namespace gluten
