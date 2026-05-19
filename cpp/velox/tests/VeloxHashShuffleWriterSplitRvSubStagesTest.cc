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

#include <gtest/gtest.h>

#include "shuffle/VeloxHashShuffleWriter.h"

#include "VeloxShuffleWriterTestBase.h"
#include "utils/Macros.h"
#include "utils/TestUtils.h"

namespace gluten {

namespace {

std::shared_ptr<PartitionWriter> makeLocalPartitionWriter(
    uint32_t numPartitions,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs) {
  GLUTEN_ASSIGN_OR_THROW(auto codec, arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  auto options = std::make_shared<LocalPartitionWriterOptions>();
  return std::make_shared<LocalPartitionWriter>(
      numPartitions, std::move(codec), getDefaultMemoryManager(), options, dataFile, localDirs);
}

} // namespace

class HashShuffleWriterSplitRvSubStagesTest : public ::testing::Test, public VeloxShuffleWriterTestBase {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  void SetUp() override {
    VeloxShuffleWriterTestBase::setUpTestData();
  }

  std::shared_ptr<VeloxShuffleWriter> createWriter(uint32_t numPartitions) {
    auto options = std::make_shared<HashShuffleWriterOptions>();
    options->partitioning = Partitioning::kHash;
    options->splitBufferSize = 4096;

    auto partitionWriter = makeLocalPartitionWriter(numPartitions, dataFile_, localDirs_);

    GLUTEN_ASSIGN_OR_THROW(
        auto writer,
        VeloxShuffleWriter::create(
            ShuffleWriterType::kHashShuffle, numPartitions, partitionWriter, options, getDefaultMemoryManager()));
    return writer;
  }

  arrow::Status writeBatch(VeloxShuffleWriter& writer, facebook::velox::RowVectorPtr rv) {
    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(rv);
    return writer.write(cb, ShuffleWriter::kMinMemLimit);
  }
};

// Verifies that the human-readable names for the 4 new sub-stages match the
// enum identifiers exactly (this is what shows up in the
// `VELOX_SHUFFLE_WRITER_LOG_FLAG` log line, so the strings are part of the
// stable observable surface).
TEST_F(HashShuffleWriterSplitRvSubStagesTest, enumNames) {
  EXPECT_EQ(
      VeloxShuffleWriter::CpuWallTimingName(VeloxShuffleWriter::CpuWallTimingSplitFixedWidth),
      "CpuWallTimingSplitFixedWidth");
  EXPECT_EQ(
      VeloxShuffleWriter::CpuWallTimingName(VeloxShuffleWriter::CpuWallTimingSplitValidity),
      "CpuWallTimingSplitValidity");
  EXPECT_EQ(
      VeloxShuffleWriter::CpuWallTimingName(VeloxShuffleWriter::CpuWallTimingSplitBinary), "CpuWallTimingSplitBinary");
  EXPECT_EQ(
      VeloxShuffleWriter::CpuWallTimingName(VeloxShuffleWriter::CpuWallTimingSplitComplex),
      "CpuWallTimingSplitComplex");
}

// A batch with one fixed-width data column should tick the fixed-width
// sub-stage and visit (but not necessarily do meaningful work in) the other
// three. We assert `count >= 1` rather than `>0 ns wall` because the
// SCOPED_TIMER timing resolution can round empty paths to 0 ns; the count
// is the reliable signal that the timer was reached.
TEST_F(HashShuffleWriterSplitRvSubStagesTest, fixedWidthBumpsItsBucket) {
  auto writer = createWriter(2);
  auto rv = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 0, 1}), // partition key
      makeFlatVector<int64_t>({100, 200, 300, 400}),
  });
  ASSERT_NOT_OK(writeBatch(*writer, rv));

  EXPECT_EQ(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitFixedWidth).count, 1);
  EXPECT_EQ(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitValidity).count, 1);
  EXPECT_EQ(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitBinary).count, 1);
  EXPECT_EQ(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitComplex).count, 1);
}

// A batch with a VARCHAR column should tick the binary sub-stage with
// nonzero wall time (there's real work copying string data per partition).
TEST_F(HashShuffleWriterSplitRvSubStagesTest, binaryBumpsItsBucket) {
  auto writer = createWriter(2);
  auto rv = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 0, 1}), // partition key
      makeFlatVector<facebook::velox::StringView>({"alpha", "beta", "gamma", "delta"}),
  });
  ASSERT_NOT_OK(writeBatch(*writer, rv));

  EXPECT_EQ(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitBinary).count, 1);
  EXPECT_GT(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitBinary).wallNanos, 0);
}

// A batch with a complex (ARRAY) column should tick the complex sub-stage
// with nonzero wall time (Presto serializer round-trip per partition).
TEST_F(HashShuffleWriterSplitRvSubStagesTest, complexBumpsItsBucket) {
  auto writer = createWriter(2);
  auto rv = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 0, 1}), // partition key
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6},
          {7, 8, 9, 10},
      }),
  });
  ASSERT_NOT_OK(writeBatch(*writer, rv));

  EXPECT_EQ(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitComplex).count, 1);
  EXPECT_GT(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitComplex).wallNanos, 0);
}

// Sanity: the outer SplitRV bucket continues to tick once per batch — the
// new sub-stage timers do not replace it, they refine it.
TEST_F(HashShuffleWriterSplitRvSubStagesTest, outerSplitRvStillCounted) {
  auto writer = createWriter(2);
  auto rv = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 0, 1}),
      makeFlatVector<int64_t>({100, 200, 300, 400}),
  });
  ASSERT_NOT_OK(writeBatch(*writer, rv));
  ASSERT_NOT_OK(writeBatch(*writer, rv));

  EXPECT_EQ(writer->cpuWallTiming(VeloxShuffleWriter::CpuWallTimingSplitRV).count, 2);
}

} // namespace gluten

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
