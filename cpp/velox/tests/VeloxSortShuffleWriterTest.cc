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

#include "tests/VeloxShuffleWriterTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace gluten {

namespace {
class FakeBufferRssClient : public RssClient {
 public:
  FakeBufferRssClient() = default;

  int32_t pushPartitionData(int32_t partitionId, const char* bytes, int64_t size) override {
    receiveTimes_++;
    return size;
  }

  void stop() override {}

  const uint32_t getReceiveTimes() const {
    return receiveTimes_;
  }

 private:
  uint32_t receiveTimes_{0};
};
} // namespace

class VeloxSortShuffleWriterTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  static size_t numPages(const VeloxSortShuffleWriter& writer) {
    return writer.pageAddresses_.size();
  }

  static uint64_t rowIdAt(const VeloxSortShuffleWriter& writer, size_t index) {
    return writer.arrayPtr_[index];
  }

  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(
      uint32_t numPartitions,
      std::shared_ptr<SortShuffleWriterOptions> writeOptions,
      std::shared_ptr<RssClient> rssClient) {
    auto options = std::make_shared<RssPartitionWriterOptions>();
    auto partitionWriter = std::make_shared<RssPartitionWriter>(
        numPartitions, nullptr, getDefaultMemoryManager(), options, std::move(rssClient));
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxSortShuffleWriter::create(
            numPartitions, std::move(partitionWriter), std::move(writeOptions), getDefaultMemoryManager()));
    return shuffleWriter;
  }
};

TEST_F(VeloxSortShuffleWriterTest, pushCompleteRows) {
  auto rssClient = std::make_shared<FakeBufferRssClient>();
  auto writeOptions = std::make_shared<SortShuffleWriterOptions>();
  // Make buffer size smallest to ensure each push only contains one row.
  writeOptions->diskWriteBufferSize = 1;

  auto rowVector = makeRowVector({
      makeFlatVector<StringView>(
          {"alice0",
           "bob1",
           "alice2",
           "bob3",
           "Alice4",
           "Bob5123456789098766notinline",
           "AlicE6",
           "boB7",
           "ALICE8",
           "BOB9"}),
  });
  std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(rowVector);

  auto shuffleWriter = createShuffleWriter(1, writeOptions, rssClient);
  auto status = shuffleWriter->write(cb, ShuffleWriter::kMinMemLimit);
  ASSERT_TRUE(shuffleWriter->stop().ok());

  // numRows should equal to push data times in rss client.
  EXPECT_EQ(10, rssClient->getReceiveTimes());
}

TEST_F(VeloxSortShuffleWriterTest, rollsOverPageBeforeCompactRowOffsetOverflows) {
  constexpr uint32_t kCompactRowOffsetLimit = 1U << 27;
  std::string largeValue(kCompactRowOffsetLimit + 1024 * 1024, 'x');
  auto values = std::vector<StringView>{StringView(largeValue), StringView("small")};
  auto rowVector = makeRowVector({makeFlatVector<StringView>(values)});
  auto writeOptions = std::make_shared<SortShuffleWriterOptions>();
  auto rssClient = std::make_shared<FakeBufferRssClient>();
  auto shuffleWriter =
      std::dynamic_pointer_cast<VeloxSortShuffleWriter>(createShuffleWriter(1, writeOptions, rssClient));

  auto status = shuffleWriter->write(std::make_shared<VeloxColumnarBatch>(rowVector), ShuffleWriter::kMinMemLimit);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // The second row must start on a new page because its offset would exceed the 27-bit row ID field.
  ASSERT_EQ(2, numPages(*shuffleWriter));
  constexpr uint64_t kOffsetMask = (1ULL << 27) - 1;
  constexpr uint64_t kPageMask = ((1ULL << 40) - 1) >> 27;
  EXPECT_EQ(0, rowIdAt(*shuffleWriter, 0) & kOffsetMask);
  EXPECT_EQ(0, (rowIdAt(*shuffleWriter, 0) >> 27) & kPageMask);
  EXPECT_EQ(0, rowIdAt(*shuffleWriter, 1) & kOffsetMask);
  EXPECT_EQ(1, (rowIdAt(*shuffleWriter, 1) >> 27) & kPageMask);

  ASSERT_TRUE(shuffleWriter->stop().ok());
  EXPECT_EQ(2, rssClient->getReceiveTimes());
}

} // namespace gluten
