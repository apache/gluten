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

#include "memory/GlutenDirectBufferedInput.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "velox/common/caching/FileIds.h"
#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/StreamIdentifier.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using facebook::velox::cache::CoalescedLoad;

namespace gluten {

class GlutenDirectBufferedInputTest : public ::testing::Test {
 protected:
  static constexpr int32_t kRegion = 64 << 10;
  static constexpr int32_t kGap = 4 << 20;

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
    tracker_ = std::make_shared<cache::ScanTracker>("test", nullptr, 256 << 10);
    rootPool_ = memory::memoryManager()->addRootPool();
    pool_ = rootPool_->addLeafChild("GlutenDirectBufferedInputTest");
    // Row group g occupies [g * kGap, g * kGap + 3 * kRegion): three streams
    // (ids 1, 2, 3), each one region. Row groups are far apart so they never
    // coalesce with each other.
    content_.assign(2 * kGap, 'x');
    for (size_t i = 0; i < content_.size(); ++i) {
      content_[i] = static_cast<char>('a' + (i / kRegion) % 26);
    }
  }

  void TearDown() override {
    executor_.reset();
  }

  std::unique_ptr<GlutenDirectBufferedInput> makeInput(bool defer) {
    auto readFile = std::make_shared<InMemoryReadFile>(content_);
    io::ReaderOptions options(pool_.get());
    options.setLoadQuantum(8 << 20);
    options.setMaxCoalesceDistance(1 << 20);
    auto& ids = fileIds();
    return std::make_unique<GlutenDirectBufferedInput>(
        readFile,
        MetricsLog::voidLog(),
        StringIdLease(ids, "testFile"),
        tracker_,
        StringIdLease(ids, "testGroup"),
        std::make_shared<io::IoStatistics>(),
        nullptr,
        executor_.get(),
        options,
        folly::F14FastMap<std::string, std::string>{},
        defer);
  }

  // Enqueues the three streams of row group 'group' into 'input'.
  std::vector<std::unique_ptr<SeekableInputStream>> enqueueRowGroup(BufferedInput& input, int32_t group) {
    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    for (int32_t id = 1; id <= 3; ++id) {
      StreamIdentifier sid(id);
      streams.push_back(input.enqueue({static_cast<uint64_t>(group * kGap + (id - 1) * kRegion), kRegion}, &sid));
    }
    return streams;
  }

  static bool waitUntilNotPlanned(const std::shared_ptr<CoalescedLoad>& load) {
    for (int32_t i = 0; i < 500; ++i) {
      if (load->state() != CoalescedLoad::State::kPlanned) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
  }

  // The load covering stream 'id' of a row group input, by the region offset
  // of its first request.
  static std::shared_ptr<DirectCoalescedLoad> loadOf(const DirectBufferedInput& input, int32_t group, int32_t id) {
    for (const auto& load : input.testingCoalescedLoads()) {
      auto direct = std::static_pointer_cast<DirectCoalescedLoad>(load);
      for (const auto& request : direct->requests()) {
        if (request.region.offset == static_cast<uint64_t>(group * kGap + (id - 1) * kRegion)) {
          return direct;
        }
      }
    }
    return nullptr;
  }

  std::string readAll(SeekableInputStream& stream) {
    std::string out;
    const void* data;
    int32_t size;
    while (stream.Next(&data, &size)) {
      out.append(static_cast<const char*>(data), size);
    }
    return out;
  }

  std::string expected(int32_t group, int32_t id) {
    return content_.substr(group * kGap + (id - 1) * kRegion, kRegion);
  }

  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<cache::ScanTracker> tracker_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::string content_;
};

// With deferral off the input is a plain DirectBufferedInput: the lazy
// streams are ignored and all three adjacent regions coalesce into one load.
// Whether that load is prefetched or read on demand depends on the process
// wide cache_prefetch_min_pct flag, which this test does not control.
TEST_F(GlutenDirectBufferedInputTest, deferralOffLoadsEverything) {
  auto input = makeInput(false);
  EXPECT_EQ(input->testingLazyState(), nullptr);
  input->setLazyColumnStreams({3});
  auto streams = enqueueRowGroup(*input, 0);
  input->load(LogType::TEST);
  EXPECT_EQ(input->testingCoalescedLoads().size(), 1);
  EXPECT_EQ(input->testingStreamToCoalescedLoadSize(), 3);
  EXPECT_EQ(readAll(*streams[2]), expected(0, 3));
  EXPECT_EQ(readAll(*streams[0]), expected(0, 1));
}

TEST_F(GlutenDirectBufferedInputTest, lazyStreamsHeldUntilHint) {
  auto input = makeInput(true);
  ASSERT_NE(input->testingLazyState(), nullptr);
  input->setLazyColumnStreams({3});

  // Two row groups, each on its own clone, as the Parquet reader does.
  auto rowGroup0 = input->clone();
  auto streams0 = enqueueRowGroup(*rowGroup0, 0);
  rowGroup0->load(LogType::TEST);
  auto rowGroup1 = input->clone();
  auto streams1 = enqueueRowGroup(*rowGroup1, 1);
  rowGroup1->load(LogType::TEST);

  auto* direct0 = dynamic_cast<DirectBufferedInput*>(rowGroup0.get());
  auto* direct1 = dynamic_cast<DirectBufferedInput*>(rowGroup1.get());
  ASSERT_NE(direct0, nullptr);
  ASSERT_NE(direct1, nullptr);

  // Streams 1 and 2 are prefetched right away; stream 3 is planned but held.
  auto eager0 = loadOf(*direct0, 0, 1);
  auto held0 = loadOf(*direct0, 0, 3);
  auto held1 = loadOf(*direct1, 1, 3);
  ASSERT_NE(eager0, nullptr);
  ASSERT_NE(held0, nullptr);
  ASSERT_NE(held1, nullptr);
  EXPECT_TRUE(waitUntilNotPlanned(eager0));
  EXPECT_EQ(held0->state(), CoalescedLoad::State::kPlanned);
  EXPECT_EQ(held1->state(), CoalescedLoad::State::kPlanned);
  // The lazy stream is not coalesced into the eager read.
  EXPECT_NE(eager0, held0);

  // The hint goes to the file's input and releases every held load.
  input->hintLazyColumnsNeeded();
  EXPECT_TRUE(waitUntilNotPlanned(held0));
  EXPECT_TRUE(waitUntilNotPlanned(held1));

  // After the hint, later row groups prefetch the lazy stream like any other.
  auto rowGroup2 = input->clone();
  auto streams2 = enqueueRowGroup(*rowGroup2, 1);
  rowGroup2->load(LogType::TEST);
  auto load2 = loadOf(*dynamic_cast<DirectBufferedInput*>(rowGroup2.get()), 1, 3);
  ASSERT_NE(load2, nullptr);
  EXPECT_TRUE(waitUntilNotPlanned(load2));

  EXPECT_EQ(readAll(*streams0[0]), expected(0, 1));
  EXPECT_EQ(readAll(*streams0[2]), expected(0, 3));
  EXPECT_EQ(readAll(*streams1[2]), expected(1, 3));
  EXPECT_EQ(readAll(*streams2[2]), expected(1, 3));
}

TEST_F(GlutenDirectBufferedInputTest, readBeforeHintMarksColumnNeeded) {
  auto input = makeInput(true);
  input->setLazyColumnStreams({2, 3});

  auto rowGroup0 = input->clone();
  auto streams0 = enqueueRowGroup(*rowGroup0, 0);
  rowGroup0->load(LogType::TEST);
  auto rowGroup1 = input->clone();
  auto streams1 = enqueueRowGroup(*rowGroup1, 1);
  rowGroup1->load(LogType::TEST);

  auto* direct0 = dynamic_cast<DirectBufferedInput*>(rowGroup0.get());
  auto* direct1 = dynamic_cast<DirectBufferedInput*>(rowGroup1.get());
  auto held1Stream2 = loadOf(*direct1, 1, 2);
  auto held1Stream3 = loadOf(*direct1, 1, 3);
  ASSERT_NE(held1Stream2, nullptr);
  ASSERT_NE(held1Stream3, nullptr);
  // Streams 2 and 3 are adjacent, yet each deferred stream gets its own held
  // load: a read of one must not drag the other in.
  EXPECT_NE(held1Stream2, held1Stream3);
  EXPECT_EQ(held1Stream2->state(), CoalescedLoad::State::kPlanned);
  EXPECT_EQ(held1Stream3->state(), CoalescedLoad::State::kPlanned);

  // Reading stream 2 of row group 0 before any hint (as the remaining filter
  // would) loads it synchronously and marks only stream 2 as needed, so the
  // held load of stream 2 in row group 1 starts in the background while
  // stream 3 stays held.
  EXPECT_EQ(readAll(*streams0[1]), expected(0, 2));
  EXPECT_TRUE(waitUntilNotPlanned(held1Stream2));
  EXPECT_EQ(held1Stream3->state(), CoalescedLoad::State::kPlanned);
  EXPECT_EQ(loadOf(*direct0, 0, 3)->state(), CoalescedLoad::State::kPlanned);

  // A later row group prefetches the now needed stream eagerly and still holds
  // the other one.
  auto rowGroup2 = input->clone();
  auto streams2 = enqueueRowGroup(*rowGroup2, 0);
  rowGroup2->load(LogType::TEST);
  auto* direct2 = dynamic_cast<DirectBufferedInput*>(rowGroup2.get());
  auto load2Stream2 = loadOf(*direct2, 0, 2);
  auto load2Stream3 = loadOf(*direct2, 0, 3);
  ASSERT_NE(load2Stream2, nullptr);
  ASSERT_NE(load2Stream3, nullptr);
  EXPECT_TRUE(waitUntilNotPlanned(load2Stream2));
  EXPECT_EQ(load2Stream3->state(), CoalescedLoad::State::kPlanned);
  // The hint releases what is still held.
  input->hintLazyColumnsNeeded();
  EXPECT_TRUE(waitUntilNotPlanned(held1Stream3));
  EXPECT_TRUE(waitUntilNotPlanned(load2Stream3));
  EXPECT_EQ(readAll(*streams2[2]), expected(0, 3));
  EXPECT_EQ(readAll(*streams1[2]), expected(1, 3));
}

// Deferral on but the reader named no lazy streams: load() falls through to
// DirectBufferedInput::load(), one coalesced load for the three regions.
TEST_F(GlutenDirectBufferedInputTest, noLazyStreamsBehavesLikeBase) {
  auto input = makeInput(true);
  auto rowGroup0 = input->clone();
  auto streams = enqueueRowGroup(*rowGroup0, 0);
  rowGroup0->load(LogType::TEST);
  auto* direct = dynamic_cast<DirectBufferedInput*>(rowGroup0.get());
  ASSERT_NE(direct, nullptr);
  EXPECT_EQ(direct->testingCoalescedLoads().size(), 1);
  EXPECT_EQ(direct->testingStreamToCoalescedLoadSize(), 3);
  EXPECT_EQ(readAll(*streams[2]), expected(0, 3));
}

} // namespace gluten
