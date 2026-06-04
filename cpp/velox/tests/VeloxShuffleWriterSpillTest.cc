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

#include <duckdb/common/enums/compression_type.hpp>

#include "shuffle/VeloxHashShuffleWriter.h"
#include "tests/VeloxShuffleWriterTestBase.h"
#include "tests/utils/TestUtils.h"
#include "utils/TestAllocationListener.h"

using namespace facebook::velox;

namespace gluten {

namespace {
void assertSpill(TestAllocationListener* listener, std::function<void()> block) {
  const auto beforeSpill = listener->reclaimedBytes();
  block();
  const auto afterSpill = listener->reclaimedBytes();

  ASSERT_GT(afterSpill, beforeSpill);
}
} // namespace

class VeloxHashShuffleWriterSpillTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  void SetUp() override {
    setUpTestData();
  }

  std::shared_ptr<VeloxShuffleWriter> createHashShuffleWriter(
      uint32_t numPartitions,
      const std::shared_ptr<HashShuffleWriterOptions>& shuffleWriterOptions,
      std::shared_ptr<LocalPartitionWriterOptions> partitionWriterOptions = nullptr,
      arrow::Compression::type compressionType = arrow::Compression::type::LZ4_FRAME) {
    if (partitionWriterOptions == nullptr) {
      partitionWriterOptions = std::make_shared<LocalPartitionWriterOptions>();
    }

    std::unique_ptr<arrow::util::Codec> codec;
    if (compressionType == arrow::Compression::type::UNCOMPRESSED) {
      codec = nullptr;
    } else {
      GLUTEN_ASSIGN_OR_THROW(codec, arrow::util::Codec::Create(compressionType));
    }

    auto partitionWriter = std::make_shared<LocalPartitionWriter>(
        numPartitions, std::move(codec), getDefaultMemoryManager(), partitionWriterOptions, dataFile_, localDirs_);

    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxHashShuffleWriter::create(
            numPartitions, partitionWriter, shuffleWriterOptions, getDefaultMemoryManager()));

    return shuffleWriter;
  }

  int64_t splitRowVectorAndSpill(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<facebook::velox::RowVectorPtr> vectors,
      bool shrink) {
    for (auto vector : vectors) {
      ASSERT_NOT_OK(splitRowVector(shuffleWriter, vector));
    }

    auto targetEvicted = shuffleWriter.cachedPayloadSize();
    if (shrink) {
      targetEvicted += shuffleWriter.partitionBufferSize();
    }
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter.reclaimFixedSize(targetEvicted, &evicted));

    return evicted;
  };
};

TEST_F(VeloxHashShuffleWriterSpillTest, memoryLeak) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter = createHashShuffleWriter(2, shuffleWriterOptions);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  ASSERT_NOT_OK(shuffleWriter->stop());

  const auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();

  ASSERT_EQ(arrowPool->bytes_allocated(), 0);

  shuffleWriter.reset();
  ASSERT_EQ(arrowPool->bytes_allocated(), 0);
}

TEST_F(VeloxHashShuffleWriterSpillTest, spillFailWithOutOfMemory) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter = createHashShuffleWriter(2, shuffleWriterOptions);

  listener_->updateLimit(0L);
  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->setThrowIfOOM(true);

  ASSERT_THROW([&] { auto status = splitRowVector(*shuffleWriter, inputVector1_); }(), GlutenException);

  // Should return OOM status because there's no partition buffer to spill.

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kInit) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter = createHashShuffleWriter(2, shuffleWriterOptions);

  // Test spill all partition buffers.
  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize + bufferSize, &evicted));
    ASSERT_EQ(evicted, payloadSize + bufferSize);
    // No cached payload after evict.
    ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
    // All partition buffers should be evicted.
    ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
  }

  // Test spill minimum-size partition buffers.
  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize + 1, &evicted));
    ASSERT_GT(evicted, payloadSize);
    // No cached payload after evict.
    ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
    ASSERT_LE(shuffleWriter->partitionBufferSize(), bufferSize);
    // Not all partition buffers was evicted.
    ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);
  }

  // Test spill empty partition buffers.
  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    // Clear buffers then the size after shrink will be 0.
    for (auto pid = 0; pid < shuffleWriter->numPartitions(); ++pid) {
      ASSERT_NOT_OK(shuffleWriter->evictPartitionBuffers(pid, true));
    }

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    // Evict payload and shrink min-size buffer.
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize + 1, &evicted));
    ASSERT_GT(evicted, payloadSize);
    ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);
    // Evict empty partition buffers.
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(bufferSize, &evicted));
    ASSERT_GT(evicted, 0);
    ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
    // Evict again. No reclaimable space.
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(1, &evicted));
    ASSERT_EQ(evicted, 0);
  }

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxHashShuffleWriterSpillTest, spillPartitionBuffersInPidOrder) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->partitioning = Partitioning::kRange;
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter =
      createHashShuffleWriter(8, shuffleWriterOptions, nullptr, arrow::Compression::type::UNCOMPRESSED);

  // pid 4 gets a larger partition buffer than pid 1. Size-based eviction chooses pid 4 first, but the selected
  // payloads must still be written to local spill files in pid order.
  auto input = makeRowVector({
      makeFlatVector<int32_t>({4, 4, 4, 4, 4, 4, 4, 4, 1}),
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8}),
  });

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, input));

  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->partitionBufferSize(), &evicted));
  ASSERT_GT(evicted, 0);
  ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxHashShuffleWriterSpillTest, kInitSingle) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->partitioning = Partitioning::kSingle;
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter = createHashShuffleWriter(1, shuffleWriterOptions);

  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto payloadSize = shuffleWriter->partitionBufferSize() + shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize, &evicted));
    ASSERT_GE(evicted, payloadSize);
    // No cached payload after evict.
    ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  }

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxHashShuffleWriterSpillTest, kSplit) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter = createHashShuffleWriter(2, shuffleWriterOptions);

  listener_->setShuffleWriter(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  listener_->updateLimit(listener_->currentBytes());

  assertSpill(listener_, [&]() {
    // Trigger spill for the next split.
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  });

  ASSERT_NOT_OK(shuffleWriter->stop());

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kSplitSingle) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->partitioning = Partitioning::kSingle;
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter = createHashShuffleWriter(1, shuffleWriterOptions);

  listener_->setShuffleWriter(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Trigger spill for the next split.
  listener_->updateLimit(listener_->currentBytes());

  assertSpill(listener_, [&]() { ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_)); });

  ASSERT_NOT_OK(shuffleWriter->stop());

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kStop) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  // Force compression.
  auto partitionWriterOptions = std::make_shared<LocalPartitionWriterOptions>();
  partitionWriterOptions->compressionThreshold = 0;
  partitionWriterOptions->mergeThreshold = 0;

  auto shuffleWriter = createHashShuffleWriter(2, shuffleWriterOptions, partitionWriterOptions);

  for (int i = 0; i < 10; ++i) {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  }
  // Reclaim bytes to shrink partition buffer.
  int64_t reclaimed = 0;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(2000, &reclaimed));
  ASSERT_GE(reclaimed, 2000);

  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->updateLimit(listener_->currentBytes());

  // Trigger spill during stop.
  assertSpill(listener_, [&] { ASSERT_NOT_OK(shuffleWriter->stop()); });

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kStopComplex) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  // Force compression.
  auto partitionWriterOptions = std::make_shared<LocalPartitionWriterOptions>();
  partitionWriterOptions->compressionThreshold = 0;
  partitionWriterOptions->mergeThreshold = 0;

  auto shuffleWriter = createHashShuffleWriter(2, shuffleWriterOptions, partitionWriterOptions);

  for (int i = 0; i < 3; ++i) {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorComplex_));
  }

  // Reclaim bytes to shrink partition buffer.
  int64_t reclaimed = 0;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(2000, &reclaimed));
  ASSERT_GE(reclaimed, 2000);

  // Reclaim from PartitionWriter to free cached bytes.
  auto payloadSize = shuffleWriter->cachedPayloadSize();
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize, &evicted));
  ASSERT_EQ(evicted, payloadSize);

  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->updateLimit(listener_->currentBytes());

  // When evicting partitioning buffers in stop, spill will be triggered by complex types allocating extra memory.
  assertSpill(listener_, [&] { ASSERT_NOT_OK(shuffleWriter->stop()); });

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, evictPartitionBuffers) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferSize = 4;
  auto shuffleWriter = createHashShuffleWriter(2, shuffleWriterOptions);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // First evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);

  // Evict again. Because no cached payload to evict, it will try to evict all partition buffers.
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->partitionBufferSize(), &evicted));
  ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
}

TEST_F(VeloxHashShuffleWriterSpillTest, kUnevictableSingle) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->partitioning = Partitioning::kSingle;
  auto shuffleWriter = createHashShuffleWriter(1, shuffleWriterOptions);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // First evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(
      shuffleWriter->partitionBufferSize() + shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_GE(evicted, 0);

  // Evict again. The evicted size should be 0.
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(1, &evicted));
  ASSERT_EQ(evicted, 0);
}

TEST_F(VeloxHashShuffleWriterSpillTest, resizeBinaryBufferTriggerSpill) {
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferReallocThreshold = 1;

  auto partitionWriterOptions = std::make_shared<LocalPartitionWriterOptions>();

  auto shuffleWriter =
      createHashShuffleWriter(1, shuffleWriterOptions, partitionWriterOptions, arrow::Compression::type::UNCOMPRESSED);

  // Split first input vector. Large average string length.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorLargeBinary1_));

  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->updateLimit(listener_->currentBytes());

  assertSpill(listener_, [&] {
    // Split second input vector. Large average string length.
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorLargeBinary2_));
  });

  ASSERT_NOT_OK(shuffleWriter->stop());

  listener_->reset();
}

} // namespace gluten
