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

#include "jni/JniHashTable.h"
#include "compute/VeloxBackend.h"
#include "tests/utils/TestAllocationListener.h"
#include "velox/vector/FlatVector.h"

#include <gtest/gtest.h>

namespace gluten {

// This test binary, like the micro benchmark, runs without a JVM. JNI_OnLoad is therefore never
// invoked and JniHashTableContext::vm_ stays null.
//
// Looking up the build side of a broadcast hash join used to dereference that null JavaVM
// unconditionally, so converting any substrait plan carrying a non-empty `hashTableId` crashed the
// process. See https://github.com/apache/gluten/issues/12504.
class JniHashTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(JniHashTableContext::getInstance().getJavaVM(), nullptr)
        << "This test asserts behaviour of a process with no JVM attached";
  }
};

TEST_F(JniHashTableTest, getJoinReportsMissWithoutJvm) {
  // Must report a miss rather than segfault. SubstraitToVeloxPlanConverter treats 0 as "no
  // pre-built table" and falls back to building one from the build side input.
  EXPECT_EQ(getJoin("no-such-broadcast-hash-table-id"), 0);
}

TEST_F(JniHashTableTest, callJavaGetReportsUnreachableCacheWithoutJvm) {
  // Keeps "the cache says there is no such table" distinguishable from "the cache cannot be
  // reached at all", rather than collapsing both into a handle of 0 at this level.
  EXPECT_FALSE(JniHashTableContext::getInstance().callJavaGet("any-id").has_value());
}

class RestoredHashTableTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto listener = std::make_unique<TestAllocationListener>();
    listener->updateLimit(0);
    listener->setThrowIfOOM(true);
    VeloxBackend::create(std::move(listener), {});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
  }

  std::vector<uint8_t> serializedTable() {
    using namespace facebook::velox;
    auto pool = broadcastLeafVeloxMemoryPool();
    std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
    hashers.push_back(exec::VectorHasher::create(BIGINT(), 0));
    auto table = exec::HashTable<true>::createForJoin(std::move(hashers), {}, pool.get(), {});
    auto values = BaseVector::create(BIGINT(), 3, pool.get());
    auto* flat = values->asFlatVector<int64_t>();
    for (int i = 0; i < 3; ++i) {
      flat->set(i, i);
    }
    DecodedVector decoded(*values);
    for (int i = 0; i < 3; ++i) {
      auto* row = table->rows()->newRow();
      if (auto offset = table->rows()->nextOffset()) {
        *reinterpret_cast<char**>(row + offset) = nullptr;
      }
      table->rows()->store(decoded, i, row, 0);
    }
    table->prepareJoinTable({}, exec::BaseHashTable::kNoSpillInputStartPartitionBit, 1000000, false, nullptr);
    std::vector<uint8_t> data(table->serializedSize());
    table->serializeTo(data.data(), data.size());
    return data;
  }
};

TEST_F(RestoredHashTableTest, restoreSucceedsWhenGlobalMemoryIsUnavailable) {
  auto globalPool = defaultLeafVeloxMemoryPool();
  EXPECT_THROW(
      {
        auto* buffer = globalPool->allocate(1);
        globalPool->free(buffer, 1);
      },
      GlutenException);

  auto data = serializedTable();
  auto pool = broadcastLeafVeloxMemoryPool();
  const auto usedBefore = pool->usedBytes();
  auto builder = deserializeHashTable(data.data(), data.size(), true, false);
  EXPECT_GT(builder->memoryUsage(), 0);
  EXPECT_GT(pool->usedBytes(), usedBefore);
  EXPECT_EQ(VeloxBackend::get()->getGlobalMemoryManager()->getListener()->currentBytes(), 0);

  auto probeTable = builder->hashTable();
  builder.reset();
  EXPECT_EQ(probeTable->rows()->numRows(), 3);
  // Release the last reference to the table. Its allocations are freed even though the
  // broadcast pool stays alive, so usedBytes returns to the value before restoration.
  probeTable.reset();
  EXPECT_EQ(pool->usedBytes(), usedBefore);
}

TEST_F(RestoredHashTableTest, failedRestoreCleansUpAllocations) {
  auto data = serializedTable();
  auto pool = broadcastLeafVeloxMemoryPool();
  const auto usedBefore = pool->usedBytes();
  // Add an extra byte to trigger a failure after the table has been allocated.
  data.push_back(0);
  EXPECT_THROW(deserializeHashTable(data.data(), data.size(), true, false), std::exception);
  EXPECT_EQ(pool->usedBytes(), usedBefore);
}

} // namespace gluten
