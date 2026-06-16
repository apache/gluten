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

#include "velox/common/memory/Memory.h"
#include "velox/external/xxhash/xxhash.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

class RowBasedChecksumTest : public test::VectorTestBase, public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }
  // Simulate the checksum computation from VeloxHashShuffleWriter.
  std::pair<int64_t, int64_t> computeChecksums(const RowVectorPtr& rv, const std::vector<uint32_t>& rowOrder) {
    row::UnsafeRowFast fast(rv);
    auto rowType = std::dynamic_pointer_cast<const RowType>(rv->type());
    auto fixedSize = row::UnsafeRowFast::fixedRowSize(rowType);
    int32_t bufSize = fixedSize.value_or(1024);
    std::vector<char> buffer(bufSize, 0);

    int64_t checksumXor = 0;
    int64_t checksumSum = 0;

    for (auto row : rowOrder) {
      auto size = fast.rowSize(row);
      if (size > static_cast<int32_t>(buffer.size())) {
        buffer.resize(size);
      }
      std::memset(buffer.data(), 0, size);
      fast.serialize(row, buffer.data());

      auto hash = static_cast<int64_t>(XXH64(buffer.data(), size, 0));
      checksumXor ^= hash;
      checksumSum += hash;
    }

    int64_t rotated = (static_cast<uint64_t>(checksumSum) << 27) | (static_cast<uint64_t>(checksumSum) >> 37);
    return {checksumXor ^ rotated, checksumSum};
  }
};

TEST_F(RowBasedChecksumTest, orderIndependence) {
  // Create a RowVector with 5 rows: (int, string)
  auto rv = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int32_t>({10, 20, 30, 40, 50}),
       makeFlatVector<StringView>({"hello", "world", "foo", "bar", "baz"})});

  // Compute checksum in original order
  std::vector<uint32_t> order1 = {0, 1, 2, 3, 4};
  auto [checksum1, _1] = computeChecksums(rv, order1);

  // Compute checksum in reversed order
  std::vector<uint32_t> order2 = {4, 3, 2, 1, 0};
  auto [checksum2, _2] = computeChecksums(rv, order2);

  // Compute checksum in shuffled order
  std::vector<uint32_t> order3 = {2, 4, 0, 3, 1};
  auto [checksum3, _3] = computeChecksums(rv, order3);

  // All should be equal (order-independent)
  EXPECT_EQ(checksum1, checksum2);
  EXPECT_EQ(checksum1, checksum3);
  EXPECT_NE(checksum1, 0); // Should be non-zero
}

TEST_F(RowBasedChecksumTest, differentDataProducesDifferentChecksum) {
  auto rv1 = makeRowVector({"a"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto rv2 = makeRowVector({"a"}, {makeFlatVector<int64_t>({1, 2, 4})}); // last value different

  std::vector<uint32_t> order = {0, 1, 2};
  auto [checksum1, _1] = computeChecksums(rv1, order);
  auto [checksum2, _2] = computeChecksums(rv2, order);

  EXPECT_NE(checksum1, checksum2);
}

TEST_F(RowBasedChecksumTest, nullHandling) {
  auto rv1 = makeRowVector({"a"}, {makeNullableFlatVector<int32_t>({1, std::nullopt, 3})});
  auto rv2 = makeRowVector({"a"}, {makeNullableFlatVector<int32_t>({1, 0, 3})}); // 0 vs null

  std::vector<uint32_t> order = {0, 1, 2};
  auto [checksum1, _1] = computeChecksums(rv1, order);
  auto [checksum2, _2] = computeChecksums(rv2, order);

  // null and 0 should produce different checksums
  EXPECT_NE(checksum1, checksum2);
}

TEST_F(RowBasedChecksumTest, deterministic) {
  auto rv =
      makeRowVector({"a", "b"}, {makeFlatVector<int64_t>({100, 200, 300}), makeFlatVector<double>({1.1, 2.2, 3.3})});

  std::vector<uint32_t> order = {0, 1, 2};
  auto [checksum1, _1] = computeChecksums(rv, order);
  auto [checksum2, _2] = computeChecksums(rv, order);

  // Same input, same order -> same result (deterministic)
  EXPECT_EQ(checksum1, checksum2);
}
