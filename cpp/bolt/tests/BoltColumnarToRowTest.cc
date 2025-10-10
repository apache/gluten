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

#include "memory/BoltColumnarBatch.h"
#include "memory/BoltMemoryManager.h"
#include "operators/serializer/BoltColumnarToRowConverter.h"
#include "operators/serializer/BoltRowToColumnarConverter.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

using namespace bytedance;
using namespace bytedance::bolt;

namespace gluten {

class BoltColumnarToRowTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void testRowBufferAddr(bolt::RowVectorPtr vector, uint8_t* expectArr, int32_t expectArrSize) {
    auto columnarToRowConverter = std::make_shared<BoltColumnarToRowConverter>(pool_, 64 << 10);

    auto cb = std::make_shared<BoltColumnarBatch>(vector);
    columnarToRowConverter->convert(cb);

    uint8_t* address = columnarToRowConverter->getBufferAddress();
    for (int i = 0; i < expectArrSize; i++) {
      ASSERT_EQ(*(address + i), *(expectArr + i));
    }
  }
};

TEST_F(BoltColumnarToRowTest, Buffer_int8_int16) {
  auto vector = makeRowVector({makeFlatVector<int8_t>({1, 2}), makeFlatVector<int16_t>({1, 2})});
  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  testRowBufferAddr(vector, expectArr, sizeof(expectArr));
}

TEST_F(BoltColumnarToRowTest, Buffer_int32_int64) {
  auto vector = makeRowVector({makeFlatVector<int32_t>({1, 2}), makeFlatVector<int64_t>({1, 2})});
  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  testRowBufferAddr(vector, expectArr, sizeof(expectArr));
}

TEST_F(BoltColumnarToRowTest, Buffer_float_double) {
  auto vector = makeRowVector({makeFlatVector<float>({1.0, 2.0}), makeFlatVector<double>({1.0, 2.0})});

  uint8_t expectArr[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64};
  testRowBufferAddr(vector, expectArr, sizeof(expectArr));
}

TEST_F(BoltColumnarToRowTest, Buffer_bool_string) {
  auto vector = makeRowVector({makeFlatVector<bool>({false, true}), makeFlatVector<StringView>({"aa", "bb"})});

  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };
  testRowBufferAddr(vector, expectArr, sizeof(expectArr));
}

TEST_F(BoltColumnarToRowTest, Buffer_int64_int64_with_null) {
  auto vector = makeRowVector(
      {makeNullableFlatVector<int64_t>({std::nullopt, 2}), makeNullableFlatVector<int64_t>({std::nullopt, 2})});

  uint8_t expectArr[] = {
      3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  testRowBufferAddr(vector, expectArr, sizeof(expectArr));
}

} // namespace gluten
