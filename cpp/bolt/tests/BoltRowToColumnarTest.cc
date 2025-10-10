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

#include "../utils/BoltArrowUtils.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/BoltColumnarBatch.h"
#include "operators/serializer//BoltColumnarToRowConverter.h"
#include "operators/serializer//BoltRowToColumnarConverter.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance;
using namespace bytedance::bolt;

namespace gluten {

class BoltRowToColumnarTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void testRowVectorEqual(bolt::RowVectorPtr vector) {
    auto columnarToRowConverter = std::make_shared<BoltColumnarToRowConverter>(pool_, 64 << 10);

    auto columnarBatch = std::make_shared<BoltColumnarBatch>(vector);
    columnarToRowConverter->convert(columnarBatch);

    int64_t numRows = vector->size();
    uint8_t* address = columnarToRowConverter->getBufferAddress();
    auto lengthVec = columnarToRowConverter->getLengths();

    std::vector<int64_t> lengthArr(lengthVec.size());
    for (int i = 0; i < lengthVec.size(); i++) {
      lengthArr[i] = lengthVec[i];
    }

    ArrowSchema cSchema;
    toArrowSchema(vector->type(), pool(), &cSchema);
    auto rowToColumnarConverter = std::make_shared<BoltRowToColumnarConverter>(&cSchema, pool_);

    auto cb = rowToColumnarConverter->convert(numRows, lengthArr.data(), address);
    auto vp = std::dynamic_pointer_cast<BoltColumnarBatch>(cb)->getRowVector();
    bolt::test::assertEqualVectors(vector, vp);
  }
};

TEST_F(BoltRowToColumnarTest, allTypes) {
  auto vector = makeRowVector({
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
           std::nullopt,
           0.1234567,
           std::nullopt,
           -0.142857,
           std::nullopt,
           0.142857,
           0.285714,
           0.428617,
           std::nullopt}),
      makeNullableFlatVector<bool>(
          {std::nullopt, true, false, std::nullopt, true, true, false, true, std::nullopt, std::nullopt}),
      makeFlatVector<bolt::StringView>(
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
      makeNullableFlatVector<bolt::StringView>(
          {"alice", "bob", std::nullopt, std::nullopt, "Alice", "Bob", std::nullopt, "alicE", std::nullopt, "boB"}),
  });
  testRowVectorEqual(vector);
}

TEST_F(BoltRowToColumnarTest, bigint) {
  auto vector = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt, 4, std::nullopt, 5, 6, std::nullopt, 7}),
  });
  testRowVectorEqual(vector);
}

TEST_F(BoltRowToColumnarTest, decimal) {
  auto vector = makeRowVector({
      makeNullableFlatVector<int128_t>(
          {123456, HugeInt::build(1045, 1789), 3678, std::nullopt, 4, std::nullopt, 5, 687987, std::nullopt, 7},
          DECIMAL(38, 2)),
      makeNullableFlatVector<int64_t>(
          {178987, 2, 3, std::nullopt, 4, std::nullopt, 5, 6, std::nullopt, 7}, DECIMAL(12, 3)),
  });
  testRowVectorEqual(vector);
}

TEST_F(BoltRowToColumnarTest, timestamp) {
  auto vector = makeRowVector({
      makeNullableFlatVector<Timestamp>(
          {Timestamp(-946684800, 0),
           Timestamp(-7266, 0),
           Timestamp(0, 0),
           Timestamp(946684800, 0),
           Timestamp(9466848000, 0),
           Timestamp(94668480000, 0),
           Timestamp(946729316, 0),
           Timestamp(946729316, 0),
           Timestamp(946729316, 0),
           Timestamp(7266, 0),
           Timestamp(-50049331200, 0),
           Timestamp(253405036800, 0),
           Timestamp(-62480037600, 0),
           std::nullopt}),
  });
  testRowVectorEqual(vector);
}

} // namespace gluten
