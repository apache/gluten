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

#include <arrow/util/compression.h>

#include "memory/VeloxMemoryManager.h"
#include "shuffle/VeloxRssSortShuffleWriter.h"
#include "tests/VeloxShuffleWriterTestBase.h"
#include "tests/utils/TestUtils.h"

#include "velox/buffer/Buffer.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace gluten {

class VeloxRssSortShuffleWriterTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  void SetUp() override {
    setUpTestData();
    strBuffer_ = AlignedBuffer::allocate<char>(200, pool());
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions) {
    auto options = std::make_shared<RssPartitionWriterOptions>();
    auto writerOptions = std::make_shared<RssSortShuffleWriterOptions>();
    auto rssClient = std::make_unique<LocalRssClient>(dataFile_);
    std::unique_ptr<arrow::util::Codec> codec;
    if (writerOptions->compressionType == arrow::Compression::type::UNCOMPRESSED) {
      codec = nullptr;
    } else {
      GLUTEN_ASSIGN_OR_THROW(codec, arrow::util::Codec::Create(writerOptions->compressionType));
    }
    auto partitionWriter = std::make_shared<RssPartitionWriter>(
        numPartitions, std::move(codec), getDefaultMemoryManager(), options, std::move(rssClient));
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxRssSortShuffleWriter::create(
            numPartitions, std::move(partitionWriter), std::move(writerOptions), getDefaultMemoryManager()));
    return shuffleWriter;
  }

  std::shared_ptr<FlatVector<StringView>> makeNonSharedStringVector() const {
    auto strBuffer = AlignedBuffer::allocate<char>(200, pool());
    auto vector = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool());
    vector->setStringBuffers({std::move(strBuffer)});
    return vector;
  }

  std::shared_ptr<FlatVector<StringView>> makeSharedStringVector() {
    auto vector = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool());
    vector->setStringBuffers({strBuffer_});
    return vector;
  }

  BufferPtr strBuffer_;

  static constexpr int64_t kMemLimit = std::numeric_limits<int64_t>::max();
};

TEST_F(VeloxRssSortShuffleWriterTest, calculateBatchesSize) {
  auto shuffleWriter = std::dynamic_pointer_cast<VeloxRssSortShuffleWriter>(createShuffleWriter(10));

  auto rowVector1 = makeRowVector({makeSharedStringVector(), makeSharedStringVector()});
  auto rowVector2 = makeRowVector({makeSharedStringVector(), makeNonSharedStringVector()});
  auto cb1 = std::make_shared<VeloxColumnarBatch>(rowVector1);
  auto cb2 = std::make_shared<VeloxColumnarBatch>(rowVector2);

  ASSERT_NOT_OK(shuffleWriter->write(cb1, kMemLimit));
  ASSERT_NOT_OK(shuffleWriter->write(cb2, kMemLimit));
  auto expectedSize = rowVector1->retainedSize() + rowVector2->retainedSize() - strBuffer_->capacity() * 2;
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
}

TEST_F(VeloxRssSortShuffleWriterTest, sharedStringInArray) {
  auto shuffleWriter = std::dynamic_pointer_cast<VeloxRssSortShuffleWriter>(createShuffleWriter(10));

  // Shared string buffer in FlatVector<StringView>.
  auto vector = makeSharedStringVector();

  // Shared string buffer in ArrayVector.
  BufferPtr offsets = allocateOffsets(1, vector->pool());
  BufferPtr sizes = allocateOffsets(1, vector->pool());
  sizes->asMutable<vector_size_t>()[0] = vector->size();

  auto arrayVector =
      std::make_shared<facebook::velox::ArrayVector>(pool(), ARRAY(VARCHAR()), nullptr, 1, offsets, sizes, vector);
  auto rowVector = makeRowVector({arrayVector, vector});
  auto cb1 = std::make_shared<VeloxColumnarBatch>(rowVector);
  ASSERT_NOT_OK(shuffleWriter->write(cb1, kMemLimit));
  auto expectedSize = rowVector->retainedSize() - strBuffer_->capacity();
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
}

TEST_F(VeloxRssSortShuffleWriterTest, sharedStringInMap) {
  auto shuffleWriter = std::dynamic_pointer_cast<VeloxRssSortShuffleWriter>(createShuffleWriter(10));

  // Shared string buffer in MapVector.
  auto keys = makeSharedStringVector();
  auto values = makeSharedStringVector();
  auto mapVector = makeMapVector({0, 10, 20, 50}, keys, values);
  auto rowVector = makeRowVector({mapVector, makeSharedStringVector()});
  auto cb = std::make_shared<VeloxColumnarBatch>(rowVector);
  ASSERT_NOT_OK(shuffleWriter->write(cb, kMemLimit));
  auto expectedSize = rowVector->retainedSize() - strBuffer_->capacity() * 2;
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
}

TEST_F(VeloxRssSortShuffleWriterTest, sharedStringInRowVector) {
  auto shuffleWriter = std::dynamic_pointer_cast<VeloxRssSortShuffleWriter>(createShuffleWriter(10));

  // Shared string buffer in RowVector.
  auto rowVectorInner = makeRowVector({makeSharedStringVector(), makeSharedStringVector()});
  auto rowVectorOuter = makeRowVector({rowVectorInner, makeSharedStringVector()});
  auto cb = std::make_shared<VeloxColumnarBatch>(rowVectorOuter);
  ASSERT_NOT_OK(shuffleWriter->write(cb, kMemLimit));
  auto expectedSize = rowVectorOuter->retainedSize() - strBuffer_->capacity() * 2;
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
}

TEST_F(VeloxRssSortShuffleWriterTest, sharedStringInDictionary) {
  auto shuffleWriter = std::dynamic_pointer_cast<VeloxRssSortShuffleWriter>(createShuffleWriter(10));

  // Vector is not flatten.
  auto vector = makeSharedStringVector();
  auto dictionaryVector = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), makeIndices(vector->size(), [](vector_size_t row) { return row; }), vector->size(), vector);
  auto rowVector = makeRowVector({dictionaryVector});
  auto cb = std::make_shared<VeloxColumnarBatch>(rowVector);
  ASSERT_NOT_OK(shuffleWriter->write(cb, kMemLimit));
  EXPECT_EQ(rowVector->retainedSize(), shuffleWriter->getInputColumnBytes());
}

} // namespace gluten