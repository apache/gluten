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

#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <vector>
#include "shuffle/Utils.h"

namespace gluten {
namespace {
class CompressedInputStreamTest : public ::testing::Test {
 protected:
  std::unique_ptr<arrow::util::Codec> codec_ = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME).ValueOrDie();

  std::string compress(const std::string& input) {
    std::string compressed(
        codec_->MaxCompressedLen(input.size(), reinterpret_cast<const uint8_t*>(input.data())), '\0');
    auto size = codec_
                    ->Compress(
                        input.size(),
                        reinterpret_cast<const uint8_t*>(input.data()),
                        compressed.size(),
                        reinterpret_cast<uint8_t*>(compressed.data()))
                    .ValueOrDie();
    compressed.resize(size);
    return compressed;
  }

  std::shared_ptr<CompressedInputStream> stream(const std::string& compressed) {
    return CompressedInputStream::Make(
               codec_.get(),
               std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(compressed)),
               arrow::default_memory_pool())
        .ValueOrDie();
  }

  std::string randomBytes(size_t size) {
    std::mt19937 random(1781);
    std::string data(size, '\0');
    for (auto& value : data) {
      value = static_cast<char>(random());
    }
    return data;
  }
};

TEST_F(CompressedInputStreamTest, CachedReadsDoNotIncreaseDecompressionTime) {
  auto input = stream(compress(std::string(2 * 1024 * 1024, 'x')));
  char value;
  EXPECT_EQ(input->Read(0, &value).ValueOrDie(), 0);
  EXPECT_EQ(input->decompressTime(), 0);
  ASSERT_EQ(input->Read(1, &value).ValueOrDie(), 1);
  EXPECT_EQ(value, 'x');
  const auto refillTime = input->decompressTime();
  EXPECT_GE(refillTime, 0);
  for (int i = 0; i < 4096; ++i) {
    ASSERT_EQ(input->Read(1, &value).ValueOrDie(), 1);
    EXPECT_EQ(value, 'x');
  }
  auto buffer = input->Read(4096).ValueOrDie();
  EXPECT_EQ(buffer->ToString(), std::string(4096, 'x'));
  EXPECT_EQ(input->Tell().ValueOrDie(), 8193);
  EXPECT_EQ(input->decompressTime(), refillTime);
}

TEST_F(CompressedInputStreamTest, VariedReadSizesCrossCompressedAndDecompressedChunks) {
  const auto expected = randomBytes(3 * 1024 * 1024 + 17);
  auto input = stream(compress(expected));
  const std::vector<int64_t> sizes = {4, 31, 1024, 65539, 7, 3};
  std::string actual;
  for (size_t iteration = 0;; ++iteration) {
    auto buffer = input->Read(sizes[iteration % sizes.size()]).ValueOrDie();
    if (buffer->size() == 0) {
      break;
    }
    actual.append(reinterpret_cast<const char*>(buffer->data()), buffer->size());
  }
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(input->Tell().ValueOrDie(), expected.size());
  EXPECT_GE(input->decompressTime(), 0);
}

TEST_F(CompressedInputStreamTest, ConcatenatedFramesAndRepeatedEof) {
  const auto first = randomBytes(131071);
  const auto second = randomBytes(65537);
  auto input = stream(compress(first) + compress(second));
  auto actual = input->Read(first.size() + second.size() + 1).ValueOrDie();
  EXPECT_EQ(actual->ToString(), first + second);
  char value;
  EXPECT_EQ(input->Read(1, &value).ValueOrDie(), 0);
  EXPECT_EQ(input->Read(1, &value).ValueOrDie(), 0);
  EXPECT_EQ(input->Tell().ValueOrDie(), first.size() + second.size());
  EXPECT_GE(input->decompressTime(), 0);
}

TEST_F(CompressedInputStreamTest, EmptyFrameAndZeroLengthRead) {
  auto input = stream(compress(""));
  char value;
  EXPECT_EQ(input->Read(0, &value).ValueOrDie(), 0);
  EXPECT_EQ(input->decompressTime(), 0);
  EXPECT_EQ(input->Read(1, &value).ValueOrDie(), 0);
  EXPECT_EQ(input->Tell().ValueOrDie(), 0);
  EXPECT_GE(input->decompressTime(), 0);
}

TEST_F(CompressedInputStreamTest, TruncatedFrameReportsError) {
  const auto expected = randomBytes(200000);
  auto compressed = compress(expected);
  compressed.resize(compressed.size() - 3);
  auto input = stream(compressed);
  auto actual = input->Read(expected.size() + 1);
  ASSERT_FALSE(actual.ok());
  EXPECT_TRUE(actual.status().IsIOError()) << actual.status();
  EXPECT_GE(input->decompressTime(), 0);
}
} // namespace
} // namespace gluten
