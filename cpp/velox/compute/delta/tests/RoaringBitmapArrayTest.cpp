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

#include "compute/delta/RoaringBitmapArray.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include <roaring/roaring.hh>

namespace gluten::delta {
namespace {

void writeUint32LittleEndian(char* data, uint32_t value) {
  auto* bytes = reinterpret_cast<uint8_t*>(data);
  bytes[0] = static_cast<uint8_t>(value & 0xFF);
  bytes[1] = static_cast<uint8_t>((value >> 8) & 0xFF);
  bytes[2] = static_cast<uint8_t>((value >> 16) & 0xFF);
  bytes[3] = static_cast<uint8_t>((value >> 24) & 0xFF);
}

void writeUint64LittleEndian(char* data, uint64_t value) {
  auto* bytes = reinterpret_cast<uint8_t*>(data);
  bytes[0] = static_cast<uint8_t>(value & 0xFF);
  bytes[1] = static_cast<uint8_t>((value >> 8) & 0xFF);
  bytes[2] = static_cast<uint8_t>((value >> 16) & 0xFF);
  bytes[3] = static_cast<uint8_t>((value >> 24) & 0xFF);
  bytes[4] = static_cast<uint8_t>((value >> 32) & 0xFF);
  bytes[5] = static_cast<uint8_t>((value >> 40) & 0xFF);
  bytes[6] = static_cast<uint8_t>((value >> 48) & 0xFF);
  bytes[7] = static_cast<uint8_t>((value >> 56) & 0xFF);
}

TEST(RoaringBitmapArrayTest, SerializeRoundTrip) {
  RoaringBitmapArray bitmap;
  bitmap.addSafe(1);
  bitmap.addSafe(7);
  bitmap.addSafe(7);
  bitmap.addSafe(1ULL << 33);

  std::vector<char> serialized(bitmap.serializedSizeInBytes());
  bitmap.serialize(serialized.data());
  EXPECT_EQ(serialized.size(), bitmap.serializeToString().size());

  RoaringBitmapArray restored;
  restored.deserialize(serialized.data(), serialized.size());

  EXPECT_TRUE(restored.containsSafe(1));
  EXPECT_TRUE(restored.containsSafe(7));
  EXPECT_TRUE(restored.containsSafe(1ULL << 33));
  EXPECT_FALSE(restored.containsSafe(2));
  EXPECT_EQ(restored.cardinality(), 3);
}

TEST(RoaringBitmapArrayTest, EmptyPortableSerializationMatchesDeltaJvmHeader) {
  RoaringBitmapArray bitmap;
  const auto serialized = bitmap.serializeToString();

  const std::vector<uint8_t> expectedBytes{0xd1, 0xd3, 0x39, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  ASSERT_EQ(serialized.size(), expectedBytes.size());
  for (size_t i = 0; i < expectedBytes.size(); ++i) {
    EXPECT_EQ(static_cast<uint8_t>(serialized[i]), expectedBytes[i]);
  }

  RoaringBitmapArray restored;
  restored.deserialize(serialized.data(), serialized.size());
  EXPECT_EQ(restored.cardinality(), 0);
  EXPECT_FALSE(restored.last().has_value());
}

TEST(RoaringBitmapArrayTest, DeserializesDeltaJvmPortablePayloadWithSparseGap) {
  // Delta 3.3.2 portable serialization for values 1, 7 and 1 << 33.
  // Delta JVM includes the empty intermediate high-key bucket in this sparse
  // case; native accepts it but writes the compact portable equivalent.
  const std::vector<uint8_t> deltaJvmBytes{
      0xd1, 0xd3, 0x39, 0x64, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3a, 0x30,
      0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x07, 0x00,
      0x01, 0x00, 0x00, 0x00, 0x3a, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x3a, 0x30,
      0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00};

  RoaringBitmapArray bitmap;
  bitmap.deserialize(reinterpret_cast<const char*>(deltaJvmBytes.data()), deltaJvmBytes.size());

  EXPECT_EQ(bitmap.cardinality(), 3);
  EXPECT_TRUE(bitmap.containsSafe(1));
  EXPECT_TRUE(bitmap.containsSafe(7));
  EXPECT_TRUE(bitmap.containsSafe(1ULL << 33));
  ASSERT_TRUE(bitmap.last().has_value());
  EXPECT_EQ(bitmap.last().value(), 1ULL << 33);
}

TEST(RoaringBitmapArrayTest, SerializesSparseGapAsCompactPortablePayload) {
  RoaringBitmapArray bitmap;
  bitmap.addSafe(1);
  bitmap.addSafe(7);
  bitmap.addSafe(1ULL << 33);

  const auto serialized = bitmap.serializeToString();

  const std::vector<uint8_t> expectedBytes{0xd1, 0xd3, 0x39, 0x64, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                           0x00, 0x00, 0x00, 0x00, 0x3a, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                                           0x00, 0x00, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x07, 0x00,
                                           0x02, 0x00, 0x00, 0x00, 0x3a, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                                           0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00};

  ASSERT_EQ(serialized.size(), expectedBytes.size());
  for (size_t i = 0; i < expectedBytes.size(); ++i) {
    EXPECT_EQ(static_cast<uint8_t>(serialized[i]), expectedBytes[i]);
  }
}

TEST(RoaringBitmapArrayTest, MergeCardinalityAndLast) {
  RoaringBitmapArray left;
  left.addSafe(1);
  left.addSafe((1ULL << 32) + 5);

  RoaringBitmapArray right;
  right.addSafe(1);
  right.addSafe((2ULL << 32) + 3);

  left.merge(right);

  EXPECT_EQ(left.cardinality(), 3);
  ASSERT_TRUE(left.last().has_value());
  EXPECT_EQ(left.last().value(), (2ULL << 32) + 3);
  EXPECT_TRUE(left.containsSafe(1));
  EXPECT_TRUE(left.containsSafe((1ULL << 32) + 5));
  EXPECT_TRUE(left.containsSafe((2ULL << 32) + 3));
}

TEST(RoaringBitmapArrayTest, EnforcesDeltaRowIndexBounds) {
  EXPECT_EQ(RoaringBitmapArray::kMaxRepresentableValue, 9223372030412324864ULL);

  RoaringBitmapArray bitmap;
  bitmap.addSafe(RoaringBitmapArray::kMaxRepresentableValue);
  EXPECT_TRUE(bitmap.containsSafe(RoaringBitmapArray::kMaxRepresentableValue));

  VELOX_ASSERT_THROW(bitmap.addSafe(RoaringBitmapArray::kMaxRepresentableValue + 1), "exceeds max representable value");
  VELOX_ASSERT_THROW(
      bitmap.containsSafe(RoaringBitmapArray::kMaxRepresentableValue + 1), "exceeds max representable value");
}

TEST(RoaringBitmapArrayTest, RejectsPayloadAboveDeltaMaxRowIndex) {
  roaring::Roaring highBitmap;
  highBitmap.add(RoaringBitmapArray::kMaxLowKeyForMaxHighKey + 1);

  std::vector<char> payload(sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) + highBitmap.getSizeInBytes(true));
  auto* cursor = payload.data();
  writeUint32LittleEndian(cursor, RoaringBitmapArray::kPortableSerializationFormatMagicNumber);
  cursor += sizeof(uint32_t);
  writeUint64LittleEndian(cursor, 1);
  cursor += sizeof(uint64_t);
  writeUint32LittleEndian(cursor, RoaringBitmapArray::kMaxHighKey);
  cursor += sizeof(uint32_t);
  cursor += highBitmap.write(cursor, true);
  ASSERT_EQ(cursor, payload.data() + payload.size());

  RoaringBitmapArray bitmap;
  VELOX_ASSERT_THROW(
      bitmap.deserialize(payload.data(), payload.size()),
      "bitmap for max high key exceeds Delta's max representable value");
}

TEST(RoaringBitmapArrayTest, RejectsBadMagic) {
  RoaringBitmapArray bitmap;
  std::vector<char> invalid(sizeof(uint32_t) + sizeof(uint64_t), '\0');
  VELOX_ASSERT_THROW(bitmap.deserialize(invalid.data(), invalid.size()), "Unexpected RoaringBitmapArray magic number");
}

TEST(RoaringBitmapArrayTest, RejectsTrailingBytes) {
  RoaringBitmapArray empty;
  auto serialized = empty.serializeToString();
  serialized.push_back('\0');

  RoaringBitmapArray bitmap;
  VELOX_ASSERT_THROW(
      bitmap.deserialize(serialized.data(), serialized.size()), "RoaringBitmapArray payload has 1 trailing bytes");
}

} // namespace
} // namespace gluten::delta
