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
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "compute/delta/RoaringBitmapArray.h"

#include <cstring>

#include "velox/common/base/Exceptions.h"

namespace gluten::delta {

namespace {

uint32_t readUint32LittleEndian(const char* data) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  return static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
      (static_cast<uint32_t>(bytes[2]) << 16) | (static_cast<uint32_t>(bytes[3]) << 24);
}

uint64_t readUint64LittleEndian(const char* data) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  return static_cast<uint64_t>(bytes[0]) | (static_cast<uint64_t>(bytes[1]) << 8) |
      (static_cast<uint64_t>(bytes[2]) << 16) | (static_cast<uint64_t>(bytes[3]) << 24) |
      (static_cast<uint64_t>(bytes[4]) << 32) | (static_cast<uint64_t>(bytes[5]) << 40) |
      (static_cast<uint64_t>(bytes[6]) << 48) | (static_cast<uint64_t>(bytes[7]) << 56);
}

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

uint32_t highBytes(uint64_t value) {
  return static_cast<uint32_t>(value >> 32);
}

uint32_t lowBytes(uint64_t value) {
  return static_cast<uint32_t>(value);
}

uint64_t composeFromHighLowBytes(uint32_t high, uint32_t low) {
  return (static_cast<uint64_t>(high) << 32) | static_cast<uint64_t>(low);
}

} // namespace

void RoaringBitmapArray::addSafe(uint64_t value) {
  VELOX_CHECK_LE(
      value,
      kMaxRepresentableValue,
      "Delta RoaringBitmapArray row index {} exceeds max representable value {}",
      value,
      kMaxRepresentableValue);
  bitmaps_[highBytes(value)].add(lowBytes(value));
}

bool RoaringBitmapArray::containsSafe(uint64_t value) const {
  VELOX_CHECK_LE(
      value,
      kMaxRepresentableValue,
      "Delta RoaringBitmapArray row index {} exceeds max representable value {}",
      value,
      kMaxRepresentableValue);
  auto it = bitmaps_.find(highBytes(value));
  if (it == bitmaps_.end()) {
    return false;
  }
  return it->second.contains(lowBytes(value));
}

void RoaringBitmapArray::merge(const RoaringBitmapArray& other) {
  for (const auto& [key, bitmap] : other.bitmaps_) {
    bitmaps_[key] |= bitmap;
  }
}

uint64_t RoaringBitmapArray::cardinality() const {
  uint64_t cardinality = 0;
  for (const auto& [_, bitmap] : bitmaps_) {
    cardinality += bitmap.cardinality();
  }
  return cardinality;
}

std::optional<uint64_t> RoaringBitmapArray::last() const {
  for (auto it = bitmaps_.rbegin(); it != bitmaps_.rend(); ++it) {
    if (!it->second.isEmpty()) {
      return composeFromHighLowBytes(it->first, it->second.maximum());
    }
  }
  return std::nullopt;
}

void RoaringBitmapArray::serializeBitmap(const BitmapMap& bitmaps, char* buffer) {
  VELOX_CHECK_NOT_NULL(buffer, "RoaringBitmapArray serialization buffer is null");
  uint64_t nonEmptyBitmapCount = 0;
  for (const auto& [_, bitmap] : bitmaps) {
    if (!bitmap.isEmpty()) {
      ++nonEmptyBitmapCount;
    }
  }

  writeUint32LittleEndian(buffer, kPortableSerializationFormatMagicNumber);
  buffer += sizeof(uint32_t);
  writeUint64LittleEndian(buffer, nonEmptyBitmapCount);
  buffer += sizeof(uint64_t);

  for (const auto& [key, bitmap] : bitmaps) {
    if (bitmap.isEmpty()) {
      continue;
    }
    writeUint32LittleEndian(buffer, key);
    buffer += sizeof(uint32_t);
    buffer += bitmap.write(buffer, true);
  }
}

void RoaringBitmapArray::serialize(char* buffer) const {
  serializeBitmap(bitmaps_, buffer);
}

std::string RoaringBitmapArray::serializeToString(bool optimize) const {
  if (!optimize) {
    std::string out(serializedSizeInBytes(bitmaps_), '\0');
    serializeBitmap(bitmaps_, out.data());
    return out;
  }

  auto bitmaps = bitmaps_;
  for (auto& [_, bitmap] : bitmaps) {
    bitmap.runOptimize();
  }

  std::string out(serializedSizeInBytes(bitmaps), '\0');
  serializeBitmap(bitmaps, out.data());
  return out;
}

void RoaringBitmapArray::deserialize(const char* buffer, size_t size) {
  VELOX_CHECK_NOT_NULL(buffer, "RoaringBitmapArray input buffer is null");
  VELOX_CHECK_GE(size, sizeof(uint32_t) + sizeof(uint64_t), "RoaringBitmapArray payload is too small: {}", size);
  const auto magic = readUint32LittleEndian(buffer);
  VELOX_CHECK_EQ(
      magic, kPortableSerializationFormatMagicNumber, "Unexpected RoaringBitmapArray magic number {}", magic);

  bitmaps_.clear();
  const char* cursor = buffer + sizeof(uint32_t);
  size_t remaining = size - sizeof(uint32_t);
  const auto bitmapCount = readUint64LittleEndian(cursor);
  cursor += sizeof(uint64_t);
  remaining -= sizeof(uint64_t);

  uint32_t previousKey = 0;
  bool hasPreviousKey = false;
  for (uint64_t i = 0; i < bitmapCount; ++i) {
    VELOX_CHECK_GE(remaining, sizeof(uint32_t), "RoaringBitmapArray payload ended before bitmap key");
    const auto key = readUint32LittleEndian(cursor);
    VELOX_CHECK_LE(key, kMaxHighKey, "RoaringBitmapArray bitmap key {} exceeds Delta's max high key", key);
    cursor += sizeof(uint32_t);
    remaining -= sizeof(uint32_t);

    if (hasPreviousKey) {
      VELOX_CHECK_GT(key, previousKey, "RoaringBitmapArray bitmap keys are not strictly ascending");
    }
    hasPreviousKey = true;
    previousKey = key;

    const auto bitmapSize = roaring::api::roaring_bitmap_portable_deserialize_size(cursor, remaining);
    VELOX_CHECK_GT(bitmapSize, 0, "Invalid serialized roaring bitmap in RoaringBitmapArray");
    VELOX_CHECK_LE(bitmapSize, remaining, "Serialized roaring bitmap exceeds remaining payload size");
    auto bitmap = roaring::Roaring::readSafe(cursor, remaining);
    VELOX_CHECK(
        key != kMaxHighKey || bitmap.isEmpty() || bitmap.maximum() <= kMaxLowKeyForMaxHighKey,
        "RoaringBitmapArray bitmap for max high key exceeds Delta's max representable value");
    bitmaps_.emplace(key, std::move(bitmap));
    cursor += bitmapSize;
    remaining -= bitmapSize;
  }

  VELOX_CHECK_EQ(remaining, 0, "RoaringBitmapArray payload has {} trailing bytes", remaining);
}

size_t RoaringBitmapArray::serializedSizeInBytes() const {
  return serializedSizeInBytes(bitmaps_);
}

size_t RoaringBitmapArray::serializedSizeInBytes(const BitmapMap& bitmaps) {
  size_t size = sizeof(uint32_t) + sizeof(uint64_t);
  for (const auto& [_, bitmap] : bitmaps) {
    if (bitmap.isEmpty()) {
      continue;
    }
    size += sizeof(uint32_t) + bitmap.getSizeInBytes(true);
  }
  return size;
}

} // namespace gluten::delta
