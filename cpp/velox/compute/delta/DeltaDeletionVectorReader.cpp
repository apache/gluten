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

#include "compute/delta/DeltaDeletionVectorReader.h"

#include <cstring>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

namespace gluten::delta {

namespace {

constexpr uint64_t kDeltaBitmapArrayMagicBytes = 4;
constexpr uint64_t kDeltaNativeBitmapArrayLengthBytes = 4;
constexpr uint64_t kDeltaStoredPayloadLengthBytes = 4;
constexpr uint32_t kDeltaPortableBitmapArrayMagicNumber = 1681511377;
constexpr uint32_t kDeltaNativeBitmapArrayMagicNumber = 1681511376;

uint32_t readUint32LittleEndian(const char* data) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  return static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
      (static_cast<uint32_t>(bytes[2]) << 16) | (static_cast<uint32_t>(bytes[3]) << 24);
}

roaring::Roaring64Map deserializeDeltaBitmapArray(std::string_view serializedPayload, const std::string& dvPath) {
  VELOX_USER_CHECK_GE(
      serializedPayload.size(),
      kDeltaBitmapArrayMagicBytes,
      "Deletion vector payload is too small for Delta bitmap array: {}",
      dvPath);

  const auto magic = readUint32LittleEndian(serializedPayload.data());
  if (magic == kDeltaPortableBitmapArrayMagicNumber) {
    const auto portablePayload = serializedPayload.substr(kDeltaBitmapArrayMagicBytes);
    return roaring::Roaring64Map::readSafe(portablePayload.data(), portablePayload.size());
  }

  if (magic == kDeltaNativeBitmapArrayMagicNumber) {
    VELOX_USER_CHECK_GE(
        serializedPayload.size(),
        kDeltaBitmapArrayMagicBytes + kDeltaNativeBitmapArrayLengthBytes,
        "Deletion vector payload is too small for Delta native bitmap array: {}",
        dvPath);

    const auto bitmapCount = readUint32LittleEndian(serializedPayload.data() + kDeltaBitmapArrayMagicBytes);
    size_t offset = kDeltaBitmapArrayMagicBytes + kDeltaNativeBitmapArrayLengthBytes;
    roaring::Roaring64Map result;

    for (uint64_t bitmapIndex = 0; bitmapIndex < bitmapCount; ++bitmapIndex) {
      VELOX_USER_CHECK_LE(
          offset + kDeltaStoredPayloadLengthBytes,
          serializedPayload.size(),
          "Deletion vector payload ended before bitmap {} size for {}",
          bitmapIndex,
          dvPath);

      const auto bitmapSize = readUint32LittleEndian(serializedPayload.data() + offset);
      offset += kDeltaStoredPayloadLengthBytes;

      VELOX_USER_CHECK_LE(
          offset + bitmapSize,
          serializedPayload.size(),
          "Deletion vector bitmap {} range exceeds payload size for {}",
          bitmapIndex,
          dvPath);

      auto bitmap = roaring::Roaring::readSafe(serializedPayload.data() + offset, bitmapSize);
      VELOX_USER_CHECK_EQ(
          bitmap.getSizeInBytes(true),
          bitmapSize,
          "Deletion vector bitmap {} size mismatch for {}: expected {}, got {}",
          bitmapIndex,
          dvPath,
          bitmapSize,
          bitmap.getSizeInBytes(true));

      const uint64_t rowBase = bitmapIndex << 32;
      for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
        result.add(rowBase | static_cast<uint64_t>(*it));
      }
      offset += bitmapSize;
    }

    VELOX_USER_CHECK_EQ(
        offset,
        serializedPayload.size(),
        "Deletion vector payload has {} unexpected trailing bytes for {}",
        serializedPayload.size() - offset,
        dvPath);
    return result;
  }

  VELOX_USER_FAIL("Unexpected Delta bitmap array magic number {} for {}", magic, dvPath);
}

} // namespace

void DeltaDeletionVectorReader::loadSerializedDeletionVectorInternal(
    std::string_view serializedPayload,
    const std::string& debugName,
    std::optional<uint64_t> expectedCardinality) {
  VELOX_USER_CHECK_GT(serializedPayload.size(), 0, "Serialized deletion vector is empty: {}", debugName);

  deletionBitmap_ = deserializeDeltaBitmapArray(serializedPayload, debugName);

  if (expectedCardinality.has_value()) {
    const auto actualCardinality = deletionBitmap_->cardinality();
    VELOX_USER_CHECK_EQ(
        actualCardinality,
        expectedCardinality.value(),
        "Deletion vector cardinality mismatch for {}: expected {}, got {}",
        debugName,
        expectedCardinality.value(),
        actualCardinality);
  }
}

void DeltaDeletionVectorReader::loadSerializedDeletionVector(
    std::string_view serializedPayload,
    std::optional<uint64_t> expectedCardinality) {
  try {
    loadSerializedDeletionVectorInternal(serializedPayload, "serialized deletion vector", expectedCardinality);
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("Failed to load serialized deletion vector: {}", e.what());
  }
}

bool DeltaDeletionVectorReader::isRowDeleted(uint64_t rowPosition) {
  if (!deletionBitmap_.has_value()) {
    return false;
  }

  return deletionBitmap_->contains(rowPosition);
}

void DeltaDeletionVectorReader::applyDeletionFilter(uint64_t baseReadOffset, uint64_t size, BufferPtr deleteBitmap) {
  VELOX_CHECK_NOT_NULL(deleteBitmap, "Delete bitmap buffer is required");

  if (!deletionBitmap_.has_value()) {
    std::memset(deleteBitmap->asMutable<uint8_t>(), 0, bits::nbytes(size));
    deleteBitmap->setSize(0);
    return;
  }

  auto* rawBitmap = deleteBitmap->asMutable<uint64_t>();
  std::memset(rawBitmap, 0, bits::nbytes(size));

  // Use an iterator-based approach instead of per-row contains() lookups.
  // This is O(deletions_in_range) rather than O(batch_size), which is
  // significantly faster when deletions are sparse relative to batch size.
  // Guard against uint64_t overflow when baseReadOffset is near UINT64_MAX.
  const uint64_t rangeEnd = (size <= UINT64_MAX - baseReadOffset) ? baseReadOffset + size : UINT64_MAX;
  auto it = deletionBitmap_->begin();
  if (!it.move_equalorlarger(baseReadOffset)) {
    // No deleted rows at or after baseReadOffset — nothing to mark.
    deleteBitmap->setSize(0);
    return;
  }

  bool hasDeletedRows = false;
  uint64_t highestDeletedIndex = 0;
  while (it != deletionBitmap_->end()) {
    const uint64_t absoluteRowPos = *it;
    if (absoluteRowPos >= rangeEnd) {
      break;
    }
    const uint64_t relativeIndex = absoluteRowPos - baseReadOffset;
    bits::setBit(rawBitmap, relativeIndex);
    hasDeletedRows = true;
    highestDeletedIndex = relativeIndex;
    ++it;
  }

  deleteBitmap->setSize(hasDeletedRows ? bits::nbytes(highestDeletedIndex + 1) : 0);
}

uint64_t DeltaDeletionVectorReader::estimatedDeletedRowCount() const {
  if (!deletionBitmap_.has_value()) {
    return 0;
  }

  // Return actual cardinality instead of estimated size
  return deletionBitmap_->cardinality();
}

} // namespace gluten::delta
