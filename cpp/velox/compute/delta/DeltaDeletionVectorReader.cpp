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
#include <limits>

#include "compute/delta/DeltaUuidUtils.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Crc.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

namespace gluten::delta {

namespace {

constexpr uint64_t kDeltaStoredPayloadLengthBytes = 4;
constexpr uint64_t kDeltaStoredChecksumBytes = 4;
constexpr uint64_t kDeltaBitmapArrayMagicBytes = 4;
constexpr uint64_t kDeltaPortableBitmapArrayLengthBytes = 8;
constexpr uint64_t kDeltaNativeBitmapArrayLengthBytes = 4;
constexpr uint32_t kDeltaPortableBitmapArrayMagicNumber = 1681511377;
constexpr uint32_t kDeltaNativeBitmapArrayMagicNumber = 1681511376;

uint32_t readUint32BigEndian(const char* data) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  return (static_cast<uint32_t>(bytes[0]) << 24) | (static_cast<uint32_t>(bytes[1]) << 16) |
      (static_cast<uint32_t>(bytes[2]) << 8) | static_cast<uint32_t>(bytes[3]);
}

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

std::string_view
extractStoredPayload(std::string_view serializedRange, uint32_t expectedPayloadSize, const std::string& dvPath) {
  VELOX_USER_CHECK_GE(
      serializedRange.size(),
      kDeltaStoredPayloadLengthBytes + kDeltaStoredChecksumBytes,
      "Deletion vector range is too small for Delta stored format: {}",
      dvPath);

  const auto storedPayloadSize = readUint32BigEndian(serializedRange.data());
  VELOX_USER_CHECK_EQ(
      storedPayloadSize,
      expectedPayloadSize,
      "Deletion vector payload size mismatch for {}: expected {}, got {}",
      dvPath,
      expectedPayloadSize,
      storedPayloadSize);

  const auto expectedRangeSize =
      kDeltaStoredPayloadLengthBytes + static_cast<uint64_t>(expectedPayloadSize) + kDeltaStoredChecksumBytes;
  VELOX_USER_CHECK_EQ(
      serializedRange.size(),
      expectedRangeSize,
      "Deletion vector range size mismatch for {}: expected {}, got {}",
      dvPath,
      expectedRangeSize,
      serializedRange.size());

  const auto payload = serializedRange.substr(kDeltaStoredPayloadLengthBytes, expectedPayloadSize);
  const auto storedChecksum = readUint32BigEndian(payload.data() + payload.size());

  bits::Crc32 crc;
  crc.process_bytes(payload.data(), payload.size());
  VELOX_USER_CHECK_EQ(
      crc.checksum(),
      storedChecksum,
      "Deletion vector checksum mismatch for {}: expected {}, got {}",
      dvPath,
      storedChecksum,
      crc.checksum());

  return payload;
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

  VELOX_USER_FAIL("Unexpected magic number {} for {}", magic, dvPath);
}

} // namespace

DeltaDeletionVectorReader::DeltaDeletionVectorReader(
    std::shared_ptr<filesystems::FileSystem> fileSystem,
    memory::MemoryPool* pool,
    std::shared_ptr<io::IoStatistics> ioStats)
    : fileSystem_(std::move(fileSystem)), pool_(pool), ioStats_(std::move(ioStats)) {}

void DeltaDeletionVectorReader::loadDeletionVector(
    const std::string& dvPath,
    std::optional<uint64_t> offset,
    std::optional<uint64_t> sizeInBytes,
    std::optional<uint64_t> expectedCardinality) {
  VELOX_CHECK_NOT_NULL(fileSystem_, "File system is required to load DV file");

  try {
    auto readFile = fileSystem_->openFileForRead(dvPath);
    auto fileSize = readFile->size();

    VELOX_USER_CHECK_GT(fileSize, 0, "Deletion vector file is empty: {}", dvPath);

    const auto startOffset = offset.value_or(0);
    const auto readsStoredRange = offset.has_value() || sizeInBytes.has_value();
    VELOX_USER_CHECK_LE(
        startOffset, fileSize, "Deletion vector offset {} exceeds file size {} for {}", startOffset, fileSize, dvPath);

    auto payloadSize = sizeInBytes;
    if (readsStoredRange && !payloadSize.has_value()) {
      VELOX_USER_CHECK_LE(
          startOffset + kDeltaStoredPayloadLengthBytes,
          fileSize,
          "Deletion vector payload size prefix exceeds file size {} for {}",
          fileSize,
          dvPath);
      const auto payloadSizeBuffer = readFile->pread(startOffset, kDeltaStoredPayloadLengthBytes);
      VELOX_CHECK_EQ(
          payloadSizeBuffer.size(),
          kDeltaStoredPayloadLengthBytes,
          "Failed to read deletion vector payload size from {}",
          dvPath);
      payloadSize = readUint32BigEndian(payloadSizeBuffer.data());
    }

    const auto bytesToRead = readsStoredRange
        ? kDeltaStoredPayloadLengthBytes + payloadSize.value() + kDeltaStoredChecksumBytes
        : fileSize - startOffset;
    VELOX_USER_CHECK_LE(
        startOffset + bytesToRead,
        fileSize,
        "Deletion vector range [{}..{}) exceeds file size {} for {}",
        startOffset,
        startOffset + bytesToRead,
        fileSize,
        dvPath);
    VELOX_USER_CHECK_GT(bytesToRead, 0, "Deletion vector range is empty: {}", dvPath);

    auto buffer = AlignedBuffer::allocate<char>(bytesToRead, pool_);
    const auto bytesRead = readFile->pread(startOffset, bytesToRead);

    VELOX_CHECK_EQ(
        bytesRead.size(),
        bytesToRead,
        "Failed to read complete deletion vector file range from {} (expected {} bytes, got {})",
        dvPath,
        bytesToRead,
        bytesRead.size());

    std::memcpy(buffer->asMutable<char>(), bytesRead.data(), bytesRead.size());

    const auto serializedPayload = readsStoredRange
        ? extractStoredPayload(std::string_view(buffer->as<char>(), bytesRead.size()), payloadSize.value(), dvPath)
        : std::string_view(buffer->as<char>(), bytesRead.size());

    deletionBitmap_ = deserializeDeltaBitmapArray(serializedPayload, dvPath);

    // Validate cardinality if provided
    if (expectedCardinality.has_value()) {
      const auto actualCardinality = deletionBitmap_->cardinality();
      VELOX_USER_CHECK_EQ(
          actualCardinality,
          expectedCardinality.value(),
          "Deletion vector cardinality mismatch for {}: expected {}, got {}",
          dvPath,
          expectedCardinality.value(),
          actualCardinality);
    }

    if (ioStats_) {
      ioStats_->incRawBytesRead(bytesToRead);
      ioStats_->queryThreadIoLatencyUs().increment(1);
    }
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("Failed to load deletion vector from {}: {}", dvPath, e.what());
  }
}

void DeltaDeletionVectorReader::loadInlineDeletionVector(
    const std::string& inlineData,
    std::optional<uint64_t> sizeInBytes,
    std::optional<uint64_t> expectedCardinality) {
  try {
    const auto decodedSize = sizeInBytes.value_or((inlineData.size() / 5) * 4);
    std::string decoded = DeltaUuidUtils::decodeBase85ToBytes(inlineData, decodedSize);

    VELOX_USER_CHECK_GT(decoded.size(), 0, "Decoded inline deletion vector is empty");

    deletionBitmap_ = deserializeDeltaBitmapArray(decoded, "inline deletion vector");

    // Validate cardinality if provided
    if (expectedCardinality.has_value()) {
      const auto actualCardinality = deletionBitmap_->cardinality();
      VELOX_USER_CHECK_EQ(
          actualCardinality,
          expectedCardinality.value(),
          "Inline deletion vector cardinality mismatch: expected {}, got {}",
          expectedCardinality.value(),
          actualCardinality);
    }
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("Failed to load inline deletion vector: {}", e.what());
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

  bool hasDeletedRows = false;
  uint64_t highestDeletedIndex = 0;
  for (uint64_t i = 0; i < size; ++i) {
    const uint64_t absoluteRowPos = baseReadOffset + i;
    if (deletionBitmap_->contains(absoluteRowPos)) {
      bits::setBit(rawBitmap, i);
      hasDeletedRows = true;
      highestDeletedIndex = i;
    }
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
