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

void writeUint32LittleEndian(char* data, uint32_t value) {
  auto* bytes = reinterpret_cast<uint8_t*>(data);
  bytes[0] = static_cast<uint8_t>(value & 0xFF);
  bytes[1] = static_cast<uint8_t>((value >> 8) & 0xFF);
  bytes[2] = static_cast<uint8_t>((value >> 16) & 0xFF);
  bytes[3] = static_cast<uint8_t>((value >> 24) & 0xFF);
}

} // namespace

void RoaringBitmapArray::addSafe(uint64_t value) {
  bitmap_.add(value);
}

bool RoaringBitmapArray::containsSafe(uint64_t value) const {
  return bitmap_.contains(value);
}

void RoaringBitmapArray::serialize(char* buffer) const {
  VELOX_CHECK_NOT_NULL(buffer, "RoaringBitmapArray serialization buffer is null");
  writeUint32LittleEndian(buffer, kPortableSerializationFormatMagicNumber);
  bitmap_.write(buffer + sizeof(uint32_t), true);
}

void RoaringBitmapArray::deserialize(const char* buffer, size_t size) {
  VELOX_CHECK_NOT_NULL(buffer, "RoaringBitmapArray input buffer is null");
  VELOX_CHECK_GE(size, sizeof(uint32_t), "RoaringBitmapArray payload is too small: {}", size);
  const auto magic = readUint32LittleEndian(buffer);
  VELOX_CHECK_EQ(
      magic, kPortableSerializationFormatMagicNumber, "Unexpected RoaringBitmapArray magic number {}", magic);
  bitmap_ = roaring::Roaring64Map::readSafe(buffer + sizeof(uint32_t), size - sizeof(uint32_t));
}

size_t RoaringBitmapArray::serializedSizeInBytes() const {
  return sizeof(uint32_t) + bitmap_.getSizeInBytes(true);
}

} // namespace gluten::delta
