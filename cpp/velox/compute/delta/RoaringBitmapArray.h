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

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <roaring/roaring.hh>
#include <string>

namespace gluten::delta {

/// Minimal 64-bit roaring bitmap wrapper for Delta deletion-vector payloads.
/// Delta's JVM implementation stores row indexes as an array of 32-bit roaring bitmaps keyed by
/// the high 32 bits. Keeping the same shape avoids Roaring64Map overhead for the common case where
/// per-file row indexes fit in a single 32-bit bitmap.
class RoaringBitmapArray {
 public:
  static constexpr uint32_t kPortableSerializationFormatMagicNumber = 1681511377;
  // Matches Delta JVM RoaringBitmapArray.MAX_REPRESENTABLE_VALUE.
  static constexpr uint32_t kMaxHighKey = 0x7ffffffe;
  static constexpr uint32_t kMaxLowKeyForMaxHighKey = 0x80000000;
  static constexpr uint64_t kMaxRepresentableValue =
      (static_cast<uint64_t>(kMaxHighKey) << 32) | kMaxLowKeyForMaxHighKey;

  void addSafe(uint64_t value);
  bool containsSafe(uint64_t value) const;
  void merge(const RoaringBitmapArray& other);

  uint64_t cardinality() const;
  std::optional<uint64_t> last() const;

  void serialize(char* buffer) const;
  std::string serializeToString(bool optimize = false) const;
  void deserialize(const char* buffer, size_t size);

  size_t serializedSizeInBytes() const;

 private:
  using BitmapMap = std::map<uint32_t, roaring::Roaring>;

  static void serializeBitmap(const BitmapMap& bitmaps, char* buffer);
  static size_t serializedSizeInBytes(const BitmapMap& bitmaps);

  BitmapMap bitmaps_;
};

} // namespace gluten::delta
