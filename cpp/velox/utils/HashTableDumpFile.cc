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

#include "utils/HashTableDumpFile.h"

#include <fmt/format.h>
#include <cctype>
#include <cstring>

#include "utils/Exception.h"

namespace gluten {
namespace {

// Bumped whenever the layout below, or the HashTableSerializer payload it wraps, changes shape.
// A dump is only ever read back by a build of the same Gluten tree, so this exists to turn a stale
// file into a clear error rather than a crash deep inside the deserializer.
constexpr char kMagic[8] = {'G', 'L', 'T', 'N', 'H', 'T', '0', '1'};

void appendPod(std::string& out, const void* data, size_t size) {
  out.append(static_cast<const char*>(data), size);
}

template <typename T>
void appendInt(std::string& out, T value) {
  appendPod(out, &value, sizeof(T));
}

template <typename T>
T readInt(const std::string& content, size_t& offset) {
  GLUTEN_CHECK(offset + sizeof(T) <= content.size(), "Truncated hash table dump");
  T value;
  std::memcpy(&value, content.data() + offset, sizeof(T));
  offset += sizeof(T);
  return value;
}

} // namespace

std::string encodeHashTableDump(const HashTableDump& dump) {
  GLUTEN_CHECK(!dump.cacheKey.empty(), "Cannot dump a hash table without a cache key");

  std::string out;
  out.reserve(sizeof(kMagic) + 32 + dump.cacheKey.size() + dump.payload.size());

  appendPod(out, kMagic, sizeof(kMagic));
  appendInt<uint8_t>(out, dump.ignoreNullKeys ? 1 : 0);
  appendInt<uint8_t>(out, dump.joinHasNullKeys ? 1 : 0);
  appendInt<uint32_t>(out, static_cast<uint32_t>(dump.cacheKey.size()));
  appendPod(out, dump.cacheKey.data(), dump.cacheKey.size());
  appendInt<uint64_t>(out, static_cast<uint64_t>(dump.payload.size()));
  appendPod(out, dump.payload.data(), dump.payload.size());

  return out;
}

HashTableDump decodeHashTableDump(const std::string& content) {
  size_t offset = 0;
  GLUTEN_CHECK(
      content.size() >= sizeof(kMagic) && std::memcmp(content.data(), kMagic, sizeof(kMagic)) == 0,
      "Not a Gluten hash table dump, or written by an incompatible version. Re-dump the stage with "
      "the same build of Gluten that runs the benchmark.");
  offset += sizeof(kMagic);

  HashTableDump dump;
  dump.ignoreNullKeys = readInt<uint8_t>(content, offset) != 0;
  dump.joinHasNullKeys = readInt<uint8_t>(content, offset) != 0;

  const auto keySize = readInt<uint32_t>(content, offset);
  GLUTEN_CHECK(offset + keySize <= content.size(), "Truncated hash table dump");
  dump.cacheKey.assign(content.data() + offset, keySize);
  offset += keySize;

  const auto payloadSize = readInt<uint64_t>(content, offset);
  GLUTEN_CHECK(offset + payloadSize == content.size(), "Truncated or over-long hash table dump");
  dump.payload.resize(payloadSize);
  if (payloadSize > 0) {
    std::memcpy(dump.payload.data(), content.data() + offset, payloadSize);
  }

  return dump;
}

std::string hashTableDumpFileName(int32_t stageId, int32_t partitionId, int32_t vId, const std::string& cacheKey) {
  std::string sanitized;
  sanitized.reserve(cacheKey.size());
  for (const char c : cacheKey) {
    sanitized.push_back((std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_') ? c : '_');
  }
  return fmt::format("hashtable_{}_{}_{}_{}.bin", stageId, partitionId, vId, sanitized);
}

} // namespace gluten
