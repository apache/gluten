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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace gluten {

/// A broadcast hash join's pre-built hash table, dumped alongside the substrait plan so that the
/// standalone micro benchmark can replay the join against the real table instead of rebuilding it
/// from a build side that was never streamed.
///
/// The payload is whatever HashTableSerializer produced. The two flags travel with it because
/// deserializing needs them and they cannot be recovered from the payload: `ignoreNullKeys`
/// selects between HashTable<true> and HashTable<false>, and `joinHasNullKeys` is build side state
/// that the join semantics depend on.
struct HashTableDump {
  std::string cacheKey;
  bool ignoreNullKeys{false};
  bool joinHasNullKeys{false};
  std::vector<uint8_t> payload;
};

/// Serializes `dump` to the self-describing on-disk form. Kept in one place so the dumping and
/// replaying sides cannot drift apart.
std::string encodeHashTableDump(const HashTableDump& dump);

/// Inverse of encodeHashTableDump. Throws GlutenException if the content is not a hash table dump
/// or was written by an incompatible version.
HashTableDump decodeHashTableDump(const std::string& content);

/// Name of the file a dump for `cacheKey` is written to. The cache key is a Spark plan id, so it
/// is filename safe, but it is sanitized anyway to keep a malformed plan from escaping the dump
/// directory.
std::string hashTableDumpFileName(int32_t stageId, int32_t partitionId, int32_t vId, const std::string& cacheKey);

} // namespace gluten
