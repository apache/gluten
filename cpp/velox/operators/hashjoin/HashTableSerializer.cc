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

#include "operators/hashjoin/HashTableSerializer.h"
#include "velox/common/base/Exceptions.h"

namespace gluten {

template <bool ignoreNullKeys>
size_t HashTableSerializer::serializedSize(const facebook::velox::exec::HashTable<ignoreNullKeys>* hashTable) {
  VELOX_CHECK_NOT_NULL(hashTable, "Hash table cannot be null");
  return hashTable->serializedSize();
}

template <bool ignoreNullKeys>
void HashTableSerializer::serializeTo(
    const facebook::velox::exec::HashTable<ignoreNullKeys>* hashTable,
    uint8_t* data,
    size_t size) {
  VELOX_CHECK_NOT_NULL(hashTable, "Hash table cannot be null");
  VELOX_CHECK_NOT_NULL(data, "Serialized buffer cannot be null");
  hashTable->serializeTo(data, size);
}

template <bool ignoreNullKeys>
std::unique_ptr<facebook::velox::exec::HashTable<ignoreNullKeys>>
HashTableSerializer::deserialize(const uint8_t* data, size_t size, facebook::velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(data, "Serialized data cannot be null");
  VELOX_CHECK_GT(size, 0, "Invalid serialized data size");
  VELOX_CHECK_NOT_NULL(pool, "Memory pool cannot be null");
  return facebook::velox::exec::HashTable<ignoreNullKeys>::deserializeFrom(data, size, pool);
}

template size_t HashTableSerializer::serializedSize<true>(const facebook::velox::exec::HashTable<true>*);

template size_t HashTableSerializer::serializedSize<false>(const facebook::velox::exec::HashTable<false>*);

template void HashTableSerializer::serializeTo<true>(const facebook::velox::exec::HashTable<true>*, uint8_t*, size_t);

template void HashTableSerializer::serializeTo<false>(const facebook::velox::exec::HashTable<false>*, uint8_t*, size_t);

template std::unique_ptr<facebook::velox::exec::HashTable<true>>
HashTableSerializer::deserialize<true>(const uint8_t*, size_t, facebook::velox::memory::MemoryPool*);

template std::unique_ptr<facebook::velox::exec::HashTable<false>>
HashTableSerializer::deserialize<false>(const uint8_t*, size_t, facebook::velox::memory::MemoryPool*);

} // namespace gluten
