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
#include <memory>
#include "velox/exec/HashTable.h"

namespace gluten {

/**
 * HashTableSerializer provides serialization and deserialization for Velox hash tables.
 * This is a thin wrapper around the HashTable's native serialize/deserialize methods
 * from IBM Velox's verified implementation.
 */
class HashTableSerializer {
 public:
  /**
   * Returns the exact serialized size of a hash table.
   *
   * @param hashTable The hash table to serialize
   * @return Exact size in bytes
   */
  template <bool ignoreNullKeys>
  static size_t serializedSize(const facebook::velox::exec::HashTable<ignoreNullKeys>* hashTable);

  /**
   * Serialize a hash table directly into a caller-provided buffer.
   *
   * @param hashTable The hash table to serialize
   * @param data Destination buffer
   * @param size Exact destination size in bytes
   */
  template <bool ignoreNullKeys>
  static void
  serializeTo(const facebook::velox::exec::HashTable<ignoreNullKeys>* hashTable, uint8_t* data, size_t size);

  /**
   * Deserialize a hash table from a memory buffer.
   * Directly uses HashTable's deserialize() method from IBM Velox.
   *
   * @param data Pointer to serialized data
   * @param size Size of serialized data
   * @param pool Memory pool for allocations
   * @return Deserialized hash table
   */
  template <bool ignoreNullKeys>
  static std::unique_ptr<facebook::velox::exec::HashTable<ignoreNullKeys>>
  deserialize(const uint8_t* data, size_t size, facebook::velox::memory::MemoryPool* pool);
};

} // namespace gluten
