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

#include <jni.h>
#include "memory/ColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "operators/hashjoin/HashTableBuilder.h"
#include "utils/ObjectStore.h"
#include "velox/exec/HashTable.h"

namespace gluten {

// Wrapper class to encapsulate JNI-related static objects for hash table operations.
// This avoids exposing global variables in the gluten namespace.
class JniHashTableContext {
 public:
  static JniHashTableContext& getInstance() {
    static JniHashTableContext instance;
    return instance;
  }

  // Delete copy and move constructors/operators
  JniHashTableContext(const JniHashTableContext&) = delete;
  JniHashTableContext& operator=(const JniHashTableContext&) = delete;
  JniHashTableContext(JniHashTableContext&&) = delete;
  JniHashTableContext& operator=(JniHashTableContext&&) = delete;

  void initialize(JNIEnv* env, JavaVM* javaVm);
  void finalize(JNIEnv* env);

  JavaVM* getJavaVM() const {
    return vm_;
  }

  ObjectStore* getHashTableObjStore() const {
    return hashTableObjStore_.get();
  }

  jlong callJavaGet(const std::string& id) const;

 private:
  JniHashTableContext() : hashTableObjStore_(ObjectStore::create()) {}

  ~JniHashTableContext() {
    // Note: The destructor is called at program exit (after main() returns).
    // By this time, JNI_OnUnload should have already been called, which invokes
    // finalize() to clean up JNI global references while the JVM is still valid.
    // The singleton itself (including hashTableObjStore_) will be destroyed here.
  }

  JavaVM* vm_{nullptr};
  std::unique_ptr<ObjectStore> hashTableObjStore_;
  jclass jniVeloxBroadcastBuildSideCache_{nullptr};
  jmethodID jniGet_{nullptr};
};

// Return the hash table builder address.
std::shared_ptr<HashTableBuilder> nativeHashTableBuild(
    const std::vector<std::string>& joinKeys,
    const std::vector<std::string>& filterBuildColumns,
    bool filterPropagatesNulls,
    std::vector<std::string> names,
    std::vector<facebook::velox::TypePtr> veloxTypeList,
    int joinType,
    bool hasMixedJoinCondition,
    bool isExistenceJoin,
    bool isNullAwareAntiJoin,
    int64_t bloomFilterPushdownSize,
    uint32_t minTableRowsForParallelJoinBuild,
    uint32_t joinBuildVectorHasherMaxNumDistinct,
    uint32_t abandonHashBuildDedupMinRows,
    uint32_t abandonHashBuildDedupMinPct,
    std::vector<std::shared_ptr<ColumnarBatch>>& batches,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool);

long getJoin(const std::string& hashTableId);

// Initialize the JNI hash table context
inline void initVeloxJniHashTable(JNIEnv* env, JavaVM* javaVm) {
  JniHashTableContext::getInstance().initialize(env, javaVm);
}

// Finalize the JNI hash table context
inline void finalizeVeloxJniHashTable(JNIEnv* env) {
  JniHashTableContext::getInstance().finalize(env);
}

// Get hash table object store
inline ObjectStore* getHashTableObjStore() {
  return JniHashTableContext::getInstance().getHashTableObjStore();
}

} // namespace gluten
