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

#include <arrow/c/abi.h>

#include <jni/JniCommon.h>
#include <algorithm>
#include "JniHashTable.h"
#include "folly/String.h"
#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/hashjoin/HashTableSerializer.h"
#include "operators/hashjoin/HashTableTaskBuilder.h"
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

namespace gluten {

void JniHashTableContext::initialize(JNIEnv* env, JavaVM* javaVm) {
  vm_ = javaVm;
  const char* classSig = "Lorg/apache/gluten/execution/VeloxBroadcastBuildSideCache;";
  jniVeloxBroadcastBuildSideCache_ = createGlobalClassReferenceOrError(env, classSig);
  jniGet_ = getStaticMethodId(env, jniVeloxBroadcastBuildSideCache_, "get", "(Ljava/lang/String;)J");
}

void JniHashTableContext::finalize(JNIEnv* env) {
  if (jniVeloxBroadcastBuildSideCache_ != nullptr) {
    env->DeleteGlobalRef(jniVeloxBroadcastBuildSideCache_);
    jniVeloxBroadcastBuildSideCache_ = nullptr;
  }
}

std::optional<jlong> JniHashTableContext::callJavaGet(const std::string& id) const {
  if (vm_ == nullptr) {
    return std::nullopt;
  }

  JNIEnv* env;
  if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    throw gluten::GlutenException("JNIEnv was not attached to current thread");
  }

  const jstring s = env->NewStringUTF(id.c_str());
  checkException(env);
  auto result = env->CallStaticLongMethod(jniVeloxBroadcastBuildSideCache_, jniGet_, s);
  env->DeleteLocalRef(s);
  checkException(env);
  return result;
}

std::shared_ptr<BroadcastHashTable> nativeHashTableBuild(
    const std::string& hashTableId,
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
    uint32_t numThreads,
    folly::Executor* executor,
    std::shared_ptr<facebook::velox::memory::MemoryPool> queryPool,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool) {
  auto rowType = std::make_shared<facebook::velox::RowType>(std::move(names), std::move(veloxTypeList));

  auto sJoin = static_cast<substrait::JoinRel_JoinType>(joinType);
  facebook::velox::core::JoinType vJoin;
  switch (sJoin) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      vJoin = facebook::velox::core::JoinType::kInner;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      vJoin = facebook::velox::core::JoinType::kFull;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      vJoin = facebook::velox::core::JoinType::kLeft;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      vJoin = facebook::velox::core::JoinType::kRight;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
      // Determine the semi join type based on extracted information.
      if (isExistenceJoin) {
        vJoin = facebook::velox::core::JoinType::kLeftSemiProject;
      } else {
        vJoin = facebook::velox::core::JoinType::kLeftSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
      // Determine the semi join type based on extracted information.
      if (isExistenceJoin) {
        vJoin = facebook::velox::core::JoinType::kRightSemiProject;
      } else {
        vJoin = facebook::velox::core::JoinType::kRightSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI: {
      // Determine the anti join type based on extracted information.
      vJoin = facebook::velox::core::JoinType::kAnti;
      break;
    }
    default:
      VELOX_NYI("Unsupported Join type: {}", std::to_string(sJoin));
  }

  using facebook::velox::core::QueryConfig;
  HashTableTaskBuildSpec spec;
  spec.joinType = vJoin;
  spec.nullAware = isNullAwareAntiJoin;
  spec.withFilter = hasMixedJoinCondition;
  for (const auto& name : filterBuildColumns) {
    if (rowType->containsChild(name) &&
        std::find(spec.filterBuildColumns.begin(), spec.filterBuildColumns.end(), name) ==
            spec.filterBuildColumns.end()) {
      spec.filterBuildColumns.push_back(name);
    }
  }
  spec.filterPropagatesNulls = filterPropagatesNulls;
  spec.buildType = rowType;
  spec.joinKeys = joinKeys;
  spec.queryConfigs = {
      {QueryConfig::kMinTableRowsForParallelJoinBuild, std::to_string(minTableRowsForParallelJoinBuild)},
      {QueryConfig::kJoinBuildVectorHasherMaxNumDistinct, std::to_string(joinBuildVectorHasherMaxNumDistinct)},
      {QueryConfig::kAbandonDedupHashMapMinRows, std::to_string(abandonHashBuildDedupMinRows)},
      {QueryConfig::kAbandonDedupHashMapMinPct, std::to_string(abandonHashBuildDedupMinPct)},
      {QueryConfig::kHashProbeBloomFilterPushdownMaxSize,
       std::to_string(std::max<int64_t>(bloomFilterPushdownSize, 0))},
  };

  std::vector<facebook::velox::RowVectorPtr> buildVectors;
  buildVectors.reserve(batches.size());
  for (const auto& batch : batches) {
    buildVectors.emplace_back(VeloxColumnarBatch::from(memoryPool.get(), batch)->getRowVector());
  }
  return buildHashTableWithTask(
      hashTableId, spec, buildVectors, numThreads, executor, std::move(queryPool), memoryPool.get());
}

long getJoin(const std::string& hashTableId) {
  const auto handle = JniHashTableContext::getInstance().callJavaGet(hashTableId);
  if (!handle.has_value()) {
    // The JVM side VeloxBroadcastBuildSideCache holding the pre-built hash tables is unreachable,
    // e.g. in the standalone micro benchmark. Report a miss, the same way that cache reports one,
    // and let the caller build the hash table from the join's build side input instead.
    LOG(WARNING) << "No JVM is attached to this process, cannot look up the pre-built hash table for cache key: "
                 << hashTableId << ". A new hash table will be built from the build side input.";
    return 0;
  }
  return handle.value();
}

size_t serializedHashTableSize(const BroadcastHashTable& hashTable) {
  const auto& table = hashTable.hashTable();
  VELOX_CHECK_NOT_NULL(table, "Hash table cannot be null");

  auto* hashTableFalse = dynamic_cast<facebook::velox::exec::HashTable<false>*>(table.get());
  if (hashTableFalse != nullptr) {
    return HashTableSerializer::serializedSize<false>(hashTableFalse);
  }

  auto* hashTableTrue = dynamic_cast<facebook::velox::exec::HashTable<true>*>(table.get());
  VELOX_CHECK_NOT_NULL(hashTableTrue, "Hash table must be either HashTable<false> or HashTable<true>");
  return HashTableSerializer::serializedSize<true>(hashTableTrue);
}

void serializeHashTableTo(const BroadcastHashTable& hashTable, uint8_t* data, size_t size) {
  VELOX_CHECK_NOT_NULL(data, "Serialized buffer cannot be null");

  const auto& table = hashTable.hashTable();
  VELOX_CHECK_NOT_NULL(table, "Hash table cannot be null");

  auto* hashTableFalse = dynamic_cast<facebook::velox::exec::HashTable<false>*>(table.get());
  if (hashTableFalse != nullptr) {
    HashTableSerializer::serializeTo<false>(hashTableFalse, data, size);
    return;
  }

  auto* hashTableTrue = dynamic_cast<facebook::velox::exec::HashTable<true>*>(table.get());
  VELOX_CHECK_NOT_NULL(hashTableTrue, "Hash table must be either HashTable<false> or HashTable<true>");
  HashTableSerializer::serializeTo<true>(hashTableTrue, data, size);
}

std::shared_ptr<BroadcastHashTable>
deserializeHashTable(const uint8_t* data, size_t size, bool ignoreNullKeys, bool joinHasNullKeys) {
  VELOX_CHECK_NOT_NULL(data, "Serialized data cannot be null");
  VELOX_CHECK_GT(size, 0, "Invalid data size");

  auto pool = defaultLeafVeloxMemoryPool();
  std::shared_ptr<facebook::velox::exec::BaseHashTable> hashTable;
  if (ignoreNullKeys) {
    hashTable = HashTableSerializer::deserialize<true>(data, size, pool.get());
  } else {
    hashTable = HashTableSerializer::deserialize<false>(data, size, pool.get());
  }
  return std::make_shared<BroadcastHashTable>(std::move(hashTable), joinHasNullKeys, 0, std::move(pool));
}

} // namespace gluten
