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

jlong JniHashTableContext::callJavaGet(const std::string& id) const {
  JNIEnv* env;
  if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    throw gluten::GlutenException("JNIEnv was not attached to current thread");
  }

  const jstring s = env->NewStringUTF(id.c_str());
  auto result = env->CallStaticLongMethod(jniVeloxBroadcastBuildSideCache_, jniGet_, s);
  return result;
}

// Return the velox's hash table.
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

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>> joinKeyTypes;
  joinKeyTypes.reserve(joinKeys.size());
  for (const auto& name : joinKeys) {
    joinKeyTypes.emplace_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(rowType->findChild(name), name));
  }

  std::vector<uint32_t> filterInputChannels;
  filterInputChannels.reserve(filterBuildColumns.size());
  for (const auto& name : filterBuildColumns) {
    if (const auto idx = rowType->getChildIdxIfExists(name)) {
      filterInputChannels.push_back(*idx);
    }
  }
  std::sort(filterInputChannels.begin(), filterInputChannels.end());
  filterInputChannels.erase(
      std::unique(filterInputChannels.begin(), filterInputChannels.end()), filterInputChannels.end());

  auto hashTableBuilder = std::make_shared<HashTableBuilder>(
      vJoin,
      isNullAwareAntiJoin,
      hasMixedJoinCondition,
      bloomFilterPushdownSize,
      joinKeyTypes,
      filterInputChannels,
      filterPropagatesNulls,
      rowType,
      memoryPool.get(),
      minTableRowsForParallelJoinBuild,
      joinBuildVectorHasherMaxNumDistinct,
      abandonHashBuildDedupMinRows,
      abandonHashBuildDedupMinPct);

  for (auto i = 0; i < batches.size(); i++) {
    auto rowVector = VeloxColumnarBatch::from(memoryPool.get(), batches[i])->getRowVector();
    hashTableBuilder->addInput(rowVector);
    if (hashTableBuilder->noMoreInput()) {
      break;
    }
  }

  return hashTableBuilder;
}

long getJoin(const std::string& hashTableId) {
  return JniHashTableContext::getInstance().callJavaGet(hashTableId);
}

size_t serializedHashTableSize(std::shared_ptr<HashTableBuilder> builder) {
  VELOX_CHECK_NOT_NULL(builder, "Hash table builder cannot be null");

  auto hashTable = builder->hashTable();
  VELOX_CHECK_NOT_NULL(hashTable, "Hash table cannot be null");

  auto* hashTableFalse = dynamic_cast<facebook::velox::exec::HashTable<false>*>(hashTable.get());
  if (hashTableFalse != nullptr) {
    return HashTableSerializer::serializedSize<false>(hashTableFalse);
  }

  auto* hashTableTrue = dynamic_cast<facebook::velox::exec::HashTable<true>*>(hashTable.get());
  VELOX_CHECK_NOT_NULL(hashTableTrue, "Hash table must be either HashTable<false> or HashTable<true>");
  return HashTableSerializer::serializedSize<true>(hashTableTrue);
}

void serializeHashTableTo(std::shared_ptr<HashTableBuilder> builder, uint8_t* data, size_t size) {
  VELOX_CHECK_NOT_NULL(builder, "Hash table builder cannot be null");
  VELOX_CHECK_NOT_NULL(data, "Serialized buffer cannot be null");

  auto hashTable = builder->hashTable();
  VELOX_CHECK_NOT_NULL(hashTable, "Hash table cannot be null");

  auto* hashTableFalse = dynamic_cast<facebook::velox::exec::HashTable<false>*>(hashTable.get());
  if (hashTableFalse != nullptr) {
    HashTableSerializer::serializeTo<false>(hashTableFalse, data, size);
    return;
  }

  auto* hashTableTrue = dynamic_cast<facebook::velox::exec::HashTable<true>*>(hashTable.get());
  VELOX_CHECK_NOT_NULL(hashTableTrue, "Hash table must be either HashTable<false> or HashTable<true>");
  HashTableSerializer::serializeTo<true>(hashTableTrue, data, size);
}

std::shared_ptr<HashTableBuilder>
deserializeHashTable(const uint8_t* data, size_t size, bool ignoreNullKeys, bool joinHasNullKeys) {
  VELOX_CHECK_NOT_NULL(data, "Serialized data cannot be null");
  VELOX_CHECK_GT(size, 0, "Invalid data size");

  auto pool = defaultLeafVeloxMemoryPool();
  auto* poolPtr = pool.get();

  std::unique_ptr<facebook::velox::exec::BaseHashTable> hashTable;
  if (ignoreNullKeys) {
    auto derived = HashTableSerializer::deserialize<true>(data, size, poolPtr);
    hashTable = std::move(derived);
  } else {
    auto derived = HashTableSerializer::deserialize<false>(data, size, poolPtr);
    hashTable = std::move(derived);
  }

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>> emptyKeys;
  std::vector<uint32_t> emptyChannels;

  auto keyTypes = hashTable->rows()->keyTypes();
  std::vector<std::string> names;
  for (size_t i = 0; i < keyTypes.size(); ++i) {
    names.push_back("key" + std::to_string(i));
  }
  auto rowType = facebook::velox::ROW(std::move(names), std::move(keyTypes));

  auto builder = std::make_shared<HashTableBuilder>(
      facebook::velox::core::JoinType::kInner,
      false,
      false,
      -1,
      emptyKeys,
      emptyChannels,
      false,
      rowType,
      poolPtr,
      1000,
      1000000,
      100000,
      0);

  builder->setHashTable(std::move(hashTable));
  // Restore the joinHasNullKeys flag
  builder->setJoinHasNullKeys(joinHasNullKeys);
  return builder;
}

} // namespace gluten
