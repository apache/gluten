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
#include "config/VeloxConfig.h"
#include "folly/String.h"
#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/hashjoin/HashTableSerializer.h"
#include "operators/hashjoin/HashTableTaskBuilder.h"
#include "operators/plannodes/IteratorSplit.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
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

namespace {

facebook::velox::core::JoinType toVeloxJoinType(int joinType, bool isExistenceJoin) {
  auto sJoin = static_cast<substrait::JoinRel_JoinType>(joinType);
  switch (sJoin) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      return facebook::velox::core::JoinType::kInner;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      return facebook::velox::core::JoinType::kFull;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      return facebook::velox::core::JoinType::kLeft;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      return facebook::velox::core::JoinType::kRight;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
      return isExistenceJoin ? facebook::velox::core::JoinType::kLeftSemiProject
                             : facebook::velox::core::JoinType::kLeftSemiFilter;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
      return isExistenceJoin ? facebook::velox::core::JoinType::kRightSemiProject
                             : facebook::velox::core::JoinType::kRightSemiFilter;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI:
      return facebook::velox::core::JoinType::kAnti;
    default:
      VELOX_NYI("Unsupported Join type: {}", std::to_string(sJoin));
  }
}

const facebook::velox::core::FilterNode* findFilterNode(const facebook::velox::core::PlanNodePtr& node) {
  if (const auto* filter = dynamic_cast<const facebook::velox::core::FilterNode*>(node.get())) {
    return filter;
  }
  for (const auto& source : node->sources()) {
    if (const auto* filter = findFilterNode(source)) {
      return filter;
    }
  }
  return nullptr;
}

template <bool ignoreNullKeys>
facebook::velox::exec::HashTable<ignoreNullKeys>* asHashTable(const BroadcastHashTable& hashTable) {
  return dynamic_cast<facebook::velox::exec::HashTable<ignoreNullKeys>*>(hashTable.table.get());
}

} // namespace

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

  const auto vJoin = toVeloxJoinType(joinType, isExistenceJoin);

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

facebook::velox::core::TypedExprPtr toVeloxJoinFilter(
    const std::string& filterPlan,
    const std::vector<std::string>& filterInputNames,
    const facebook::velox::config::ConfigBase* veloxCfg,
    facebook::velox::memory::MemoryPool* pool) {
  if (filterPlan.empty()) {
    return nullptr;
  }
  ::substrait::Plan plan;
  VELOX_CHECK(plan.ParseFromString(filterPlan), "Failed to parse the join filter plan");
  SubstraitToVeloxPlanConverter converter(
      pool,
      veloxCfg,
      {},
      VeloxConnectorIds{
          .hive = kHiveConnectorId,
          .iceberg = kIcebergConnectorId,
          .iterator = kIteratorConnectorId,
          .cudfHive = kCudfHiveConnectorId},
      std::nullopt,
      std::nullopt,
      /*validationMode=*/true);
  const auto root = converter.toVeloxPlan(plan);
  const auto* filterNode = findFilterNode(root);
  VELOX_CHECK_NOT_NULL(filterNode, "The join filter plan has no filter");

  // The converter names the filter input columns by position. Map them back to
  // the column names of the join inputs.
  const auto& inputType = filterNode->sources()[0]->outputType();
  VELOX_CHECK_EQ(inputType->size(), filterInputNames.size());
  std::unordered_map<std::string, facebook::velox::core::TypedExprPtr> mapping;
  for (auto i = 0; i < inputType->size(); ++i) {
    mapping.emplace(
        inputType->nameOf(i),
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(inputType->childAt(i), filterInputNames[i]));
  }
  return filterNode->filter()->rewriteInputNames(mapping);
}

std::shared_ptr<BroadcastHashTable> nativeHashTableBuildWithTask(
    const std::string& hashTableId,
    const std::vector<std::string>& joinKeys,
    facebook::velox::core::TypedExprPtr filter,
    std::vector<std::string> names,
    std::vector<facebook::velox::TypePtr> veloxTypeList,
    int joinType,
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
    std::shared_ptr<facebook::velox::memory::MemoryPool> inputPool) {
  using facebook::velox::core::QueryConfig;
  HashTableTaskBuildSpec spec;
  spec.joinType = toVeloxJoinType(joinType, isExistenceJoin);
  spec.nullAware = isNullAwareAntiJoin;
  spec.filter = std::move(filter);
  spec.buildType = std::make_shared<facebook::velox::RowType>(std::move(names), std::move(veloxTypeList));
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
    buildVectors.emplace_back(VeloxColumnarBatch::from(inputPool.get(), batch)->getRowVector());
  }
  return buildHashTableWithTask(hashTableId, spec, buildVectors, numThreads, executor, std::move(queryPool));
}

std::shared_ptr<BroadcastHashTable> toBroadcastHashTable(std::shared_ptr<HashTableBuilder> builder) {
  auto result = std::make_shared<BroadcastHashTable>();
  result->table = builder->hashTable();
  result->joinHasNullKeys = builder->joinHasNullKeys();
  result->memoryUsage = builder->hashTableMemoryUsage();
  result->owner = std::move(builder);
  return result;
}

long getJoin(const std::string& hashTableId) {
  return JniHashTableContext::getInstance().callJavaGet(hashTableId);
}

size_t serializedHashTableSize(const BroadcastHashTable& hashTable) {
  VELOX_CHECK_NOT_NULL(hashTable.table, "Hash table cannot be null");
  if (auto* table = asHashTable<false>(hashTable)) {
    return HashTableSerializer::serializedSize<false>(table);
  }
  auto* table = asHashTable<true>(hashTable);
  VELOX_CHECK_NOT_NULL(table, "Hash table must be either HashTable<false> or HashTable<true>");
  return HashTableSerializer::serializedSize<true>(table);
}

void serializeHashTableTo(const BroadcastHashTable& hashTable, uint8_t* data, size_t size) {
  VELOX_CHECK_NOT_NULL(hashTable.table, "Hash table cannot be null");
  VELOX_CHECK_NOT_NULL(data, "Serialized buffer cannot be null");
  if (auto* table = asHashTable<false>(hashTable)) {
    HashTableSerializer::serializeTo<false>(table, data, size);
    return;
  }
  auto* table = asHashTable<true>(hashTable);
  VELOX_CHECK_NOT_NULL(table, "Hash table must be either HashTable<false> or HashTable<true>");
  HashTableSerializer::serializeTo<true>(table, data, size);
}

std::shared_ptr<BroadcastHashTable> deserializeHashTable(
    const uint8_t* data,
    size_t size,
    bool ignoreNullKeys,
    bool joinHasNullKeys,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool) {
  VELOX_CHECK_NOT_NULL(data, "Serialized data cannot be null");
  VELOX_CHECK_GT(size, 0, "Invalid data size");

  auto result = std::make_shared<BroadcastHashTable>();
  if (ignoreNullKeys) {
    result->table = HashTableSerializer::deserialize<true>(data, size, pool.get());
  } else {
    result->table = HashTableSerializer::deserialize<false>(data, size, pool.get());
  }
  result->joinHasNullKeys = joinHasNullKeys;
  result->memoryUsage = result->table->allocatedBytes();
  result->owner = std::move(pool);
  return result;
}

} // namespace gluten
