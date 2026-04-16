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

#include "SubstraitToVeloxPlan.h"

#include "TypeUtils.h"
#include "VariantToVectorConverter.h"
#include "jni/JniHashTable.h"
#include "operators/hashjoin/HashTableBuilder.h"
#include "operators/plannodes/RowVectorStream.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/exec/TableWriter.h"
#include "velox/type/Type.h"

#include "utils/ConfigExtractor.h"
#include "utils/ObjectStore.h"
#include "utils/VeloxWriterUtils.h"

#include "config.pb.h"
#include "config/GlutenConfig.h"
#include "config/VeloxConfig.h"

#ifdef GLUTEN_ENABLE_GPU
#include "operators/plannodes/CudfVectorStream.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveDataSink.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveTableHandle.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
using namespace cudf_velox::connector::hive;
#endif

namespace gluten {
namespace {

bool useCudfTableHandle(const std::vector<std::shared_ptr<SplitInfo>>& splitInfos) {
#ifdef GLUTEN_ENABLE_GPU
  if (splitInfos.empty()) {
    return false;
  }
  return splitInfos[0]->canUseCudfConnector();
#else
  return false;
#endif
}

core::SortOrder toSortOrder(const ::substrait::SortField& sortField) {
  switch (sortField.direction()) {
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
      return core::kAscNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
      return core::kAscNullsLast;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
      return core::kDescNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
      return core::kDescNullsLast;
    default:
      VELOX_FAIL("Sort direction is not supported.");
  }
}

/// Holds the information required to create
/// a project node to simulate the emit
/// behavior in Substrait.
struct EmitInfo {
  std::vector<core::TypedExprPtr> expressions;
  std::vector<std::string> projectNames;
};

/// Helper function to extract the attributes required to create a ProjectNode
/// used for interpreting Substrait Emit.
EmitInfo getEmitInfo(const ::substrait::RelCommon& relCommon, const core::PlanNodePtr& node) {
  const auto& emit = relCommon.emit();
  int emitSize = emit.output_mapping_size();
  EmitInfo emitInfo;
  emitInfo.projectNames.resize(emitSize);
  emitInfo.expressions.resize(emitSize);
  const auto& outputType = node->outputType();
  for (int i = 0; i < emitSize; i++) {
    int32_t mapId = emit.output_mapping(i);
    emitInfo.projectNames[i] = outputType->nameOf(mapId);
    emitInfo.expressions[i] =
        std::make_shared<core::FieldAccessTypedExpr>(outputType->childAt(mapId), outputType->nameOf(mapId));
  }
  return emitInfo;
}

/// @brief Get the input type from both sides of join.
/// @param leftNode the plan node of left side.
/// @param rightNode the plan node of right side.
/// @return the input type.
RowTypePtr getJoinInputType(const core::PlanNodePtr& leftNode, const core::PlanNodePtr& rightNode) {
  auto outputSize = leftNode->outputType()->size() + rightNode->outputType()->size();
  std::vector<std::string> outputNames;
  std::vector<TypePtr> outputTypes;
  outputNames.reserve(outputSize);
  outputTypes.reserve(outputSize);
  for (const auto& node : {leftNode, rightNode}) {
    const auto& names = node->outputType()->names();
    outputNames.insert(outputNames.end(), names.begin(), names.end());
    const auto& types = node->outputType()->children();
    outputTypes.insert(outputTypes.end(), types.begin(), types.end());
  }
  return std::make_shared<const RowType>(std::move(outputNames), std::move(outputTypes));
}

/// @brief Get the direct output type of join.
/// @param leftNode the plan node of left side.
/// @param rightNode the plan node of right side.
/// @param joinType the join type.
/// @return the output type.
RowTypePtr getJoinOutputType(
    const core::PlanNodePtr& leftNode,
    const core::PlanNodePtr& rightNode,
    const core::JoinType& joinType) {
  // Decide output type.
  // Output of right semi join cannot include columns from the left side.
  bool outputMayIncludeLeftColumns = !(core::isRightSemiFilterJoin(joinType) || core::isRightSemiProjectJoin(joinType));

  // Output of left semi and anti joins cannot include columns from the right
  // side.
  bool outputMayIncludeRightColumns =
      !(core::isLeftSemiFilterJoin(joinType) || core::isLeftSemiProjectJoin(joinType) || core::isAntiJoin(joinType));

  if (outputMayIncludeLeftColumns && outputMayIncludeRightColumns) {
    return getJoinInputType(leftNode, rightNode);
  }

  if (outputMayIncludeLeftColumns) {
    if (core::isLeftSemiProjectJoin(joinType)) {
      std::vector<std::string> outputNames = leftNode->outputType()->names();
      std::vector<TypePtr> outputTypes = leftNode->outputType()->children();
      outputNames.emplace_back("exists");
      outputTypes.emplace_back(BOOLEAN());
      return std::make_shared<const RowType>(std::move(outputNames), std::move(outputTypes));
    } else {
      return leftNode->outputType();
    }
  }

  if (outputMayIncludeRightColumns) {
    if (core::isRightSemiProjectJoin(joinType)) {
      std::vector<std::string> outputNames = rightNode->outputType()->names();
      std::vector<TypePtr> outputTypes = rightNode->outputType()->children();
      outputNames.emplace_back("exists");
      outputTypes.emplace_back(BOOLEAN());
      return std::make_shared<const RowType>(std::move(outputNames), std::move(outputTypes));
    } else {
      return rightNode->outputType();
    }
  }
  VELOX_FAIL("Output should include left or right columns.");
}

// The base names can be referred from 'velox/expression/FunctionSignature.cpp'.
std::string typeBaseName(const TypePtr& type) {
  if (type->isDecimal()) {
    return "decimal";
  }
  if (type->isPrimitiveType()) {
    return type->toString();
  }

  if (type->kind() == TypeKind::ARRAY) {
    return "array";
  }

  if (type->kind() == TypeKind::MAP) {
    return "map";
  }

  if (type->kind() == TypeKind::ROW) {
    return "row";
  }
  VELOX_UNREACHABLE("Unknown type: {}", type->toString());
}

// Get the function name suffix used by merge_extract companion function when having the same intermediate type across
// signatures. Correponds to Velox 'toSuffixString', and the base name can be referred from
// 'velox/expression/FunctionSignature.cpp'.
std::string companionFunctionSuffix(const TypePtr& type) {
  // For primitive and decimal types, return their names.
  if (type->isDecimal() || type->isPrimitiveType()) {
    return typeBaseName(type);
  }

  if (type->kind() == TypeKind::ARRAY) {
    return "array_" + companionFunctionSuffix(std::dynamic_pointer_cast<const ArrayType>(type)->elementType());
  }
  if (type->kind() == TypeKind::MAP) {
    auto mapType = std::dynamic_pointer_cast<const MapType>(type);
    return "map_" + companionFunctionSuffix(mapType->keyType()) + "_" + companionFunctionSuffix(mapType->valueType());
  }

  std::string name;
  if (type->kind() == TypeKind::ROW) {
    name = "row";
  }
  std::string result = name;
  const auto rowType = asRowType(type);
  for (const auto& child : rowType->children()) {
    result += '_';
    result += companionFunctionSuffix(child);
  }
  result += "_end";
  result += name;
  return result;
}

// Compatible with 'validateBaseTypeAndCollectTypeParams' in 'velox/expression/FunctionSignature.cpp'.
bool isBuiltinTypeName(const std::string& typeName) {
  return TypeKindName::tryToTypeKind(typeName).has_value() || isDecimalName(typeName) || isDateName(typeName) ||
      hasType(typeName);
}

// Checks if the companion function entry has a matching return type for the given result type.
bool hasMatchingReturnType(const exec::CompanionSignatureEntry& entry, const TypePtr& returnType) {
  const auto returnTypeName = boost::algorithm::to_upper_copy(typeBaseName(returnType));
  for (const auto& signature : entry.signatures) {
    const auto signatureTypeName = boost::algorithm::to_upper_copy(signature->returnType().baseName());
    // Non-built-in type is allowed for a generic type match.
    if (!isBuiltinTypeName(signatureTypeName) || signatureTypeName == returnTypeName) {
      return true;
    }
  }
  return false;
}

} // namespace

bool SplitInfo::canUseCudfConnector() {
  bool isEmpty = partitionColumns.empty();

  if (!isEmpty) {
    // Check if all maps are empty
    bool allMapsEmpty = true;
    for (const auto& m : partitionColumns) {
      if (!m.empty()) {
        allMapsEmpty = false;
        break;
      }
    }
    isEmpty = allMapsEmpty;
  }
  return isEmpty && format == dwio::common::FileFormat::PARQUET;
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::processEmit(
    const ::substrait::RelCommon& relCommon,
    const core::PlanNodePtr& noEmitNode) {
  switch (relCommon.emit_kind_case()) {
    case ::substrait::RelCommon::EmitKindCase::kDirect:
      return noEmitNode;
    case ::substrait::RelCommon::EmitKindCase::kEmit: {
      auto emitInfo = getEmitInfo(relCommon, noEmitNode);
      return std::make_shared<core::ProjectNode>(
          nextPlanNodeId(), std::move(emitInfo.projectNames), std::move(emitInfo.expressions), noEmitNode);
    }
    default:
      VELOX_FAIL("unrecognized emit kind");
  }
}

core::AggregationNode::Step SubstraitToVeloxPlanConverter::toAggregationStep(const ::substrait::AggregateRel& aggRel) {
  // TODO Simplify Velox's aggregation steps
  if (aggRel.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(aggRel.advanced_extension(), "allowFlush=")) {
    return core::AggregationNode::Step::kPartial;
  }
  return core::AggregationNode::Step::kSingle;
}

/// Get aggregation function step for AggregateFunction.
/// The returned step value will be used to decide which Velox aggregate function or companion function
/// is used for the actual data processing.
core::AggregationNode::Step SubstraitToVeloxPlanConverter::toAggregationFunctionStep(
    const ::substrait::AggregateFunction& sAggFuc) {
  const auto& phase = sAggFuc.phase();
  switch (phase) {
    case ::substrait::AGGREGATION_PHASE_UNSPECIFIED:
      VELOX_FAIL("Aggregation phase not specified.");
      break;
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kPartial;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kIntermediate;
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT:
      return core::AggregationNode::Step::kSingle;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
      return core::AggregationNode::Step::kFinal;
    default:
      VELOX_FAIL("Unexpected aggregation phase.");
  }
}

std::string SubstraitToVeloxPlanConverter::toAggregationFunctionName(
    const std::string& baseName,
    const core::AggregationNode::Step& step,
    const TypePtr& resultType) {
  std::string suffix;
  switch (step) {
    case core::AggregationNode::Step::kPartial:
      suffix = "_partial";
      break;
    case core::AggregationNode::Step::kFinal: {
      const auto functionName = baseName + "_merge_extract";
      auto signatures = exec::getAggregateFunctionSignatures(functionName);
      if (signatures.has_value() && signatures.value().size() > 0) {
        // The merge_extract function is registered without suffix.
        return functionName;
      }

      // The merge_extract function should be registered with suffix based on result type.
      const auto fullName = functionName + "_" + companionFunctionSuffix(resultType);
      signatures = exec::getAggregateFunctionSignatures(fullName);
      if (signatures.has_value() && signatures.value().size() > 0) {
        return fullName;
      }

      // When a function uses generic type variables in its signature (e.g., ..._T), infer the function by matching the
      // result type allow non-built-in names.
      auto companionSigs = exec::getCompanionFunctionSignatures(baseName);
      if (companionSigs.has_value()) {
        for (const auto& entry : companionSigs->mergeExtract) {
          if (hasMatchingReturnType(entry, resultType)) {
            return entry.functionName;
          }
        }
      }
      VELOX_FAIL("Cannot find function signature for {} in final aggregation step.", baseName);
    }
    case core::AggregationNode::Step::kIntermediate:
      suffix = "_merge";
      break;
    case core::AggregationNode::Step::kSingle:
      suffix = "";
      break;
    default:
      VELOX_FAIL("Unexpected aggregation node step.");
  }
  return baseName + suffix;
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::JoinRel& sJoin) {
  if (!sJoin.has_left()) {
    VELOX_FAIL("Left Rel is expected in JoinRel.");
  }
  if (!sJoin.has_right()) {
    VELOX_FAIL("Right Rel is expected in JoinRel.");
  }

  auto leftNode = toVeloxPlan(sJoin.left());
  auto rightNode = toVeloxPlan(sJoin.right());

  // Map join type.
  core::JoinType joinType;
  bool isNullAwareAntiJoin = false;
  switch (sJoin.type()) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      joinType = core::JoinType::kInner;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      joinType = core::JoinType::kFull;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      joinType = core::JoinType::kLeft;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      joinType = core::JoinType::kRight;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
      // Determine the semi join type based on extracted information.
      if (sJoin.has_advanced_extension() &&
          SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isExistenceJoin=")) {
        joinType = core::JoinType::kLeftSemiProject;
      } else {
        joinType = core::JoinType::kLeftSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
      // Determine the semi join type based on extracted information.
      if (sJoin.has_advanced_extension() &&
          SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isExistenceJoin=")) {
        joinType = core::JoinType::kRightSemiProject;
      } else {
        joinType = core::JoinType::kRightSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI: {
      // Determine the anti join type based on extracted information.
      if (sJoin.has_advanced_extension() &&
          SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isNullAwareAntiJoin=")) {
        isNullAwareAntiJoin = true;
      }
      joinType = core::JoinType::kAnti;
      break;
    }
    default:
      VELOX_NYI("Unsupported Join type: {}", std::to_string(sJoin.type()));
  }

  // extract join keys from join expression
  std::vector<const ::substrait::Expression::FieldReference*> leftExprs, rightExprs;
  extractJoinKeys(sJoin.expression(), leftExprs, rightExprs);
  VELOX_CHECK_EQ(leftExprs.size(), rightExprs.size());
  size_t numKeys = leftExprs.size();

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> leftKeys, rightKeys;
  leftKeys.reserve(numKeys);
  rightKeys.reserve(numKeys);
  auto inputRowType = getJoinInputType(leftNode, rightNode);
  for (size_t i = 0; i < numKeys; ++i) {
    leftKeys.emplace_back(exprConverter_->toVeloxExpr(*leftExprs[i], inputRowType));
    rightKeys.emplace_back(exprConverter_->toVeloxExpr(*rightExprs[i], inputRowType));
  }

  core::TypedExprPtr filter;
  if (sJoin.has_post_join_filter()) {
    filter = exprConverter_->toVeloxExpr(sJoin.post_join_filter(), inputRowType);
  }

  if (sJoin.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isSMJ=")) {
    // Create MergeJoinNode node
    return std::make_shared<core::MergeJoinNode>(
        nextPlanNodeId(),
        joinType,
        leftKeys,
        rightKeys,
        filter,
        leftNode,
        rightNode,
        getJoinOutputType(leftNode, rightNode, joinType));

  } else if (
      sJoin.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isBHJ=")) {
    std::string hashTableId = sJoin.hashtableid();

    std::shared_ptr<core::OpaqueHashTable> opaqueSharedHashTable = nullptr;
    bool joinHasNullKeys = false;

    try {
      auto hashTableBuilder = ObjectStore::retrieve<gluten::HashTableBuilder>(getJoin(hashTableId));
      joinHasNullKeys = hashTableBuilder->joinHasNullKeys();
      auto originalShared = hashTableBuilder->hashTable();
      opaqueSharedHashTable = std::shared_ptr<core::OpaqueHashTable>(
          originalShared, reinterpret_cast<core::OpaqueHashTable*>(originalShared.get()));

      LOG(INFO) << "Successfully retrieved and aliased HashTable for reuse. ID: " << hashTableId;
    } catch (const std::exception& e) {
      LOG(WARNING)
          << "Error retrieving HashTable from ObjectStore: " << e.what()
          << ". Falling back to building new table. To ensure correct results, please verify that spark.gluten.velox.buildHashTableOncePerExecutor.enabled is set to false.";
      opaqueSharedHashTable = nullptr;
    }

    // Create HashJoinNode node
    return std::make_shared<core::HashJoinNode>(
        nextPlanNodeId(),
        joinType,
        isNullAwareAntiJoin,
        leftKeys,
        rightKeys,
        filter,
        leftNode,
        rightNode,
        getJoinOutputType(leftNode, rightNode, joinType),
        false,
        false,
        joinHasNullKeys,
        opaqueSharedHashTable);
  } else {
    // Create HashJoinNode node
    return std::make_shared<core::HashJoinNode>(
        nextPlanNodeId(),
        joinType,
        isNullAwareAntiJoin,
        leftKeys,
        rightKeys,
        filter,
        leftNode,
        rightNode,
        getJoinOutputType(leftNode, rightNode, joinType));
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::CrossRel& crossRel) {
  // Support basic cross join without any filters
  if (!crossRel.has_left()) {
    VELOX_FAIL("Left Rel is expected in CrossRel.");
  }
  if (!crossRel.has_right()) {
    VELOX_FAIL("Right Rel is expected in CrossRel.");
  }

  auto leftNode = toVeloxPlan(crossRel.left());
  auto rightNode = toVeloxPlan(crossRel.right());

  // Map join type.
  core::JoinType joinType;
  switch (crossRel.type()) {
    case ::substrait::CrossRel_JoinType::CrossRel_JoinType_JOIN_TYPE_INNER:
      joinType = core::JoinType::kInner;
      break;
    case ::substrait::CrossRel_JoinType::CrossRel_JoinType_JOIN_TYPE_LEFT:
      joinType = core::JoinType::kLeft;
      break;
    case ::substrait::CrossRel_JoinType::CrossRel_JoinType_JOIN_TYPE_LEFT_SEMI:
      if (crossRel.has_advanced_extension() &&
          SubstraitParser::configSetInOptimization(crossRel.advanced_extension(), "isExistenceJoin=")) {
        joinType = core::JoinType::kLeftSemiProject;
      } else {
        VELOX_NYI("Unsupported Join type: {}", std::to_string(crossRel.type()));
      }
      break;
    default:
      VELOX_NYI("Unsupported Join type: {}", std::to_string(crossRel.type()));
  }

  auto inputRowType = getJoinInputType(leftNode, rightNode);
  core::TypedExprPtr joinConditions;
  if (crossRel.has_expression()) {
    joinConditions = exprConverter_->toVeloxExpr(crossRel.expression(), inputRowType);
  }

  return std::make_shared<core::NestedLoopJoinNode>(
      nextPlanNodeId(),
      joinType,
      joinConditions,
      leftNode,
      rightNode,
      getJoinOutputType(leftNode, rightNode, joinType));
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::AggregateRel& aggRel) {
  auto childNode = convertSingleInput<::substrait::AggregateRel>(aggRel);
  core::AggregationNode::Step aggStep = toAggregationStep(aggRel);
  const auto& inputType = childNode->outputType();
  std::vector<core::FieldAccessTypedExprPtr> veloxGroupingExprs;

  // Get the grouping expressions.
  VELOX_CHECK(
      aggRel.groupings().size() <= 1, "At most one grouping is supported, but got {}.", aggRel.groupings().size());
  if (aggRel.groupings().size() == 1) {
    for (const auto& groupingExpr : aggRel.groupings()[0].grouping_expressions()) {
      // Velox's groupings are limited to be Field.
      veloxGroupingExprs.emplace_back(exprConverter_->toVeloxExpr(groupingExpr.selection(), inputType));
    }
  }

  // Parse measures and get the aggregate expressions.
  // Each measure represents one aggregate expression.
  std::vector<core::AggregationNode::Aggregate> aggregates;
  aggregates.reserve(aggRel.measures().size());

  for (const auto& measure : aggRel.measures()) {
    core::FieldAccessTypedExprPtr mask;
    ::substrait::Expression substraitAggMask = measure.filter();
    // Get Aggregation Masks.
    if (measure.has_filter()) {
      if (substraitAggMask.ByteSizeLong() > 0) {
        mask = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
            exprConverter_->toVeloxExpr(substraitAggMask, inputType));
      }
    }
    const auto& aggFunction = measure.measure();
    std::vector<core::TypedExprPtr> aggParams;
    aggParams.reserve(aggFunction.arguments().size());
    for (const auto& arg : aggFunction.arguments()) {
      aggParams.emplace_back(exprConverter_->toVeloxExpr(arg.value(), inputType));
    }

    auto aggVeloxType = SubstraitParser::parseType(aggFunction.output_type());
    auto baseFuncName = SubstraitParser::findVeloxFunction(functionMap_, aggFunction.function_reference());
    auto funcName = toAggregationFunctionName(baseFuncName, toAggregationFunctionStep(aggFunction), aggVeloxType);

    auto aggExpr = std::make_shared<const core::CallTypedExpr>(aggVeloxType, std::move(aggParams), funcName);
    std::vector<TypePtr> rawInputTypes =
        SubstraitParser::sigToTypes(SubstraitParser::findFunctionSpec(functionMap_, aggFunction.function_reference()));
    aggregates.emplace_back(core::AggregationNode::Aggregate{aggExpr, rawInputTypes, mask, {}, {}});
  }

  std::vector<core::FieldAccessTypedExprPtr> preGroupingExprs;
  if (aggRel.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(aggRel.advanced_extension(), "isStreaming=")) {
    preGroupingExprs.reserve(veloxGroupingExprs.size());
    preGroupingExprs.insert(preGroupingExprs.begin(), veloxGroupingExprs.begin(), veloxGroupingExprs.end());
  }

  // Get the output names of Aggregation.
  std::vector<std::string> aggOutNames;
  aggOutNames.reserve(aggRel.measures().size());
  for (int idx = veloxGroupingExprs.size(); idx < veloxGroupingExprs.size() + aggRel.measures().size(); idx++) {
    aggOutNames.emplace_back(SubstraitParser::makeNodeName(planNodeId_, idx));
  }

  auto aggregationNode = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      aggStep,
      veloxGroupingExprs,
      preGroupingExprs,
      aggOutNames,
      aggregates,
      /*ignoreNullKeys=*/false,
      /*noGroupsSpanBatches=*/false,
      childNode);

  if (aggRel.has_common()) {
    return processEmit(aggRel.common(), std::move(aggregationNode));
  } else {
    return aggregationNode;
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ProjectRel& projectRel) {
  auto childNode = convertSingleInput<::substrait::ProjectRel>(projectRel);
  // Construct Velox Expressions.
  const auto& projectExprs = projectRel.expressions();
  const auto& inputType = childNode->outputType();
  const size_t totalSize = projectExprs.size() + inputType->size();
  std::vector<std::string> projectNames;
  std::vector<core::TypedExprPtr> expressions;
  projectNames.reserve(totalSize);
  expressions.reserve(totalSize);

  // Note that Substrait projection adds the project expressions on top of the
  // input to the projection node. Thus we need to add the input columns first
  // and then add the projection expressions.

  // First, adding the project names and expressions from the input to
  // the project node.
  for (uint32_t idx = 0; idx < inputType->size(); idx++) {
    const auto& fieldName = inputType->nameOf(idx);
    projectNames.emplace_back(fieldName);
    expressions.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(inputType->childAt(idx), fieldName));
  }

  // Then, adding project expression related project names and expressions.
  const size_t startIdx = expressions.size();
  for (int i = 0; i < projectExprs.size(); i++) {
    expressions.emplace_back(exprConverter_->toVeloxExpr(projectExprs[i], inputType));
    projectNames.emplace_back(SubstraitParser::makeNodeName(planNodeId_, startIdx + i));
  }

  if (projectRel.has_common()) {
    auto relCommon = projectRel.common();
    const auto& emit = relCommon.emit();
    int emitSize = emit.output_mapping_size();
    std::vector<std::string> emitProjectNames;
    std::vector<core::TypedExprPtr> emitExpressions;
    emitProjectNames.reserve(emitSize);
    emitExpressions.reserve(emitSize);

    for (int i = 0; i < emitSize; i++) {
      int32_t mapId = emit.output_mapping(i);
      emitProjectNames.emplace_back(std::move(projectNames[mapId]));
      emitExpressions.emplace_back(std::move(expressions[mapId]));
    }

    return std::make_shared<core::ProjectNode>(
        nextPlanNodeId(), std::move(emitProjectNames), std::move(emitExpressions), std::move(childNode));
  } else {
    return std::make_shared<core::ProjectNode>(
        nextPlanNodeId(), std::move(projectNames), std::move(expressions), std::move(childNode));
  }
}

std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
    const std::string& targetDirectory,
    const std::string& fileName,
    dwio::common::FileFormat fileFormat,
    common::CompressionKind compression,
    const bool& isBucketed,
    const std::optional<std::string>& writeDirectory = std::nullopt,
    const connector::hive::LocationHandle::TableType& tableType =
        connector::hive::LocationHandle::TableType::kExisting) {
  std::string targetFileName = "";
  if (fileFormat == dwio::common::FileFormat::PARQUET && !isBucketed) {
    targetFileName = fileName;
  }
  return std::make_shared<connector::hive::LocationHandle>(
      targetDirectory, writeDirectory.value_or(targetDirectory), tableType, targetFileName);
}

std::shared_ptr<connector::hive::HiveInsertTableHandle> makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    const std::shared_ptr<connector::hive::HiveBucketProperty>& bucketProperty,
    const std::shared_ptr<connector::hive::LocationHandle>& locationHandle,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
    const dwio::common::FileFormat& tableStorageFormat = dwio::common::FileFormat::PARQUET,
    const std::optional<common::CompressionKind>& compressionKind = {}) {
  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>> columnHandles;
  columnHandles.reserve(tableColumnNames.size());
  std::vector<std::string> bucketedBy;
  std::vector<TypePtr> bucketedTypes;
  std::vector<std::shared_ptr<const connector::hive::HiveSortingColumn>> sortedBy;
  if (bucketProperty != nullptr) {
    bucketedBy = bucketProperty->bucketedBy();
    bucketedTypes = bucketProperty->bucketedTypes();
    sortedBy = bucketProperty->sortedBy();
  }
  int32_t numPartitionColumns{0};
  int32_t numSortingColumns{0};
  int32_t numBucketColumns{0};
  for (int i = 0; i < tableColumnNames.size(); ++i) {
    for (int j = 0; j < bucketedBy.size(); ++j) {
      if (bucketedBy[j] == tableColumnNames[i]) {
        ++numBucketColumns;
      }
    }
    for (int j = 0; j < sortedBy.size(); ++j) {
      if (sortedBy[j]->sortColumn() == tableColumnNames[i]) {
        ++numSortingColumns;
      }
    }
    if (std::find(partitionedBy.cbegin(), partitionedBy.cend(), tableColumnNames.at(i)) != partitionedBy.cend()) {
      ++numPartitionColumns;
      columnHandles.emplace_back(std::make_shared<connector::hive::HiveColumnHandle>(
          tableColumnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kPartitionKey,
          tableColumnTypes.at(i),
          tableColumnTypes.at(i)));
    } else {
      columnHandles.emplace_back(std::make_shared<connector::hive::HiveColumnHandle>(
          tableColumnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kRegular,
          tableColumnTypes.at(i),
          tableColumnTypes.at(i)));
    }
  }
  VELOX_CHECK_EQ(numPartitionColumns, partitionedBy.size());
  VELOX_CHECK_EQ(numBucketColumns, bucketedBy.size());
  VELOX_CHECK_EQ(numSortingColumns, sortedBy.size());
  return std::make_shared<connector::hive::HiveInsertTableHandle>(
      columnHandles,
      locationHandle,
      tableStorageFormat,
      bucketProperty,
      compressionKind,
      std::unordered_map<std::string, std::string>{},
      writerOptions);
}

#ifdef GLUTEN_ENABLE_GPU
std::shared_ptr<CudfHiveInsertTableHandle> makeCudfHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    std::shared_ptr<cudf_velox::connector::hive::LocationHandle> locationHandle,
    const std::optional<common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions) {
  std::vector<std::shared_ptr<const CudfHiveColumnHandle>> columnHandles;
  columnHandles.reserve(tableColumnNames.size());

  for (int i = 0; i < tableColumnNames.size(); ++i) {
    columnHandles.push_back(std::make_shared<CudfHiveColumnHandle>(
        tableColumnNames.at(i),
        tableColumnTypes.at(i),
        cudf::data_type{cudf_velox::veloxToCudfTypeId(tableColumnTypes.at(i))}));
  }

  return std::make_shared<CudfHiveInsertTableHandle>(
      columnHandles, locationHandle, compressionKind, serdeParameters, writerOptions);
}
#endif

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::WriteRel& writeRel) {
  core::PlanNodePtr childNode;
  if (writeRel.has_input()) {
    childNode = toVeloxPlan(writeRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in WriteRel.");
  }
  const auto& inputType = childNode->outputType();

  std::vector<std::string> tableColumnNames;
  std::vector<std::string> partitionedKey;
  std::vector<ColumnType> columnTypes;
  tableColumnNames.reserve(writeRel.table_schema().names_size());

  VELOX_CHECK(writeRel.has_table_schema(), "WriteRel should have the table schema to store the column information");
  const auto& tableSchema = writeRel.table_schema();
  SubstraitParser::parseColumnTypes(tableSchema, columnTypes);

  for (const auto& name : tableSchema.names()) {
    tableColumnNames.emplace_back(name);
  }

  for (int i = 0; i < tableSchema.names_size(); i++) {
    if (columnTypes[i] == ColumnType::kPartitionKey) {
      partitionedKey.emplace_back(tableColumnNames[i]);
    }
  }

  std::shared_ptr<connector::hive::HiveBucketProperty> bucketProperty = nullptr;
  if (writeRel.has_bucket_spec()) {
    const auto& bucketSpec = writeRel.bucket_spec();
    const auto& numBuckets = bucketSpec.num_buckets();

    std::vector<std::string> bucketedBy;
    for (const auto& name : bucketSpec.bucket_column_names()) {
      bucketedBy.emplace_back(name);
    }

    std::vector<TypePtr> bucketedTypes;
    bucketedTypes.reserve(bucketedBy.size());
    std::vector<TypePtr> tableColumnTypes = inputType->children();
    for (const auto& name : bucketedBy) {
      auto it = std::find(tableColumnNames.begin(), tableColumnNames.end(), name);
      VELOX_CHECK(it != tableColumnNames.end(), "Invalid bucket {}", name);
      std::size_t index = std::distance(tableColumnNames.begin(), it);
      bucketedTypes.emplace_back(tableColumnTypes[index]);
    }

    std::vector<std::shared_ptr<const connector::hive::HiveSortingColumn>> sortedBy;
    for (const auto& name : bucketSpec.sort_column_names()) {
      sortedBy.emplace_back(std::make_shared<connector::hive::HiveSortingColumn>(name, core::SortOrder{true, true}));
    }

    bucketProperty = std::make_shared<connector::hive::HiveBucketProperty>(
        connector::hive::HiveBucketProperty::Kind::kHiveCompatible, numBuckets, bucketedBy, bucketedTypes, sortedBy);
  }

  std::string writePath;
  if (writeFilesTempPath_.has_value()) {
    writePath = writeFilesTempPath_.value();
  } else {
    VELOX_CHECK(validationMode_, "WriteRel should have the write path before initializing the plan.");
    writePath = "";
  }

  std::string fileName;
  if (writeFileName_.has_value()) {
    fileName = writeFileName_.value();
  } else {
    VELOX_CHECK(validationMode_, "WriteRel should have the write path before initializing the plan.");
    fileName = "";
  }

  GLUTEN_CHECK(writeRel.named_table().has_advanced_extension(), "Advanced extension not found in WriteRel");
  const auto& ext = writeRel.named_table().advanced_extension();
  GLUTEN_CHECK(ext.has_optimization(), "Extension optimization not found in WriteRel");
  const auto& opt = ext.optimization();
  gluten::ConfigMap confMap;
  opt.UnpackTo(&confMap);
  std::unordered_map<std::string, std::string> writeConfs;
  for (const auto& item : *(confMap.mutable_configs())) {
    writeConfs.emplace(item.first, item.second);
  }

  // Currently only support parquet format.
  const std::string& formatShortName = writeConfs["format"];
  GLUTEN_CHECK(formatShortName == "parquet", "Unsupported file write format: " + formatShortName);
  dwio::common::FileFormat fileFormat = dwio::common::FileFormat::PARQUET;

  const std::shared_ptr<facebook::velox::parquet::WriterOptions> writerOptions = makeParquetWriteOption(writeConfs);
  // Spark's default compression code is snappy.
  const auto& compressionKind =
      writerOptions->compressionKind.value_or(common::CompressionKind::CompressionKind_SNAPPY);
  std::shared_ptr<core::InsertTableHandle> tableHandle = std::make_shared<core::InsertTableHandle>(
      kHiveConnectorId,
      makeHiveInsertTableHandle(
          tableColumnNames, /*inputType->names() clolumn name is different*/
          inputType->children(),
          partitionedKey,
          bucketProperty,
          makeLocationHandle(writePath, fileName, fileFormat, compressionKind, bucketProperty != nullptr),
          writerOptions,
          fileFormat,
          compressionKind));
  return std::make_shared<core::TableWriteNode>(
      nextPlanNodeId(),
      inputType,
      tableColumnNames,
      std::nullopt, /*columnStatsSpec*/
      tableHandle,
      (!partitionedKey.empty()),
      exec::TableWriteTraits::outputType(std::nullopt),
      connector::CommitStrategy::kNoCommit,
      childNode);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ExpandRel& expandRel) {
  core::PlanNodePtr childNode;
  if (expandRel.has_input()) {
    childNode = toVeloxPlan(expandRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in ExpandRel.");
  }

  const auto& inputType = childNode->outputType();

  std::vector<std::vector<core::TypedExprPtr>> projectSetExprs;
  projectSetExprs.reserve(expandRel.fields_size());

  for (const auto& projections : expandRel.fields()) {
    std::vector<core::TypedExprPtr> projectExprs;
    projectExprs.reserve(projections.switching_field().duplicates_size());

    for (const auto& projectExpr : projections.switching_field().duplicates()) {
      if (projectExpr.has_selection()) {
        auto expression = exprConverter_->toVeloxExpr(projectExpr.selection(), inputType);
        projectExprs.emplace_back(expression);
      } else if (projectExpr.has_literal()) {
        auto expression = exprConverter_->toVeloxExpr(projectExpr.literal());
        projectExprs.emplace_back(expression);
      } else {
        VELOX_FAIL("The project in Expand Operator only support field or literal.");
      }
    }
    projectSetExprs.emplace_back(projectExprs);
  }

  auto projectSize = expandRel.fields()[0].switching_field().duplicates_size();
  std::vector<std::string> names;
  names.reserve(projectSize);
  for (int idx = 0; idx < projectSize; idx++) {
    names.push_back(SubstraitParser::makeNodeName(planNodeId_, idx));
  }

  return std::make_shared<core::ExpandNode>(nextPlanNodeId(), projectSetExprs, std::move(names), childNode);
}

namespace {

void extractUnnestFieldExpr(
    std::shared_ptr<const core::PlanNode> child,
    int32_t index,
    std::vector<core::FieldAccessTypedExprPtr>& unnestFields) {
  if (auto projNode = std::dynamic_pointer_cast<const core::ProjectNode>(child)) {
    auto name = projNode->names()[index];
    auto expr = projNode->projections()[index];
    auto type = expr->type();

    auto unnestFieldExpr = std::make_shared<core::FieldAccessTypedExpr>(type, name);
    VELOX_CHECK_NOT_NULL(unnestFieldExpr, " the key in unnest Operator only support field");
    unnestFields.emplace_back(unnestFieldExpr);
  } else {
    auto name = child->outputType()->names()[index];
    auto field = child->outputType()->childAt(index);
    auto unnestFieldExpr = std::make_shared<core::FieldAccessTypedExpr>(field, name);
    unnestFields.emplace_back(unnestFieldExpr);
  }
}

} // namespace

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::GenerateRel& generateRel) {
  core::PlanNodePtr childNode;
  if (generateRel.has_input()) {
    childNode = toVeloxPlan(generateRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in GenerateRel.");
  }
  const auto& inputType = childNode->outputType();

  std::vector<core::FieldAccessTypedExprPtr> replicated;
  std::vector<core::FieldAccessTypedExprPtr> unnest;

  const auto& generator = generateRel.generator();
  const auto& requiredChildOutput = generateRel.child_output();

  replicated.reserve(requiredChildOutput.size());
  for (const auto& output : requiredChildOutput) {
    auto expression = exprConverter_->toVeloxExpr(output, inputType);
    auto exprField = dynamic_cast<const core::FieldAccessTypedExpr*>(expression.get());
    VELOX_CHECK(exprField != nullptr, " the output in Generate Operator only support field");

    replicated.emplace_back(std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression));
  }

  auto injectedProject = generateRel.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(generateRel.advanced_extension(), "injectedProject=");

  if (injectedProject) {
    // Child should be either ProjectNode or CudfValueStreamNode (GPU) in case of project fallback.
    VELOX_CHECK(
        (std::dynamic_pointer_cast<const core::ProjectNode>(childNode) != nullptr ||
         std::dynamic_pointer_cast<const core::TableScanNode>(childNode) != nullptr
#ifdef GLUTEN_ENABLE_GPU
         || std::dynamic_pointer_cast<const CudfValueStreamNode>(childNode) != nullptr
#endif
         ) &&
            childNode->outputType()->size() > requiredChildOutput.size(),
        "injectedProject is true, but the ProjectNode or TableScanNode or CudfValueStreamNode (in case of projection fallback)"
        " is missing or does not have the corresponding projection field");

    bool isStack = generateRel.has_advanced_extension() &&
        SubstraitParser::configSetInOptimization(generateRel.advanced_extension(), "isStack=");
    // Generator function's input is NOT a field reference.
    if (!isStack) {
      // For generator function which is not stack, e.g. explode(array(1,2,3)), a sample
      // input substrait plan is like the following:
      //
      //  Generate explode([1,2,3] AS _pre_0#129), false, [col#126]
      //  +- Project [fake_column#128, [1,2,3] AS _pre_0#129]
      //   +- RewrittenNodeWall Scan OneRowRelation[fake_column#128]
      // The last projection column in GeneratorRel's child(Project) is the column we need to unnest
      auto index = childNode->outputType()->size() - 1;
      extractUnnestFieldExpr(childNode, index, unnest);
    } else {
      // For stack function, e.g. stack(2, 1,2,3), a sample
      // input substrait plan is like the following:
      //
      // Generate stack(2, id#122, name#123, id1#124, name1#125), false, [col0#137, col1#138]
      // +- Project [id#122, name#123, id1#124, name1#125, array(id#122, id1#124) AS _pre_0#141, array(name#123,
      // name1#125) AS _pre_1#142]
      //   +- RewrittenNodeWall LocalTableScan [id#122, name#123, id1#124, name1#125]
      //
      // The last `numFields` projections are the fields we want to unnest.
      auto generatorFunc = generator.scalar_function();
      auto numRows = SubstraitParser::getLiteralValue<int32_t>(generatorFunc.arguments(0).value().literal());
      auto numFields = static_cast<int32_t>(std::ceil((generatorFunc.arguments_size() - 1.0) / numRows));
      auto totalProjectCount = childNode->outputType()->size();

      for (auto i = totalProjectCount - numFields; i < totalProjectCount; ++i) {
        extractUnnestFieldExpr(childNode, i, unnest);
      }
    }
  } else {
    // Generator function's input is a field reference, e.g. explode(col), generator
    // function's first argument is the field reference we need to unnest.
    // This assumption holds for all the supported generator function:
    // explode, posexplode, inline.
    auto generatorFunc = generator.scalar_function();
    auto unnestExpr = exprConverter_->toVeloxExpr(generatorFunc.arguments(0).value(), inputType);
    auto unnestFieldExpr = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(unnestExpr);
    VELOX_CHECK_NOT_NULL(unnestFieldExpr, " the key in unnest Operator only support field");
    unnest.emplace_back(unnestFieldExpr);
  }

  std::vector<std::string> unnestNames;
  int unnestIndex = 0;
  for (const auto& variable : unnest) {
    if (variable->type()->isArray()) {
      unnestNames.emplace_back(SubstraitParser::makeNodeName(planNodeId_, unnestIndex++));
    } else if (variable->type()->isMap()) {
      unnestNames.emplace_back(SubstraitParser::makeNodeName(planNodeId_, unnestIndex++));
      unnestNames.emplace_back(SubstraitParser::makeNodeName(planNodeId_, unnestIndex++));
    } else {
      VELOX_FAIL(
          "Unexpected type of unnest variable. Expected ARRAY or MAP, but got {}.", variable->type()->toString());
    }
  }

  std::optional<std::string> ordinalityName = std::nullopt;
  std::optional<std::string> markerName = std::nullopt;
  if (generateRel.has_advanced_extension()) {
    if (SubstraitParser::configSetInOptimization(generateRel.advanced_extension(), "isPosExplode=")) {
      ordinalityName = std::make_optional<std::string>("pos");
    }
    if (SubstraitParser::configSetInOptimization(generateRel.advanced_extension(), "isOuter=")) {
      markerName = std::make_optional<std::string>("marker");
    }
  }

  return std::make_shared<core::UnnestNode>(
      nextPlanNodeId(), replicated, unnest, std::move(unnestNames), ordinalityName, markerName, childNode);
}

const core::WindowNode::Frame SubstraitToVeloxPlanConverter::createWindowFrame(
    const ::substrait::Expression_WindowFunction_Bound& lower_bound,
    const ::substrait::Expression_WindowFunction_Bound& upper_bound,
    const ::substrait::WindowType& type,
    const RowTypePtr& inputType) {
  core::WindowNode::Frame frame;
  switch (type) {
    case ::substrait::WindowType::ROWS:
      frame.type = core::WindowNode::WindowType::kRows;
      break;
    case ::substrait::WindowType::RANGE:
      frame.type = core::WindowNode::WindowType::kRange;
      break;
    default:
      VELOX_FAIL("the window type only support ROWS and RANGE, and the input type is ", std::to_string(type));
  }

  auto specifiedBound =
      [&](bool hasOffset, int64_t offset, const ::substrait::Expression& columnRef) -> core::TypedExprPtr {
    if (hasOffset) {
      VELOX_CHECK(
          frame.type != core::WindowNode::WindowType::kRange,
          "for RANGE frame offset, we should pre-calculate the range frame boundary and pass the column reference, but got a constant offset.");
      return std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(offset));
    } else {
      VELOX_CHECK(
          frame.type != core::WindowNode::WindowType::kRows, "for ROW frame offset, we should pass a constant offset.");
      return exprConverter_->toVeloxExpr(columnRef, inputType);
    }
  };

  auto boundTypeConversion = [&](::substrait::Expression_WindowFunction_Bound boundType)
      -> std::tuple<core::WindowNode::BoundType, core::TypedExprPtr> {
    if (boundType.has_current_row()) {
      return std::make_tuple(core::WindowNode::BoundType::kCurrentRow, nullptr);
    } else if (boundType.has_unbounded_following()) {
      return std::make_tuple(core::WindowNode::BoundType::kUnboundedFollowing, nullptr);
    } else if (boundType.has_unbounded_preceding()) {
      return std::make_tuple(core::WindowNode::BoundType::kUnboundedPreceding, nullptr);
    } else if (boundType.has_following()) {
      auto following = boundType.following();
      return std::make_tuple(
          core::WindowNode::BoundType::kFollowing,
          specifiedBound(following.has_offset(), following.offset(), following.ref()));
    } else if (boundType.has_preceding()) {
      auto preceding = boundType.preceding();
      return std::make_tuple(
          core::WindowNode::BoundType::kPreceding,
          specifiedBound(preceding.has_offset(), preceding.offset(), preceding.ref()));
    } else {
      VELOX_FAIL("The BoundType is not supported.");
    }
  };
  std::tie(frame.startType, frame.startValue) = boundTypeConversion(lower_bound);
  std::tie(frame.endType, frame.endValue) = boundTypeConversion(upper_bound);
  return frame;
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::WindowRel& windowRel) {
  core::PlanNodePtr childNode;
  if (windowRel.has_input()) {
    childNode = toVeloxPlan(windowRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in WindowRel.");
  }

  const auto& inputType = childNode->outputType();

  // Parse measures and get the window expressions.
  // Each measure represents one window expression.
  std::vector<core::WindowNode::Function> windowNodeFunctions;
  std::vector<std::string> windowColumnNames;

  windowNodeFunctions.reserve(windowRel.measures().size());
  for (const auto& smea : windowRel.measures()) {
    const auto& windowFunction = smea.measure();
    std::string funcName = SubstraitParser::findVeloxFunction(functionMap_, windowFunction.function_reference());
    std::vector<core::TypedExprPtr> windowParams;
    auto& argumentList = windowFunction.arguments();
    windowParams.reserve(argumentList.size());
    const auto& options = windowFunction.options();
    // For functions in kOffsetWindowFunctions (see Spark OffsetWindowFunctions),
    // we expect the first option name is `ignoreNulls` if ignoreNulls is true.
    bool ignoreNulls = false;
    if (!options.empty() && options.at(0).name() == "ignoreNulls") {
      ignoreNulls = true;
    }
    for (const auto& arg : argumentList) {
      windowParams.emplace_back(exprConverter_->toVeloxExpr(arg.value(), inputType));
    }
    auto windowVeloxType = SubstraitParser::parseType(windowFunction.output_type());
    auto windowCall = std::make_shared<const core::CallTypedExpr>(windowVeloxType, std::move(windowParams), funcName);
    auto upperBound = windowFunction.upper_bound();
    auto lowerBound = windowFunction.lower_bound();
    auto type = windowFunction.window_type();

    windowColumnNames.push_back(windowFunction.column_name());

    windowNodeFunctions.push_back(
        {std::move(windowCall), createWindowFrame(lowerBound, upperBound, type, inputType), ignoreNulls});
  }

  // Construct partitionKeys
  std::vector<core::FieldAccessTypedExprPtr> partitionKeys;
  std::unordered_set<std::string> keyNames;
  const auto& partitions = windowRel.partition_expressions();
  partitionKeys.reserve(partitions.size());
  for (const auto& partition : partitions) {
    auto expression = exprConverter_->toVeloxExpr(partition, inputType);
    core::FieldAccessTypedExprPtr veloxPartitionKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression);
    VELOX_USER_CHECK_NOT_NULL(veloxPartitionKey, "Window Operator only supports field partition key.");
    // Constructs unique parition keys.
    if (keyNames.insert(veloxPartitionKey->name()).second) {
      partitionKeys.emplace_back(veloxPartitionKey);
    }
  }
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  const auto& [rawSortingKeys, rawSortingOrders] = processSortField(windowRel.sorts(), inputType);
  for (vector_size_t i = 0; i < rawSortingKeys.size(); ++i) {
    // Constructs unique sort keys and excludes keys overlapped with partition keys.
    if (keyNames.insert(rawSortingKeys[i]->name()).second) {
      sortingKeys.emplace_back(rawSortingKeys[i]);
      sortingOrders.emplace_back(rawSortingOrders[i]);
    }
  }

  return std::make_shared<core::WindowNode>(
      nextPlanNodeId(),
      partitionKeys,
      sortingKeys,
      sortingOrders,
      windowColumnNames,
      windowNodeFunctions,
      true /*inputsSorted*/,
      childNode);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(
    const ::substrait::WindowGroupLimitRel& windowGroupLimitRel) {
  core::PlanNodePtr childNode;
  if (windowGroupLimitRel.has_input()) {
    childNode = toVeloxPlan(windowGroupLimitRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in WindowGroupLimitRel.");
  }
  const auto& inputType = childNode->outputType();
  // Construct partitionKeys
  std::vector<core::FieldAccessTypedExprPtr> partitionKeys;
  std::unordered_set<std::string> keyNames;
  const auto& partitions = windowGroupLimitRel.partition_expressions();
  partitionKeys.reserve(partitions.size());
  for (const auto& partition : partitions) {
    auto expression = exprConverter_->toVeloxExpr(partition, inputType);
    core::FieldAccessTypedExprPtr veloxPartitionKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression);
    VELOX_USER_CHECK_NOT_NULL(veloxPartitionKey, "Window Group Limit Operator only supports field partition key.");
    // Constructs unique partition keys.
    if (keyNames.insert(veloxPartitionKey->name()).second) {
      partitionKeys.emplace_back(veloxPartitionKey);
    }
  }
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  const auto& [rawSortingKeys, rawSortingOrders] = processSortField(windowGroupLimitRel.sorts(), inputType);
  for (vector_size_t i = 0; i < rawSortingKeys.size(); ++i) {
    // Constructs unique sort keys and excludes keys overlapped with partition keys.
    if (keyNames.insert(rawSortingKeys[i]->name()).second) {
      sortingKeys.emplace_back(rawSortingKeys[i]);
      sortingOrders.emplace_back(rawSortingOrders[i]);
    }
  }
  const std::optional<std::string> rowNumberColumnName = std::nullopt;

  if (sortingKeys.empty()) {
    // Handle if all sorting keys are also used as partition keys.

    return std::make_shared<core::RowNumberNode>(
        nextPlanNodeId(),
        partitionKeys,
        rowNumberColumnName,
        static_cast<int32_t>(windowGroupLimitRel.limit()),
        childNode);
  }

  auto windowFunc = core::TopNRowNumberNode::RankFunction::kRowNumber;
  if (windowGroupLimitRel.has_advanced_extension()) {
    if (SubstraitParser::checkWindowFunction(windowGroupLimitRel.advanced_extension(), "rank")) {
      windowFunc = core::TopNRowNumberNode::RankFunction::kRank;
    } else if (SubstraitParser::checkWindowFunction(windowGroupLimitRel.advanced_extension(), "dense_rank")) {
      windowFunc = core::TopNRowNumberNode::RankFunction::kDenseRank;
    }
  }

  return std::make_shared<core::TopNRowNumberNode>(
      nextPlanNodeId(),
      windowFunc,
      partitionKeys,
      sortingKeys,
      sortingOrders,
      rowNumberColumnName,
      static_cast<int32_t>(windowGroupLimitRel.limit()),
      childNode);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::SetRel& setRel) {
  switch (setRel.op()) {
    case ::substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL: {
      std::vector<core::PlanNodePtr> children;
      for (int32_t i = 0; i < setRel.inputs_size(); ++i) {
        const auto& input = setRel.inputs(i);
        children.push_back(toVeloxPlan(input));
      }
      GLUTEN_CHECK(!children.empty(), "At least one source is required for Velox LocalPartition");

      // Velox doesn't allow different field names in schemas of LocalPartitionNode's children.
      // Add project nodes to unify the schemas.
      const RowTypePtr outRowType = asRowType(children[0]->outputType());
      std::vector<std::string> outNames;
      for (int32_t colIdx = 0; colIdx < outRowType->size(); ++colIdx) {
        // Using field names from the unified output row type instead child type names
        const auto name = outRowType->nameOf(colIdx);
        outNames.push_back(name);
      }

      std::vector<core::PlanNodePtr> projectedChildren;
      for (int32_t i = 0; i < children.size(); ++i) {
        const auto& child = children[i];
        const RowTypePtr& childRowType = child->outputType();
        std::vector<core::TypedExprPtr> expressions;
        for (int32_t colIdx = 0; colIdx < outNames.size(); ++colIdx) {
          const auto fa =
              std::make_shared<core::FieldAccessTypedExpr>(childRowType->childAt(colIdx), childRowType->nameOf(colIdx));
          const auto cast = std::make_shared<core::CastTypedExpr>(outRowType->childAt(colIdx), fa, false);
          expressions.push_back(cast);
        }
        auto project = std::make_shared<core::ProjectNode>(nextPlanNodeId(), outNames, expressions, child);
        projectedChildren.push_back(project);
      }
      return std::make_shared<core::LocalPartitionNode>(
          nextPlanNodeId(),
          core::LocalPartitionNode::Type::kGather,
          false,
          std::make_shared<core::GatherPartitionFunctionSpec>(),
          projectedChildren);
    }
    default:
      throw GlutenException("Unsupported SetRel op: " + std::to_string(setRel.op()));
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::SortRel& sortRel) {
  auto childNode = convertSingleInput<::substrait::SortRel>(sortRel);
  auto [sortingKeys, sortingOrders] = processSortField(sortRel.sorts(), childNode->outputType());
  return std::make_shared<core::OrderByNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, false /*isPartial*/, childNode);
}

std::pair<std::vector<core::FieldAccessTypedExprPtr>, std::vector<core::SortOrder>>
SubstraitToVeloxPlanConverter::processSortField(
    const ::google::protobuf::RepeatedPtrField<::substrait::SortField>& sortFields,
    const RowTypePtr& inputType) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  std::unordered_set<std::string> uniqueKeys;
  for (const auto& sort : sortFields) {
    GLUTEN_CHECK(sort.has_expr(), "Sort field must have expr");
    auto expression = exprConverter_->toVeloxExpr(sort.expr(), inputType);
    auto fieldExpr = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression);
    VELOX_USER_CHECK_NOT_NULL(fieldExpr, "Sort Operator only supports field sorting key");
    if (uniqueKeys.insert(fieldExpr->name()).second) {
      sortingKeys.emplace_back(fieldExpr);
      sortingOrders.emplace_back(toSortOrder(sort));
    }
  }
  return {sortingKeys, sortingOrders};
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::FilterRel& filterRel) {
  auto childNode = convertSingleInput<::substrait::FilterRel>(filterRel);
  auto filterNode = std::make_shared<core::FilterNode>(
      nextPlanNodeId(), exprConverter_->toVeloxExpr(filterRel.condition(), childNode->outputType()), childNode);

  if (filterRel.has_common()) {
    return processEmit(filterRel.common(), std::move(filterNode));
  } else {
    return filterNode;
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::FetchRel& fetchRel) {
  auto childNode = convertSingleInput<::substrait::FetchRel>(fetchRel);
  return std::make_shared<core::LimitNode>(
      nextPlanNodeId(),
      static_cast<int32_t>(fetchRel.offset()),
      static_cast<int32_t>(fetchRel.count()),
      false /*isPartial*/,
      childNode);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::TopNRel& topNRel) {
  auto childNode = convertSingleInput<::substrait::TopNRel>(topNRel);
  auto [sortingKeys, sortingOrders] = processSortField(topNRel.sorts(), childNode->outputType());
  return std::make_shared<core::TopNNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, static_cast<int32_t>(topNRel.n()), false /*isPartial*/, childNode);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::constructValueStreamNode(
    const ::substrait::ReadRel& readRel,
    int32_t streamIdx) {
  // Use TableScanNode with iterator connector for runtime iterator inputs
  // Get output schema from ReadRel
  uint64_t colNum = 0;
  std::vector<TypePtr> veloxTypeList;
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    colNum = baseSchema.names().size();
    veloxTypeList = SubstraitParser::parseNamedStruct(baseSchema);
  }

  auto nodeId = ValueStreamConnectorFactory::nodeIdOf(streamIdx);
  std::vector<std::string> outNames;
  outNames.reserve(colNum);
  for (int idx = 0; idx < colNum; idx++) {
    // TODO: We'd use the designated names in readRel rather than assigning new names.
    auto colName = fmt::format("node_{}_{}", nodeId, idx);
    outNames.emplace_back(colName);
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));

  // Create TableHandle
  bool dynamicFilterEnabled =
      veloxCfg_->get<bool>(kValueStreamDynamicFilterEnabled, kValueStreamDynamicFilterEnabledDefault);
  auto tableHandle = std::make_shared<ValueStreamTableHandle>(kIteratorConnectorId, dynamicFilterEnabled);

  // Create column assignments
  connector::ColumnHandleMap assignments;
  for (int idx = 0; idx < outputType->size(); idx++) {
    auto name = outputType->nameOf(idx);
    auto type = outputType->childAt(idx);
    assignments[name] = std::make_shared<ValueStreamColumnHandle>(name, type);
  }

  // Create TableScanNode
  auto tableScanNode = std::make_shared<core::TableScanNode>(nodeId, outputType, tableHandle, assignments);

  // Mark this as a stream-based split
  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->leafType = SplitInfo::LeafType::SPLIT_AWARE_STREAM;
  splitInfoMap_[tableScanNode->id()] = splitInfo;

  return tableScanNode;
}

#ifdef GLUTEN_ENABLE_GPU
core::PlanNodePtr SubstraitToVeloxPlanConverter::constructCudfValueStreamNode(
    const ::substrait::ReadRel& readRel,
    int32_t streamIdx) {
  // Get the input schema of this iterator.
  uint64_t colNum = 0;
  std::vector<TypePtr> veloxTypeList;
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    // Input names is not used. Instead, new input/output names will be created
    // because the ValueStreamNode in Velox does not support name change.
    colNum = baseSchema.names().size();
    veloxTypeList = SubstraitParser::parseNamedStruct(baseSchema);
  }

  std::vector<std::string> outNames;
  outNames.reserve(colNum);
  for (int idx = 0; idx < colNum; idx++) {
    auto colName = SubstraitParser::makeNodeName(planNodeId_, idx);
    outNames.emplace_back(colName);
  }

  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  std::shared_ptr<ResultIterator> iterator;
  if (!validationMode_) {
    VELOX_CHECK_LT(streamIdx, inputIters_.size(), "Could not find stream index {} in input iterator list.", streamIdx);
    iterator = std::move(inputIters_[streamIdx]);
  }
  auto node = std::make_shared<CudfValueStreamNode>(nextPlanNodeId(), outputType, std::move(iterator));

  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->leafType = SplitInfo::LeafType::TRIVIAL_LEAF;
  splitInfoMap_[node->id()] = splitInfo;
  return node;
}
#endif

core::PlanNodePtr SubstraitToVeloxPlanConverter::constructValuesNode(
    const ::substrait::ReadRel& readRel,
    int32_t streamIdx) {
  // ValuesNode is only used for validation/benchmarking with query trace
  // It loads all data from the iterator at plan construction time
  VELOX_CHECK_LT(streamIdx, inputIters_.size(), "Could not find stream index {} in input iterator list.", streamIdx);
  const auto iter = std::move(inputIters_[streamIdx]);
  std::vector<RowVectorPtr> rowVectors;
  while (iter->hasNext()) {
    auto batch = iter->next();
    auto veloxBatch = VeloxColumnarBatch::from(defaultLeafVeloxMemoryPool().get(), batch);
    rowVectors.emplace_back(veloxBatch->getRowVector());
  }
  auto node = std::make_shared<facebook::velox::core::ValuesNode>(nextPlanNodeId(), std::move(rowVectors));
  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->leafType = SplitInfo::LeafType::TRIVIAL_LEAF;
  splitInfoMap_[node->id()] = splitInfo;
  return node;
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ReadRel& readRel) {
  // emit is not allowed in TableScanNode and ValuesNode related
  // outputs
  if (readRel.has_common()) {
    VELOX_USER_CHECK(
        !readRel.common().has_emit(), "Emit not supported for ValuesNode and TableScanNode related Substrait plans.");
  }

  auto streamIdx = getStreamIndex(readRel);
  if (streamIdx >= 0) {
    // Check if the ReadRel specifies an input of stream. If yes, build TableScanNode with iterator connector.
    const bool isQueryTraceEnabled = veloxCfg_->get<bool>(kQueryTraceEnabled, false);
    if (isQueryTraceEnabled) {
      // Only used in benchmark enable query trace, replace ValueStreamNode to ValuesNode to support serialization.
      return constructValuesNode(readRel, streamIdx);
    }
#ifdef GLUTEN_ENABLE_GPU
    if (veloxCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
      return constructCudfValueStreamNode(readRel, streamIdx);
    }
#endif
    return constructValueStreamNode(readRel, streamIdx);
  }

  // Otherwise, will create TableScan node for ReadRel.
  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->leafType = SplitInfo::LeafType::TABLE_SCAN;
  if (!validationMode_) {
    VELOX_CHECK_LT(splitInfoIdx_, splitInfos_.size(), "Plan must have readRel and related split info.");
    splitInfo = splitInfos_[splitInfoIdx_++];
  }

  // Get output names and types.
  std::vector<std::string> colNameList;
  std::vector<TypePtr> veloxTypeList;
  std::vector<ColumnType> columnTypes;
  // Convert field names into lower case when not case-sensitive.
  bool asLowerCase = !veloxCfg_->get<bool>(kCaseSensitive, false);
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    colNameList.reserve(baseSchema.names().size());
    for (const auto& name : baseSchema.names()) {
      std::string fieldName = name;
      if (asLowerCase) {
        folly::toLowerAscii(fieldName);
      }
      colNameList.emplace_back(fieldName);
    }
    veloxTypeList = SubstraitParser::parseNamedStruct(baseSchema, asLowerCase);
    SubstraitParser::parseColumnTypes(baseSchema, columnTypes);
  }

  auto names = colNameList;
  auto types = veloxTypeList;

  // The columns we project from the file.
  auto baseSchema = ROW(std::move(names), std::move(types));
  // The columns present in the table, if not available default to the baseSchema.
  auto tableSchema = splitInfo->tableSchema ? splitInfo->tableSchema : baseSchema;

  // Build dataColumns from tableSchema, excluding partition columns.
  // HiveTableHandle::dataColumns() is used as fileSchema for the reader.
  // Partition columns should not be validated against the file's physical types
  // (their values come from the partition path, not from the file).
  std::unordered_set<std::string> partitionColNames;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    if (columnTypes[idx] == ColumnType::kPartitionKey) {
      partitionColNames.insert(colNameList[idx]);
    }
  }
  RowTypePtr dataColumns;
  if (partitionColNames.empty()) {
    dataColumns = tableSchema;
  } else {
    std::vector<std::string> dataColNames;
    std::vector<TypePtr> dataColTypes;
    for (int idx = 0; idx < tableSchema->size(); idx++) {
      if (partitionColNames.find(tableSchema->nameOf(idx)) == partitionColNames.end()) {
        dataColNames.push_back(tableSchema->nameOf(idx));
        dataColTypes.push_back(tableSchema->childAt(idx));
      }
    }
    dataColumns = ROW(std::move(dataColNames), std::move(dataColTypes));
  }

  connector::ConnectorTableHandlePtr tableHandle;
  auto remainingFilter = readRel.has_filter() ? exprConverter_->toVeloxExpr(readRel.filter(), baseSchema) : nullptr;
  auto connectorId = kHiveConnectorId;
  if (useCudfTableHandle(splitInfos_) && veloxCfg_->get<bool>(kCudfEnableTableScan, kCudfEnableTableScanDefault) &&
      veloxCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
#ifdef GLUTEN_ENABLE_GPU
    connectorId = kCudfHiveConnectorId;
#endif
  }
  common::SubfieldFilters subfieldFilters;
  tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      connectorId, "hive_table", std::move(subfieldFilters), remainingFilter, dataColumns);

  // Get assignments and out names.
  std::vector<std::string> outNames;
  outNames.reserve(colNameList.size());
  connector::ColumnHandleMap assignments;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    auto outName = SubstraitParser::makeNodeName(planNodeId_, idx);
    auto columnType = columnTypes[idx];
    assignments[outName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colNameList[idx], columnType, veloxTypeList[idx], veloxTypeList[idx]);
    outNames.emplace_back(outName);
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));

  if (readRel.has_virtual_table()) {
    return toVeloxPlan(readRel, outputType);
  } else {
    auto tableScanNode = std::make_shared<core::TableScanNode>(
        nextPlanNodeId(), std::move(outputType), std::move(tableHandle), assignments);
    // Set split info map.
    splitInfoMap_[tableScanNode->id()] = splitInfo;
    return tableScanNode;
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& readRel,
    const RowTypePtr& type) {
  ::substrait::ReadRel_VirtualTable readVirtualTable = readRel.virtual_table();
  int64_t numVectors = readVirtualTable.values_size();
  int64_t numColumns = type->size();
  int64_t valueFieldNums = readVirtualTable.values(numVectors - 1).fields_size();
  std::vector<RowVectorPtr> vectors;
  vectors.reserve(numVectors);

  int64_t batchSize;
  // For the empty vectors, eg,vectors = makeRowVector(ROW({}, {}), 1).
  if (numColumns == 0) {
    batchSize = 1;
  } else {
    batchSize = valueFieldNums / numColumns;
  }

  for (int64_t index = 0; index < numVectors; ++index) {
    std::vector<VectorPtr> children;
    ::substrait::Expression_Literal_Struct rowValue = readRel.virtual_table().values(index);
    auto fieldSize = rowValue.fields_size();
    VELOX_CHECK_EQ(fieldSize, batchSize * numColumns);

    for (int64_t col = 0; col < numColumns; ++col) {
      const TypePtr& outputChildType = type->childAt(col);
      std::vector<variant> batchChild;
      batchChild.reserve(batchSize);
      for (int64_t batchId = 0; batchId < batchSize; batchId++) {
        // each value in the batch
        auto fieldIdx = col * batchSize + batchId;
        ::substrait::Expression_Literal field = rowValue.fields(fieldIdx);

        auto expr = exprConverter_->toVeloxExpr(field);
        if (auto constantExpr = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
          if (!constantExpr->hasValueVector()) {
            batchChild.emplace_back(constantExpr->value());
          } else {
            VELOX_UNSUPPORTED("Values node with complex type values is not supported yet");
          }
        } else {
          VELOX_FAIL("Expected constant expression");
        }
      }
      children.emplace_back(setVectorFromVariants(outputChildType, batchChild, pool_));
    }

    vectors.emplace_back(std::make_shared<RowVector>(pool_, type, nullptr, batchSize, children));
  }

  return std::make_shared<core::ValuesNode>(nextPlanNodeId(), std::move(vectors));
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::Rel& rel) {
  if (rel.has_aggregate()) {
    return toVeloxPlan(rel.aggregate());
  } else if (rel.has_project()) {
    return toVeloxPlan(rel.project());
  } else if (rel.has_filter()) {
    return toVeloxPlan(rel.filter());
  } else if (rel.has_join()) {
    return toVeloxPlan(rel.join());
  } else if (rel.has_cross()) {
    return toVeloxPlan(rel.cross());
  } else if (rel.has_read()) {
    return toVeloxPlan(rel.read());
  } else if (rel.has_sort()) {
    return toVeloxPlan(rel.sort());
  } else if (rel.has_expand()) {
    return toVeloxPlan(rel.expand());
  } else if (rel.has_generate()) {
    return toVeloxPlan(rel.generate());
  } else if (rel.has_fetch()) {
    return toVeloxPlan(rel.fetch());
  } else if (rel.has_top_n()) {
    return toVeloxPlan(rel.top_n());
  } else if (rel.has_window()) {
    return toVeloxPlan(rel.window());
  } else if (rel.has_write()) {
    return toVeloxPlan(rel.write());
  } else if (rel.has_windowgrouplimit()) {
    return toVeloxPlan(rel.windowgrouplimit());
  } else if (rel.has_set()) {
    return toVeloxPlan(rel.set());
  } else {
    VELOX_NYI("Substrait conversion not supported for Rel.");
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::RelRoot& root) {
  // TODO: Use the names as the output names for the whole computing.
  // const auto& names = root.names();
  if (root.has_input()) {
    const auto& rel = root.input();
    return toVeloxPlan(rel);
  } else {
    VELOX_FAIL("Input is expected in RelRoot.");
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::Plan& substraitPlan) {
  VELOX_CHECK(checkTypeExtension(substraitPlan), "The type extension only have unknown type.");
  // Construct the function map based on the Substrait representation,
  // and initialize the expression converter with it.
  constructFunctionMap(substraitPlan);

  // In fact, only one RelRoot or Rel is expected here.
  VELOX_CHECK_EQ(substraitPlan.relations_size(), 1);
  const auto& rel = substraitPlan.relations(0);
  if (rel.has_root()) {
    return toVeloxPlan(rel.root());
  } else if (rel.has_rel()) {
    return toVeloxPlan(rel.rel());
  } else {
    VELOX_FAIL("RelRoot or Rel is expected in Plan.");
  }
}

std::string SubstraitToVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

void SubstraitToVeloxPlanConverter::constructFunctionMap(const ::substrait::Plan& substraitPlan) {
  // Construct the function map based on the Substrait representation.
  for (const auto& extension : substraitPlan.extensions()) {
    if (!extension.has_extension_function()) {
      continue;
    }
    const auto& sFmap = extension.extension_function();
    auto id = sFmap.function_anchor();
    auto name = sFmap.name();
    functionMap_[id] = name;
  }
  exprConverter_ = std::make_unique<SubstraitVeloxExprConverter>(pool_, functionMap_);
}

void SubstraitToVeloxPlanConverter::constructFunctionMap(std::unordered_map<uint64_t, std::string> substraitPlan) {
  functionMap_ = std::move(substraitPlan);
  exprConverter_ = std::make_unique<SubstraitVeloxExprConverter>(pool_, functionMap_);
}

std::string SubstraitToVeloxPlanConverter::findFuncSpec(uint64_t id) {
  return SubstraitParser::findFunctionSpec(functionMap_, id);
}

int32_t SubstraitToVeloxPlanConverter::getStreamIndex(const ::substrait::ReadRel& sRead) {
  if (sRead.has_local_files()) {
    const auto& fileList = sRead.local_files().items();
    if (fileList.size() == 0) {
      // bucketed scan may contains empty file list
      return -1;
    }
    // The stream input will be specified with the format of
    // "iterator:${index}".
    std::string filePath = fileList[0].uri_file();
    std::string prefix = "iterator:";
    std::size_t pos = filePath.find(prefix);
    if (pos == std::string::npos) {
      return -1;
    }

    // Get the index.
    std::string idxStr = filePath.substr(pos + prefix.size(), filePath.size());
    try {
      return stoi(idxStr);
    } catch (const std::exception& err) {
      VELOX_FAIL(err.what());
    }
  }
  return -1;
}

void SubstraitToVeloxPlanConverter::extractJoinKeys(
    const ::substrait::Expression& joinExpression,
    std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
    std::vector<const ::substrait::Expression::FieldReference*>& rightExprs) {
  std::stack<const ::substrait::Expression*> expressions;
  expressions.push(&joinExpression);
  while (!expressions.empty()) {
    auto visited = expressions.top();
    expressions.pop();
    if (visited->rex_type_case() == ::substrait::Expression::RexTypeCase::kScalarFunction) {
      const auto& funcName = SubstraitParser::getNameBeforeDelimiter(
          SubstraitParser::findVeloxFunction(functionMap_, visited->scalar_function().function_reference()));
      const auto& args = visited->scalar_function().arguments();
      if (funcName == "and") {
        expressions.push(&args[1].value());
        expressions.push(&args[0].value());
      } else if (funcName == "eq" || funcName == "equalto" || funcName == "decimal_equalto") {
        VELOX_CHECK(std::all_of(args.cbegin(), args.cend(), [](const ::substrait::FunctionArgument& arg) {
          return arg.value().has_selection();
        }));
        leftExprs.push_back(&args[0].value().selection());
        rightExprs.push_back(&args[1].value().selection());
      } else {
        VELOX_NYI("Join condition {} not supported.", funcName);
      }
    } else {
      VELOX_FAIL("Unable to parse from join expression: {}", joinExpression.DebugString());
    }
  }
}

bool SubstraitToVeloxPlanConverter::checkTypeExtension(const ::substrait::Plan& substraitPlan) {
  for (const auto& sExtension : substraitPlan.extensions()) {
    if (!sExtension.has_extension_type()) {
      continue;
    }

    // Only support UNKNOWN type in UserDefined type extension.
    if (sExtension.extension_type().name() != "UNKNOWN") {
      return false;
    }
  }
  return true;
}

} // namespace gluten
