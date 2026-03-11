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

#include "SubstraitToVeloxExpr.h"
#include "TypeUtils.h"
#include "velox/connectors/hive/FileProperties.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"

namespace gluten {
class ResultIterator;

struct SplitInfo {
  enum class LeafType {
    /// A streaming node that accepts iterator splits.
    SPLIT_AWARE_STREAM = 0,
    /// A table scan node that accepts scan splits.
    TABLE_SCAN = 1,
    /// A leaf node that doesn't rely on splits.
    TRIVIAL_LEAF = 2
  };

  /// The type of the associated Velox leaf query plan node.
  LeafType leafType = LeafType::TRIVIAL_LEAF;

  /// The Partition index.
  u_int32_t partitionIndex;

  /// The partition columns associated with partitioned table.
  std::vector<std::unordered_map<std::string, std::string>> partitionColumns;

  /// The metadata columns associated with partitioned table.
  std::vector<std::unordered_map<std::string, std::string>> metadataColumns;

  /// The file paths to be scanned.
  std::vector<std::string> paths;

  /// The file starts in the scan.
  std::vector<u_int64_t> starts;

  /// The lengths to be scanned.
  std::vector<u_int64_t> lengths;

  /// The file format of the files to be scanned.
  dwio::common::FileFormat format;

  /// The file sizes and modification times of the files to be scanned.
  std::vector<std::optional<facebook::velox::FileProperties>> properties;

  /// The schema of the table being scanned.
  RowTypePtr tableSchema;

  /// Make SplitInfo polymorphic
  virtual ~SplitInfo() = default;

  bool canUseCudfConnector();
};

/// This class is used to convert the Substrait plan into Velox plan.
class SubstraitToVeloxPlanConverter {
 public:
  explicit SubstraitToVeloxPlanConverter(
      memory::MemoryPool* pool,
      const facebook::velox::config::ConfigBase* veloxCfg,
      const std::vector<std::shared_ptr<ResultIterator>>& inputIters,
      const std::optional<std::string> writeFilesTempPath = std::nullopt,
      const std::optional<std::string> writeFileName = std::nullopt,
      bool validationMode = false)
      : pool_(pool),
        veloxCfg_(veloxCfg),
        inputIters_(inputIters),
        writeFilesTempPath_(writeFilesTempPath),
        writeFileName_(writeFileName),
        validationMode_(validationMode) {
    VELOX_USER_CHECK_NOT_NULL(veloxCfg_);
  }

  /// Used to convert Substrait WriteRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WriteRel& writeRel);

  /// Used to convert Substrait ExpandRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ExpandRel& expandRel);

  /// Used to convert Substrait GenerateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::GenerateRel& generateRel);

  /// Used to convert Substrait WindowRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WindowRel& windowRel);

  /// Used to convert Substrait WindowGroupLimitRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WindowGroupLimitRel& windowGroupLimitRel);

  /// Used to convert Substrait SetRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::SetRel& setRel);

  /// Used to convert Substrait JoinRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::JoinRel& joinRel);

  /// Used to convert Substrait CrossRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::CrossRel& crossRel);

  /// Used to convert Substrait AggregateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::AggregateRel& aggRel);

  /// Convert Substrait ProjectRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ProjectRel& projectRel);

  /// Convert Substrait FilterRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FilterRel& filterRel);

  /// Convert Substrait FetchRel into Velox LimitNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FetchRel& fetchRel);

  /// Convert Substrait TopNRel into Velox TopNNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::TopNRel& topNRel);

  /// Convert Substrait ReadRel into Velox Values Node.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& readRel, const RowTypePtr& type);

  /// Convert Substrait SortRel into Velox OrderByNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::SortRel& sortRel);

  /// Convert Substrait ReadRel into Velox PlanNode.
  /// Index: the index of the partition this item belongs to.
  /// Starts: the start positions in byte to read from the items.
  /// Lengths: the lengths in byte to read from the items.
  /// FileProperties: the file sizes and modification times of the files to be scanned.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& sRead);

  // Construct a table scan node accepting value streams as input.
  core::PlanNodePtr constructValueStreamNode(const ::substrait::ReadRel& sRead, int32_t streamIdx);

  // Construct a cuDF value stream node.
  core::PlanNodePtr constructCudfValueStreamNode(const ::substrait::ReadRel& sRead, int32_t streamIdx);

  // This is only used in benchmark and enable query trace, which will load all the data to ValuesNode.
  core::PlanNodePtr constructValuesNode(const ::substrait::ReadRel& sRead, int32_t streamIdx);

  /// Used to convert Substrait Rel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::Rel& sRel);

  /// Used to convert Substrait RelRoot into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::RelRoot& sRoot);

  /// Used to convert Substrait Plan into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::Plan& substraitPlan);

  // return the raw ptr of ExprConverter
  SubstraitVeloxExprConverter* getExprConverter() {
    return exprConverter_.get();
  }

  /// Used to construct the function map between the index
  /// and the Substrait function name. Initialize the expression
  /// converter based on the constructed function map.
  void constructFunctionMap(const ::substrait::Plan& substraitPlan);

  void constructFunctionMap(std::unordered_map<uint64_t, std::string> substraitPlan);

  /// Will return the function map used by this plan converter.
  const std::unordered_map<uint64_t, std::string>& getFunctionMap() const {
    return functionMap_;
  }

  /// Return the splitInfo map used by this plan converter.
  const std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() const {
    return splitInfoMap_;
  }

  /// Used to insert certain plan node as input. The plan node
  /// id will start from the setted one.
  void insertInputNode(uint64_t inputIdx, const std::shared_ptr<const core::PlanNode>& inputNode, int planNodeId) {
    inputNodesMap_[inputIdx] = inputNode;
    planNodeId_ = planNodeId;
  }

  void setSplitInfos(std::vector<std::shared_ptr<SplitInfo>> splitInfos) {
    splitInfos_ = splitInfos;
  }

  /// The input iterators not inlined to VeloxPlan. They should be then manually added to the Velox task
  /// via WholeStageResultIterator#addIteratorSplits. Empty if no input iterators remaining.
  const std::vector<std::shared_ptr<ResultIterator>>& remainingInputIterators() const {
    return inputIters_;
  }

  /// Used to check if ReadRel specifies an input of stream.
  /// If yes, the index of input stream will be returned.
  /// If not, -1 will be returned.
  int32_t getStreamIndex(const ::substrait::ReadRel& sRel);

  /// Used to find the function specification in the constructed function map.
  std::string findFuncSpec(uint64_t id);

  /// Extract join keys from joinExpression.
  /// joinExpression is a boolean condition that describes whether each record
  /// from the left set “match” the record from the right set. The condition
  /// must only include the following operations: AND, ==, field references.
  /// Field references correspond to the direct output order of the data.
  void extractJoinKeys(
      const ::substrait::Expression& joinExpression,
      std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
      std::vector<const ::substrait::Expression::FieldReference*>& rightExprs);

  /// Get aggregation step from AggregateRel.
  /// If returned Partial, it means the aggregate generated can leveraging flushing and abandoning like
  /// what streaming pre-aggregation can do in MPP databases.
  core::AggregationNode::Step toAggregationStep(const ::substrait::AggregateRel& sAgg);

  /// Get aggregation function step for AggregateFunction.
  /// The returned step value will be used to decide which Velox aggregate function or companion function
  /// is used for the actual data processing.
  core::AggregationNode::Step toAggregationFunctionStep(const ::substrait::AggregateFunction& sAggFuc);

  /// We use companion functions if the aggregate is not single.
  std::string toAggregationFunctionName(
      const std::string& baseName,
      const core::AggregationNode::Step& step,
      const TypePtr& resultType);

  /// Helper Function to convert Substrait sortField to Velox sortingKeys and
  /// sortingOrders.
  /// Note that, this method would deduplicate the sorting keys which have the same field name.
  std::pair<std::vector<core::FieldAccessTypedExprPtr>, std::vector<core::SortOrder>> processSortField(
      const ::google::protobuf::RepeatedPtrField<::substrait::SortField>& sortField,
      const RowTypePtr& inputType);

 private:
  /// Integrate Substrait emit feature. Here a given 'substrait::RelCommon'
  /// is passed and check if emit is defined for this relation. Basically a
  /// ProjectNode is added on top of 'noEmitNode' to represent output order
  /// specified in 'relCommon::emit'. Return 'noEmitNode' as is
  /// if output order is 'kDriect'.
  core::PlanNodePtr processEmit(const ::substrait::RelCommon& relCommon, const core::PlanNodePtr& noEmitNode);

  /// Check the Substrait type extension only has one unknown extension.
  static bool checkTypeExtension(const ::substrait::Plan& substraitPlan);

  /// Returns unique ID to use for plan node. Produces sequential numbers
  /// starting from zero.
  std::string nextPlanNodeId();

  /// Used to convert AggregateRel into Velox plan node.
  /// The output of child node will be used as the input of Aggregation.
  std::shared_ptr<const core::PlanNode> toVeloxAgg(
      const ::substrait::AggregateRel& sAgg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);

  /// Helper function to convert the input of Substrait Rel to Velox Node.
  template <typename T>
  core::PlanNodePtr convertSingleInput(T rel) {
    VELOX_CHECK(rel.has_input(), "Child Rel is expected here.");
    return toVeloxPlan(rel.input());
  }

  const core::WindowNode::Frame createWindowFrame(
      const ::substrait::Expression_WindowFunction_Bound& lower_bound,
      const ::substrait::Expression_WindowFunction_Bound& upper_bound,
      const ::substrait::WindowType& type,
      const RowTypePtr& inputType);

  /// The unique identification for each PlanNode.
  int planNodeId_ = 0;

  /// The map storing the relations between the function id and the function
  /// name. Will be constructed based on the Substrait representation.
  std::unordered_map<uint64_t, std::string> functionMap_;

  /// The map storing the split stats for each PlanNode.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>> splitInfoMap_;

  /// The map storing the pre-built plan nodes which can be accessed through
  /// index. This map is only used when the computation of a Substrait plan
  /// depends on other input nodes.
  std::unordered_map<uint64_t, std::shared_ptr<const core::PlanNode>> inputNodesMap_;

  int32_t splitInfoIdx_{0};
  std::vector<std::shared_ptr<SplitInfo>> splitInfos_;

  /// The Expression converter used to convert Substrait representations into
  /// Velox expressions.
  std::unique_ptr<SubstraitVeloxExprConverter> exprConverter_;

  /// Memory pool.
  memory::MemoryPool* pool_;

  /// A map of custom configs.
  const facebook::velox::config::ConfigBase* veloxCfg_;

  /// Input row-vectors for query trace mode (ValuesNode / cuDF ValueStream support)
  std::vector<std::shared_ptr<ResultIterator>> inputIters_;

  /// The temporary path used to write files.
  std::optional<std::string> writeFilesTempPath_;
  std::optional<std::string> writeFileName_;

  /// A flag used to specify validation.
  bool validationMode_ = false;
};

} // namespace gluten
