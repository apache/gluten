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

#include <google/protobuf/arena.h>
#include <string>
#include <typeinfo>

#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

#include "SubstraitExtensionCollector.h"
#include "VeloxToSubstraitExpr.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

namespace gluten {

/// Convert the Velox plan into Substrait plan.
class VeloxToSubstraitPlanConvertor {
 public:
  /// Convert Velox PlanNode into Substrait Plan.
  /// @param vPlan Velox query plan to convert.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @return A pointer to Substrait plan object allocated on the arena and
  /// representing the input Velox plan.
  ::substrait::Plan& toSubstrait(google::protobuf::Arena& arena, const core::PlanNodePtr& planNode);

 private:
  /// Convert Velox PlanNode into Substrait Rel.
  void toSubstrait(google::protobuf::Arena& arena, const core::PlanNodePtr& planNode, ::substrait::Rel* rel);

  /// Convert Velox FilterNode into Substrait FilterRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::FilterNode>& filterNode,
      ::substrait::FilterRel* filterRel);

  /// Convert Velox ValuesNode into Substrait ReadRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::ValuesNode>& valuesNode,
      ::substrait::ReadRel* readRel);

  /// Convert Velox ProjectNode into Substrait ProjectRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::ProjectNode>& projectNode,
      ::substrait::ProjectRel* projectRel);

  /// Convert Velox Aggregation Node into Substrait AggregateRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::AggregationNode>& aggregateNode,
      ::substrait::AggregateRel* aggregateRel);

  /// Convert Velox OrderBy Node into Substrait SortRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::OrderByNode>& orderByNode,
      ::substrait::SortRel* sortRel);

  /// Convert Velox TopN Node into Substrait SortRel->FetchRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::TopNNode>& topNNode,
      ::substrait::FetchRel* fetchRel);

  /// Helper function to process sortingKeys and sortingOrders in Velox to
  /// convert them to the sortField of SortRel in Substrait.
  const ::substrait::SortRel& processSortFields(
      google::protobuf::Arena& arena,
      const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<core::SortOrder>& sortingOrders,
      const RowTypePtr& inputType);

  /// Convert Velox Limit Node into Substrait FetchRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::LimitNode>& limitNode,
      ::substrait::FetchRel* fetchRel);

  /// Check there only have one source for the velox node and return it.
  const core::PlanNodePtr& getSingleSource(const core::PlanNodePtr& node);

  /// The Expression converter used to convert Velox representations into
  /// Substrait expressions.
  std::unique_ptr<VeloxToSubstraitExprConvertor> exprConvertor_;

  /// The Type converter used to conver velox representation into Substrait
  /// type.
  std::shared_ptr<VeloxToSubstraitTypeConvertor> typeConvertor_ = std::make_shared<VeloxToSubstraitTypeConvertor>();

  /// The Extension collector storing the relations between the function
  /// signature and the function reference number.
  SubstraitExtensionCollectorPtr extensionCollector_;
};

} // namespace gluten
