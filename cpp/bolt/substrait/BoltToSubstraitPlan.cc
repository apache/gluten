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

#include "BoltToSubstraitPlan.h"
#include <google/protobuf/wrappers.pb.h>
#include "utils/Exception.h"

namespace gluten {
namespace {

struct AggregateCompanion {
  std::string functionName;
  core::AggregationNode::Step step;
};

AggregateCompanion toAggregateCompanion(const core::AggregationNode::Aggregate& aggregate) {
  const auto& companionName = aggregate.call->name();
  auto offset = companionName.find_last_of('_');
  if (offset == std::string::npos) {
    return {companionName, core::AggregationNode::Step::kSingle};
  }
  // found '_'
  const auto& suffix = companionName.substr(offset + 1);
  if (suffix.empty()) {
    // the last char is '_'
    return {companionName, core::AggregationNode::Step::kSingle};
  }
  const auto& functionName = companionName.substr(0, offset);
  if (suffix == "_partial") {
    return {functionName, core::AggregationNode::Step::kPartial};
  }
  if (suffix == "_merge_extract") {
    return {functionName, core::AggregationNode::Step::kFinal};
  }
  if (suffix == "_merge") {
    return {functionName, core::AggregationNode::Step::kIntermediate};
  }
  // others, not a companion function
  return {companionName, core::AggregationNode::Step::kSingle};
}

::substrait::AggregationPhase toAggregationPhase(const core::AggregationNode::Step& step) {
  switch (step) {
    case core::AggregationNode::Step::kPartial: {
      return ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE;
    }
    case core::AggregationNode::Step::kIntermediate: {
      return ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE;
    }
    case core::AggregationNode::Step::kSingle: {
      return ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT;
    }
    case core::AggregationNode::Step::kFinal: {
      return ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT;
    }
    default:
      BOLT_UNSUPPORTED("Unsupported Aggregate Step '{}' in Substrait ", mapAggregationStepToName(step));
  }
}

::substrait::SortField_SortDirection toSortDirection(core::SortOrder sortOrder) {
  if (sortOrder.isNullsFirst()) {
    if (sortOrder.isAscending()) {
      return ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST;
    } else {
      return ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST;
    }
  } else {
    if (sortOrder.isAscending()) {
      return ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST;
    } else {
      return ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST;
    }
  }
}

} // namespace

::substrait::Plan& BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const core::PlanNodePtr& plan) {
  // Construct the extension colllector.
  extensionCollector_ = std::make_shared<SubstraitExtensionCollector>();
  // Construct the expression converter.
  exprConvertor_ = std::make_unique<BoltToSubstraitExprConvertor>(extensionCollector_);

  auto substraitPlan = google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);

  // Add unknown type in extension.
  auto unknownType = substraitPlan->add_extensions()->mutable_extension_type();

  unknownType->set_extension_uri_reference(0);
  unknownType->set_type_anchor(0);
  unknownType->set_name("UNKNOWN");

  // Do conversion.
  ::substrait::RelRoot* rootRel = substraitPlan->add_relations()->mutable_root();

  toSubstrait(arena, plan, rootRel->mutable_input());

  // Add extensions for all functions and types seen in the plan.
  extensionCollector_->addExtensionsToPlan(substraitPlan);

  // Set RootRel names.
  for (const auto& name : plan->outputType()->names()) {
    rootRel->add_names(name);
  }

  return *substraitPlan;
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const core::PlanNodePtr& planNode,
    ::substrait::Rel* rel) {
  if (auto filterNode = std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
    auto filterRel = rel->mutable_filter();
    toSubstrait(arena, filterNode, filterRel);
    return;
  }
  if (auto valuesNode = std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
    ::substrait::ReadRel* readRel = rel->mutable_read();
    toSubstrait(arena, valuesNode, readRel);
    return;
  }
  if (auto projectNode = std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
    ::substrait::ProjectRel* projectRel = rel->mutable_project();
    toSubstrait(arena, projectNode, projectRel);
    return;
  }
  if (auto aggregationNode = std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
    ::substrait::AggregateRel* aggregateRel = rel->mutable_aggregate();
    toSubstrait(arena, aggregationNode, aggregateRel);
    return;
  }
  if (auto orderbyNode = std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
    toSubstrait(arena, orderbyNode, rel->mutable_sort());
    return;
  }
  if (auto topNNode = std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
    toSubstrait(arena, topNNode, rel->mutable_fetch());
    return;
  }
  if (auto limitNode = std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
    toSubstrait(arena, limitNode, rel->mutable_fetch());
    return;
  }
  BOLT_UNSUPPORTED("Unsupported plan node '{}' .", planNode->name());
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::FilterNode>& filterNode,
    ::substrait::FilterRel* filterRel) {
  const auto& source = getSingleSource(filterNode);

  toSubstrait(arena, source, filterRel->mutable_input());

  // Construct substrait expr(Filter condition).
  auto filterCondition = filterNode->filter();
  auto inputType = source->outputType();
  filterRel->mutable_condition()->MergeFrom(exprConvertor_->toSubstraitExpr(arena, filterCondition, inputType));

  filterRel->mutable_common()->mutable_direct();
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::ValuesNode>& valuesNode,
    ::substrait::ReadRel* readRel) {
  const auto& outputType = valuesNode->outputType();

  ::substrait::ReadRel_VirtualTable* virtualTable = readRel->mutable_virtual_table();

  for (const auto& vector : valuesNode->values()) {
    ::substrait::Expression_Literal_Struct* litValue = virtualTable->add_values();

    for (const auto& column : vector->children()) {
      ::substrait::Expression_Literal* substraitField =
          google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(&arena);

      substraitField->MergeFrom(exprConvertor_->toSubstraitLiteral(arena, column, litValue));
    }
  }

  readRel->mutable_base_schema()->MergeFrom(typeConvertor_->toSubstraitNamedStruct(arena, outputType));

  readRel->mutable_common()->mutable_direct();
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::ProjectNode>& projectNode,
    ::substrait::ProjectRel* projectRel) {
  const auto& projections = projectNode->projections();

  const auto& source = getSingleSource(projectNode);

  // Process the source Node.
  toSubstrait(arena, source, projectRel->mutable_input());

  // Remap the output.
  ::substrait::RelCommon_Emit* projRelEmit = projectRel->mutable_common()->mutable_emit();

  int64_t projectionSize = projections.size();

  auto inputType = source->outputType();
  int64_t inputTypeSize = inputType->size();

  for (int64_t i = 0; i < projectionSize; i++) {
    const auto& boltExpr = projections.at(i);

    projectRel->add_expressions()->MergeFrom(exprConvertor_->toSubstraitExpr(arena, boltExpr, inputType));

    // Add outputMapping for each expression.
    projRelEmit->add_output_mapping(inputTypeSize + i);
  }

  return;
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::AggregationNode>& aggregateNode,
    ::substrait::AggregateRel* aggregateRel) {
  // Process the source Node.
  const auto& source = getSingleSource(aggregateNode);
  toSubstrait(arena, source, aggregateRel->mutable_input());

  // Convert aggregate grouping keys, such as: group by key1, key2.
  auto inputType = source->outputType();
  auto groupingKeys = aggregateNode->groupingKeys();
  int64_t groupingKeySize = groupingKeys.size();
  ::substrait::AggregateRel_Grouping* aggGroupings = aggregateRel->add_groupings();

  for (int64_t i = 0; i < groupingKeySize; i++) {
    aggGroupings->add_grouping_expressions()->mutable_selection()->MergeFrom(
        exprConvertor_->toSubstraitExpr(arena, groupingKeys.at(i), inputType));
  }

  // AggregatesSize should be equal to or greater than the aggregateMasks Size.
  // Two cases: 1. aggregateMasksSize = 0, aggregatesSize > aggregateMasksSize.
  // 2. aggregateMasksSize != 0, aggregatesSize = aggregateMasksSize.
  auto aggregates = aggregateNode->aggregates();
  int64_t aggregatesSize = aggregates.size();

  for (int64_t i = 0; i < aggregatesSize; i++) {
    const auto& aggregate = aggregates.at(i);

    ::substrait::AggregateRel_Measure* aggMeasures = aggregateRel->add_measures();

    // Set substrait filter.
    ::substrait::Expression* aggFilter = aggMeasures->mutable_filter();
    if (const auto& mask = aggregate.mask) {
      aggFilter->mutable_selection()->MergeFrom(exprConvertor_->toSubstraitExpr(arena, mask, inputType));
    } else {
      // Set null.
      aggFilter = nullptr;
    }

    // Process measure, eg:sum(a).
    ::substrait::AggregateFunction* aggFunction = aggMeasures->mutable_measure();

    // Use aggregate node's step information to write advanced extension 'allowFlush'.
    const auto& step = aggregateNode->step();
    switch (step) {
      case core::AggregationNode::Step::kPartial: {
        substrait::extensions::AdvancedExtension ae{};
        google::protobuf::StringValue msg;
        msg.set_value("allowFlush=1");
        ae.mutable_optimization()->PackFrom(msg);
        aggregateRel->mutable_advanced_extension()->MergeFrom(ae);
        break;
      }
      case core::AggregationNode::Step::kSingle:
        break;
      case core::AggregationNode::Step::kFinal:
      case core::AggregationNode::Step::kIntermediate:
        BOLT_USER_FAIL("Step not supported");
        break;
    }

    // Set aggFunction args.
    std::vector<TypePtr> arguments;
    arguments.reserve(aggregate.call->inputs().size());
    for (const auto& expr : aggregate.call->inputs()) {
      // If the expr is CallTypedExpr, people need to do project firstly.
      if (auto aggregatesExprInput = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
        BOLT_NYI("In Bolt Plan, the aggregates type cannot be CallTypedExpr");
      } else {
        aggFunction->add_arguments()->mutable_value()->MergeFrom(
            exprConvertor_->toSubstraitExpr(arena, expr, inputType));

        arguments.emplace_back(expr->type());
      }
    }

    const auto& aggregateCompanion = toAggregateCompanion(aggregate);
    auto referenceNumber =
        extensionCollector_->getReferenceNumber(aggregateCompanion.functionName, aggregate.rawInputTypes);

    aggFunction->set_function_reference(referenceNumber);

    aggFunction->mutable_output_type()->MergeFrom(typeConvertor_->toSubstraitType(arena, aggregate.call->type()));

    // Set substrait aggregate Function phase.
    aggFunction->set_phase(toAggregationPhase(aggregateCompanion.step));
  }

  // Direct output.
  aggregateRel->mutable_common()->mutable_direct();
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::OrderByNode>& orderByNode,
    ::substrait::SortRel* sortRel) {
  const auto& source = getSingleSource(orderByNode);
  toSubstrait(arena, source, sortRel->mutable_input());

  sortRel->MergeFrom(
      processSortFields(arena, orderByNode->sortingKeys(), orderByNode->sortingOrders(), source->outputType()));

  BOLT_CHECK(!orderByNode->isPartial(), "Substrait doesn't support partial order by yet");
  sortRel->mutable_common()->mutable_direct();
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::TopNNode>& topNNode,
    ::substrait::FetchRel* fetchRel) {
  const auto& source = getSingleSource(topNNode);

  // Construct the sortRel as the FetchRel input.
  ::substrait::SortRel* sortRel = fetchRel->mutable_input()->mutable_sort();
  toSubstrait(arena, source, sortRel->mutable_input());

  sortRel->MergeFrom(
      processSortFields(arena, topNNode->sortingKeys(), topNNode->sortingOrders(), source->outputType()));

  sortRel->mutable_common()->mutable_direct();

  BOLT_CHECK(!topNNode->isPartial(), "Substrait doesn't support partial topN yet");

  fetchRel->set_offset(0);
  fetchRel->set_count(topNNode->count());
  fetchRel->mutable_common()->mutable_direct();
}

const ::substrait::SortRel& BoltToSubstraitPlanConvertor::processSortFields(
    google::protobuf::Arena& arena,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    const bytedance::bolt::RowTypePtr& inputType) {
  ::substrait::SortRel* sortRel = google::protobuf::Arena::CreateMessage<::substrait::SortRel>(&arena);

  BOLT_CHECK_EQ(
      sortingKeys.size(), sortingOrders.size(), "Number of sorting keys and sorting orders must be the same");

  for (int64_t i = 0; i < sortingKeys.size(); i++) {
    ::substrait::SortField* sortField = sortRel->add_sorts();
    sortField->mutable_expr()->mutable_selection()->MergeFrom(
        exprConvertor_->toSubstraitExpr(arena, sortingKeys[i], inputType));

    sortField->set_direction(toSortDirection(sortingOrders[i]));
  }
  return *sortRel;
}

void BoltToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::LimitNode>& limitNode,
    ::substrait::FetchRel* fetchRel) {
  const auto& source = getSingleSource(limitNode);
  toSubstrait(arena, source, fetchRel->mutable_input());

  fetchRel->set_offset(limitNode->offset());
  fetchRel->set_count(limitNode->count());

  BOLT_CHECK(!limitNode->isPartial(), "Substrait doesn't support partial limit yet");

  fetchRel->mutable_common()->mutable_direct();
}

const core::PlanNodePtr& BoltToSubstraitPlanConvertor::getSingleSource(const core::PlanNodePtr& node) {
  const auto& sources = node->sources();

  BOLT_USER_CHECK_EQ(1, sources.size(), "Plan node must have exactly one source.");
  return sources[0];
}

} // namespace gluten
