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

#include "GroupLimitRelParser.h"
#include <algorithm>
#include <memory>
#include <unordered_set>
#include <utility>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/WindowDescription.h>
#include <Operator/BranchStep.h>
#include <Operator/GraceMergingAggregatedStep.h>
#include <Operator/WindowGroupLimitStep.h>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Parser/RelParsers/SortParsingUtils.h>
#include <Parser/RelParsers/SortRelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/AggregateUtil.h>
#include <Common/ArrayJoinHelper.h>
#include <Common/GlutenConfig.h>
#include <Common/PlanUtil.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{
namespace Setting
{
extern const SettingsMaxThreads max_threads;

}
}

namespace local_engine
{
using namespace DB;
GroupLimitRelParser::GroupLimitRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr
GroupLimitRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    const auto win_rel_def = rel.windowgrouplimit();
    google::protobuf::StringValue optimize_info_str;
    optimize_info_str.ParseFromString(win_rel_def.advanced_extension().optimization().value());
    auto optimization_info = WindowGroupOptimizationInfo::parse(optimize_info_str.value());
    if (optimization_info.is_aggregate_group_limit)
    {
        AggregateGroupLimitRelParser aggregate_group_limit_parser(parser_context);
        auto plan = aggregate_group_limit_parser.parse(std::move(current_plan_), rel, rel_stack_);
        steps = aggregate_group_limit_parser.getSteps();
        return std::move(plan);
    }
    else
    {
        WindowGroupLimitRelParser window_parser(parser_context);
        auto plan = window_parser.parse(std::move(current_plan_), rel, rel_stack_);
        steps = window_parser.getSteps();
        return std::move(plan);
    }
}

static std::vector<size_t> parsePartitionFields(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions)
{
    std::vector<size_t> fields;
    for (const auto & expr : expressions)
        if (auto field_index = SubstraitParserUtils::getStructFieldIndex(expr))
            fields.push_back(*field_index);
        else if (expr.has_literal())
            continue;
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow expression: {}", expr.DebugString());
    return fields;
}

std::vector<size_t> parseSortFields(const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields)
{
    std::vector<size_t> fields;
    for (const auto sort_field : sort_fields)
        if (sort_field.expr().has_literal())
            continue;
        else if (auto field_index = SubstraitParserUtils::getStructFieldIndex(sort_field.expr()))
            fields.push_back(*field_index);
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown expression: {}", sort_field.expr().DebugString());
    return fields;
}


WindowGroupLimitRelParser::WindowGroupLimitRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr
WindowGroupLimitRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    const auto win_rel_def = rel.windowgrouplimit();
    google::protobuf::StringValue optimize_info_str;
    optimize_info_str.ParseFromString(win_rel_def.advanced_extension().optimization().value());
    auto optimization_info = WindowGroupOptimizationInfo::parse(optimize_info_str.value());
    window_function_name = optimization_info.window_function;

    current_plan = std::move(current_plan_);

    auto partition_fields = parsePartitionFields(win_rel_def.partition_expressions());
    auto sort_fields = parseSortFields(win_rel_def.sorts());
    size_t limit = static_cast<size_t>(win_rel_def.limit());

    auto window_group_limit_step = std::make_unique<WindowGroupLimitStep>(
        current_plan->getCurrentHeader(), window_function_name, partition_fields, sort_fields, limit);
    window_group_limit_step->setStepDescription("Window group limit");
    steps.emplace_back(window_group_limit_step.get());
    current_plan->addStep(std::move(window_group_limit_step));

    return std::move(current_plan);
}

AggregateGroupLimitRelParser::AggregateGroupLimitRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

// used to decide which branch
size_t selectBranchOnPartitionKeysCardinality(
    const std::vector<size_t> & partition_keys, double high_card_threshold, const std::list<DB::Chunk> & chunks)
{
    size_t total_rows = 0;
    std::unordered_set<UInt32> ids;
    for (const auto & chunk : chunks)
    {
        total_rows += chunk.getNumRows();
        DB::WeakHash32 hash(chunk.getNumRows());
        const auto & cols = chunk.getColumns();
        for (auto i : partition_keys)
            hash.update(cols[i]->getWeakHash32());
        const auto & data = hash.getData();
        for (size_t n = 0, sz = chunk.getNumRows(); n < sz; ++n)
            ids.insert(data[n]);
    }
    LOG_DEBUG(
        getLogger("AggregateGroupLimitRelParser"),
        "Approximate distinct keys {}, total rows: {}, thrshold: {}",
        ids.size(),
        total_rows,
        high_card_threshold);
    return ids.size() * 1.0 / (total_rows + 1) <= high_card_threshold ? 0 : 1;
}

DB::QueryPlanPtr AggregateGroupLimitRelParser::parse(
    DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    // calculate window's topk by aggregation.
    // 1. add a pre-projecttion. Make two tuple arguments for the aggregation function. One is the required columns for the output, the other
    //   is the required columns for sorting.
    // 2. Collect the sorting directions for each sorting field, Let them as the aggregation function's parameters.
    // 3. Add a aggregation step.
    // 4. Add a post-projecttion. Explode the aggregation function's result, since the result is an array.

    current_plan = std::move(current_plan_);
    input_header = current_plan->getCurrentHeader();
    win_rel_def = &rel.windowgrouplimit();

    google::protobuf::StringValue optimize_info_str;
    optimize_info_str.ParseFromString(win_rel_def->advanced_extension().optimization().value());
    auto optimization_info = WindowGroupOptimizationInfo::parse(optimize_info_str.value());
    limit = static_cast<size_t>(win_rel_def->limit());
    aggregate_function_name = getAggregateFunctionName(optimization_info.window_function);

    if (limit < 1)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid limit: {}", limit);

    auto win_config = WindowConfig::loadFromContext(getContext());
    auto high_card_threshold = win_config.aggregate_topk_high_cardinality_threshold;

    // Aggregation doesn't perform well on high cardinality keys. We make two execution pathes here.
    // - if the partition keys are low cardinality, run it by aggregation
    // - if the partition keys are high cardinality, run it by window.
    auto partition_fields = parsePartitionFields(win_rel_def->partition_expressions());
    auto branch_in_header = current_plan->getCurrentHeader();
    auto branch_step = std::make_unique<StaticBranchStep>(
        getContext(),
        branch_in_header,
        2,
        win_config.aggregate_topk_sample_rows,
        [partition_fields, high_card_threshold](const std::list<DB::Chunk> & chunks) -> size_t
        { return selectBranchOnPartitionKeysCardinality(partition_fields, high_card_threshold, chunks); });
    branch_step->setStepDescription("Window TopK");
    steps.push_back(branch_step.get());
    current_plan->addStep(std::move(branch_step));

    // If all partition keys are low cardinality keys, use aggregattion to get topk of each partition
    auto aggregation_plan = BranchStepHelper::createSubPlan(branch_in_header, 1);
    collectPartitionAndSortFields();
    prePrejectionForAggregateArguments(*aggregation_plan);
    addGroupLmitAggregationStep(*aggregation_plan);
    postProjectionForExplodingArrays(*aggregation_plan);
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Aggregate topk plan:\n{}", PlanUtil::explainPlan(*aggregation_plan));

    auto window_plan = BranchStepHelper::createSubPlan(branch_in_header, 1);
    addSortStep(*window_plan);
    addWindowLimitStep(*window_plan);
    auto convert_actions_dag = DB::ActionsDAG::makeConvertingActions(
        window_plan->getCurrentHeader()->getColumnsWithTypeAndName(),
        aggregation_plan->getCurrentHeader()->getColumnsWithTypeAndName(),
        DB::ActionsDAG::MatchColumnsMode::Position,
        getContext());
    auto convert_step = std::make_unique<DB::ExpressionStep>(window_plan->getCurrentHeader(), std::move(convert_actions_dag));
    convert_step->setStepDescription("Rename rank column name");
    window_plan->addStep(std::move(convert_step));
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Window topk plan:\n{}", PlanUtil::explainPlan(*window_plan));

    std::vector<DB::QueryPlanPtr> branch_plans;
    branch_plans.emplace_back(std::move(aggregation_plan));
    branch_plans.emplace_back(std::move(window_plan));
    auto unite_branches_step = std::make_unique<UniteBranchesStep>(getContext(), branch_in_header, std::move(branch_plans), 1);
    unite_branches_step->setStepDescription("Unite TopK branches");
    steps.push_back(unite_branches_step.get());

    current_plan->addStep(std::move(unite_branches_step));
    return std::move(current_plan);
}

String AggregateGroupLimitRelParser::getAggregateFunctionName(const String & window_function_name)
{
    if (window_function_name == "row_number")
        return "rowNumGroupArraySorted";
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported window function: {}", window_function_name);
}

void AggregateGroupLimitRelParser::collectPartitionAndSortFields()
{
    partition_fields = parsePartitionFields(win_rel_def->partition_expressions());
    auto full_sort_fields = parseSortFields(win_rel_def->sorts());

    std::set<size_t> partition_fields_set(partition_fields.begin(), partition_fields.end());
    std::set<size_t> full_sort_fields_set(full_sort_fields.begin(), full_sort_fields.end());
    std::set<size_t> selected_sort_fields_set;
    // Remove partition keys from sort keys
    std::set_difference(
        full_sort_fields_set.begin(),
        full_sort_fields_set.end(),
        partition_fields_set.begin(),
        partition_fields_set.end(),
        std::inserter(selected_sort_fields_set, selected_sort_fields_set.begin()));
    if (selected_sort_fields_set.empty())
    {
        // FIXME: support empty sort keys.
        sort_fields.push_back(*partition_fields_set.begin());
    }
    else
    {
        sort_fields = std::vector<size_t>(selected_sort_fields_set.begin(), selected_sort_fields_set.end());
    }
}

// Build one tuple column as the aggregate function's arguments
void AggregateGroupLimitRelParser::prePrejectionForAggregateArguments(DB::QueryPlan & plan)
{
    auto projection_actions = std::make_shared<DB::ActionsDAG>(input_header->getColumnsWithTypeAndName());

    std::set<size_t> unique_partition_fields(partition_fields.begin(), partition_fields.end());
    std::set<size_t> unique_sort_fields(sort_fields.begin(), sort_fields.end());

    DB::NameSet required_column_names;
    auto build_tuple = [&](const DB::DataTypes & data_types,
                           const Strings & names,
                           const DB::ActionsDAG::NodeRawConstPtrs & elements,
                           const String & name_prefix,
                           String & result_name)
    {
        result_name = expression_parser->getUniqueName(name_prefix);
        auto tuple = expression_parser->toFunctionNode(*projection_actions, "tuple", elements, result_name);
        auto tuple_type = std::make_shared<DB::DataTypeTuple>(data_types, names);
        DB::ActionsDAG::NodeRawConstPtrs cast_args;
        cast_args.push_back(tuple);
        cast_args.push_back(
            expression_parser->addConstColumn(*projection_actions, std::make_shared<DataTypeString>(), tuple_type->getName()));
        tuple = expression_parser->toFunctionNode(*projection_actions, "CAST", cast_args, result_name);
        projection_actions->addOrReplaceInOutputs(*tuple);
        required_column_names.insert(tuple->result_name);
    };

    DB::DataTypes aggregate_data_tuple_types;
    Strings aggregate_data_tuple_names;
    DB::ActionsDAG::NodeRawConstPtrs aggregate_data_tuple_nodes;
    for (size_t i = 0; i < input_header->columns(); ++i)
    {
        const auto & col = input_header->getByPosition(i);
        if (unique_partition_fields.count(i))
        {
            required_column_names.insert(col.name);
            aggregate_grouping_keys.push_back(col.name);
        }
        
        if (!unique_partition_fields.count(i) || unique_sort_fields.count(i))
        {
            aggregate_data_tuple_types.push_back(col.type);
            aggregate_data_tuple_names.push_back(col.name);
            aggregate_data_tuple_nodes.push_back(projection_actions->getInputs()[i]);
        }
    }
    build_tuple(
        aggregate_data_tuple_types,
        aggregate_data_tuple_names,
        aggregate_data_tuple_nodes,
        "aggregate_data_tuple",
        aggregate_tuple_column_name);

    projection_actions->removeUnusedActions(required_column_names);
    LOG_DEBUG(
        getLogger("AggregateGroupLimitRelParser"),
        "Projection for building tuples for aggregate function:\n{}",
        projection_actions->dumpDAG());

    auto expression_step = std::make_unique<DB::ExpressionStep>(input_header, std::move(*projection_actions));
    expression_step->setStepDescription("Pre-projection for aggregate group limit arguments");
    plan.addStep(std::move(expression_step));
}

DB::AggregateDescription AggregateGroupLimitRelParser::buildAggregateDescription(DB::QueryPlan & plan)
{
    DB::AggregateDescription agg_desc;
    agg_desc.column_name = aggregate_tuple_column_name;
    agg_desc.argument_names = {aggregate_tuple_column_name};
    auto & parameters = agg_desc.parameters;
    parameters.push_back(static_cast<UInt32>(limit));
    std::set<String> sort_field_names;
    for (auto i : sort_fields)
        sort_field_names.insert(input_header->getByPosition(i).name);
    auto full_sort_desc = parseSortFields(*input_header, win_rel_def->sorts());
    DB::SortDescription sort_desc;
    for (const auto & sort_column : full_sort_desc)
        if (sort_field_names.count(sort_column.column_name))
            sort_desc.push_back(sort_column);
    auto sort_directions = buildSQLLikeSortDescription(sort_desc);
    parameters.push_back(sort_directions);

    const auto & header = *plan.getCurrentHeader();
    DB::DataTypes arg_types;
    arg_types.push_back(header.getByName(aggregate_tuple_column_name).type);

    DB::AggregateFunctionProperties properties;
    agg_desc.function = getAggregateFunction(aggregate_function_name, arg_types, properties, parameters);
    return agg_desc;
}

void AggregateGroupLimitRelParser::addGroupLmitAggregationStep(DB::QueryPlan & plan)
{
    const auto & settings = getContext()->getSettingsRef();

    DB::AggregateDescriptions agg_descs = {buildAggregateDescription(plan)};
    auto params = AggregatorParamsHelper::buildParams(
        getContext(), aggregate_grouping_keys, agg_descs, AggregatorParamsHelper::Mode::INIT_TO_COMPLETED);
    auto agg_step = std::make_unique<GraceMergingAggregatedStep>(getContext(), plan.getCurrentHeader(), params, true);
    plan.addStep(std::move(agg_step));
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Plan after add group limit:\n{}", PlanUtil::explainPlan(plan));
}

void AggregateGroupLimitRelParser::postProjectionForExplodingArrays(DB::QueryPlan & plan)
{
    const auto & header = *plan.getCurrentHeader();

    /// flatten the array column.
    auto agg_result_index = header.columns() - 1;
    auto array_join_actions_dag = ArrayJoinHelper::applyArrayJoinOnOneColumn(header, agg_result_index);
    auto new_steps = ArrayJoinHelper::addArrayJoinStep(getContext(), plan, array_join_actions_dag, false);

    const auto & array_join_output_header = *plan.getCurrentHeader();
    DB::ActionsDAG flatten_actions_dag(array_join_output_header.getColumnsWithTypeAndName());
    DB::Names flatten_output_column_names;
    for (size_t i = 0; i < array_join_output_header.columns() - 1; ++i)
    {
        const auto & col = array_join_output_header.getByPosition(i);
        flatten_output_column_names.push_back(col.name);
    }
    auto last_column = array_join_output_header.getByPosition(array_join_output_header.columns() - 1);
    const auto * tuple_column = typeid_cast<const DB::ColumnTuple *>(last_column.column.get());
    const auto * tuple_datatype = typeid_cast<const DB::DataTypeTuple *>(last_column.type.get());
    const auto & field_names = tuple_datatype->getElementNames();
    DB::DataTypePtr tuple_index_type = std::make_shared<DB::DataTypeUInt32>();
    const auto * tuple_node = flatten_actions_dag.getInputs().back();
    for (size_t i = 0; i < field_names.size(); ++i)
    {
        DB::ActionsDAG::NodeRawConstPtrs tuple_index_args;
        tuple_index_args.push_back(tuple_node);
        tuple_index_args.push_back(expression_parser->addConstColumn(flatten_actions_dag, tuple_index_type, i + 1));
        const auto * field_node = expression_parser->toFunctionNode(flatten_actions_dag, "tupleElement", tuple_index_args, field_names[i]);
        flatten_actions_dag.addOrReplaceInOutputs(*field_node);
        flatten_output_column_names.push_back(field_node->result_name);
    }
    flatten_actions_dag.removeUnusedActions(flatten_output_column_names);
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Actions dag for untupling aggregate result:\n{}", flatten_actions_dag.dumpDAG());
    auto flatten_expression_step = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(flatten_actions_dag));
    flatten_expression_step->setStepDescription("Untuple the aggregation result");
    plan.addStep(std::move(flatten_expression_step));

    const auto & flatten_tuple_output_header = plan.getCurrentHeader();
    auto window_result_column = flatten_tuple_output_header->getByPosition(flatten_tuple_output_header->columns() - 1);
    /// The result column is put at the end of the header.
    auto output_header{*input_header};
    output_header.insert(window_result_column);
    auto adjust_pos_actions_dag = DB::ActionsDAG::makeConvertingActions(
        flatten_tuple_output_header->getColumnsWithTypeAndName(),
        output_header.getColumnsWithTypeAndName(),
        DB::ActionsDAG::MatchColumnsMode::Name,
        getContext());
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Actions dag for replacing columns:\n{}", adjust_pos_actions_dag.dumpDAG());
    auto adjust_pos_expression_step = std::make_unique<DB::ExpressionStep>(flatten_tuple_output_header, std::move(adjust_pos_actions_dag));
    adjust_pos_expression_step->setStepDescription("Adjust position of the output columns");
    plan.addStep(std::move(adjust_pos_expression_step));
}

void AggregateGroupLimitRelParser::addSortStep(DB::QueryPlan & plan)
{
    const auto & header = *plan.getCurrentHeader();
    auto full_sort_descr = parseSortFields(header, win_rel_def->partition_expressions());
    auto sort_descr = parseSortFields(header, win_rel_def->sorts());
    full_sort_descr.insert(full_sort_descr.end(), sort_descr.begin(), sort_descr.end());

    DB::SortingStep::Settings settings(getContext()->getSettingsRef());
    auto config = MemoryConfig::loadFromContext(getContext());
    settings.worth_external_sort_mem_ratio = config.spill_mem_ratio;
    auto sorting_step = std::make_unique<DB::SortingStep>(plan.getCurrentHeader(), full_sort_descr, 0, settings);
    sorting_step->setStepDescription("Sorting step");
    plan.addStep(std::move(sorting_step));
}

static DB::WindowFrame buildWindowFrame(const std::string & ch_function_name)
{
    DB::WindowFrame frame;
    // default window frame is [unbounded preceding, current row]
    if (ch_function_name == "row_number")
    {
        frame.type = DB::WindowFrame::FrameType::ROWS;
        frame.begin_type = DB::WindowFrame::BoundaryType::Offset;
        frame.begin_offset = 1;
    }
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow window function: {}", ch_function_name);
    return frame;
}

static DB::WindowFunctionDescription buildWindowFunctionDescription(const std::string & ch_function_name)
{
    DB::WindowFunctionDescription description;
    if (ch_function_name == "row_number")
    {
        description.column_name = ch_function_name;
        description.function_node = nullptr;
        DB::AggregateFunctionProperties agg_props;
        auto agg_func = RelParser::getAggregateFunction(ch_function_name, {}, agg_props, {});
        description.aggregate_function = agg_func;
    }
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow window function: {}", ch_function_name);
    return description;
}


// TODO: WindowGroupLimitStep has bad performance, need to improve it. So we still use window + filter here.
void AggregateGroupLimitRelParser::addWindowLimitStep(DB::QueryPlan & plan)
{
    google::protobuf::StringValue optimize_info_str;
    optimize_info_str.ParseFromString(win_rel_def->advanced_extension().optimization().value());
    auto optimization_info = WindowGroupOptimizationInfo::parse(optimize_info_str.value());
    auto window_function_name = optimization_info.window_function;

    const auto & in_header = plan.getCurrentHeader();
    DB::WindowDescription win_descr;
    win_descr.frame = buildWindowFrame(window_function_name);
    win_descr.partition_by = parseSortFields(*in_header, win_rel_def->partition_expressions());
    win_descr.order_by = parseSortFields(*in_header, win_rel_def->sorts());
    win_descr.full_sort_description = win_descr.partition_by;
    win_descr.full_sort_description.insert(win_descr.full_sort_description.end(), win_descr.order_by.begin(), win_descr.order_by.end());
    DB::WriteBufferFromOwnString ss;
    ss << "partition by " << DB::dumpSortDescription(win_descr.partition_by);
    ss << " order by " << DB::dumpSortDescription(win_descr.order_by);
    ss << " " << win_descr.frame.toString();
    win_descr.window_name = ss.str();

    auto win_func_description = buildWindowFunctionDescription(window_function_name);
    win_descr.window_functions.push_back(win_func_description);

    auto win_step = std::make_unique<WindowStep>(in_header, win_descr, win_descr.window_functions, false);
    win_step->setStepDescription("Window (" + win_descr.window_name + ")", 1000);
    plan.addStep(std::move(win_step));

    const auto & win_result_header = plan.getCurrentHeader();
    DB::ActionsDAG limit_actions_dag(win_result_header->getColumnsWithTypeAndName());
    const auto * rank_value_node = limit_actions_dag.getInputs().back();
    const auto * limit_value_node = expression_parser->addConstColumn(limit_actions_dag, std::make_shared<DB::DataTypeInt32>(), limit);
    const auto * cmp_node = expression_parser->toFunctionNode(limit_actions_dag, "lessOrEquals", {rank_value_node, limit_value_node});
    auto cmp_column_name = cmp_node->result_name;
    limit_actions_dag.addOrReplaceInOutputs(*cmp_node);
    auto filter_step = std::make_unique<DB::FilterStep>(win_result_header, std::move(limit_actions_dag), cmp_column_name, true);
    plan.addStep(std::move(filter_step));
}

void registerWindowGroupLimitRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<GroupLimitRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kWindowGroupLimit, builder);
}
}
