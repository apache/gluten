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
#include "AggregateRelParser.h"
#include <memory>
#include <AggregateFunctions/Combinators/AggregateFunctionIf.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Operator/DefaultHashAggregateResult.h>
#include <Operator/GraceAggregatingStep.h>
#include <Operator/GraceMergingAggregatedStep.h>
#include <Operator/StreamingAggregatingStep.h>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <boost/algorithm/string/join.hpp>
#include <google/protobuf/wrappers.pb.h>
#include <Common/CHUtil.h>
#include <Common/GlutenConfig.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool enable_memory_bound_merging_of_aggregation_results;
extern const SettingsUInt64 aggregation_in_order_max_block_bytes;
extern const SettingsUInt64 max_block_size;
}
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TYPE;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}
namespace local_engine
{
using namespace DB;
AggregateRelParser::AggregateRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr
AggregateRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    rel_stack = &rel_stack_;
    setup(std::move(query_plan), rel);

    addPreProjection();
    LOG_TRACE(logger, "header after pre-projection is: {}", plan->getCurrentHeader()->dumpStructure());
    if (has_final_stage)
    {
        addMergingAggregatedStep();
        LOG_TRACE(logger, "header after merging is: {}", plan->getCurrentHeader()->dumpStructure());

        addPostProjection();
        LOG_TRACE(logger, "header after post-projection is: {}", plan->getCurrentHeader()->dumpStructure());
    }
    else if (has_complete_stage)
    {
        addCompleteModeAggregatedStep();
        LOG_TRACE(logger, "header after complete aggregate is: {}", plan->getCurrentHeader()->dumpStructure());

        addPostProjection();
        LOG_TRACE(logger, "header after post-projection is: {}", plan->getCurrentHeader()->dumpStructure());
    }
    else
    {
        addAggregatingStep();
        LOG_TRACE(logger, "header after aggregating is: {}", plan->getCurrentHeader()->dumpStructure());
    }

    /// Add a check here to help find bugs, Don't remove it.
    /// Thre order of result of result columns must be ordered grouping keys ++ ordered aggregate expression results
    const auto & aggregation_output_header = *plan->getCurrentHeader();
    for (size_t i = 0; i < grouping_keys.size(); ++i)
    {
        auto pos = aggregation_output_header.getPositionByName(grouping_keys[i]);
        if (pos != i)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "The order of aggregation result columns is invalid");
    }

    /// If the groupings is empty, we still need to return one row with default values even if the input is empty.
    if ((rel.aggregate().groupings().empty() || rel.aggregate().groupings()[0].grouping_expressions().empty())
        && (has_final_stage || has_complete_stage || rel.aggregate().measures().empty()))
    {
        LOG_TRACE(&Poco::Logger::get("AggregateRelParser"), "default aggregate result step");
        auto default_agg_result = std::make_unique<DefaultHashAggregateResultStep>(plan->getCurrentHeader());
        default_agg_result->setStepDescription("Default aggregate result");
        steps.push_back(default_agg_result.get());
        plan->addStep(std::move(default_agg_result));
    }
    return std::move(plan);
}

void AggregateRelParser::setup(DB::QueryPlanPtr query_plan, const substrait::Rel & rel)
{
    plan = std::move(query_plan);
    aggregate_rel = &rel.aggregate();

    std::set<substrait::AggregationPhase> phase_set;
    for (const auto & measure : aggregate_rel->measures())
    {
        auto phase = measure.measure().phase();
        phase_set.insert(phase);
    }
    has_first_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE);
    has_inter_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE);
    has_final_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);
    has_complete_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_RESULT);
    bool next_step_is_agg = false;
    google::protobuf::StringValue raw_extra_params;
    raw_extra_params.ParseFromString(aggregate_rel->advanced_extension().optimization().value());
    auto extra_params = AggregateOptimizationInfo::parse(raw_extra_params.value());
    if (aggregate_rel->measures().empty())
    {
        /// For aggregate without aggregate functions. we use other ways the determine wchich stage it is.
        /// In general, when has_required_child_distribution_expressions is true, this aggregate stage is the final
        /// merge aggregated step. But there are exceptions.
        /// 1. when the query is a distinct like aggregation. for example.
        ///   select n_regionkey, count(distinct n_nationkey) from nation group by n_regionkey
        /// There should be four aggregation steps.
        ///   step1: partial aggregation on keys (n_regionkey, n_nationkey) with empty aggregation functions.
        ///   step2: aggregation on keys (n_regionkey, n_nationkey) with empty aggregation functions after shuffle.
        ///   step3: aggregation on keys (n_regionkey) with partial aggregation count(n_nationkey)
        ///   step4: aggregation on keys (n_regionkey) with final aggregation merge count(n_nationkey) after shuffle
        /// If step3 is followed by another aggregation, we must let has_final_stage = false. Otherwise it will make
        /// columns position missmatch.
        /// 2. the two aggregation stages are merged into one by rule `MergeTwoPhasesHashBaseAggregate`. In this case
        /// we also let has_first_stage = false;
        /// FIXME: Really don't like this implementation. It's too easy to be broken.
        next_step_is_agg = rel_stack->empty() ? false : rel_stack->back()->rel_type_case() == substrait::Rel::RelTypeCase::kAggregate;
        bool is_last_stage = extra_params.has_required_child_distribution_expressions && !next_step_is_agg;
        has_first_stage = !extra_params.has_pre_partial_aggregate;
        has_final_stage = extra_params.has_pre_partial_aggregate && is_last_stage;
        has_complete_stage = !extra_params.has_pre_partial_aggregate && is_last_stage;
    }

    if (phase_set.size() > 1 && has_final_stage)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "AggregateRelParser: multiple aggregation phases with final stage are not supported");
    }
    if (phase_set.size() > 1 && has_complete_stage)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "AggregateRelParser: multiple aggregation phases with complete mode are not supported");
    }

    const auto & input_header = *plan->getCurrentHeader();
    for (const auto & measure : aggregate_rel->measures())
    {
        AggregateInfo agg_info;
        auto arg = measure.measure().arguments(0).value();
        agg_info.signature_function_name = *parseSignatureFunctionName(measure.measure().function_reference());
        auto function_parser = AggregateFunctionParserFactory::instance().get(agg_info.signature_function_name, parser_context);
        if (!function_parser)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported aggregate function: {}", agg_info.signature_function_name);
        /// Put function_parser, parser_func_info and function_name into agg_info for reducing repeated builds.
        agg_info.function_parser = function_parser;
        agg_info.parser_func_info = AggregateFunctionParser::CommonFunctionInfo(measure);
        agg_info.function_name = function_parser->getCHFunctionName(agg_info.parser_func_info);
        agg_info.measure = &measure;
        aggregates.push_back(agg_info);
    }

    if (aggregate_rel->groupings_size() == 1)
    {
        for (const auto & expr : aggregate_rel->groupings(0).grouping_expressions())
        {
            auto field_index = SubstraitParserUtils::getStructFieldIndex(expr);
            if (field_index)
                grouping_keys.push_back(input_header.getByPosition(*field_index).name);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported group expression: {}", expr.DebugString());
        }
    }
    else if (aggregate_rel->groupings_size() != 0)
    {
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupport multible groupings");
    }
}

/// Projections for function arguments.
/// The projections are built by the function parsers.
void AggregateRelParser::addPreProjection()
{
    const auto & input_header = *plan->getCurrentHeader();
    ActionsDAG projection_action{input_header.getColumnsWithTypeAndName()};
    std::string dag_footprint = projection_action.dumpDAG();
    for (auto & agg_info : aggregates)
    {
        auto arg_nodes = agg_info.function_parser->parseFunctionArguments(agg_info.parser_func_info, projection_action);
        // This may remove elements from arg_nodes, because some of them are converted to CH func parameters.
        agg_info.params = agg_info.function_parser->parseFunctionParameters(agg_info.parser_func_info, arg_nodes, projection_action);
        for (auto & arg_node : arg_nodes)
        {
            agg_info.arg_column_names.emplace_back(arg_node->result_name);
            agg_info.arg_column_types.emplace_back(arg_node->result_type);
            projection_action.addOrReplaceInOutputs(*arg_node);
        }
    }
    if (projection_action.dumpDAG() != dag_footprint)
    {
        /// Avoid unnecessary evaluation
        projection_action.removeUnusedActions();
        auto projection_step = std::make_unique<DB::ExpressionStep>(plan->getCurrentHeader(), std::move(projection_action));
        projection_step->setStepDescription("Projection before aggregate");
        steps.emplace_back(projection_step.get());
        plan->addStep(std::move(projection_step));
    }
}

void AggregateRelParser::buildAggregateDescriptions(AggregateDescriptions & descriptions)
{
    const auto & current_plan_header = *plan->getCurrentHeader();
    auto build_result_column_name
        = [this, current_plan_header](
              const String & function_name, const Array & params, const Strings & arg_names, substrait::AggregationPhase phase)
    {
        if (phase == substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT
            || phase == substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE)
        {
            assert(arg_names.size() == 1);
            return arg_names[0];
        }

        String result = function_name;
        if (!params.empty())
        {
            result += "(";
            for (size_t i = 0; i < params.size(); ++i)
            {
                if (i != 0)
                    result += ",";
                result += DB::fieldToString(params[i]);
            }
            result += ")";
        }

        result += "(";
        result += boost::algorithm::join(arg_names, ",");
        result += ")";
        // Make the name unique to avoid name collision(issue #6878).
        auto res = this->getUniqueName(result);
        // Just a check for remining this issue.
        if (current_plan_header.findByName(res))
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Name ({}) collision in header: {}", res, current_plan_header.dumpStructure());
        return res;
    };

    for (auto & agg_info : aggregates)
    {
        AggregateDescription description;
        const auto & measure = agg_info.measure->measure();
        description.column_name
            = build_result_column_name(agg_info.function_name, agg_info.params, agg_info.arg_column_names, measure.phase());

        agg_info.measure_column_name = description.column_name;
        // std::cout << "description.column_name:" << description.column_name << std::endl;
        description.argument_names = agg_info.arg_column_names;
        DB::AggregateFunctionProperties properties;

        if (!agg_info.function_name.ends_with("State"))
        {
            // May apply `PartialMerge` and `If` on the original function.
            auto [combinator_function_name, combinator_function_arg_types] = agg_info.function_parser->tryApplyCHCombinator(
                agg_info.parser_func_info, agg_info.function_name, agg_info.arg_column_types);
            description.function
                = getAggregateFunction(combinator_function_name, combinator_function_arg_types, properties, agg_info.params);
        }
        else
        {
            // If the function is a state function, we don't need to apply `PartialMerge`.
            // In INITIAL_TO_INTERMEDIATE or INITIAL_TO_RESULT phase, we do arguments -> xxState.
            // In INTERMEDIATE_TO_RESULT phase, we do xxState -> xxState.
            if (agg_info.parser_func_info.phase == substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
                || agg_info.parser_func_info.phase == substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_RESULT)
            {
                description.function = getAggregateFunction(agg_info.function_name, agg_info.arg_column_types, properties, agg_info.params);
            }
            else if (agg_info.parser_func_info.phase == substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT)
            {
                if (agg_info.arg_column_types.size() != 1)
                    throw Exception(
                        DB::ErrorCodes::BAD_ARGUMENTS,
                        "Only support one argument aggregate function in phase {}",
                        agg_info.parser_func_info.phase);
                const auto * type = checkAndGetDataType<DataTypeAggregateFunction>(agg_info.arg_column_types[0].get());
                if (!type)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Argument for function {} must have type AggregateFunction as arguments",
                        agg_info.function_name);
                auto nested_types = type->getArgumentsDataTypes();
                description.function = getAggregateFunction(agg_info.function_name, nested_types, properties, agg_info.params);
            }
            else
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported phase for state function: {}", agg_info.function_name);
            }
        }
        description.parameters = agg_info.params;
        descriptions.emplace_back(description);
    }
}

void AggregateRelParser::addMergingAggregatedStep()
{
    AggregateDescriptions aggregate_descriptions;
    buildAggregateDescriptions(aggregate_descriptions);
    const auto & settings = getContext()->getSettingsRef();
    auto config = StreamingAggregateConfig::loadFromContext(getContext());
    if (config.enable_streaming_aggregating)
    {
        auto params = AggregatorParamsHelper::buildParams(
            getContext(), grouping_keys, aggregate_descriptions, AggregatorParamsHelper::Mode::PARTIAL_TO_FINISHED);
        auto merging_step = std::make_unique<GraceMergingAggregatedStep>(getContext(), plan->getCurrentHeader(), params, false);
        steps.emplace_back(merging_step.get());
        plan->addStep(std::move(merging_step));
    }
    else
    {
        auto params = AggregatorParamsHelper::buildParams(
            getContext(),
            grouping_keys,
            aggregate_descriptions,
            AggregatorParamsHelper::Mode::PARTIAL_TO_FINISHED,
            AggregatorParamsHelper::Algorithm::CHTwoStageAggregate);
        /// We don't use the grouping set feature in CH, so grouping_sets_params_list should always be empty.
        DB::GroupingSetsParamsList grouping_sets_params_list;
        auto merging_step = std::make_unique<DB::MergingAggregatedStep>(
            plan->getCurrentHeader(),
            params,
            grouping_sets_params_list,
            true,
            false,
            1,
            false,
            settings[Setting::max_block_size],
            settings[Setting::aggregation_in_order_max_block_bytes],
            settings[Setting::enable_memory_bound_merging_of_aggregation_results]);
        steps.emplace_back(merging_step.get());
        plan->addStep(std::move(merging_step));
    }
}

void AggregateRelParser::addCompleteModeAggregatedStep()
{
    AggregateDescriptions aggregate_descriptions;
    buildAggregateDescriptions(aggregate_descriptions);
    const auto & settings = getContext()->getSettingsRef();
    auto config = StreamingAggregateConfig::loadFromContext(getContext());
    if (config.enable_streaming_aggregating)
    {
        auto params = AggregatorParamsHelper::buildParams(
            getContext(), grouping_keys, aggregate_descriptions, AggregatorParamsHelper::Mode::INIT_TO_COMPLETED);
        auto merging_step = std::make_unique<GraceMergingAggregatedStep>(getContext(), plan->getCurrentHeader(), params, true);
        steps.emplace_back(merging_step.get());
        plan->addStep(std::move(merging_step));
    }
    else
    {
        auto params = AggregatorParamsHelper::buildParams(
            getContext(),
            grouping_keys,
            aggregate_descriptions,
            AggregatorParamsHelper::Mode::INIT_TO_COMPLETED,
            AggregatorParamsHelper::Algorithm::CHTwoStageAggregate);

        auto aggregating_step = std::make_unique<AggregatingStep>(
            plan->getCurrentHeader(),
            params,
            GroupingSetsParamsList(),
            true,
            settings[Setting::max_block_size],
            settings[Setting::aggregation_in_order_max_block_bytes],
            1,
            1,
            false,
            false,
            SortDescription(),
            SortDescription(),
            false,
            false,
            false);
        steps.emplace_back(aggregating_step.get());
        plan->addStep(std::move(aggregating_step));
    }
}

void AggregateRelParser::addAggregatingStep()
{
    AggregateDescriptions aggregate_descriptions;
    buildAggregateDescriptions(aggregate_descriptions);
    const auto & settings = getContext()->getSettingsRef();

    auto config = StreamingAggregateConfig::loadFromContext(getContext());
    bool is_distinct_aggreate = false;
    if (!rel_stack->empty())
    {
        const auto & next_rel = *(rel_stack->back());
        if (next_rel.rel_type_case() == substrait::Rel::RelTypeCase::kAggregate)
            is_distinct_aggreate = true;
    }

    if (config.enable_streaming_aggregating)
    {
        // Disable spilling to disk.
        // If group_by_two_level_threshold_bytes != 0, `Aggregator` will use memory usage as a condition to convert
        // the hash table into a two level one. The method of determining the amount of memory used by the hash table is
        // unreliable. It will appear that a small hash table is converted into a two level structure, resulting in a
        // lot of small blocks. So we disable this condition, reamain `group_by_two_level_threshold` as the condition to
        // convert a single level hash table into a two level one.
        auto params = AggregatorParamsHelper::buildParams(
            getContext(), grouping_keys, aggregate_descriptions, AggregatorParamsHelper::Mode::INIT_TO_PARTIAL);

        if (!is_distinct_aggreate)
        {
            auto aggregating_step = std::make_unique<StreamingAggregatingStep>(getContext(), plan->getCurrentHeader(), params);
            steps.emplace_back(aggregating_step.get());
            plan->addStep(std::move(aggregating_step));
        }
        else
        {
            /// If it's an aggregation query involves distinct, for example,
            ///     select n_regionkey, count(distinct(n_name)), sum(n_nationkey) from nation group by n_regionkey
            /// The running steps are as follow in general.
            ///   step1: partial aggregation on keys (n_regionkey, n_name) with empty aggregation functions.
            ///   step2: exchagne shuffle
            ///   step3: aggregation on keys (n_regionkey, n_name) with partial aggregation sum(n_nationkey)
            ///   step4: aggregation on keys (n_regionkey) with partial aggregation merge sum(n_nationkey), and partial aggregation count(n_name)
            ///   step5: exchange shuffle
            ///   step6: aggregation on keys (n_regionkey) with final aggregation merge sum(n_nationkey) and count(n_name)
            /// We cannot use streaming aggregating strategy in step3. Otherwise it will generate multiple blocks with same n_name in them. This
            /// will make the result for count(distinct(n_name)) wrong. step3 must finish all inputs before it puts any block into step4.
            /// So we introduce GraceAggregatingStep here, it can handle mass data with high cardinality.
            auto aggregating_step = std::make_unique<GraceAggregatingStep>(getContext(), plan->getCurrentHeader(), params, has_first_stage);
            steps.emplace_back(aggregating_step.get());
            plan->addStep(std::move(aggregating_step));
        }
    }
    else
    {
        auto params = AggregatorParamsHelper::buildParams(
            getContext(),
            grouping_keys,
            aggregate_descriptions,
            AggregatorParamsHelper::Mode::INIT_TO_PARTIAL,
            AggregatorParamsHelper::Algorithm::CHTwoStageAggregate);

        auto aggregating_step = std::make_unique<AggregatingStep>(
            plan->getCurrentHeader(),
            params,
            GroupingSetsParamsList(),
            false,
            settings[Setting::max_block_size],
            settings[Setting::aggregation_in_order_max_block_bytes],
            1,
            1,
            false,
            false,
            SortDescription(),
            SortDescription(),
            false,
            false,
            false);
        steps.emplace_back(aggregating_step.get());
        plan->addStep(std::move(aggregating_step));
    }
}

// Only be called in final stage.
void AggregateRelParser::addPostProjection()
{
    const auto & input_header = *plan->getCurrentHeader();
    ActionsDAG project_actions_dag{input_header.getColumnsWithTypeAndName()};
    auto dag_footprint = project_actions_dag.dumpDAG();

    if (has_final_stage)
    {
        for (const auto & agg_info : aggregates)
        {
            for (const auto * input_node : project_actions_dag.getInputs())
                if (input_node->result_name == agg_info.measure_column_name)
                    agg_info.function_parser->convertNodeTypeIfNeeded(agg_info.parser_func_info, input_node, project_actions_dag, false);
        }
    }
    else if (has_complete_stage)
    {
        // on the complete mode, it must consider the nullability when converting node type
        for (const auto & agg_info : aggregates)
        {
            for (const auto * output_node : project_actions_dag.getOutputs())
                if (output_node->result_name == agg_info.measure_column_name)
                    agg_info.function_parser->convertNodeTypeIfNeeded(agg_info.parser_func_info, output_node, project_actions_dag, true);
        }
    }
    if (project_actions_dag.dumpDAG() != dag_footprint)
    {
        QueryPlanStepPtr convert_step = std::make_unique<ExpressionStep>(plan->getCurrentHeader(), std::move(project_actions_dag));
        convert_step->setStepDescription("Post-projection for aggregate");
        steps.emplace_back(convert_step.get());
        plan->addStep(std::move(convert_step));
    }
}

void registerAggregateParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<AggregateRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kAggregate, builder);
}
}
