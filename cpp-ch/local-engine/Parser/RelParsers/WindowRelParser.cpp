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
#include "WindowRelParser.h"
#include <exception>
#include <memory>
#include <valarray>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/WindowDescription.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/RelParsers/SortParsingUtils.h>
#include <Parser/RelParsers/SortRelParser.h>
#include <Parser/TypeParser.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <base/types.h>
#include <google/protobuf/util/json_util.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}
}
namespace local_engine
{
using namespace DB;
WindowRelParser::WindowRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr
WindowRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    const auto & win_rel_pb = rel.window();
    current_plan = std::move(current_plan_);

    // The output header is : original columns ++ window columns
    output_header = *current_plan->getCurrentHeader();
    for (const auto & measure : win_rel_pb.measures())
    {
        const auto & win_function = measure.measure();
        ColumnWithTypeAndName named_col;
        named_col.name = win_function.column_name();
        named_col.type = TypeParser::parseType(win_function.output_type());
        named_col.column = named_col.type->createColumn();
        output_header.insert(named_col);
    }

    initWindowsInfos(win_rel_pb);
    tryAddProjectionBeforeWindow();

    auto window_descriptions = parseWindowDescriptions();

    /// In spark plan, there is already a sort step before each window, so we don't need to add sort steps here.
    for (auto & it : window_descriptions)
    {
        auto & win = it.second;

        auto window_step = std::make_unique<DB::WindowStep>(current_plan->getCurrentHeader(), win, win.window_functions, false);
        window_step->setStepDescription("Window step for window '" + win.window_name + "'", 1000);
        steps.emplace_back(window_step.get());
        current_plan->addStep(std::move(window_step));
    }

    tryAddProjectionAfterWindow();

    return std::move(current_plan);
}

DB::WindowDescription WindowRelParser::parseWindowDescription(const WindowInfo & win_info)
{
    DB::WindowDescription win_descr;
    win_descr.frame = parseWindowFrame(win_info);
    win_descr.partition_by = parseSortFields(*current_plan->getCurrentHeader(), win_info.partition_exprs);
    win_descr.order_by = parseSortFields(*current_plan->getCurrentHeader(), win_info.sort_fields);
    win_descr.full_sort_description = win_descr.partition_by;
    win_descr.full_sort_description.insert(win_descr.full_sort_description.end(), win_descr.order_by.begin(), win_descr.order_by.end());

    // window_name is used to identify the window description
    DB::WriteBufferFromOwnString ss;
    ss << "partition by " << DB::dumpSortDescription(win_descr.partition_by);
    ss << " order by " << DB::dumpSortDescription(win_descr.order_by);
    ss << " " << win_descr.frame.toString();
    win_descr.window_name = ss.str();
    return win_descr;
}

/// In CH, it put all functions into one window description if they have the same partition expressions and sort fields.
std::unordered_map<String, WindowDescription> WindowRelParser::parseWindowDescriptions()
{
    std::unordered_map<String, WindowDescription> window_descriptions;
    for (size_t i = 0; i < win_infos.size(); ++i)
    {
        auto & win_info = win_infos[i];
        const auto & measure = *win_info.measure;
        const auto & win_function = measure.measure();
        auto win_description = parseWindowDescription(win_info);

        /// Check whether there is already a window description with the same name
        /// if the partition expressions and sort fields are the same, the window functtions belong
        /// to the same window description.
        WindowDescription * description = nullptr;
        const auto win_it = window_descriptions.find(win_description.window_name);
        if (win_it != window_descriptions.end())
            description = &win_it->second;
        else
        {
            window_descriptions[win_description.window_name] = win_description;
            description = &window_descriptions[win_description.window_name];
        }

        auto win_func = parseWindowFunctionDescription(
            win_info.function_name, win_function, win_info.arg_column_names, win_info.arg_column_types, win_info.params);
        description->window_functions.emplace_back(win_func);
    }
    return window_descriptions;
}

DB::WindowFrame WindowRelParser::parseWindowFrame(const WindowInfo & win_info)
{
    DB::WindowFrame win_frame;
    const auto & signature_function_name = win_info.signature_function_name;
    const auto & window_function = win_info.measure->measure();
    win_frame.type = parseWindowFrameType(signature_function_name, window_function);
    parseBoundType(window_function.lower_bound(), true, win_frame.begin_type, win_frame.begin_offset, win_frame.begin_preceding);
    parseBoundType(window_function.upper_bound(), false, win_frame.end_type, win_frame.end_offset, win_frame.end_preceding);

    // special cases
    if (signature_function_name == "lead" || signature_function_name == "lag")
    {
        win_frame.begin_preceding = true;
        win_frame.end_preceding = false;
    }
    return win_frame;
}

DB::WindowFrame::FrameType
WindowRelParser::parseWindowFrameType(const std::string & function_name, const substrait::Expression::WindowFunction & window_function)
{
    // It's weird! The frame type only could be rows in spark for rank(). But in clickhouse
    // it's should be range. If run rank() over rows frame, the result is different. The rank number
    // is different for the same values.
    static const std::unordered_map<std::string, substrait::WindowType> special_function_frame_type
        = {{"rank", substrait::RANGE}, {"dense_rank", substrait::RANGE}, {"percent_rank", substrait::RANGE}};

    substrait::WindowType frame_type;
    auto iter = special_function_frame_type.find(function_name);
    if (iter != special_function_frame_type.end())
        frame_type = iter->second;
    else
        frame_type = window_function.window_type();

    if (frame_type == substrait::ROWS)
        return DB::WindowFrame::FrameType::ROWS;
    else if (frame_type == substrait::RANGE)
        return DB::WindowFrame::FrameType::RANGE;
    else
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unknow window frame type:{}", frame_type);
}

void WindowRelParser::parseBoundType(
    const substrait::Expression::WindowFunction::Bound & bound,
    bool is_begin_or_end,
    DB::WindowFrame::BoundaryType & bound_type,
    Field & offset,
    bool & preceding_direction)
{
    /// some default settings.
    offset = 0;

    if (bound.has_preceding())
    {
        const auto & preceding = bound.preceding();
        bound_type = DB::WindowFrame::BoundaryType::Offset;
        preceding_direction = preceding.offset() >= 0;
        if (preceding.offset() < 0)
            offset = 0 - preceding.offset();
        else
            offset = preceding.offset();
    }
    else if (bound.has_following())
    {
        const auto & following = bound.following();
        bound_type = DB::WindowFrame::BoundaryType::Offset;
        preceding_direction = following.offset() < 0;
        if (following.offset() < 0)
            offset = 0 - following.offset();
        else
            offset = following.offset();
    }
    else if (bound.has_current_row())
    {
        bound_type = DB::WindowFrame::BoundaryType::Current;
        offset = 0;
        preceding_direction = is_begin_or_end;
    }
    else if (bound.has_unbounded_preceding())
    {
        bound_type = DB::WindowFrame::BoundaryType::Unbounded;
        offset = 0;
        preceding_direction = true;
    }
    else if (bound.has_unbounded_following())
    {
        bound_type = DB::WindowFrame::BoundaryType::Unbounded;
        offset = 0;
        preceding_direction = false;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unknown bound type:{}", bound.DebugString());
    }
}

WindowFunctionDescription WindowRelParser::parseWindowFunctionDescription(
    const String & ch_function_name,
    const substrait::Expression::WindowFunction & window_function,
    const DB::Names & arg_names,
    const DB::DataTypes & arg_types,
    const DB::Array & params)
{
    WindowFunctionDescription description;
    description.column_name = window_function.column_name();
    description.function_node = nullptr;
    DB::AggregateFunctionProperties agg_function_props;
    auto agg_function_ptr = RelParser::getAggregateFunction(ch_function_name, arg_types, agg_function_props, params);

    description.argument_names = arg_names;
    description.argument_types = arg_types;
    description.aggregate_function = agg_function_ptr;
    return description;
}

void WindowRelParser::initWindowsInfos(const substrait::WindowRel & win_rel)
{
    win_infos.reserve(win_rel.measures_size());
    for (const auto & measure : win_rel.measures())
    {
        WindowInfo win_info;
        win_info.result_column_name = measure.measure().column_name();
        win_info.measure = &measure;
        win_info.signature_function_name = *parseSignatureFunctionName(measure.measure().function_reference());
        win_info.parser_func_info = AggregateFunctionParser::CommonFunctionInfo(measure);
        win_info.function_parser = AggregateFunctionParserFactory::instance().get(win_info.signature_function_name, parser_context);
        win_info.function_name = win_info.function_parser->getCHFunctionName(win_info.parser_func_info);
        win_info.partition_exprs = win_rel.partition_expressions();
        win_info.sort_fields = win_rel.sorts();
        win_infos.emplace_back(win_info);
    }
}

void WindowRelParser::tryAddProjectionBeforeWindow()
{
    const auto & header = *current_plan->getCurrentHeader();
    ActionsDAG actions_dag{header.getColumnsWithTypeAndName()};
    auto dag_footprint = actions_dag.dumpDAG();

    for (auto & win_info : win_infos)
    {
        auto arg_nodes = win_info.function_parser->parseFunctionArguments(win_info.parser_func_info, actions_dag);
        // This may remove elements from arg_nodes, because some of them are converted to CH func parameters.
        win_info.params = win_info.function_parser->parseFunctionParameters(win_info.parser_func_info, arg_nodes, actions_dag);
        for (auto & arg_node : arg_nodes)
        {
            win_info.arg_column_names.emplace_back(arg_node->result_name);
            win_info.arg_column_types.emplace_back(arg_node->result_type);
            actions_dag.addOrReplaceInOutputs(*arg_node);
        }
    }

    if (actions_dag.dumpDAG() != dag_footprint)
    {
        auto project_step = std::make_unique<ExpressionStep>(current_plan->getCurrentHeader(), std::move(actions_dag));
        project_step->setStepDescription("Add projections before window");
        steps.emplace_back(project_step.get());
        current_plan->addStep(std::move(project_step));
    }
}

void WindowRelParser::tryAddProjectionAfterWindow()
{
    // The final result header is : original header ++ [window aggregate columns]
    const auto & header = *current_plan->getCurrentHeader();
    ActionsDAG actions_dag{header.getColumnsWithTypeAndName()};
    auto dag_footprint = actions_dag.dumpDAG();

    for (size_t i = 0; i < win_infos.size(); ++i)
    {
        auto & win_info = win_infos[i];
        const auto * win_result_node = &actions_dag.findInOutputs(win_info.result_column_name);
        win_info.function_parser->convertNodeTypeIfNeeded(win_info.parser_func_info, win_result_node, actions_dag, false);
    }

    if (actions_dag.dumpDAG() != dag_footprint)
    {
        auto project_step = std::make_unique<ExpressionStep>(current_plan->getCurrentHeader(), std::move(actions_dag));
        project_step->setStepDescription("Add projections for window result");
        steps.emplace_back(project_step.get());
        current_plan->addStep(std::move(project_step));
    }

    // This projeciton will remove the const columns from the window function arguments
    const auto & current_header = *current_plan->getCurrentHeader();
    if (!DB::blocksHaveEqualStructure(output_header, current_header))
    {
        ActionsDAG convert_action = ActionsDAG::makeConvertingActions(
            current_header.getColumnsWithTypeAndName(), output_header.getColumnsWithTypeAndName(), DB::ActionsDAG::MatchColumnsMode::Name, getContext());
        QueryPlanStepPtr convert_step = std::make_unique<DB::ExpressionStep>(current_plan->getCurrentHeader(), std::move(convert_action));
        convert_step->setStepDescription("Convert window Output");
        steps.emplace_back(convert_step.get());
        current_plan->addStep(std::move(convert_step));
    }
}

void registerWindowRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<WindowRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kWindow, builder);
}
}
