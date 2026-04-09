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
#include "AggregateFunctionParser.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/ExpressionParser.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/TypeParser.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FUNCTION;
}
}

namespace local_engine
{
using namespace DB;
AggregateFunctionParser::AggregateFunctionParser(ParserContextPtr parser_context_) : parser_context(parser_context_)
{
    expression_parser = std::make_unique<ExpressionParser>(parser_context);
}

AggregateFunctionParser::~AggregateFunctionParser()
{
}

String AggregateFunctionParser::getUniqueName(const String & name) const
{
    return expression_parser->getUniqueName(name);
}

const DB::ActionsDAG::Node *
AggregateFunctionParser::addColumnToActionsDAG(DB::ActionsDAG & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const
{
    return expression_parser->addConstColumn(actions_dag, type, field);
}

const DB::ActionsDAG::Node * AggregateFunctionParser::toFunctionNode(
    DB::ActionsDAG & action_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
{
    return expression_parser->toFunctionNode(action_dag, func_name, args);
}

const DB::ActionsDAG::Node * AggregateFunctionParser::toFunctionNode(
    DB::ActionsDAG & action_dag, const String & func_name, const String & result_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
{
    return expression_parser->toFunctionNode(action_dag, func_name, args, result_name);
}

const DB::ActionsDAG::Node * AggregateFunctionParser::parseExpression(DB::ActionsDAG & actions_dag, const substrait::Expression & rel) const
{
    return expression_parser->parseExpression(actions_dag, rel);
}

std::pair<DataTypePtr, Field> AggregateFunctionParser::parseLiteral(const substrait::Expression_Literal & literal) const
{
    return LiteralParser::parse(literal);
}

DB::ActionsDAG::NodeRawConstPtrs
AggregateFunctionParser::parseFunctionArguments(const CommonFunctionInfo & func_info, DB::ActionsDAG & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs collected_args;
    for (const auto & arg : func_info.arguments)
    {
        auto arg_value = arg.value();
        const DB::ActionsDAG::Node * arg_node = parseExpression(actions_dag, arg_value);

        // If the aggregate result is required to be nullable, make all inputs be nullable at the first stage.
        auto required_output_type = DB::WhichDataType(TypeParser::parseType(func_info.output_type));
        if (required_output_type.isNullable()
            && (func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
                || func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT)
            && !arg_node->result_type->isNullable())
        {
            DB::ActionsDAG::NodeRawConstPtrs args;
            args.emplace_back(arg_node);
            const auto * node = toFunctionNode(actions_dag, "toNullable", args);
            actions_dag.addOrReplaceInOutputs(*node);
            arg_node = node;
        }

        collected_args.push_back(arg_node);
    }

    if (func_info.has_filter)
    {
        // With `If` combinator, the function take one more argument which refers to the condition.
        const auto * action_node = parseExpression(actions_dag, func_info.filter);
        collected_args.emplace_back(action_node);
    }
    return collected_args;
}

std::pair<String, DB::DataTypes> AggregateFunctionParser::tryApplyCHCombinator(
    const CommonFunctionInfo & func_info, const String & ch_func_name, const DB::DataTypes & argument_types) const
{
    auto get_aggregate_function
        = [](const String & name, const DB::DataTypes & argument_types, const DB::Array & parameters) -> DB::AggregateFunctionPtr
    {
        DB::AggregateFunctionProperties properties;
        auto func = RelParser::getAggregateFunction(name, argument_types, properties, parameters);
        if (!func)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown aggregate function {}", name);

        return func;
    };

    String combinator_function_name = ch_func_name;
    DB::DataTypes combinator_argument_types = argument_types;

    if (func_info.phase != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
        && func_info.phase != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_RESULT)
    {
        if (argument_types.size() != 1)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Only support one argument aggregate function in phase {}", func_info.phase);

        // Add a check here for safty.
        if (func_info.has_filter)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Apply filter in phase {} not supported", func_info.phase);

        const auto * aggr_func_type = DB::checkAndGetDataType<DB::DataTypeAggregateFunction>(argument_types[0].get());
        if (!aggr_func_type)
        {
            // FIXME. This is should be fixed. It's the case that count(distinct(xxx)) with other aggregate functions.
            // Gluten breaks the rule that intermediate result should have a special format name here.
            LOG_INFO(logger, "Intermediate aggregate function data is expected in phase {} for {}", func_info.phase, ch_func_name);

            auto arg_type = DB::removeNullable(argument_types[0]);
            if (auto * tupe_type = typeid_cast<const DB::DataTypeTuple *>(arg_type.get()))
                combinator_argument_types = tupe_type->getElements();

            auto agg_function = get_aggregate_function(ch_func_name, argument_types, aggr_func_type->getParameters());
            auto agg_intermediate_result_type = agg_function->getStateType();
            combinator_argument_types = {agg_intermediate_result_type};
        }
        else
        {
            // Special case for handling the intermedidate result from aggregate functions with filter.
            // It's safe to use AggregateFunctionxxx to parse intermediate result from AggregateFunctionxxxIf,
            // since they have the same binary representation
            // reproduce this case by
            //  select
            //    count(a),count(b), count(1), count(distinct(a)), count(distinct(b))
            //  from values (1, null), (2,2) as data(a,b)
            // with `first_value` enable
            if (endsWith(aggr_func_type->getFunction()->getName(), "If") && ch_func_name != aggr_func_type->getFunction()->getName())
            {
                auto original_args_types = aggr_func_type->getArgumentsDataTypes();
                combinator_argument_types = DataTypes(original_args_types.begin(), std::prev(original_args_types.end()));
                auto agg_function = get_aggregate_function(ch_func_name, combinator_argument_types, aggr_func_type->getParameters());
                combinator_argument_types = {agg_function->getStateType()};
            }
        }
        combinator_function_name += "PartialMerge";
    }
    else if (func_info.has_filter)
    {
        // Apply `If` aggregate function combinator on the original aggregate function.
        combinator_function_name += "If";
    }
    return {combinator_function_name, combinator_argument_types};
}

const DB::ActionsDAG::Node * AggregateFunctionParser::convertNodeTypeIfNeeded(
    const CommonFunctionInfo & func_info, const DB::ActionsDAG::Node * func_node, DB::ActionsDAG & actions_dag, bool with_nullability) const
{
    const auto & output_type = func_info.output_type;
    bool need_convert_type = !TypeParser::isTypeMatched(output_type, func_node->result_type, !with_nullability);
    if (need_convert_type)
    {
        func_node = ActionsDAGUtil::convertNodeType(actions_dag, func_node, TypeParser::parseType(output_type), func_node->result_name);
        actions_dag.addOrReplaceInOutputs(*func_node);
    }

    func_node = convertNanToNullIfNeed(func_info, func_node, actions_dag);

    if (output_type.has_decimal())
    {
        String checkDecimalOverflowSparkOrNull = "checkDecimalOverflowSparkOrNull";
        DB::ActionsDAG::NodeRawConstPtrs overflow_args
            = {func_node,
               expression_parser->addConstColumn(actions_dag, std::make_shared<DataTypeInt32>(), output_type.decimal().precision()),
               expression_parser->addConstColumn(actions_dag, std::make_shared<DataTypeInt32>(), output_type.decimal().scale())};
        func_node = toFunctionNode(actions_dag, checkDecimalOverflowSparkOrNull, func_node->result_name, overflow_args);
        actions_dag.addOrReplaceInOutputs(*func_node);
    }

    return func_node;
}

const DB::ActionsDAG::Node * AggregateFunctionParser::convertNanToNullIfNeed(
    const CommonFunctionInfo & func_info, const DB::ActionsDAG::Node * func_node, DB::ActionsDAG & actions_dag) const
{
    const auto function_name = getCHFunctionName(func_info);
    if ((function_name != "corr" && function_name != "varSamp" && function_name != "covarSamp")
        || !func_node->result_type->isNullable())
        return func_node;

    /// result is nullable.
    /// if result is NaN, convert it to NULL.
    auto is_nan_func_node = toFunctionNode(actions_dag, "isNaN", getUniqueName("isNaN"), {func_node});
    auto nullable_col = func_node->result_type->createColumn();
    nullable_col->insertDefault();
    const auto * null_node
        = &actions_dag.addColumn(DB::ColumnWithTypeAndName(std::move(nullable_col), func_node->result_type, getUniqueName("null")));
    DB::ActionsDAG::NodeRawConstPtrs convert_nan_func_args = {is_nan_func_node, null_node, func_node};
    func_node = toFunctionNode(actions_dag, "if", func_node->result_name, convert_nan_func_args);
    actions_dag.addOrReplaceInOutputs(*func_node);
    return func_node;
}

AggregateFunctionParserFactory & AggregateFunctionParserFactory::instance()
{
    static AggregateFunctionParserFactory factory;
    return factory;
}

void AggregateFunctionParserFactory::registerAggregateFunctionParser(const String & name, Value value)
{
    if (!parsers.emplace(name, value).second)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Aggregate function parser {} is already registered", name);
}

AggregateFunctionParserPtr AggregateFunctionParserFactory::get(const String & name, ParserContextPtr parser_context) const
{
    auto parser = tryGet(name, parser_context);
    if (!parser)
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_FUNCTION, "Unknown aggregate function {}", name);
    return parser;
}

AggregateFunctionParserPtr AggregateFunctionParserFactory::tryGet(const String & name, ParserContextPtr parser_context) const
{
    auto it = parsers.find(name);
    if (it == parsers.end())
        return nullptr;
    return it->second(parser_context);
}
}
