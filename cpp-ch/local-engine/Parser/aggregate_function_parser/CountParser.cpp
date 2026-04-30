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
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/aggregate_function_parser/CountParser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

String CountParser::getCHFunctionName(const CommonFunctionInfo &) const
{
    return "count";
}

String CountParser::getCHFunctionName(DB::DataTypes &) const
{
    return "count";
}

DB::ActionsDAG::NodeRawConstPtrs CountParser::parseFunctionArguments(
    const CommonFunctionInfo & func_info, DB::ActionsDAG & actions_dag) const
{
    if (func_info.arguments.size() < 1)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {} requires at least one argument", getName());
    }

    const DB::ActionsDAG::Node * last_arg_node = nullptr;
    if (func_info.arguments.size() == 1)
    {
        last_arg_node = parseExpression(actions_dag, func_info.arguments[0].value());
    }
    else
    {
        auto uint8_type = std::make_shared<DB::DataTypeUInt8>();
        DB::Field null_field;
        auto nullable_uint8_type = std::make_shared<DB::DataTypeNullable>(uint8_type);
        auto nullable_uint_col = nullable_uint8_type->createColumnConst(1, nullable_uint8_type->getDefault());
        const auto * const_1_node
            = &actions_dag.addColumn(DB::ColumnWithTypeAndName(uint8_type->createColumnConst(1, 1), uint8_type, getUniqueName("1")));
        const auto * null_node
            = &actions_dag.addColumn(DB::ColumnWithTypeAndName(std::move(nullable_uint_col), nullable_uint8_type, getUniqueName("null")));

        DB::ActionsDAG::NodeRawConstPtrs multi_if_args;
        for (const auto & arg : func_info.arguments)
        {
            auto arg_value = arg.value();
            const DB::ActionsDAG::Node * current_arg_node = parseExpression(actions_dag, arg_value);
            const auto * cond_node = toFunctionNode(actions_dag, "isNull", {current_arg_node});
            multi_if_args.emplace_back(cond_node);
            multi_if_args.emplace_back(null_node);
        }
        multi_if_args.emplace_back(const_1_node);
        last_arg_node = toFunctionNode(actions_dag, "multiIf", multi_if_args);
    }
    if (func_info.has_filter)
    {
        // With `If` combinator, the function take one more argument which refers to the condition.
        return {last_arg_node, parseExpression(actions_dag, func_info.filter)};
    }
    return {last_arg_node};
}

static const AggregateFunctionParserRegister<CountParser> register_count;
}
