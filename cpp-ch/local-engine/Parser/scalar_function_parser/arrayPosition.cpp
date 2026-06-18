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
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Parser/FunctionParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
using namespace DB;
class FunctionParserArrayPosition : public FunctionParser
{
public:
    explicit FunctionParserArrayPosition(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserArrayPosition() override = default;

    static constexpr auto name = "array_position";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        /**
            parse array_position(arr, value) as
            if (isNull(arr))
                null
            else if (isNull(value))
                null
            else
                indexOf(assumeNotNull(arr), value)

            Main difference between Spark array_position and CH indexOf:
            1. Spark array_position returns null if either of the arguments are null
            2. CH indexOf function cannot accept Nullable(Array()) type as first argument
        */

        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        auto ch_function_name = getCHFunctionName(substrait_func);

        const auto * arr_arg = parsed_args[0];
        const auto * val_arg = parsed_args[1];

        auto is_arr_nullable = arr_arg->result_type->isNullable();
        auto is_val_nullable = val_arg->result_type->isNullable();

        if (!is_arr_nullable && !is_val_nullable)
        {
            const auto * ch_function_node = toFunctionNode(actions_dag, ch_function_name, {arr_arg, val_arg});
            return convertNodeTypeIfNeeded(substrait_func, ch_function_node, actions_dag);
        }

        /// should return nullable result
        const auto * arr_is_null_node = toFunctionNode(actions_dag, "isNull", {arr_arg});
        const auto * val_is_null_node = toFunctionNode(actions_dag, "isNull", {val_arg});

        const auto * arr_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {arr_arg});
        const auto * ch_function_node = toFunctionNode(actions_dag, ch_function_name, {arr_not_null_node, val_arg});
        DataTypePtr wrap_arr_nullable_type = wrapNullableType(true, ch_function_node->result_type);

        const auto * wrap_index_of_node = ActionsDAGUtil::convertNodeType(
            actions_dag, ch_function_node, wrap_arr_nullable_type, getContext(), ch_function_node->result_name);
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, wrap_arr_nullable_type, Field{});
        const auto * or_condition_node = toFunctionNode(actions_dag, "or", {arr_is_null_node, val_is_null_node});

        const auto * if_node = toFunctionNode(actions_dag, "if", {or_condition_node, null_const_node, wrap_index_of_node});
        return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
    }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override
    {
        return "indexOf";
    }
};

static FunctionParserRegister<FunctionParserArrayPosition> register_array_position;
}
