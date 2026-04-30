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
#include <Parser/ExpressionParser.h>
#include <Parser/FunctionParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
using namespace DB;
class FunctionParserLocate : public FunctionParser
{
public:
    explicit FunctionParserLocate(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserLocate() override = default;

    static constexpr auto name = "locate";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const override
    {
        /// Parse locate(substr, str, start_pos) as if(isNull(start_pos), 0, positionUTF8Spark(str, substr, start_pos)
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 3)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly three arguments", getName());

        const auto * substr_arg = parsed_args[0];
        const auto * str_arg = parsed_args[1];
        const auto * start_pos_arg = ActionsDAGUtil::convertNodeType(actions_dag, parsed_args[2], makeNullable(UINT()), getContext());
        const auto * is_start_pos_null_node = toFunctionNode(actions_dag, "isNull", {start_pos_arg});
        const auto * const_1_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeUInt64>(), 0);
        const auto * position_node = toFunctionNode(actions_dag, "positionUTF8Spark", {str_arg, substr_arg, start_pos_arg});
        const auto * result_node = toFunctionNode(actions_dag, "if", {is_start_pos_null_node, const_1_node, position_node});
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserLocate> register_locate;
}
