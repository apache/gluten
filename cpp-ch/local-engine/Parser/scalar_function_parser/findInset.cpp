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
#include <DataTypes/DataTypeNullable.h>
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

class FunctionParserFindInSet : public FunctionParser
{
public:
    explicit FunctionParserFindInSet(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserFindInSet() override = default;

    static constexpr auto name = "find_in_set";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /*
            parse find_in_set(str, str_array) as
            if (isNull(str))
                null
            else if (isNull(str_array))
                null
            else indexOf(assumeNotNull(splitByChar(',', str_array)), str)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * str_arg = parsed_args[0];
        const auto * str_array_arg = parsed_args[1];

        auto str_is_nullable = str_arg->result_type->isNullable();
        auto str_array_is_nullable = str_array_arg->result_type->isNullable();

        const auto * comma_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), ",");
        const auto * split_node = toFunctionNode(actions_dag, "splitByChar", {comma_const_node, str_array_arg});
        const auto * split_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {split_node});
        const auto * index_of_node = toFunctionNode(actions_dag, "indexOf", {split_not_null_node, str_arg});

        if (!str_is_nullable && !str_array_is_nullable)
            return convertNodeTypeIfNeeded(substrait_func, index_of_node, actions_dag);

        auto nullable_result_type = makeNullable(INT());
        const auto * nullable_index_of_node = ActionsDAGUtil::convertNodeType(
            actions_dag, index_of_node, nullable_result_type, getContext(), index_of_node->result_name);
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, nullable_result_type, DB::Field());

        const auto * str_is_null_node = toFunctionNode(actions_dag, "isNull", {str_arg});
        const auto * str_array_is_null_node = toFunctionNode(actions_dag, "isNull", {str_array_arg});
        const auto * or_condition_node = toFunctionNode(actions_dag, "or", {str_is_null_node, str_array_is_null_node});

        const auto * result_node = toFunctionNode(actions_dag, "if", {or_condition_node, null_const_node, nullable_index_of_node});
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserFindInSet> register_find_in_set;
}
