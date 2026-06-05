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
#include <DataTypes/DataTypesNumber.h>
#include <Parser/FunctionParser.h>
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

class FunctionParserElt : public FunctionParser
{
public:
    explicit FunctionParserElt(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserElt() override = default;

    static constexpr auto name = "elt";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /*
            parse elt(index, e1, e2, e3, ...) as
            if (isNull(index))
                null
            else if (index <= 0)
                null
            else if (index > len)
                null
            else
                arrayElement(array(e1, e2, e3, ...), index)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() < 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments", getName());

        const auto * index_arg = parsed_args[0];
        DB::ActionsDAG::NodeRawConstPtrs array_args;
        for (size_t i = 1; i < parsed_args.size(); ++i)
        {
            array_args.push_back(parsed_args[i]);
        }

        const auto * array_node = toFunctionNode(actions_dag, "array", array_args);
        const auto * array_element_node = toFunctionNode(actions_dag, "arrayElement", {array_node, index_arg});

        auto result_type = array_element_node->result_type;
        auto nullable_result_type = makeNullable(result_type);

        const auto * nullable_array_element_node = ActionsDAGUtil::convertNodeType(
            actions_dag, array_element_node, nullable_result_type, getContext(), array_element_node->result_name);

        const auto * null_const_node = addColumnToActionsDAG(actions_dag, nullable_result_type, DB::Field());
        const auto * is_null_node = toFunctionNode(actions_dag, "isNull", {index_arg});

        const auto * zero_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 0);
        const auto * len_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), array_args.size());
        const auto * less_or_equal_node = toFunctionNode(actions_dag, "lessOrEquals", {index_arg, zero_const_node});
        const auto * greater_node = toFunctionNode(actions_dag, "greater", {index_arg, len_const_node});
        const auto * or_condition_node = toFunctionNode(actions_dag, "or", {is_null_node, less_or_equal_node, greater_node});
        const auto * result_node = toFunctionNode(actions_dag, "if", {or_condition_node, null_const_node, nullable_array_element_node});
        return result_node;
    }
};

static FunctionParserRegister<FunctionParserElt> register_elt;
}
