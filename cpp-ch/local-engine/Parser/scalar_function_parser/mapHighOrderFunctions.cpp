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

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Parser/scalar_function_parser/lambdaFunction.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

using namespace DB;
namespace local_engine
{

template <bool transform_keys = true>
class FunctionParserMapTransformImpl : public FunctionParser
{
public:
    static constexpr auto name = transform_keys ? "transform_keys" : "transform_values";
    String getName() const override { return name; }

    explicit FunctionParserMapTransformImpl(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserMapTransformImpl() override = default;

    const ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const override
    {
        /// Parse spark transform_keys(map, func) as CH mapFromArrays(arrayMap(func, cast(map as array)), mapValues(map))
        /// Parse spark transform_values(map, func) as CH mapFromArrays(mapKeys(map), arrayMap(func, cast(map as array)))
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "{} function must have three arguments", getName());

        auto lambda_args = collectLambdaArguments(parser_context, substrait_func.arguments()[1].value().scalar_function());
        if (lambda_args.size() != 2)
            throw Exception(
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "The lambda function in {} must have two arguments", getName());

        const auto * map_node = parsed_args[0];
        const auto * func_node = parsed_args[1];
        const auto & map_type = map_node->result_type;
        auto array_type = checkAndGetDataType<DataTypeMap>(removeNullable(map_type).get())->getNestedType();
        if (map_type->isNullable())
            array_type = std::make_shared<DataTypeNullable>(array_type);
        const auto * array_node = ActionsDAGUtil::convertNodeTypeIfNeeded(actions_dag, map_node, array_type, getContext());
        const auto * transformed_node = toFunctionNode(actions_dag, "arrayMap", {func_node, array_node});

        const ActionsDAG::Node * result_node = nullptr;
        if constexpr (transform_keys)
        {
            const auto * nontransformed_node = toFunctionNode(actions_dag, "mapValues", {parsed_args[0]});
            result_node = toFunctionNode(actions_dag, "mapFromArrays", {transformed_node, nontransformed_node});
        }
        else
        {
            const auto * nontransformed_node = toFunctionNode(actions_dag, "mapKeys", {parsed_args[0]});
            result_node = toFunctionNode(actions_dag, "mapFromArrays", {nontransformed_node, transformed_node});
        }
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

using FunctionParserTransformKeys = FunctionParserMapTransformImpl<true>;
using FunctionParserTransformValues = FunctionParserMapTransformImpl<false>;

static FunctionParserRegister<FunctionParserTransformKeys> register_transform_keys;
static FunctionParserRegister<FunctionParserTransformValues> register_transform_values;
}