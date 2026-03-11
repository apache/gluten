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

#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
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

namespace local_engine
{
using namespace DB;

class FunctionParserArrayFilter : public FunctionParser
{
public:
    static constexpr auto name = "filter";
    explicit FunctionParserArrayFilter(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserArrayFilter() override = default;

    String getName() const override { return name; }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        return "arrayFilter";
    }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        assert(parsed_args.size() == 2);
        if (collectLambdaArguments(parser_context, substrait_func.arguments()[1].value().scalar_function()).size() == 1)
            return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0]});

        /// filter with index argument.
        const auto * range_end_node = toFunctionNode(actions_dag, "length", {toFunctionNode(actions_dag, "assumeNotNull", {parsed_args[0]})});
        range_end_node = ActionsDAGUtil::convertNodeType(
            actions_dag, range_end_node, makeNullable(INT()), range_end_node->result_name);
        const auto * index_array_node = toFunctionNode(
            actions_dag,
            "range",
            {addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 0), range_end_node});
        return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0], index_array_node});
    }
};
static FunctionParserRegister<FunctionParserArrayFilter> register_array_filter;

class FunctionParserArrayTransform : public FunctionParser
{
public:
    static constexpr auto name = "transform";
    explicit FunctionParserArrayTransform(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserArrayTransform() override = default;
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        return "arrayMap";
    }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto ch_func_name = getCHFunctionName(substrait_func);
        auto lambda_args = collectLambdaArguments(parser_context, substrait_func.arguments()[1].value().scalar_function());
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        assert(parsed_args.size() == 2);
        if (lambda_args.size() == 1)
        {
            /// Convert Array(T) to Array(U) if needed, Array(T) is the type of the first argument of transform.
            /// U is the argument type of lambda function. In some cases Array(T) is not equal to Array(U).
            /// e.g. in the second query of https://github.com/apache/gluten/issues/6561, T is String, and U is Nullable(String)
            /// The difference of both types will result in runtime exceptions in function capture.
            const auto & src_array_type = parsed_args[0]->result_type;
            DataTypePtr dst_array_type = std::make_shared<DataTypeArray>(lambda_args.front().type);
            if (src_array_type->isNullable())
                dst_array_type = std::make_shared<DataTypeNullable>(dst_array_type);
            const auto * dst_array_arg = ActionsDAGUtil::convertNodeTypeIfNeeded(actions_dag, parsed_args[0], dst_array_type);
            return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], dst_array_arg});
        }

        /// transform with index argument.
        const auto * range_end_node = toFunctionNode(actions_dag, "length", {toFunctionNode(actions_dag, "assumeNotNull", {parsed_args[0]})});
        range_end_node = ActionsDAGUtil::convertNodeType(
            actions_dag, range_end_node, makeNullable(INT()), range_end_node->result_name);
        const auto * index_array_node = toFunctionNode(
            actions_dag,
            "range",
            {addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 0), range_end_node});
        return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0], index_array_node});
    }
};
static FunctionParserRegister<FunctionParserArrayTransform> register_array_map;

class FunctionParserArrayAggregate : public FunctionParser
{
public:
    static constexpr auto name = "aggregate";
    explicit FunctionParserArrayAggregate(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserArrayAggregate() override = default;
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        return "arrayFold";
    }
    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        assert(parsed_args.size() == 3);

        const auto * function_type = typeid_cast<const DataTypeFunction *>(parsed_args[2]->result_type.get());
        if (!function_type)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "The third argument of aggregate function must be a lambda function");

        if (!parsed_args[1]->result_type->equals(*(function_type->getReturnType())))
        {
            parsed_args[1] = ActionsDAGUtil::convertNodeType(
                actions_dag,
                parsed_args[1],
                function_type->getReturnType(),
                parsed_args[1]->result_name);
        }

        /// arrayFold cannot accept nullable(array)
        const auto * array_col_node = parsed_args[0];
        if (parsed_args[0]->result_type->isNullable())
        {
            array_col_node = toFunctionNode(actions_dag, "assumeNotNull", {parsed_args[0]});
        }
        const auto * func_node = toFunctionNode(actions_dag, ch_func_name, {parsed_args[2], array_col_node, parsed_args[1]});
        /// For null array, result is null.
        /// TODO: make a new version of arrayFold that can handle nullable array.
        const auto * is_null_node = toFunctionNode(actions_dag, "isNull", {parsed_args[0]});
        const auto * null_node = addColumnToActionsDAG(actions_dag, DB::makeNullable(func_node->result_type), DB::Null());
        return toFunctionNode(actions_dag, "if", {is_null_node, null_node, func_node});
    }
};
static FunctionParserRegister<FunctionParserArrayAggregate> register_array_aggregate;

class FunctionParserArraySort : public FunctionParser
{
public:
    static constexpr auto name = "array_sort";

    explicit FunctionParserArraySort(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserArraySort() override = default;

    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        return "arraySortSpark";
    }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);

        if (parsed_args.size() < 1 || parsed_args.size() > 2)
            throw DB::Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "array_sort function must have one or two arguments");

        /// When only one argument is provided, we use the default comparator.
        if (parsed_args.size() == 1)
            return toFunctionNode(actions_dag, ch_func_name, {parsed_args[0]});

        auto lambda_args = collectLambdaArguments(parser_context, substrait_func.arguments()[1].value().scalar_function());
        if (lambda_args.size() != 2 || !lambda_args.front().type->equals(*(lambda_args.back().type)))
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "array_sort function must have a lambda function with two arguments of the same type");

        /// In case lambda argument types are T, and the array has type Array(Nullable(T)) or Nullable(Array(Nullable(T))).
        /// We need to convert the array type to Array(T) or Nullable(Array(T)) to match the lambda argument types, otherwise it will cause runtime exceptions
        /// in function capture.
        const auto & src_array_type = parsed_args[0]->result_type;
        DataTypePtr dst_array_type = std::make_shared<DataTypeArray>(lambda_args.front().type);
        if (src_array_type->isNullable())
            dst_array_type = std::make_shared<DataTypeNullable>(dst_array_type);
        parsed_args[0] = ActionsDAGUtil::convertNodeTypeIfNeeded(actions_dag, parsed_args[0], dst_array_type);

        return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0]});
    }
};
static FunctionParserRegister<FunctionParserArraySort> register_array_sort;

class FunctionParserZipWith: public FunctionParser
{
public:
    static constexpr auto name = "zip_with";
    explicit FunctionParserZipWith(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserZipWith() override = default;
    String getName() const override { return name; }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Parse spark zip_with(arr1, arr2, func) as CH arrayMap(func, arrayZipUnaligned(arr1, arr2))
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 3)
            throw DB::Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "zip_with function must have three arguments");

        auto lambda_args = collectLambdaArguments(parser_context, substrait_func.arguments()[2].value().scalar_function());
        if (lambda_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "The lambda function in zip_with must have two arguments");

        const auto * array_zip_unaligned = toFunctionNode(actions_dag, "arrayZipUnaligned", {parsed_args[0], parsed_args[1]});
        const auto * array_map = toFunctionNode(actions_dag, "arrayMap", {parsed_args[2], array_zip_unaligned});
        return convertNodeTypeIfNeeded(substrait_func, array_map, actions_dag);
    }
};
static FunctionParserRegister<FunctionParserZipWith> register_zip_with;


}
