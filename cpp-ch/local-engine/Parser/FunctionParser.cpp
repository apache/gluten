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

#include "FunctionParser.h"
#include <memory>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/TypeParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/logger_useful.h>
#include "ExpressionParser.h"

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_FUNCTION;
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
using namespace DB;

FunctionParser::FunctionParser(ParserContextPtr ctx)
    : parser_context(ctx)
{
    expression_parser = std::make_unique<ExpressionParser>(parser_context);
}

String FunctionParser::getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const
{
    // no meaning
    /// There is no any simple equivalent ch function.
    return "";
}

String FunctionParser::getUniqueName(const String & name) const
{
    return expression_parser->getUniqueName(name);
}


const DB::ActionsDAG::Node *
FunctionParser::addColumnToActionsDAG(DB::ActionsDAG & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const
{
    return expression_parser->addConstColumn(actions_dag, type, field);
}

const DB::ActionsDAG::Node *
FunctionParser::toFunctionNode(DB::ActionsDAG & action_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
{
    return expression_parser->toFunctionNode(action_dag, func_name, args);
}

const DB::ActionsDAG::Node * FunctionParser::toFunctionNode(
    DB::ActionsDAG & action_dag, const String & func_name, const String & result_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
{
    return expression_parser->toFunctionNode(action_dag, func_name, args, result_name);
}

const ActionsDAG::Node * FunctionParser::parseFunctionWithDAG(
    const substrait::Expression & rel, std::string & result_name, DB::ActionsDAG & actions_dag, bool keep_result) const
{
    const auto * node = expression_parser->parseFunction(rel.scalar_function(), actions_dag, keep_result);
    result_name = node->result_name;
    return node;
}

const DB::ActionsDAG::Node * FunctionParser::parseExpression(DB::ActionsDAG & actions_dag, const substrait::Expression & rel) const
{
    return expression_parser->parseExpression(actions_dag, rel);
}

std::pair<DataTypePtr, Field> FunctionParser::parseLiteral(const substrait::Expression_Literal & literal) const
{
    return LiteralParser::parse(literal);
}

ActionsDAG::NodeRawConstPtrs
FunctionParser::parseFunctionArguments(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const
{
    return expression_parser->parseFunctionArguments(actions_dag, substrait_func);
}


const ActionsDAG::Node * FunctionParser::parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const
{
    auto ch_func_name = getCHFunctionName(substrait_func);
    auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
    const auto * func_node = toFunctionNode(actions_dag, ch_func_name, parsed_args);
    return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
}

const ActionsDAG::Node * FunctionParser::convertNodeTypeIfNeeded(
    const substrait::Expression_ScalarFunction & substrait_func, const ActionsDAG::Node * func_node, ActionsDAG & actions_dag) const
{
    const auto & output_type = substrait_func.output_type();
    const ActionsDAG::Node * result_node = nullptr;

    auto convert_type_if_needed = [&]()
    {
        if (!TypeParser::isTypeMatched(output_type, func_node->result_type))
        {
            auto result_type = TypeParser::parseType(output_type);
            if (DB::isDecimalOrNullableDecimal(result_type))
            {
                return ActionsDAGUtil::convertNodeType(
                    actions_dag,
                    func_node,
                    // as stated in isTypeMatched， currently we don't change nullability of the result type
                    func_node->result_type->isNullable() ? local_engine::wrapNullableType(true, result_type)
                                                         : local_engine::removeNullable(result_type),
                    getContext(),
                    func_node->result_name,
                    CastType::accurateOrNull);
            }
            else
            {
                // as stated in isTypeMatched， currently we don't change nullability of the result type
                auto target_type = func_node->result_type->isNullable() ? local_engine::wrapNullableType(true, result_type)
                                                                        : local_engine::removeNullable(result_type);
                return ActionsDAGUtil::convertNodeType(actions_dag, func_node, target_type, getContext(), func_node->result_name);
            }
        }
        else
            return func_node;
    };
    result_node = convert_type_if_needed();

    /// Notice that in CH Bool and UInt8 have different serialization and deserialization methods, which will cause issue when executing cast(bool as string) in spark in spark.
    auto convert_uint8_to_bool_if_needed = [&]() -> const auto *
    {
        auto * mutable_result_node = const_cast<ActionsDAG::Node *>(result_node);
        auto denull_result_type = DB::removeNullable(result_node->result_type);
        if (isUInt8(denull_result_type) && output_type.has_bool_())
        {
            auto bool_type = DB::DataTypeFactory::instance().get("Bool");
            if (result_node->result_type->isNullable())
                bool_type = DB::makeNullable(bool_type);

            mutable_result_node->result_type = std::move(bool_type);
            return mutable_result_node;
        }
        else
            return result_node;
    };
    result_node = convert_uint8_to_bool_if_needed();

    return result_node;
}

void FunctionParserFactory::registerFunctionParser(const String & name, Value value)
{
    if (!parsers.emplace(name, value).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionParserFactory: function parser name '{}' is not unique", name);
}

FunctionParserPtr FunctionParserFactory::get(const String & name, ParserContextPtr ctx)
{
    auto res = tryGet(name, ctx);
    if (!res)
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function parser {}", name);

    return res;
}

FunctionParserPtr FunctionParserFactory::tryGet(const String & name, ParserContextPtr ctx)
{
    auto it = parsers.find(name);
    if (it != parsers.end())
    {
        auto creator = it->second;
        return creator(ctx);
    }
    else
        return {};
}

FunctionParserFactory & FunctionParserFactory::instance()
{
    static FunctionParserFactory factory;
    return factory;
}

}
