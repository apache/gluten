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

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/aggregate_function_parser/PercentileParserBase.h>
#include <substrait/algebra.pb.h>
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
void PercentileParserBase::assertArgumentsSize(substrait::AggregationPhase phase, size_t size, size_t expect) const
{
    if (size != expect)
        throw Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} in phase {} requires exactly {} arguments but got {} arguments",
            getName(),
            magic_enum::enum_name(phase),
            expect,
            size);
}

const substrait::Expression::Literal &
PercentileParserBase::assertAndGetLiteral(substrait::AggregationPhase phase, const substrait::Expression & expr) const
{
    if (!expr.has_literal())
        throw Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "The argument of function {} in phase {} must be literal, but is {}",
            getName(),
            magic_enum::enum_name(phase),
            expr.DebugString());
    return expr.literal();
}

String PercentileParserBase::getCHFunctionName(const CommonFunctionInfo & func_info) const
{
    const auto & output_type = func_info.output_type;
    return output_type.has_list() ? getCHPluralName() : getCHSingularName();
}

String PercentileParserBase::getCHFunctionName(DB::DataTypes & types) const
{
    /// Always invoked during second stage
    assertArgumentsSize(substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT, types.size(), expectedTupleElementsNumberInSecondStage());

    auto type = removeNullable(types[PERCENTAGE_INDEX]);
    types.resize(1);

    if (getName() == "percentile")
    {
        /// Corresponding CH function requires two arguments: quantileExactWeightedInterpolated(xxx)(col, weight)
        types.push_back(std::make_shared<DataTypeUInt64>());
    }

    return isArray(type) ? getCHPluralName() : getCHSingularName();
}

DB::Array PercentileParserBase::parseFunctionParameters(
    const CommonFunctionInfo & func_info, DB::ActionsDAG::NodeRawConstPtrs & arg_nodes, DB::ActionsDAG & actions_dag) const
{
    if (func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
        || func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT || func_info.phase == substrait::AGGREGATION_PHASE_UNSPECIFIED)
    {
        Array params;
        const auto & arguments = func_info.arguments;
        assertArgumentsSize(func_info.phase, arguments.size(), expectedArgumentsNumberInFirstStage());

        auto param_indexes = getArgumentsThatAreParameters();
        for (auto idx : param_indexes)
        {
            const auto & expr = arguments[idx].value();
            const auto & literal = assertAndGetLiteral(func_info.phase, expr);
            auto [type, field] = parseLiteral(literal);

            if (idx == PERCENTAGE_INDEX && isArray(removeNullable(type)))
            {
                /// Multiple percentages for quantilesXXX
                const Array & percentags = field.safeGet<Array>();
                for (const auto & percentage : percentags)
                    params.emplace_back(percentage);
            }
            else
            {
                params.emplace_back(std::move(field));
            }
        }

        /// Collect arguments in substrait plan that are not CH parameters as CH arguments
        ActionsDAG::NodeRawConstPtrs new_arg_nodes;
        for (size_t i = 0; i < arg_nodes.size(); ++i)
        {
            if (std::find(param_indexes.begin(), param_indexes.end(), i) == param_indexes.end())
            {
                if (getName() == "percentile" && i == 2)
                {
                    /// In spark percentile(col, percentage, weight), the last argument weight is a signed integer
                    /// But CH requires weight as an unsigned integer
                    DataTypePtr dst_type = std::make_shared<DataTypeUInt64>();
                    if (arg_nodes[i]->result_type->isNullable())
                        dst_type = std::make_shared<DataTypeNullable>(dst_type);

                    arg_nodes[i] = ActionsDAGUtil::convertNodeTypeIfNeeded(actions_dag, arg_nodes[i], dst_type, getContext());
                }

                new_arg_nodes.emplace_back(arg_nodes[i]);
            }
        }
        new_arg_nodes.swap(arg_nodes);

        return params;
    }
    else
    {
        assertArgumentsSize(func_info.phase, arg_nodes.size(), 1);
        const auto & result_type = arg_nodes[0]->result_type;
        const auto * aggregate_function_type = DB::checkAndGetDataType<DB::DataTypeAggregateFunction>(result_type.get());
        if (!aggregate_function_type)
            throw Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "The first argument type of function {} in phase {} must be AggregateFunction, but is {}",
                getName(),
                magic_enum::enum_name(func_info.phase),
                result_type->getName());

        return aggregate_function_type->getParameters();
    }
}
}
