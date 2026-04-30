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
#include "PlanUtil.h"
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/BlockTypeUtils.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

namespace Setting
{
extern const SettingsUInt64 query_plan_max_step_description_length;
}
}

namespace local_engine::PlanUtil
{

std::string explainPlan(const DB::QueryPlan & plan)
{
    constexpr DB::ExplainPlanOptions buf_opt{
        .header = true,
        .actions = true,
        .indexes = true,
    };
    DB::WriteBufferFromOwnString buf;
    plan.explainPlan(buf, buf_opt);

    return buf.str();
}

void checkOuputType(const DB::QueryPlan & plan)
{
    // QueryPlan::checkInitialized is a private method, so we assume plan is initialized, otherwise there is a core dump here.
    // It's okay, because it's impossible for us not to initialize where we call this method.
    const auto & step = *plan.getRootNode()->step;

    if (!step.hasOutputHeader())
        return;
    for (const auto & elem : *step.getOutputHeader())
    {
        const DB::DataTypePtr & ch_type = elem.type;
        const auto ch_type_without_nullable = DB::removeNullable(ch_type);
        const DB::WhichDataType which(ch_type_without_nullable);
        if (which.isDateTime64())
        {
            const auto * ch_type_datetime64 = checkAndGetDataType<DB::DataTypeDateTime64>(ch_type_without_nullable.get());
            if (ch_type_datetime64->getScale() != 6)
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support converting from {}", ch_type->getName());
        }
        else if (which.isDecimal())
        {
            if (which.isDecimal256())
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support converting from {}", ch_type->getName());

            const auto scale = getDecimalScale(*ch_type_without_nullable);
            const auto precision = getDecimalPrecision(*ch_type_without_nullable);
            if (scale == 0 && precision == 0)
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support converting from {}", ch_type->getName());
        }
    }
}

DB::IQueryPlanStep * adjustQueryPlanHeader(DB::QueryPlan & plan, const DB::Block & to_header, const String & step_desc, const DB::ContextPtr context)
{
    auto convert_actions_dag = DB::ActionsDAG::makeConvertingActions(
        plan.getCurrentHeader()->getColumnsWithTypeAndName(),
        to_header.getColumnsWithTypeAndName(),
        DB::ActionsDAG::MatchColumnsMode::Name,
        context);
    auto expression_step = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(convert_actions_dag));
    expression_step->setStepDescription(step_desc, context->getSettingsRef()[DB::Setting::query_plan_max_step_description_length]);
    plan.addStep(std::move(expression_step));
    return plan.getRootNode()->step.get();
}

DB::IQueryPlanStep * addRemoveNullableStep(DB::QueryPlan & plan, const DB::ContextPtr & context, const std::set<String> & columns)
{
    if (columns.empty())
        return nullptr;
    DB::ActionsDAG remove_nullable_actions_dag{plan.getCurrentHeader()->getColumnsWithTypeAndName()};
    for (const auto & col_name : columns)
    {
        if (const auto * required_node = remove_nullable_actions_dag.tryFindInOutputs(col_name))
        {
            auto function_builder = DB::FunctionFactory::instance().get("assumeNotNull", context);
            DB::ActionsDAG::NodeRawConstPtrs args = {required_node};
            const auto & node = remove_nullable_actions_dag.addFunction(function_builder, args, col_name);
            remove_nullable_actions_dag.addOrReplaceInOutputs(node);
        }
    }
    auto expression_step = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(remove_nullable_actions_dag));
    expression_step->setStepDescription("Remove nullable properties");
    plan.addStep(std::move(expression_step));
    return plan.getRootNode()->step.get();
}

DB::IQueryPlanStep * renamePlanHeader(DB::QueryPlan & plan, const BuildNamesWithAliases & buildAliases, const String & step_desc)
{
    DB::ActionsDAG actions_dag{blockToRowType(*plan.getCurrentHeader())};
    DB::NamesWithAliases aliases;
    buildAliases(*plan.getCurrentHeader(), aliases);
    actions_dag.project(aliases);
    auto expression_step = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(actions_dag));
    expression_step->setStepDescription(step_desc, 1000);
    plan.addStep(std::move(expression_step));
    return plan.getRootNode()->step.get();
}
}