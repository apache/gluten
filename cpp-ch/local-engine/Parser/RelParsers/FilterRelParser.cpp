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

#include "FilterRelParser.h"
#include <Processors/QueryPlan/FilterStep.h>
#include <Rewriter/ExpressionRewriter.h>
#include <Common/CHUtil.h>
#include <Common/PlanUtil.h>

namespace local_engine
{
FilterRelParser::FilterRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}
DB::QueryPlanPtr
FilterRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    ExpressionsRewriter rewriter(parser_context);
    substrait::Rel final_rel = rel;
    rewriter.rewrite(final_rel);

    const auto & filter_rel = final_rel.filter();
    std::string filter_name;

    const auto & input_header = *query_plan->getCurrentHeader();
    DB::ActionsDAG actions_dag{input_header.getColumnsWithTypeAndName()};
    const auto condition_node = parseExpression(actions_dag, filter_rel.condition());
    if (filter_rel.condition().has_scalar_function() || filter_rel.condition().has_literal())
    {
        actions_dag.addOrReplaceInOutputs(*condition_node);
    }
    filter_name = condition_node->result_name;

    bool remove_filter_column = true;
    auto input_names = query_plan->getCurrentHeader()->getNames();
    DB::NameSet input_with_condition(input_names.begin(), input_names.end());
    if (input_with_condition.contains(condition_node->result_name))
        remove_filter_column = false;
    else
        input_with_condition.insert(condition_node->result_name);

    actions_dag.removeUnusedActions(input_with_condition);
    NonNullableColumnsResolver non_nullable_columns_resolver(input_header, parser_context, filter_rel.condition());
    auto non_nullable_columns = non_nullable_columns_resolver.resolve();

    auto filter_step
        = std::make_unique<DB::FilterStep>(query_plan->getCurrentHeader(), std::move(actions_dag), filter_name, remove_filter_column);
    filter_step->setStepDescription("WHERE");
    steps.emplace_back(filter_step.get());
    query_plan->addStep(std::move(filter_step));

    // header maybe changed, need to rollback it
    if (!blocksHaveEqualStructure(input_header, *query_plan->getCurrentHeader()))
    {
        steps.emplace_back(PlanUtil::adjustQueryPlanHeader(*query_plan, input_header, "Rollback filter header", parser_context->queryContext()));
    }

    // remove nullable
    if (auto remove_null_step = PlanUtil::addRemoveNullableStep(*query_plan, parser_context->queryContext(), non_nullable_columns))
    {
        steps.emplace_back(remove_null_step);
    }
    return query_plan;
}

void registerFilterRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr ctx) -> std::unique_ptr<RelParser> { return std::make_unique<FilterRelParser>(ctx); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kFilter, builder);
}
}
