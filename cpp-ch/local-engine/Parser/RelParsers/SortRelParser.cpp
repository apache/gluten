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
#include "SortRelParser.h"
#include <Parser/RelParsers/RelParser.h>
#include <Parser/RelParsers/SortParsingUtils.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
using namespace DB;
SortRelParser::SortRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr
SortRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    size_t limit = parseLimit(rel_stack_);
    const auto & sort_rel = rel.sort();
    auto sort_descr = parseSortFields(*query_plan->getCurrentHeader(), sort_rel.sorts());
    SortingStep::Settings settings(getContext()->getSettingsRef());
    auto config = MemoryConfig::loadFromContext(getContext());
    settings.worth_external_sort_mem_ratio = config.spill_mem_ratio;
    auto sorting_step = std::make_unique<DB::SortingStep>(query_plan->getCurrentHeader(), sort_descr, limit, settings);
    sorting_step->setStepDescription("Sorting step");
    steps.emplace_back(sorting_step.get());
    query_plan->addStep(std::move(sorting_step));
    return query_plan;
}

size_t SortRelParser::parseLimit(std::list<const substrait::Rel *> & rel_stack_)
{
    if (rel_stack_.empty())
        return 0;
    const auto & last_rel = *rel_stack_.back();
    if (last_rel.has_fetch())
    {
        const auto & fetch_rel = last_rel.fetch();
        return fetch_rel.count();
    }
    return 0;
}

void registerSortRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<SortRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kSort, builder);
}
}
