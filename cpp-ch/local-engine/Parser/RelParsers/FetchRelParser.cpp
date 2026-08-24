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

#include <memory>
#include <optional>
#include <Parser/RelParsers/RelParser.h>
#include <Processors/QueryPlan/LimitStep.h>

namespace local_engine
{
class FetchRelParser : public RelParser
{
public:
    explicit FetchRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_) { }
    ~FetchRelParser() override = default;

    DB::QueryPlanPtr parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> &)
    {
        const auto & limit = rel.fetch();
        size_t count = limit.has_count_expr() ? limit.count_expr().literal().i64() : 0;
        size_t offset = limit.has_offset_expr() ? limit.offset_expr().literal().i64() : 0;
        auto limit_step = std::make_unique<DB::LimitStep>(query_plan->getCurrentHeader(), count, offset);
        limit_step->setStepDescription("LIMIT");
        steps.push_back(limit_step.get());
        query_plan->addStep(std::move(limit_step));
        return query_plan;
    }
    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override { return &rel.fetch().input(); }
};

void registerFetchRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_unique<FetchRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kFetch, builder);
}

}
