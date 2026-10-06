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
#include "ExpandRelParser.h"

#include <vector>
#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <Operator/AdvancedExpandStep.h>
#include <Operator/ExpandStep.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Processors/QueryPlan/QueryPlan.h>
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
ExpandRelParser::ExpandRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

void updateType(DB::DataTypePtr & type, const DB::DataTypePtr & new_type)
{
    if (type == nullptr || (!type->isNullable() && new_type->isNullable()))
        type = new_type;
}

DB::QueryPlanPtr
ExpandRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    if (!isLazyAggregateExpand(rel.expand()))
        return normalParse(std::move(query_plan), rel, rel_stack);
    else
        return lazyAggregateExpandParse(std::move(query_plan), rel, rel_stack);
}

ExpandField ExpandRelParser::buildExpandField(const DB::Block & header, const substrait::ExpandRel & expand_rel)
{
    std::vector<std::vector<ExpandFieldKind>> expand_kinds;
    std::vector<std::vector<DB::Field>> expand_fields;
    std::vector<DB::DataTypePtr> types;
    std::vector<std::string> names;
    std::set<String> distinct_names;

    expand_kinds.reserve(expand_rel.fields_size());
    expand_fields.reserve(expand_rel.fields_size());

    for (const auto & projections : expand_rel.fields())
    {
        auto expand_col_size = projections.switching_field().duplicates_size();

        std::vector<ExpandFieldKind> kinds;
        std::vector<DB::Field> fields;

        kinds.reserve(expand_col_size);
        fields.reserve(expand_col_size);

        if (types.empty())
            types.resize(expand_col_size, nullptr);
        if (names.empty())
            names.resize(expand_col_size);

        for (int i = 0; i < expand_col_size; ++i)
        {
            const auto & project_expr = projections.switching_field().duplicates(i);
            if (auto field_index = SubstraitParserUtils::getStructFieldIndex(project_expr))
            {
                kinds.push_back(ExpandFieldKind::EXPAND_FIELD_KIND_SELECTION);
                fields.push_back(*field_index);
                if (*field_index >= header.columns())
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR, "Field index out of range: {}, header: {}", *field_index, header.dumpStructure());
                }
                updateType(types[i], header.getByPosition(*field_index).type);
                const auto & name = header.getByPosition(*field_index).name;
                if (names[i].empty())
                {
                    if (distinct_names.contains(name))
                    {
                        auto unique_name = getUniqueName(name);
                        distinct_names.emplace(unique_name);
                        names[i] = unique_name;
                    }
                    else
                    {
                        distinct_names.emplace(name);
                        names[i] = name;
                    }
                }
            }
            else if (project_expr.has_literal())
            {
                auto [type, field] = parseLiteral(project_expr.literal());
                // A NULL literal is nullable even when the type carried by the
                // Substrait literal does not explicitly encode nullability.
                // Keep that information in the Expand output type so the
                // generated column contains a real null map.
                if (field.isNull() && type && !type->isNullable())
                    type = DB::makeNullable(type);
                kinds.push_back(ExpandFieldKind::EXPAND_FIELD_KIND_LITERAL);
                fields.push_back(field);
                updateType(types[i], type);
            }
            else
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported expression in projections");
            }
        }

        expand_kinds.push_back(std::move(kinds));
        expand_fields.push_back(std::move(fields));
    }

    for (int i = 0; i < names.size(); ++i)
        if (names[i].empty())
            names[i] = getUniqueName("expand_" + std::to_string(i));

    ExpandField expand_field(names, types, expand_kinds, expand_fields);
    return expand_field;
}

bool ExpandRelParser::isLazyAggregateExpand(const substrait::ExpandRel & expand_rel)
{
    const auto & input_rel = expand_rel.input();
    if (input_rel.rel_type_case() != substrait::Rel::RelTypeCase::kAggregate)
        return false;
    const auto & aggregate_rel = input_rel.aggregate();
    for (const auto & measure : aggregate_rel.measures())
        if (measure.measure().phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
            return false;
    return true;
}

DB::QueryPlanPtr ExpandRelParser::normalParse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> &)
{
    const auto & expand_rel = rel.expand();
    const auto & header = *query_plan->getCurrentHeader();
    auto expand_field = buildExpandField(header, expand_rel);
    auto expand_step = std::make_unique<ExpandStep>(query_plan->getCurrentHeader(), std::move(expand_field));
    expand_step->setStepDescription("Expand Step");
    steps.emplace_back(expand_step.get());
    query_plan->addStep(std::move(expand_step));
    return query_plan;
}

DB::QueryPlanPtr ExpandRelParser::lazyAggregateExpandParse(
    DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    const auto & input_header = query_plan->getCurrentHeader();
    const auto & expand_rel = rel.expand();
    auto expand_field = buildExpandField(*input_header, expand_rel);
    auto aggregate_rel = rel.expand().input().aggregate();
    auto aggregate_descriptions = buildAggregations(*input_header, expand_field, aggregate_rel);

    size_t grouping_keys = aggregate_rel.groupings(0).expression_references_size();

    auto expand_step
        = std::make_unique<AdvancedExpandStep>(getContext(), input_header, grouping_keys, aggregate_descriptions, expand_field);
    expand_step->setStepDescription("Advanced Expand Step");
    steps.emplace_back(expand_step.get());
    query_plan->addStep(std::move(expand_step));
    return query_plan;
}

DB::AggregateDescriptions ExpandRelParser::buildAggregations(
    const DB::Block & input_header, const ExpandField & expand_field, const substrait::AggregateRel & aggregate_rel)
{
    auto header = AdvancedExpandStep::buildOutputHeader(input_header, expand_field);
    DB::AggregateDescriptions descriptions;
    DB::ColumnsWithTypeAndName aggregate_columns;
    for (const auto & col : header.getColumnsWithTypeAndName())
        if (typeid_cast<const DB::ColumnAggregateFunction *>(col.column.get()))
            aggregate_columns.push_back(col);

    for (size_t i = 0; i < aggregate_rel.measures_size(); ++i)
    {
        /// The output header of the aggregate is [grouping keys] ++ [aggregation columns]
        const auto & measure = aggregate_rel.measures(i);
        const auto & col = aggregate_columns[i];
        DB::AggregateDescription description;
        auto aggregate_col = typeid_cast<const DB::ColumnAggregateFunction *>(col.column.get());

        description.column_name = col.name;
        description.argument_names = {col.name};

        auto aggregate_function = aggregate_col->getAggregateFunction();
        description.parameters = aggregate_function->getParameters();

        // Need apply "PartialMerge" combinator for the aggregate function.
        auto function_name_with_combinator = aggregate_function->getName() + "PartialMerge";
        DB::AggregateFunctionProperties aggregate_function_properties;
        description.function
            = getAggregateFunction(function_name_with_combinator, {col.type}, aggregate_function_properties, description.parameters);

        descriptions.emplace_back(description);
    }
    return descriptions;
}

void registerExpandRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<ExpandRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kExpand, builder);
}
}
