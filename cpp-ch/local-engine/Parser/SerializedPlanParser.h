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
#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/RelMetric.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/SourceFromJavaIter.h>
#include <base/types.h>
#include <substrait/plan.pb.h>

namespace local_engine
{

std::string join(const DB::ActionsDAG::NodeRawConstPtrs & v, char c);

class SerializedPlanParser;
class LocalExecutor;
class ParserContext;
class ExpressionParser;

// Give a condition expression `cond_rel_`, found all columns with nullability that must not containt
// null after this filter.
// It's used to remove nullability of the columns for performance reason.
class NonNullableColumnsResolver
{
public:
    explicit NonNullableColumnsResolver(
        const DB::Block & header_, std::shared_ptr<const ParserContext> parser_context_, const substrait::Expression & cond_rel_);
    ~NonNullableColumnsResolver() = default;

    // return column names
    std::set<String> resolve();

private:
    DB::Block header;
    std::shared_ptr<const ParserContext> parser_context;
    const substrait::Expression & cond_rel;
    std::unique_ptr<ExpressionParser> expression_parser;

    std::set<String> collected_columns;

    void visit(const substrait::Expression & expr);
    void visitNonNullable(const substrait::Expression & expr);
};

class SerializedPlanParser
{
private:
    std::unique_ptr<LocalExecutor> createExecutor(DB::QueryPlanPtr query_plan, const substrait::Plan & s_plan) const;

public:
    explicit SerializedPlanParser(std::shared_ptr<const ParserContext> parser_context_);

    /// visible for UT
    DB::QueryPlanPtr parse(const substrait::Plan & plan);
    std::unique_ptr<LocalExecutor> createExecutor(const substrait::Plan & plan);
    DB::QueryPipelineBuilderPtr buildQueryPipeline(DB::QueryPlan & query_plan) const;
    ///
    std::unique_ptr<LocalExecutor> createExecutor(const std::string_view plan);

    void addInputIter(jobject iter, bool materialize_input)
    {
        input_iters.emplace_back(iter);
        materialize_inputs.emplace_back(materialize_input);
    }

    std::pair<jobject, bool> getInputIter(size_t index)
    {
        if (index > input_iters.size())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Index({}) is overflow input_iters's size({})", index, input_iters.size());
        return {input_iters[index], materialize_inputs[index]};
    }

    void addSplitInfo(std::string && split_info) { split_infos.emplace_back(std::move(split_info)); }

    int nextSplitInfoIndex()
    {
        if (split_info_index >= split_infos.size())
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "split info index out of range, split_info_index: {}, split_infos.size(): {}",
                split_info_index,
                split_infos.size());
        return split_info_index++;
    }

    const String & nextSplitInfo()
    {
        auto next_index = nextSplitInfoIndex();
        return split_infos.at(next_index);
    }

    RelMetricPtr getMetric() { return metrics.empty() ? nullptr : metrics.at(0); }

    std::vector<DB::QueryPlanPtr> extra_plan_holder;

private:
    DB::QueryPlanPtr parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack);
    static void adjustOutput(const DB::QueryPlanPtr & query_plan, const substrait::Plan & plan, const DB::ContextPtr context);

    std::vector<jobject> input_iters;
    std::vector<std::string> split_infos;
    int split_info_index = 0;
    std::vector<bool> materialize_inputs;
    DB::ContextPtr context;
    std::shared_ptr<const ParserContext> parser_context;
    std::vector<RelMetricPtr> metrics;
};

}
