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

#include <memory>
#include <optional>
#include <Parser/RelParsers/RelParser.h>
#include <substrait/algebra.pb.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{
class TableJoin;
}

namespace local_engine
{

class StorageJoinFromReadBuffer;


/// Cross rel is for joins without joining keys. For example,
///   SELECT * FROM t1 LEFT JOIN t2
class CrossRelParser : public RelParser
{
public:
    explicit CrossRelParser(ParserContextPtr parser_context_);
    ~CrossRelParser() override = default;

    DB::QueryPlanPtr parse(
        std::vector<DB::QueryPlanPtr> & input_plans_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;
    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;

    std::vector<const substrait::Rel *> getInputs(const substrait::Rel & rel) override;
    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override;
    std::vector<DB::QueryPlanPtr> extraPlans() override { return std::move(extra_plan_holder); }

private:
    DB::ContextPtr context;
    std::vector<DB::QueryPlanPtr> extra_plan_holder;


    DB::QueryPlanPtr parseJoin(const substrait::NestedLoopJoinRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right);
    void renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join);
    void addConvertStep(DB::TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right);
    void addPostFilter(DB::QueryPlan & query_plan, const substrait::NestedLoopJoinRel & join);
    bool applyJoinFilter(
        DB::TableJoin & table_join,
        const substrait::NestedLoopJoinRel & join_rel,
        DB::QueryPlan & left,
        DB::QueryPlan & right,
        bool allow_mixed_condition);

    void addConstJoinKeys(DB::QueryPlan & left, DB::QueryPlan & right);
    DB::Names collectOutputColumnsName(const DB::QueryPlan & left, const DB::QueryPlan & right);
};

}
