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

#include "ArrayJoinHelper.h"
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoin.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Poco/Logger.h>
#include <Common/DebugUtils.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsUInt64 max_block_size;
extern const SettingsBool enable_lazy_columns_replication;
}
}

namespace local_engine
{
namespace ArrayJoinHelper
{
const DB::ActionsDAG::Node * findArrayJoinNode(const DB::ActionsDAG & actions_dag)
{
    const DB::ActionsDAG::Node * array_join_node = nullptr;
    const auto & nodes = actions_dag.getNodes();
    for (const auto & node : nodes)
    {
        if (node.type == DB::ActionsDAG::ActionType::ARRAY_JOIN)
        {
            if (array_join_node)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expect single ARRAY JOIN node in generate rel");
            array_join_node = &node;
        }
    }
    return array_join_node;
}

struct SplittedActionsDAGs
{
    DB::ActionsDAG before_array_join; /// Optional
    DB::ActionsDAG array_join;
    DB::ActionsDAG after_array_join; /// Optional
};

/// Split actions_dag of generate rel into 3 parts: before array join + during array join + after array join
static SplittedActionsDAGs splitActionsDAGInGenerate(const DB::ActionsDAG & actions_dag)
{
    SplittedActionsDAGs res;

    auto array_join_node = findArrayJoinNode(actions_dag);
    std::unordered_set<const DB::ActionsDAG::Node *> first_split_nodes(array_join_node->children.begin(), array_join_node->children.end());
    auto first_split_result = actions_dag.split(first_split_nodes);
    res.before_array_join = std::move(first_split_result.first);

    array_join_node = findArrayJoinNode(first_split_result.second);
    std::unordered_set<const DB::ActionsDAG::Node *> second_split_nodes = {array_join_node};
    auto second_split_result = first_split_result.second.split(second_split_nodes);
    res.array_join = std::move(second_split_result.first);
    second_split_result.second.removeUnusedActions();
    res.after_array_join = std::move(second_split_result.second);
    return res;
}

DB::ActionsDAG applyArrayJoinOnOneColumn(const DB::Block & header, size_t column_index)
{
    auto arrayColumn = header.getByPosition(column_index);
    if (!typeid_cast<const DB::DataTypeArray *>(arrayColumn.type.get()))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expect array column in array join");
    DB::ActionsDAG actions_dag(header.getColumnsWithTypeAndName());
    const auto * array_column_node = actions_dag.getInputs()[column_index];
    auto array_join_name = array_column_node->result_name;
    const auto * array_join_node = &actions_dag.addArrayJoin(*array_column_node, array_join_name);
    actions_dag.addOrReplaceInOutputs(*array_join_node);
    return std::move(actions_dag);
}


std::vector<DB::IQueryPlanStep *>
addArrayJoinStep(DB::ContextPtr context, DB::QueryPlan & plan, const DB::ActionsDAG & actions_dag, bool is_left)
{
    auto logger = getLogger("ArrayJoinHelper");
    std::vector<DB::IQueryPlanStep *> steps;
    if (findArrayJoinNode(actions_dag))
    {
        /// If generator in generate rel is explode/posexplode, transform arrayJoin function to ARRAY JOIN STEP to apply max_block_size
        /// which avoids OOM when several lateral view explode/posexplode is used in spark sqls
        LOG_TEST(logger, "original actions_dag:\n{}", debug::dumpActionsDAG(actions_dag));
        auto splitted_actions_dags = splitActionsDAGInGenerate(actions_dag);
        LOG_TEST(logger, "actions_dag before arrayJoin:\n{}", debug::dumpActionsDAG(splitted_actions_dags.before_array_join));
        LOG_TEST(logger, "actions_dag during arrayJoin:\n{}", debug::dumpActionsDAG(splitted_actions_dags.array_join));
        LOG_TEST(logger, "actions_dag after arrayJoin:\n{}", debug::dumpActionsDAG(splitted_actions_dags.after_array_join));

        auto ignore_actions_dag = [](const DB::ActionsDAG & actions_dag_) -> bool
        {
            /*
            We should ignore actions_dag like:
            0 : INPUT () (no column) String a
            1 : INPUT () (no column) String b
            Output nodes: 0, 1
             */
            return actions_dag_.getOutputs().size() == actions_dag_.getNodes().size()
                && actions_dag_.getInputs().size() == actions_dag_.getNodes().size();
        };

        /// Pre-projection before array join
        if (!ignore_actions_dag(splitted_actions_dags.before_array_join))
        {
            auto step_before_array_join
                = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(splitted_actions_dags.before_array_join));
            step_before_array_join->setStepDescription("Pre-projection In Generate");
            steps.emplace_back(step_before_array_join.get());
            plan.addStep(std::move(step_before_array_join));
            // LOG_DEBUG(logger, "plan1:{}", PlanUtil::explainPlan(*query_plan));
        }

        /// ARRAY JOIN
        DB::Names array_joined_columns{findArrayJoinNode(splitted_actions_dags.array_join)->result_name};
        DB::ArrayJoin array_join;
        array_join.columns = std::move(array_joined_columns);
        array_join.is_left = is_left;
        auto array_join_step = std::make_unique<DB::ArrayJoinStep>(
            plan.getCurrentHeader(), std::move(array_join), false, context->getSettingsRef()[DB::Setting::max_block_size], context->getSettingsRef()[DB::Setting::enable_lazy_columns_replication]);
        array_join_step->setStepDescription("ARRAY JOIN In Generate");
        steps.emplace_back(array_join_step.get());
        plan.addStep(std::move(array_join_step));
        // LOG_DEBUG(logger, "plan2:{}", PlanUtil::explainPlan(*query_plan));

        /// Post-projection after array join(Optional)
        if (!ignore_actions_dag(splitted_actions_dags.after_array_join))
        {
            auto step_after_array_join
                = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(splitted_actions_dags.after_array_join));
            step_after_array_join->setStepDescription("Post-projection In Generate");
            steps.emplace_back(step_after_array_join.get());
            plan.addStep(std::move(step_after_array_join));
            // LOG_DEBUG(logger, "plan3:{}", PlanUtil::explainPlan(*query_plan));
        }
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expect array join node in actions_dag");
    }

    return steps;
}


}
} // namespace local_engine