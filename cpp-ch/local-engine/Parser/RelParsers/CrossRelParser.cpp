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
#include "CrossRelParser.h"
#include <optional>

#include <Core/Settings.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Join/BroadCastJoinBuilder.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Parser/ExpressionParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/CHUtil.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_block_size;
extern const SettingsUInt64 min_joined_block_size_rows;
extern const SettingsUInt64 min_joined_block_size_bytes;
}
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
using namespace DB;

std::shared_ptr<DB::TableJoin> createCrossTableJoin(substrait::CrossRel_JoinType join_type)
{
    auto global_context = QueryContext::globalContext();
    auto table_join = std::make_shared<TableJoin>(
        global_context->getSettingsRef(), global_context->getGlobalTemporaryVolume(), global_context->getTempDataOnDisk());

    std::pair<DB::JoinKind, DB::JoinStrictness> kind_and_strictness = JoinUtil::getCrossJoinKindAndStrictness(join_type);
    table_join->setKind(kind_and_strictness.first);
    table_join->setStrictness(kind_and_strictness.second);
    return table_join;
}

CrossRelParser::CrossRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_), context(parser_context_->queryContext())
{
}

DB::QueryPlanPtr
CrossRelParser::parse(DB::QueryPlanPtr /*query_plan*/, const substrait::Rel & /*rel*/, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call parse().");
}

std::vector<const substrait::Rel *> CrossRelParser::getInputs(const substrait::Rel & rel)
{
    const auto & join = rel.cross();
    if (!join.has_left() || !join.has_right())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "left table or right table is missing.");
    }

    return {&join.left(), &join.right()};
}
std::optional<const substrait::Rel *> CrossRelParser::getSingleInput(const substrait::Rel & /*rel*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call getSingleInput().");
}

// For non-cross join, CH uses constant join keys. We keep the same implementation here.
void CrossRelParser::addConstJoinKeys(DB::QueryPlan & left, DB::QueryPlan & right)
{
    auto data_type_u8 = std::make_shared<DataTypeUInt8>();
    auto const_key_col = data_type_u8->createColumnConst(1, UInt8(0));

    String left_key = JoinUtil::CROSS_REL_LEFT_CONST_KEY_COLUMN;
    auto left_columns = left.getCurrentHeader()->getColumnsWithTypeAndName();
    DB::ActionsDAG left_project_actions(left_columns);
    const auto & left_key_node = left_project_actions.addColumn({const_key_col, data_type_u8, left_key});
    left_project_actions.addOrReplaceInOutputs(left_key_node);
    auto left_project_step = std::make_unique<ExpressionStep>(left.getCurrentHeader(), std::move(left_project_actions));
    left_project_step->setStepDescription("Add const join key for cross rel left");
    left.addStep(std::move(left_project_step));

    String right_key = JoinUtil::CROSS_REL_RIGHT_CONST_KEY_COLUMN;
    auto right_columns = right.getCurrentHeader()->getColumnsWithTypeAndName();
    DB::ActionsDAG right_project_actions(right_columns);
    const auto & right_key_node = right_project_actions.addColumn({const_key_col, data_type_u8, right_key});
    right_project_actions.addOrReplaceInOutputs(right_key_node);
    auto right_project_step = std::make_unique<ExpressionStep>(right.getCurrentHeader(), std::move(right_project_actions));
    right_project_step->setStepDescription("Add const join key for cross rel right");
    right.addStep(std::move(right_project_step));
}

DB::QueryPlanPtr
CrossRelParser::parse(std::vector<DB::QueryPlanPtr> & input_plans_, const substrait::Rel & rel, std::list<const substrait::Rel *> &)
{
    assert(input_plans_.size() == 2);
    const auto & join = rel.cross();
    std::pair<DB::JoinKind, DB::JoinStrictness> kind_and_strictness = JoinUtil::getCrossJoinKindAndStrictness(join.type());
    if (kind_and_strictness.first != JoinKind::Cross)
        addConstJoinKeys(*input_plans_[0], *input_plans_[1]);
    return parseJoin(join, std::move(input_plans_[0]), std::move(input_plans_[1]));
}

void CrossRelParser::renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join)
{
    /// To support mixed join conditions, we must make sure that the column names in the right be the same as
    /// storage_join's right sample block.
    auto right_ori_header = right.getCurrentHeader()->getColumnsWithTypeAndName();
    if (right_ori_header.size() > 0 && right_ori_header[0].name != BlockUtil::VIRTUAL_ROW_COUNT_COLUMN)
    {
        ActionsDAG right_project = ActionsDAG::makeConvertingActions(
            right_ori_header, storage_join.getRightSampleBlock().getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Position, getContext());
        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right.getCurrentHeader(), std::move(right_project));
        project_step->setStepDescription("Rename Broadcast Table Name");
        steps.emplace_back(project_step.get());
        right.addStep(std::move(project_step));
    }

    /// If the columns name in right table is duplicated with left table, we need to rename the left table's columns,
    /// avoid the columns name in the right table be changed in `addConvertStep`.
    /// This could happen in tpc-ds q44.
    DB::ColumnsWithTypeAndName new_left_cols;
    const auto & right_header = *right.getCurrentHeader();
    auto left_prefix = getUniqueName("left");
    for (const auto & col : *left.getCurrentHeader())
        if (right_header.has(col.name))
            new_left_cols.emplace_back(col.column, col.type, left_prefix + col.name);
        else
            new_left_cols.emplace_back(col.column, col.type, col.name);
    auto left_header = left.getCurrentHeader()->getColumnsWithTypeAndName();
    ActionsDAG left_project = ActionsDAG::makeConvertingActions(left_header, new_left_cols, ActionsDAG::MatchColumnsMode::Position, getContext());

    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(left.getCurrentHeader(), std::move(left_project));
    project_step->setStepDescription("Rename Left Table Name for broadcast join");
    steps.emplace_back(project_step.get());
    left.addStep(std::move(project_step));
}

DB::QueryPlanPtr CrossRelParser::parseJoin(const substrait::CrossRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right)
{
    google::protobuf::StringValue optimization_info;
    optimization_info.ParseFromString(join.advanced_extension().optimization().value());
    auto join_opt_info = JoinOptimizationInfo::parse(optimization_info.value());
    const auto & storage_join_key = join_opt_info.storage_join_key;
    auto storage_join = join_opt_info.is_broadcast ? BroadCastJoinBuilder::getJoin(storage_join_key) : nullptr;
    if (storage_join)
        renamePlanColumns(*left, *right, *storage_join);
    auto table_join = createCrossTableJoin(join.type());
    DB::Block right_header_before_convert_step{*right->getCurrentHeader()};
    addConvertStep(*table_join, *left, *right);

    // Add a check to find error easily.
    if (!blocksHaveEqualStructure(right_header_before_convert_step, *right->getCurrentHeader()))
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "For broadcast join, we must not change the columns name in the right table.\nleft header:{},\nright header: {} -> {}",
            left->getCurrentHeader()->dumpNames(),
            right_header_before_convert_step.dumpNames(),
            right->getCurrentHeader()->dumpNames());
    }

    Names after_join_names = collectOutputColumnsName(*left, *right);

    if (table_join->kind() != JoinKind::Cross)
    {
        table_join->addDisjunct();
        auto & join_clause = table_join->getClauses().back();
        String left_key = JoinUtil::CROSS_REL_LEFT_CONST_KEY_COLUMN;
        String right_key = JoinUtil::CROSS_REL_RIGHT_CONST_KEY_COLUMN;
        join_clause.addKey(left_key, right_key, false);
    }

    QueryPlanPtr query_plan;
    if (storage_join)
    {
        auto broadcast_hash_join = storage_join->getJoinLocked(table_join, context);
        // table_join->resetKeys();
        QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(left->getCurrentHeader(), broadcast_hash_join, 8192);

        join_step->setStepDescription("STORAGE_JOIN");
        steps.emplace_back(join_step.get());
        left->addStep(std::move(join_step));
        query_plan = std::move(left);
        /// hold right plan for profile
        extra_plan_holder.emplace_back(std::move(right));

        addPostFilter(*query_plan, join);
        JoinUtil::adjustJoinOutput(*query_plan, after_join_names);
    }
    else
    {
        JoinPtr hash_join = std::make_shared<HashJoin>(table_join, right->getCurrentHeader());
        QueryPlanStepPtr join_step = std::make_unique<DB::JoinStep>(
            left->getCurrentHeader(),
            right->getCurrentHeader(),
            hash_join,
            context->getSettingsRef()[Setting::max_block_size],
            context->getSettingsRef()[Setting::min_joined_block_size_rows],
            context->getSettingsRef()[Setting::min_joined_block_size_bytes],
            1,
            /* required_output_ = */ NameSet{},
            false,
            /* use_new_analyzer_ = */ false);
        join_step->setStepDescription("CROSS_JOIN");
        steps.emplace_back(join_step.get());
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
        JoinUtil::adjustJoinOutput(*query_plan, after_join_names);
    }

    return query_plan;
}


void CrossRelParser::addPostFilter(DB::QueryPlan & query_plan, const substrait::CrossRel & join_rel)
{
    if (!join_rel.has_expression())
        return;

    auto expression = join_rel.expression();
    std::string filter_name;
    ActionsDAG actions_dag(query_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    if (!expression.has_scalar_function())
    {
        // It may be singular_or_list
        const auto * in_node = expression_parser->parseExpression(actions_dag, expression);
        filter_name = in_node->result_name;
    }
    else
    {
        const auto * func_node = expression_parser->parseFunction(expression.scalar_function(), actions_dag, true);
        filter_name = func_node->result_name;
    }
    auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(), std::move(actions_dag), filter_name, true);
    filter_step->setStepDescription("Post Join Filter");
    steps.emplace_back(filter_step.get());
    query_plan.addStep(std::move(filter_step));
}

void CrossRelParser::addConvertStep(TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right)
{
    /// If the columns name in right table is duplicated with left table, we need to rename the right table's columns.
    NameSet left_columns_set;
    for (const auto & col : left.getCurrentHeader()->getNames())
        left_columns_set.emplace(col);

    table_join.setColumnsFromJoinedTable(
        right.getCurrentHeader()->getNamesAndTypesList(),
        left_columns_set,
        getUniqueName("right") + ".",
        left.getCurrentHeader()->getNamesAndTypesList());

    // fix right table key duplicate
    NamesWithAliases right_table_alias;
    for (size_t idx = 0; idx < table_join.columnsFromJoinedTable().size(); idx++)
    {
        auto origin_name = right.getCurrentHeader()->getByPosition(idx).name;
        auto dedup_name = table_join.columnsFromJoinedTable().getNames().at(idx);
        if (origin_name != dedup_name)
        {
            right_table_alias.emplace_back(NameWithAlias(origin_name, dedup_name));
        }
    }
    if (!right_table_alias.empty())
    {
        ActionsDAG rename_dag(right.getCurrentHeader()->getNamesAndTypesList());
        const auto & original_right_columns = *right.getCurrentHeader();
        for (const auto & column_alias : right_table_alias)
        {
            if (original_right_columns.has(column_alias.first))
            {
                auto pos = original_right_columns.getPositionByName(column_alias.first);
                const auto & alias = rename_dag.addAlias(*rename_dag.getInputs()[pos], column_alias.second);
                rename_dag.getOutputs()[pos] = &alias;
            }
        }

        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right.getCurrentHeader(), std::move(rename_dag));
        project_step->setStepDescription("Right Table Rename");
        steps.emplace_back(project_step.get());
        right.addStep(std::move(project_step));
    }

    for (const auto & column : table_join.columnsFromJoinedTable())
    {
        table_join.addJoinedColumn(column);
    }
    std::optional<ActionsDAG> left_convert_actions;
    std::optional<ActionsDAG> right_convert_actions;
    std::tie(left_convert_actions, right_convert_actions) = table_join.createConvertingActions(
        left.getCurrentHeader()->getColumnsWithTypeAndName(), right.getCurrentHeader()->getColumnsWithTypeAndName(), getContext());

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right.getCurrentHeader(), std::move(*right_convert_actions));
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        right.addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(left.getCurrentHeader(), std::move(*left_convert_actions));
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        left.addStep(std::move(converting_step));
    }
}

DB::Names CrossRelParser::collectOutputColumnsName(const DB::QueryPlan & left, const DB::QueryPlan & right)
{
    Names join_result_names;
    auto is_unused_column = [](const String & name)
    {
        return name == JoinUtil::CROSS_REL_LEFT_CONST_KEY_COLUMN || name == JoinUtil::CROSS_REL_RIGHT_CONST_KEY_COLUMN
            || name == BlockUtil::VIRTUAL_ROW_COUNT_COLUMN;
    };
    for (auto & col : left.getCurrentHeader()->getColumnsWithTypeAndName())
    {
        if (!is_unused_column(col.name))
            join_result_names.emplace_back(col.name);
    }
    for (auto & col : right.getCurrentHeader()->getColumnsWithTypeAndName())
    {
        if (!is_unused_column(col.name))
            join_result_names.emplace_back(col.name);
    }
    return join_result_names;
}

void registerCrossRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<CrossRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kCross, builder);
}

}
