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
#include "JoinRelParser.h"
#include <optional>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Join/BroadCastJoinBuilder.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Operator/EarlyStopStep.h>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Parser/ExpressionParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/CHUtil.h>
#include <Common/GlutenConfig.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
extern const SettingsJoinAlgorithm join_algorithm;
extern const SettingsUInt64 max_block_size;
extern const SettingsUInt64 min_joined_block_size_rows;
extern const SettingsUInt64 min_joined_block_size_bytes;
extern const SettingsNonZeroUInt64 grace_hash_join_initial_buckets;
extern const SettingsNonZeroUInt64 grace_hash_join_max_buckets;
}
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
extern const int BAD_ARGUMENTS;
}
}
using namespace DB;

namespace local_engine
{
std::shared_ptr<DB::TableJoin> createDefaultTableJoin(substrait::JoinRel_JoinType join_type, const JoinOptimizationInfo & join_opt_info, ContextPtr & context)
{
    auto table_join
        = std::make_shared<TableJoin>(context->getSettingsRef(), context->getGlobalTemporaryVolume(), context->getTempDataOnDisk());

    std::pair<DB::JoinKind, DB::JoinStrictness> kind_and_strictness = JoinUtil::getJoinKindAndStrictness(join_type, join_opt_info.is_existence_join);
    table_join->setKind(kind_and_strictness.first);
    if (!join_opt_info.is_any_join)
        table_join->setStrictness(kind_and_strictness.second);
    else
        table_join->setStrictness(DB::JoinStrictness::Any);
    return table_join;
}

JoinRelParser::JoinRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_), context(parser_context_->queryContext())
{
}

DB::QueryPlanPtr
JoinRelParser::parse(DB::QueryPlanPtr /*query_plan*/, const substrait::Rel & /*rel*/, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call parse().");
}

std::vector<const substrait::Rel *> JoinRelParser::getInputs(const substrait::Rel & rel)
{
    const auto & join = rel.join();
    if (!join.has_left() || !join.has_right())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "left table or right table is missing.");

    return {&join.left(), &join.right()};
}
std::optional<const substrait::Rel *> JoinRelParser::getSingleInput(const substrait::Rel & /*rel*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call getSingleInput().");
}

DB::QueryPlanPtr JoinRelParser::parse(
    std::vector<DB::QueryPlanPtr> & input_plans_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    assert(input_plans_.size() == 2);
    const auto & join = rel.join();
    return parseJoin(join, std::move(input_plans_[0]), std::move(input_plans_[1]));
}

std::unordered_set<DB::JoinTableSide> JoinRelParser::extractTableSidesFromExpression(
    const substrait::Expression & expr, const DB::Block & left_header, const DB::Block & right_header)
{
    std::unordered_set<DB::JoinTableSide> table_sides;
    if (expr.has_scalar_function())
    {
        for (const auto & arg : expr.scalar_function().arguments())
        {
            auto table_sides_from_arg = extractTableSidesFromExpression(arg.value(), left_header, right_header);
            table_sides.insert(table_sides_from_arg.begin(), table_sides_from_arg.end());
        }
    }
    else if (auto field = SubstraitParserUtils::getStructFieldIndex(expr))
    {
        if (*field < left_header.columns())
            table_sides.insert(DB::JoinTableSide::Left);
        else
            table_sides.insert(DB::JoinTableSide::Right);
    }
    else if (expr.has_singular_or_list())
    {
        auto child_table_sides = extractTableSidesFromExpression(expr.singular_or_list().value(), left_header, right_header);
        table_sides.insert(child_table_sides.begin(), child_table_sides.end());
        for (const auto & option : expr.singular_or_list().options())
        {
            child_table_sides = extractTableSidesFromExpression(option, left_header, right_header);
            table_sides.insert(child_table_sides.begin(), child_table_sides.end());
        }
    }
    else if (expr.has_cast())
    {
        auto child_table_sides = extractTableSidesFromExpression(expr.cast().input(), left_header, right_header);
        table_sides.insert(child_table_sides.begin(), child_table_sides.end());
    }
    else if (expr.has_if_then())
    {
        for (const auto & if_child : expr.if_then().ifs())
        {
            auto child_table_sides = extractTableSidesFromExpression(if_child.if_(), left_header, right_header);
            table_sides.insert(child_table_sides.begin(), child_table_sides.end());
            child_table_sides = extractTableSidesFromExpression(if_child.then(), left_header, right_header);
            table_sides.insert(child_table_sides.begin(), child_table_sides.end());
        }
        auto child_table_sides = extractTableSidesFromExpression(expr.if_then().else_(), left_header, right_header);
        table_sides.insert(child_table_sides.begin(), child_table_sides.end());
    }
    else if (expr.has_literal())
    {
        // nothing
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Illegal expression '{}'", expr.DebugString());
    }
    return table_sides;
}


void JoinRelParser::renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join)
{
    /// To support mixed join conditions, we must make sure that the column names in the right be the same as
    /// storage_join's right sample block.
    ActionsDAG right_project = ActionsDAG::makeConvertingActions(
        right.getCurrentHeader()->getColumnsWithTypeAndName(),
        storage_join.getRightSampleBlock().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position,
        getContext());

    QueryPlanStepPtr right_project_step = std::make_unique<ExpressionStep>(right.getCurrentHeader(), std::move(right_project));
    right_project_step->setStepDescription("Rename Broadcast Table Name");
    steps.emplace_back(right_project_step.get());
    right.addStep(std::move(right_project_step));

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
    ActionsDAG left_project = ActionsDAG::makeConvertingActions(
        left.getCurrentHeader()->getColumnsWithTypeAndName(), new_left_cols, ActionsDAG::MatchColumnsMode::Position, getContext());

    QueryPlanStepPtr left_project_step = std::make_unique<ExpressionStep>(left.getCurrentHeader(), std::move(left_project));
    left_project_step->setStepDescription("Rename Left Table Name for broadcast join");
    steps.emplace_back(left_project_step.get());
    left.addStep(std::move(left_project_step));
}

DB::QueryPlanPtr JoinRelParser::parseJoin(const substrait::JoinRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right)
{
    auto join_config = JoinConfig::loadFromContext(getContext());
    google::protobuf::StringValue optimization_info;
    optimization_info.ParseFromString(join.advanced_extension().optimization().value());
    auto join_opt_info = JoinOptimizationInfo::parse(optimization_info.value());
    LOG_DEBUG(getLogger("JoinRelParser"), "optimization info:{}", optimization_info.value());
    auto storage_join = join_opt_info.is_broadcast ? BroadCastJoinBuilder::getJoin(join_opt_info.storage_join_key) : nullptr;
    if (storage_join)
        renamePlanColumns(*left, *right, *storage_join);

    auto table_join = createDefaultTableJoin(join.type(), join_opt_info, context);
    DB::Block right_header_before_convert_step{*right->getCurrentHeader()};
    addConvertStep(*table_join, *left, *right);

    // Add a check to find error easily.
    if (storage_join)
    {
        bool is_col_names_changed = false;
        const auto & current_right_header = *right->getCurrentHeader();
        if (right_header_before_convert_step.columns() != current_right_header.columns())
            is_col_names_changed = true;
        if (!is_col_names_changed)
        {
            for (size_t i = 0; i < right_header_before_convert_step.columns(); i++)
            {
                if (right_header_before_convert_step.getByPosition(i).name != current_right_header.getByPosition(i).name)
                {
                    is_col_names_changed = true;
                    break;
                }
            }
        }
        if (is_col_names_changed)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "For broadcast join, we must not change the columns name in the right table.\nleft header:{},\nright header: {} -> {}",
                left->getCurrentHeader()->dumpStructure(),
                right_header_before_convert_step.dumpStructure(),
                right->getCurrentHeader()->dumpStructure());
        }
    }

    Names after_join_names;
    auto left_names = left->getCurrentHeader()->getNames();
    after_join_names.insert(after_join_names.end(), left_names.begin(), left_names.end());
    auto right_name = table_join->columnsFromJoinedTable().getNames();
    after_join_names.insert(after_join_names.end(), right_name.begin(), right_name.end());

    const auto & left_header = *left->getCurrentHeader();
    const auto & right_header = *right->getCurrentHeader();

    QueryPlanPtr query_plan;

    /// some examples to explain when the post_join_filter is not empty
    /// - on t1.key = t2.key and t1.v1 > 1 and t2.v1 > 1, 't1.v1> 1' is in the  post filter. but 't2.v1 > 1'
    ///   will be pushed down into right table by spark and is not in the post filter. 't1.key = t2.key ' is
    ///   in JoinRel::expression.
    /// - on t1.key = t2. key and t1.v1 > t2.v2, 't1.v1 > t2.v2' is in the post filter.
    collectJoinKeys(*table_join, join, left_header, right_header);

    if (storage_join)
    {
        if (join_opt_info.is_null_aware_anti_join && join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI)
        {
            if (storage_join->has_null_key_value)
            {
                // if there is a null key value on the build side, it will return the empty result
                auto empty_step = std::make_unique<EarlyStopStep>(left->getCurrentHeader());
                left->addStep(std::move(empty_step));
            }
            else if (!storage_join->is_empty_hash_table)
            {
                auto input_header = *left->getCurrentHeader();
                DB::ActionsDAG filter_is_not_null_dag{input_header.getColumnsWithTypeAndName()};
                // when is_null_aware_anti_join is true, there is only one join key
                auto field_index = SubstraitParserUtils::getStructFieldIndex(join.expression().scalar_function().arguments(0).value());
                if (!field_index)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "The join key is not found in the expression.");
                const auto * key_field = filter_is_not_null_dag.getInputs()[*field_index];

                auto result_node = filter_is_not_null_dag.tryFindInOutputs(key_field->result_name);
                // add a function isNotNull to filter the null key on the left side
                const auto * cond_node = buildFunctionNode(filter_is_not_null_dag, "isNotNull", {result_node});
                filter_is_not_null_dag.addOrReplaceInOutputs(*cond_node);
                auto filter_step = std::make_unique<FilterStep>(
                    left->getCurrentHeader(), std::move(filter_is_not_null_dag), cond_node->result_name, true);
                left->addStep(std::move(filter_step));
            }
            // other case: is_empty_hash_table, don't need to handle
        }
        applyJoinFilter(*table_join, join, *left, *right, true);
        auto broadcast_hash_join = storage_join->getJoinLocked(table_join, context);

        QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(left->getCurrentHeader(), broadcast_hash_join, 8192);

        join_step->setStepDescription("STORAGE_JOIN");
        steps.emplace_back(join_step.get());
        left->addStep(std::move(join_step));
        query_plan = std::move(left);
        /// hold right plan for profile
        extra_plan_holder.emplace_back(std::move(right));
    }
    else if (join_opt_info.is_smj)
    {
        bool need_post_filter = !applyJoinFilter(*table_join, join, *left, *right, false);

        /// If applyJoinFilter returns false, it means there are mixed conditions in the post_join_filter.
        /// It should be a inner join.
        /// TODO: make smj support mixed conditions
        if (need_post_filter && table_join->kind() != DB::JoinKind::Inner)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Sort merge join doesn't support mixed join conditions, except inner join.");

        SharedHeader rigth_sample_block = right->getCurrentHeader();
        JoinPtr smj_join = std::make_shared<FullSortingMergeJoin>(table_join, rigth_sample_block, -1);
        MultiEnum<DB::JoinAlgorithm> join_algorithm = context->getSettingsRef()[Setting::join_algorithm];
        QueryPlanStepPtr join_step = std::make_unique<DB::JoinStep>(
            left->getCurrentHeader(),
            right->getCurrentHeader(),
            smj_join,
            context->getSettingsRef()[Setting::max_block_size],
            context->getSettingsRef()[Setting::min_joined_block_size_rows],
            context->getSettingsRef()[Setting::min_joined_block_size_bytes],
            1,
            /* required_output_ = */ NameSet{},
            false,
            /* use_new_analyzer_ = */ false);

        join_step->setStepDescription("SORT_MERGE_JOIN");
        steps.emplace_back(join_step.get());
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
        if (need_post_filter)
            addPostFilter(*query_plan, join);
    }
    else
    {
        std::vector<DB::TableJoin::JoinOnClause> join_on_clauses;
        if (table_join->getClauses().empty())
            table_join->addDisjunct();
        bool is_multi_join_on_clauses
            = couldRewriteToMultiJoinOnClauses(table_join->getOnlyClause(), join_on_clauses, join, left_header, right_header);
        if (is_multi_join_on_clauses && join_config.prefer_multi_join_on_clauses && join_opt_info.right_table_rows > 0
            && join_opt_info.partitions_num > 0
            && join_opt_info.right_table_rows / join_opt_info.partitions_num < join_config.multi_join_on_clauses_build_side_rows_limit)
        {
            query_plan = buildMultiOnClauseHashJoin(table_join, std::move(left), std::move(right), join_on_clauses);
        }
        else
        {
            query_plan = buildSingleOnClauseHashJoin(join, table_join, std::move(left), std::move(right));
        }
    }

    JoinUtil::adjustJoinOutput(*query_plan, after_join_names);
    /// Need to project the right table column into boolean type
    if (join_opt_info.is_existence_join)
        existenceJoinPostProject(*query_plan, left_names);

    return query_plan;
}


/// We use left any join to implement ExistenceJoin.
/// The result columns of ExistenceJoin are left table columns + one flag column.
/// The flag column indicates whether a left row is matched or not. We build the flag column here.
/// The input plan's header is left table columns + right table columns. If one row in the right row is null,
/// we mark the flag 0, otherwise mark it 1.
void JoinRelParser::existenceJoinPostProject(DB::QueryPlan & plan, const DB::Names & left_input_cols)
{
    DB::ActionsDAG actions_dag{plan.getCurrentHeader()->getColumnsWithTypeAndName()};
    const auto * right_col_node = actions_dag.getInputs().back();
    auto function_builder = DB::FunctionFactory::instance().get("isNotNull", getContext());
    const auto * not_null_node = &actions_dag.addFunction(function_builder, {right_col_node}, right_col_node->result_name);
    actions_dag.addOrReplaceInOutputs(*not_null_node);
    DB::Names required_cols = left_input_cols;
    required_cols.emplace_back(not_null_node->result_name);
    actions_dag.removeUnusedActions(required_cols);
    auto project_step = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(actions_dag));
    project_step->setStepDescription("ExistenceJoin Post Project");
    steps.emplace_back(project_step.get());
    plan.addStep(std::move(project_step));
}

void JoinRelParser::addConvertStep(TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right)
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
            right_table_alias.emplace_back(NameWithAlias(origin_name, dedup_name));
    }
    if (!right_table_alias.empty())
    {
        ActionsDAG rename_dag{right.getCurrentHeader()->getNamesAndTypesList()};
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
        table_join.addJoinedColumn(column);
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

/// Join keys are collected from substrait::JoinRel::expression() which only contains the equal join conditions.
void JoinRelParser::collectJoinKeys(
    TableJoin & table_join, const substrait::JoinRel & join_rel, const DB::Block & left_header, const DB::Block & right_header)
{
    if (!join_rel.has_expression())
        return;
    /// Support only one join clause.
    table_join.addDisjunct();
    const auto & expr = join_rel.expression();
    auto & join_clause = table_join.getClauses().back();
    std::list<const substrait::Expression *> expressions_stack;
    expressions_stack.push_back(&expr);
    while (!expressions_stack.empty())
    {
        /// Must handle the expressions in DF order. It matters in sort merge join.
        const auto * current_expr = expressions_stack.back();
        expressions_stack.pop_back();
        if (!current_expr->has_scalar_function())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Function expression is expected");
        auto function_name = parseFunctionName(current_expr->scalar_function());
        if (!function_name)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid function expression");
        if (*function_name == "equals")
        {
            String left_key, right_key;
            size_t left_pos = 0, right_pos = 0;
            for (const auto & arg : current_expr->scalar_function().arguments())
            {
                auto field_index = SubstraitParserUtils::getStructFieldIndex(arg.value());
                if (!field_index)
                {
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "A column reference is expected");
                }
                auto col_pos_ref = *field_index;
                if (col_pos_ref < left_header.columns())
                {
                    left_pos = col_pos_ref;
                    left_key = left_header.getByPosition(col_pos_ref).name;
                }
                else
                {
                    right_pos = col_pos_ref - left_header.columns();
                    right_key = right_header.getByPosition(col_pos_ref - left_header.columns()).name;
                }
            }
            if (left_key.empty() || right_key.empty())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid key equal join condition");
            join_clause.addKey(left_key, right_key, false);
        }
        else if (*function_name == "and")
        {
            expressions_stack.push_back(&current_expr->scalar_function().arguments().at(1).value());
            expressions_stack.push_back(&current_expr->scalar_function().arguments().at(0).value());
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow function: {}", *function_name);
        }
    }
}

bool JoinRelParser::applyJoinFilter(
    DB::TableJoin & table_join,
    const substrait::JoinRel & join_rel,
    DB::QueryPlan & left,
    DB::QueryPlan & right,
    bool allow_mixed_condition)
{
    if (!join_rel.has_post_join_filter())
        return true;
    const auto & expr = join_rel.post_join_filter();

    const auto & left_header = *left.getCurrentHeader();
    const auto & right_header = *right.getCurrentHeader();
    ColumnsWithTypeAndName mixed_columns;
    std::unordered_set<String> added_column_name;
    for (const auto & col : left_header.getColumnsWithTypeAndName())
    {
        mixed_columns.emplace_back(col);
        added_column_name.insert(col.name);
    }
    for (const auto & col : right_header.getColumnsWithTypeAndName())
    {
        const auto & renamed_col_name = table_join.renamedRightColumnNameWithAlias(col.name);
        if (added_column_name.find(col.name) != added_column_name.end())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Right column's name conflict with left column: {}", col.name);
        mixed_columns.emplace_back(col);
        added_column_name.insert(col.name);
    }
    DB::Block mixed_header(mixed_columns);

    auto table_sides = extractTableSidesFromExpression(expr, left_header, right_header);

    auto get_input_expressions = [](const DB::Block & header)
    {
        std::vector<substrait::Expression> exprs;
        for (size_t i = 0; i < header.columns(); ++i)
        {
            substrait::Expression expr = SubstraitParserUtils::buildStructFieldExpression(i);
            exprs.emplace_back(expr);
        }
        return exprs;
    };

    /// If the columns in the expression are all from one table, use analyzer_left_filter_condition_column_name
    /// and analyzer_left_filter_condition_column_name to filt the join result data. It requires to build the filter
    /// column at first.
    /// If the columns in the expression are from both tables, use mixed_join_expression to filt the join result data.
    /// the filter columns will be built inner the join step.
    if (table_sides.size() == 1)
    {
        auto table_side = *table_sides.begin();
        if (table_side == DB::JoinTableSide::Left)
        {
            auto input_exprs = get_input_expressions(left_header);
            input_exprs.push_back(expr);
            auto actions_dag = expressionsToActionsDAG(input_exprs, left_header);
            table_join.getClauses().back().analyzer_left_filter_condition_column_name = actions_dag.getOutputs().back()->result_name;
            QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(left.getCurrentHeader(), std::move(actions_dag));
            before_join_step->setStepDescription("Before JOIN LEFT");
            steps.emplace_back(before_join_step.get());
            left.addStep(std::move(before_join_step));
        }
        else
        {
            /// since the field reference in expr is the index of left_header ++ right_header, so we use
            /// mixed_header to build the actions_dag
            auto input_exprs = get_input_expressions(mixed_header);
            input_exprs.push_back(expr);
            auto actions_dag = expressionsToActionsDAG(input_exprs, mixed_header);

            /// clear unused columns in actions_dag
            for (const auto & col : left_header.getColumnsWithTypeAndName())
                actions_dag.removeUnusedResult(col.name);
            actions_dag.removeUnusedActions();

            table_join.getClauses().back().analyzer_right_filter_condition_column_name = actions_dag.getOutputs().back()->result_name;
            QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(right.getCurrentHeader(), std::move(actions_dag));
            before_join_step->setStepDescription("Before JOIN RIGHT");
            steps.emplace_back(before_join_step.get());
            right.addStep(std::move(before_join_step));
        }
    }
    else if (table_sides.size() == 2)
    {
        if (!allow_mixed_condition)
            return false;
        auto mixed_join_expressions_actions = expressionsToActionsDAG({expr}, mixed_header);
        mixed_join_expressions_actions.removeUnusedActions();
        table_join.getMixedJoinExpression()
            = std::make_shared<DB::ExpressionActions>(std::move(mixed_join_expressions_actions), ExpressionActionsSettings(context));
    }
    else
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Not any table column is used in the join condition.\n{}", join_rel.DebugString());
    }
    return true;
}

void JoinRelParser::addPostFilter(DB::QueryPlan & query_plan, const substrait::JoinRel & join)
{
    std::string filter_name;
    ActionsDAG actions_dag{query_plan.getCurrentHeader()->getColumnsWithTypeAndName()};
    if (!join.post_join_filter().has_scalar_function())
    {
        // It may be singular_or_list
        const auto * in_node = expression_parser->parseExpression(actions_dag, join.post_join_filter());
        filter_name = in_node->result_name;
    }
    else
    {
        const auto * func_node = expression_parser->parseFunction(join.post_join_filter().scalar_function(), actions_dag, true);
        filter_name = func_node->result_name;
    }
    auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(), std::move(actions_dag), filter_name, true);
    filter_step->setStepDescription("Post Join Filter");
    steps.emplace_back(filter_step.get());
    query_plan.addStep(std::move(filter_step));
}

/// Only support following pattern: a1 = b1 or a2 = b2 or (a3 = b3 and a4 = b4)
bool JoinRelParser::couldRewriteToMultiJoinOnClauses(
    const DB::TableJoin::JoinOnClause & prefix_clause,
    std::vector<DB::TableJoin::JoinOnClause> & clauses,
    const substrait::JoinRel & join_rel,
    const DB::Block & left_header,
    const DB::Block & right_header)
{
    if (!join_rel.has_post_join_filter())
        return false;
    const auto & filter_expr = join_rel.post_join_filter();

    auto check_function = [&](const String function_name_, const substrait::Expression & e)
    {
        if (!e.has_scalar_function())
            return false;
        auto function_name = parseFunctionName(e.scalar_function());
        return function_name.has_value() && *function_name == function_name_;
    };

    std::function<void(std::vector<const substrait::Expression *> &, const substrait::Expression &)> dfs_visit_or_expr
        = [&](std::vector<const substrait::Expression *> & or_exprs, const substrait::Expression & e) -> void
    {
        if (!check_function("or", e))
        {
            or_exprs.push_back(&e);
            return;
        }
        const auto & args = e.scalar_function().arguments();
        dfs_visit_or_expr(or_exprs, args[0].value());
        dfs_visit_or_expr(or_exprs, args[1].value());
    };

    std::function<void(std::vector<const substrait::Expression *> &, const substrait::Expression &)> dfs_visit_and_expr
        = [&](std::vector<const substrait::Expression *> & and_exprs, const substrait::Expression & e) -> void
    {
        if (!check_function("and", e))
        {
            and_exprs.push_back(&e);
            return;
        }
        const auto & args = e.scalar_function().arguments();
        dfs_visit_and_expr(and_exprs, args[0].value());
        dfs_visit_and_expr(and_exprs, args[1].value());
    };

    auto visit_equal_expr = [&](const substrait::Expression & e) -> std::optional<std::pair<String, String>>
    {
        if (!check_function("equals", e))
            return {};
        const auto & args = e.scalar_function().arguments();
        auto l_field_ref = SubstraitParserUtils::getStructFieldIndex(args[0].value());
        auto r_field_ref = SubstraitParserUtils::getStructFieldIndex(args[1].value());
        if (!l_field_ref.has_value() || !r_field_ref.has_value())
            return {};
        size_t l_pos = *l_field_ref;
        size_t r_pos = *r_field_ref;
        size_t l_cols = left_header.columns();
        size_t total_cols = l_cols + right_header.columns();

        if (l_pos < l_cols && r_pos >= l_cols && r_pos < total_cols)
            return std::make_pair(left_header.getByPosition(l_pos).name, right_header.getByPosition(r_pos - l_cols).name);
        else if (r_pos < l_cols && l_pos >= l_cols && l_pos < total_cols)
            return std::make_pair(left_header.getByPosition(r_pos).name, right_header.getByPosition(l_pos - l_cols).name);
        return {};
    };


    std::vector<const substrait::Expression *> or_exprs;
    dfs_visit_or_expr(or_exprs, filter_expr);
    if (or_exprs.empty())
        return false;
    for (const auto * or_expr : or_exprs)
    {
        DB::TableJoin::JoinOnClause new_clause = prefix_clause;
        clauses.push_back(new_clause);
        auto & current_clause = clauses.back();
        std::vector<const substrait::Expression *> and_exprs;
        dfs_visit_and_expr(and_exprs, *or_expr);
        for (const auto * and_expr : and_exprs)
        {
            auto join_keys = visit_equal_expr(*and_expr);
            if (!join_keys)
                return false;
            current_clause.addKey(join_keys->first, join_keys->second, false);
        }
    }
    return true;
}

DB::QueryPlanPtr JoinRelParser::buildMultiOnClauseHashJoin(
    std::shared_ptr<DB::TableJoin> table_join,
    DB::QueryPlanPtr left_plan,
    DB::QueryPlanPtr right_plan,
    const std::vector<DB::TableJoin::JoinOnClause> & join_on_clauses)
{
    DB::TableJoin::JoinOnClause & base_join_on_clause = table_join->getOnlyClause();
    base_join_on_clause = join_on_clauses[0];
    for (size_t i = 1; i < join_on_clauses.size(); ++i)
    {
        table_join->addDisjunct();
        auto & join_on_clause = table_join->getClauses().back();
        join_on_clause = join_on_clauses[i];
    }

    LOG_INFO(getLogger("JoinRelParser"), "multi join on clauses:\n{}", DB::TableJoin::formatClauses(table_join->getClauses()));

    JoinPtr hash_join = std::make_shared<HashJoin>(table_join, right_plan->getCurrentHeader());
    QueryPlanStepPtr join_step = std::make_unique<DB::JoinStep>(
        left_plan->getCurrentHeader(),
        right_plan->getCurrentHeader(),
        hash_join,
        context->getSettingsRef()[Setting::max_block_size],
        context->getSettingsRef()[Setting::min_joined_block_size_rows],
        context->getSettingsRef()[Setting::min_joined_block_size_bytes],
        1,
        /* required_output_ = */ NameSet{},
        false,
        /* use_new_analyzer_ = */ false);
    join_step->setStepDescription("Multi join on clause hash join");
    steps.emplace_back(join_step.get());
    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::move(left_plan));
    plans.emplace_back(std::move(right_plan));
    auto query_plan = std::make_unique<QueryPlan>();
    query_plan->unitePlans(std::move(join_step), {std::move(plans)});
    return query_plan;
}

DB::QueryPlanPtr JoinRelParser::buildSingleOnClauseHashJoin(
    const substrait::JoinRel & join_rel, std::shared_ptr<DB::TableJoin> table_join, DB::QueryPlanPtr left_plan, DB::QueryPlanPtr right_plan)
{
    applyJoinFilter(*table_join, join_rel, *left_plan, *right_plan, true);
    /// Following is some configurations for grace hash join.
    /// - spark.gluten.sql.columnar.backend.ch.runtime_settings.join_algorithm=grace_hash. This will
    ///   enable grace hash join.
    /// - spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_in_join=3145728. This setup
    ///   the memory limitation fro grace hash join. If the memory consumption exceeds the limitation,
    ///   data will be spilled to disk. Don't set the limitation too small, otherwise the buckets number
    ///   will be too large and the performance will be bad.
    JoinPtr hash_join = nullptr;
    MultiEnum<DB::JoinAlgorithm> join_algorithm = context->getSettingsRef()[Setting::join_algorithm];
    if (join_algorithm.isSet(DB::JoinAlgorithm::GRACE_HASH))
    {
        hash_join = std::make_shared<GraceHashJoin>(
            context->getSettingsRef()[Setting::grace_hash_join_initial_buckets],
            context->getSettingsRef()[Setting::grace_hash_join_max_buckets],
            table_join, left_plan->getCurrentHeader(), right_plan->getCurrentHeader(), context->getTempDataOnDisk());
    }
    else
    {
        hash_join = std::make_shared<HashJoin>(table_join, right_plan->getCurrentHeader());
    }
    QueryPlanStepPtr join_step = std::make_unique<DB::JoinStep>(
        left_plan->getCurrentHeader(),
        right_plan->getCurrentHeader(),
        hash_join,
        context->getSettingsRef()[Setting::max_block_size],
        context->getSettingsRef()[Setting::min_joined_block_size_rows],
        context->getSettingsRef()[Setting::min_joined_block_size_bytes],
        1,
        /* required_output_ = */ NameSet{},
        false,
        /* use_new_analyzer_ = */ false);

    join_step->setStepDescription("HASH_JOIN");
    steps.emplace_back(join_step.get());
    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::move(left_plan));
    plans.emplace_back(std::move(right_plan));

    auto query_plan = std::make_unique<QueryPlan>();
    query_plan->unitePlans(std::move(join_step), {std::move(plans)});
    return query_plan;
}

void registerJoinRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<JoinRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kJoin, builder);
}

}
