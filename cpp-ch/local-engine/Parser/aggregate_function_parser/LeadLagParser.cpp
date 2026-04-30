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
#include "LeadLagParser.h"

#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace local_engine
{
using namespace DB;
DB::ActionsDAG::NodeRawConstPtrs
LeadParser::parseFunctionArguments(const CommonFunctionInfo & func_info, DB::ActionsDAG & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs args;
    const auto & arg0 = func_info.arguments[0].value();
    const auto & arg1 = func_info.arguments[1].value();
    /// The 3rd arg is default value
    /// when it is set to null, the 1st arg must be nullable
    const auto & arg2 = func_info.arguments[2].value();
    const auto * arg0_col = parseExpression(actions_dag, arg0);
    auto arg0_col_name = arg0_col->result_name;
    auto arg0_col_type= arg0_col->result_type;
    const DB::ActionsDAG::Node * node = nullptr;
    if (arg2.has_literal() && arg2.literal().has_null() && !arg0_col_type->isNullable())
    {
        node = ActionsDAGUtil::convertNodeType(
            actions_dag,
            arg0_col,
            DB::makeNullable(arg0_col_type),
            getContext(),
            arg0_col_name);
        actions_dag.addOrReplaceInOutputs(*node);
        args.push_back(node);
    }
    else
    {
        args.push_back(arg0_col);
    }

    node = parseExpression(actions_dag, arg1);
    node = ActionsDAGUtil::convertNodeType(actions_dag, node, BIGINT(), getContext());
    actions_dag.addOrReplaceInOutputs(*node);
    args.push_back(node);

    if (arg2.has_literal() && !arg2.literal().has_null())
    {
        node = parseExpression(actions_dag, arg2);
        actions_dag.addOrReplaceInOutputs(*node);
        args.push_back(node);
    }    
    return args;
}
AggregateFunctionParserRegister<LeadParser> lead_register;

DB::ActionsDAG::NodeRawConstPtrs
LagParser::parseFunctionArguments(const CommonFunctionInfo & func_info, DB::ActionsDAG & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs args;
    const auto & arg0 = func_info.arguments[0].value();
    const auto & arg1 = func_info.arguments[1].value();
    /// The 3rd arg is default value
    /// when it is set to null, the 1st arg must be nullable
    const auto & arg2 = func_info.arguments[2].value();
    const auto * arg0_col = parseExpression(actions_dag, arg0);
    auto arg0_col_name = arg0_col->result_name;
    auto arg0_col_type = arg0_col->result_type;
    const DB::ActionsDAG::Node * node = nullptr;
    if (arg2.has_literal() && arg2.literal().has_null() && !arg0_col->result_type->isNullable())
    {
        node = ActionsDAGUtil::convertNodeType(
            actions_dag,
            arg0_col,
            makeNullable(arg0_col_type),
            getContext(),
            arg0_col_name);
        actions_dag.addOrReplaceInOutputs(*node);
        args.push_back(node);
    }
    else
    {
        args.push_back(arg0_col);
    }

    // lag's offset is negative
    auto literal_result = parseLiteral(arg1.literal());
    assert(literal_result.second.safeGet<Int32>() < 0);
    auto real_field = 0 - literal_result.second.safeGet<Int32>();
    node = &actions_dag.addColumn(ColumnWithTypeAndName(
        literal_result.first->createColumnConst(1, real_field), literal_result.first, getUniqueName(toString(real_field))));
    node = ActionsDAGUtil::convertNodeType(actions_dag, node, BIGINT(), getContext());
    actions_dag.addOrReplaceInOutputs(*node);
    args.push_back(node);

    if (arg2.has_literal() && !arg2.literal().has_null())
    {
        node = parseExpression(actions_dag, arg2);
        actions_dag.addOrReplaceInOutputs(*node);
        args.push_back(node);
    }
    return args;
}
AggregateFunctionParserRegister<LagParser> lag_register;
}
