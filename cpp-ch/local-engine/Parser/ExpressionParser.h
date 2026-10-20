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
#include <atomic>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <substrait/plan.pb.h>


namespace local_engine
{
class ParserContext;
class SerializedPlanParser;

class LiteralParser
{
public:
    /// Parse a substrait literal into a CH field
    /// returns are the type and field value.
    static std::pair<DB::DataTypePtr, DB::Field> parse(const substrait::Expression_Literal & literal);
};


class ExpressionParser
{
public:
    using NodeRawConstPtr = const DB::ActionsDAG::Node *;
    explicit ExpressionParser(const std::shared_ptr<const ParserContext> & context_) : context(context_) { }
    ~ExpressionParser() = default;

    /// Append a counter-suffix to name
    String getUniqueName(const String & name) const;

    NodeRawConstPtr addConstColumn(DB::ActionsDAG & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const;

    /// Parse expr and add an expression node in actions_dag
    NodeRawConstPtr parseExpression(DB::ActionsDAG & actions_dag, const substrait::Expression & rel) const;
    /// Build an actions dag that contains expressions. header is used as input columns for the actions dag.
    DB::ActionsDAG expressionsToActionsDAG(const std::vector<substrait::Expression> & expressions, const DB::Block & header) const;

    // Parse func's arguments into actions dag, and return the node ptrs.
    DB::ActionsDAG::NodeRawConstPtrs
    parseFunctionArguments(DB::ActionsDAG & actions_dag, const substrait::Expression_ScalarFunction & func) const;
    NodeRawConstPtr
    parseFunction(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag, bool add_to_output = false) const;
    // Add a new function node into the actions dag
    NodeRawConstPtr toFunctionNode(
        DB::ActionsDAG & actions_dag,
        const String & ch_function_name,
        const DB::ActionsDAG::NodeRawConstPtrs & args,
        const String & result_name_ = "") const;

    /// Return the function name in signature.
    String getFunctionNameInSignature(const substrait::Expression_ScalarFunction & func_) const;
    String getFunctionNameInSignature(UInt32 func_ref_) const;

    /// Return the CH function name corresponding to func_
    String getFunctionName(const substrait::Expression_ScalarFunction & func_) const;
    String safeGetFunctionName(const substrait::Expression_ScalarFunction & func_) const;

private:
    static std::atomic<UInt64> unique_name_counter;
    std::shared_ptr<const ParserContext> context;

    bool reuseCSE() const;

    DB::ActionsDAG::NodeRawConstPtrs
    parseArrayJoin(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag, bool position) const;
    DB::ActionsDAG::NodeRawConstPtrs parseArrayJoinArguments(
        const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag, bool position, bool & is_map) const;

    DB::ActionsDAG::NodeRawConstPtrs parseStack(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag) const;

    DB::ActionsDAG::NodeRawConstPtrs parseJsonTuple(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag) const;

    static bool areEqualNodes(NodeRawConstPtr a, NodeRawConstPtr b);
    NodeRawConstPtr findFirstStructureEqualNode(NodeRawConstPtr target, const DB::ActionsDAG & actions_dag) const;
};
}
