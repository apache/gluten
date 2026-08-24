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
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <base/types.h>
#include <substrait/algebra.pb.h>
#include <Poco/Logger.h>
#include <Common/IFactoryWithAliases.h>

namespace local_engine
{
class ExpressionParser;
class AggregateFunctionParser
{
public:
    /// CommonFunctionInfo is commmon representation for different function types,
    struct CommonFunctionInfo
    {
        /// basic common function informations
        using Arguments = google::protobuf::RepeatedPtrField<substrait::FunctionArgument>;
        using SortFields = google::protobuf::RepeatedPtrField<substrait::SortField>;
        Int32 function_ref;
        Arguments arguments;
        substrait::Type output_type;

        /// Following is for aggregate and window functions.
        substrait::AggregationPhase phase;
        SortFields sort_fields;
        // only be used in aggregate functions at present.
        substrait::Expression filter;
        bool is_in_window = false;
        bool is_aggregate_function = false;
        bool has_filter = false;

        CommonFunctionInfo() { function_ref = -1; }

        CommonFunctionInfo(const substrait::ConsistentPartitionWindowRel::WindowRelFunction & win_function)
            : function_ref(win_function.function_reference())
            , arguments(win_function.arguments())
            , output_type(win_function.output_type())
            , phase(win_function.phase())
        {
            // Substrait 0.98's WindowRelFunction has no per-function sorts (they are hoisted to the rel);
            // Gluten always left them empty, so sort_fields stays default-constructed.
            is_in_window = true;
            is_aggregate_function = true;
        }

        CommonFunctionInfo(const substrait::AggregateRel::Measure & agg_measure)
            : function_ref(agg_measure.measure().function_reference())
            , arguments(agg_measure.measure().arguments())
            , output_type(agg_measure.measure().output_type())
            , phase(agg_measure.measure().phase())
            , sort_fields(agg_measure.measure().sorts())
            , filter(agg_measure.filter())
        {
            has_filter = agg_measure.has_filter();
            is_aggregate_function = true;
        }
    };

    AggregateFunctionParser(ParserContextPtr parser_context_);

    virtual ~AggregateFunctionParser();

    virtual String getName() const = 0;

    /// In some special cases, different arguments size or different arguments types may refer to different
    /// CH function implementation.
    virtual String getCHFunctionName(const CommonFunctionInfo & func_info) const = 0;

    /// In most cases, arguments size and types are enough to determine the CH function implementation.
    /// It is only be used in TypeParser::buildBlockFromNamedStruct
    /// Users are allowed to modify arg types to make it fit for AggregateFunctionFactory::instance().get(...) in TypeParser::buildBlockFromNamedStruct
    virtual String getCHFunctionName(DB::DataTypes & args) const = 0;

    /// Do some preprojections for the function arguments, and return the necessary arguments for the CH function.
    virtual DB::ActionsDAG::NodeRawConstPtrs
    parseFunctionArguments(const CommonFunctionInfo & func_info, DB::ActionsDAG & actions_dag) const;

    // `PartialMerge` is applied on the merging stages.
    // `If` is applied when the aggreate function has a filter. This should only happen on the 1st stage.
    // If no combinator is applied, return (ch_func_name,arg_column_types)
    virtual std::pair<String, DB::DataTypes>
    tryApplyCHCombinator(const CommonFunctionInfo & func_info, const String & ch_func_name, const DB::DataTypes & arg_column_types) const;

    /// Make a postprojection for the function result.
    virtual const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const CommonFunctionInfo & func_info,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAG & actions_dag,
        bool with_nullability) const;

    /// Parameters are only used in aggregate functions at present. e.g. percentiles(0.5)(x).
    /// 0.5 is the parameter of percentiles function.
    virtual DB::Array parseFunctionParameters(
        const CommonFunctionInfo & /*func_info*/, DB::ActionsDAG::NodeRawConstPtrs & /*arg_nodes*/, DB::ActionsDAG & /*actions_dag*/) const
    {
        return DB::Array();
    }

    /// Return the default parameters of the function. It's useful for creating a default function instance.
    virtual DB::Array getDefaultFunctionParameters() const { return DB::Array(); }

protected:
    DB::ContextPtr getContext() const { return parser_context->queryContext(); }

    String getUniqueName(const String & name) const;

    const DB::ActionsDAG::Node *
    addColumnToActionsDAG(DB::ActionsDAG & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const;

    const DB::ActionsDAG::Node *
    toFunctionNode(DB::ActionsDAG & action_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const;

    const DB::ActionsDAG::Node * toFunctionNode(
        DB::ActionsDAG & action_dag,
        const String & func_name,
        const String & result_name,
        const DB::ActionsDAG::NodeRawConstPtrs & args) const;

    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAG & actions_dag, const substrait::Expression & rel) const;

    std::pair<DB::DataTypePtr, DB::Field> parseLiteral(const substrait::Expression_Literal & literal) const;

    const DB::ActionsDAG::Node * convertNanToNullIfNeed(
        const CommonFunctionInfo & func_info, const DB::ActionsDAG::Node * func_node, DB::ActionsDAG & actions_dag) const;

    ParserContextPtr parser_context;
    std::unique_ptr<ExpressionParser> expression_parser;
    Poco::Logger * logger = &Poco::Logger::get("AggregateFunctionParserFactory");
};

using AggregateFunctionParserPtr = std::shared_ptr<AggregateFunctionParser>;
using AggregateFunctionParserCreator = std::function<AggregateFunctionParserPtr(ParserContextPtr)>;

class AggregateFunctionParserFactory : public DB::IFactoryWithAliases<AggregateFunctionParserCreator>
{
public:
    using Parsers = std::unordered_map<String, Value>;

    static AggregateFunctionParserFactory & instance();

    void registerAggregateFunctionParser(const String & name, Value value);

    template <typename Parser>
    void registerAggregateFunctionParser(const String & name)
    {
        auto creator
            = [](ParserContextPtr parser_context) -> AggregateFunctionParserPtr { return std::make_shared<Parser>(parser_context); };
        registerAggregateFunctionParser(name, creator);
    }

    AggregateFunctionParserPtr get(const String & name, ParserContextPtr parser_context) const;
    AggregateFunctionParserPtr tryGet(const String & name, ParserContextPtr parser_context) const;

    const Parsers & getMap() const override { return parsers; }

private:
    Parsers parsers;

    /// Always empty
    Parsers case_insensitive_parsers;

    const Parsers & getCaseInsensitiveMap() const override { return case_insensitive_parsers; }

    String getFactoryName() const override { return "AggregateFunctionParserFactory"; }
};

template <typename Parser>
struct AggregateFunctionParserRegister
{
    AggregateFunctionParserRegister() { AggregateFunctionParserFactory::instance().registerAggregateFunctionParser<Parser>(Parser::name); }
};
}
