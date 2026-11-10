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
#include <Common/SparkRegexpToRe2.h>
#include <DataTypes/DataTypeString.h>
#include <Parser/FunctionParser.h>
#include <Common/re2.h>

namespace DB::ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace local_engine
{
class FunctionSplitParser : public FunctionParser
{
public:
    FunctionSplitParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionSplitParser() override = default;
    static constexpr auto name = "split";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "splitByRegexpSpark"; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        const auto & args = substrait_func.arguments();
        for (const auto & arg : args)
            parsed_args.emplace_back(parseExpression(actions_dag, arg.value()));
        /// In Spark: split(str, regex [, limit] )
        /// In CH: splitByRegexpSpark(regexp, str [, limit])
        if (parsed_args.size() >= 2)
        {
            const auto & pattern = args[1].value();
            if (!pattern.has_literal() || !pattern.literal().has_string())
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires a constant string pattern", getName());
            const auto translated = spark_regexp::re2::translatePattern(pattern.literal().string());
            const ::re2::RE2 regex(translated);
            if (!regex.ok())
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} cannot compile translated regexp pattern: {}", getName(), regex.error());
            parsed_args[1] = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), translated);
            std::swap(parsed_args[0], parsed_args[1]);
        }
        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<FunctionSplitParser> register_split;
}

