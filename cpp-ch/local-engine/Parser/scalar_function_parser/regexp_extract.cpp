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
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace local_engine
{
/// Translate literal patterns once before constructing the ClickHouse function.
/// Unsupported Java constructs must not silently acquire RE2 semantics.
class SparkRegexpFunctionParser : public FunctionParser
{
public:
    using FunctionParser::FunctionParser;

protected:
    const DB::ActionsDAG::Node * parseRegexpFunction(
        const substrait::Expression_ScalarFunction & function,
        DB::ActionsDAG & dag,
        size_t min_args,
        size_t max_args) const
    {
        const auto & args = function.arguments();
        if (args.size() < min_args || args.size() > max_args)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires {} to {} arguments", getName(), min_args, max_args);
        const auto & pattern = args[1].value();
        if (!pattern.has_literal() || !pattern.literal().has_string())
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires a constant string pattern", getName());

        const String translated = spark_regexp::re2::translatePattern(pattern.literal().string());
        const ::re2::RE2 regex(translated);
        if (!regex.ok())
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} cannot compile translated regexp pattern: {}", getName(), regex.error());
        auto parsed_args = parseFunctionArguments(function, dag);
        parsed_args[1] = addColumnToActionsDAG(dag, std::make_shared<DB::DataTypeString>(), translated);
        const auto * node = toFunctionNode(dag, getCHFunctionName(function), parsed_args);
        return convertNodeTypeIfNeeded(function, node, dag);
    }
};

class FunctionParserRegexpExtract : public SparkRegexpFunctionParser
{
public:
    using SparkRegexpFunctionParser::SparkRegexpFunctionParser;
    static constexpr auto name = "regexp_extract";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "regexpExtract"; }
    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & dag) const override
    {
        return parseRegexpFunction(func, dag, 3, 3);
    }
};

class FunctionParserRegexpExtractAll : public SparkRegexpFunctionParser
{
public:
    using SparkRegexpFunctionParser::SparkRegexpFunctionParser;
    static constexpr auto name = "regexp_extract_all";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "regexpExtractAllSpark"; }
    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & dag) const override
    {
        return parseRegexpFunction(func, dag, 2, 3);
    }
};

class FunctionParserRegexpReplace : public SparkRegexpFunctionParser
{
public:
    using SparkRegexpFunctionParser::SparkRegexpFunctionParser;
    static constexpr auto name = "regexp_replace";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "replaceRegexpAll"; }
    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & dag) const override
    {
        // The Scala transformer handles replacement backreferences and requires position = 1.
        return parseRegexpFunction(func, dag, 3, 3);
    }
};

class FunctionParserRlike : public SparkRegexpFunctionParser
{
public:
    using SparkRegexpFunctionParser::SparkRegexpFunctionParser;
    static constexpr auto name = "rlike";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "match"; }
    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & dag) const override
    {
        return parseRegexpFunction(func, dag, 2, 2);
    }
};

static FunctionParserRegister<FunctionParserRegexpExtract> register_regexp_extract;
static FunctionParserRegister<FunctionParserRegexpExtractAll> register_regexp_extract_all;
static FunctionParserRegister<FunctionParserRegexpReplace> register_regexp_replace;
static FunctionParserRegister<FunctionParserRlike> register_rlike;
}
