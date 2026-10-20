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
#include <unordered_map>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/WindowDescription.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/RelParsers/RelParser.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class WindowRelParser : public RelParser
{
public:
    explicit WindowRelParser(ParserContextPtr parser_context_);
    ~WindowRelParser() override = default;
    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;
    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override { return &rel.window().input(); }

private:
    struct WindowInfo
    {
        const substrait::ConsistentPartitionWindowRel::WindowRelFunction * window_function = nullptr;
        String result_column_name;
        DB::Strings arg_column_names;
        DB::DataTypes arg_column_types;
        DB::Array params;
        String signature_function_name;
        // function name in CH
        String function_name;
        // For avoiding repeated builds.
        AggregateFunctionParser::CommonFunctionInfo parser_func_info;
        // For avoiding repeated builds.
        AggregateFunctionParserPtr function_parser;

        google::protobuf::RepeatedPtrField<substrait::Expression> partition_exprs;
        google::protobuf::RepeatedPtrField<substrait::SortField> sort_fields;
    };
    DB::QueryPlanPtr current_plan;
    // The final output schema.
    DB::Block output_header;
    Poco::Logger * logger = &Poco::Logger::get("WindowRelParser");
    std::vector<WindowInfo> win_infos;

    /// There will be window descrptions generated for different window frame type;
    std::unordered_map<String, DB::WindowDescription> parseWindowDescriptions();

    // Build a window description in CH with respect to a window function, since the same
    // function may have different window frame in CH and spark.
    DB::WindowDescription parseWindowDescription(const WindowInfo & win_info);
    DB::WindowFrame parseWindowFrame(const WindowInfo & win_info);
    DB::WindowFrame::FrameType parseWindowFrameType(
        const std::string & function_name, const substrait::ConsistentPartitionWindowRel::WindowRelFunction & window_function);
    static void parseBoundType(
        const substrait::Expression::WindowFunction::Bound & bound,
        bool is_begin_or_end,
        DB::WindowFrame::BoundaryType & bound_type,
        DB::Field & offset,
        bool & preceding);
    DB::WindowFunctionDescription parseWindowFunctionDescription(
        const String & ch_function_name,
        const substrait::ConsistentPartitionWindowRel::WindowRelFunction & window_function,
        const DB::Names & arg_names,
        const DB::DataTypes & arg_types,
        const DB::Array & params);

    void initWindowsInfos(const substrait::ConsistentPartitionWindowRel & win_rel);
    void tryAddProjectionBeforeWindow();
    void tryAddProjectionAfterWindow();
};


}
