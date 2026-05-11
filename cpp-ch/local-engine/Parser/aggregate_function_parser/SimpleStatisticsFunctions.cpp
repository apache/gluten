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
#include <Parser/AggregateFunctionParser.h>
#include <DataTypes/DataTypeNullable.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
/// For stddev
struct StddevNameStruct
{
    static constexpr auto spark_name = "stddev";
    static constexpr auto ch_name = "stddev";
};

struct StddevSampNameStruct
{
    static constexpr auto spark_name = "stddev_samp";
    static constexpr auto ch_name = "stddev_samp";
};
template <typename NameStruct>
class AggregateFunctionParserStddev final : public AggregateFunctionParser
{
public:
    AggregateFunctionParserStddev(ParserContextPtr parser_context_) : AggregateFunctionParser(parser_context_) { }
    ~AggregateFunctionParserStddev() override = default;
    String getName() const override { return NameStruct::spark_name; }
    static constexpr auto name = NameStruct::spark_name;
    String getCHFunctionName(const CommonFunctionInfo &) const override { return NameStruct::ch_name; }
    String getCHFunctionName(DB::DataTypes &) const override { return NameStruct::ch_name; }
    const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const CommonFunctionInfo & func_info,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAG & actions_dag,
        bool with_nullability) const override
    {
        /// result is nullable.
        /// if result is NaN, convert it to NULL.
        auto is_nan_func_node = toFunctionNode(actions_dag, "isNaN", getUniqueName("isNaN"), {func_node});
        auto null_type = DB::makeNullable(func_node->result_type);
        auto nullable_col = null_type->createColumnConst(1, null_type->getDefault());
        const auto * null_node
            = &actions_dag.addColumn(DB::ColumnWithTypeAndName(std::move(nullable_col), null_type, getUniqueName("null")));
        DB::ActionsDAG::NodeRawConstPtrs convert_nan_func_args = {is_nan_func_node, null_node, func_node};

        func_node = toFunctionNode(actions_dag, "if", func_node->result_name, convert_nan_func_args);
        actions_dag.addOrReplaceInOutputs(*func_node);
        return func_node;
    }
};

static const AggregateFunctionParserRegister<AggregateFunctionParserStddev<StddevNameStruct>> registerer_stddev;
static const AggregateFunctionParserRegister<AggregateFunctionParserStddev<StddevSampNameStruct>> registerer_stddev_samp;
}
