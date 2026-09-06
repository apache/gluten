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
#include <DataTypes/DataTypesNumber.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
namespace local_engine
{
// tuple indecies start from 1, in spark, start from 0
#define REGISTER_TUPLE_ELEMENT_PARSER(class_name, substrait_name, ch_name) \
    class SparkFunctionParser##class_name : public FunctionParser \
    { \
    public: \
        SparkFunctionParser##class_name(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}\
        ~SparkFunctionParser##class_name() override = default; \
        static constexpr auto name = #substrait_name; \
        String getName () const override { return name; } \
        String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return #ch_name; } \
        const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override \
        { \
            DB::ActionsDAG::NodeRawConstPtrs parsed_args; \
            auto ch_function_name = getCHFunctionName(substrait_func); \
            const auto & args = substrait_func.arguments(); \
            parsed_args.emplace_back(parseExpression(actions_dag, args[0].value())); \
            if (!args[1].value().has_literal()) \
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{}'s sceond argument must be a literal", #substrait_name); \
            auto [data_type, field] = parseLiteral(args[1].value().literal()); \
            if (!DB::WhichDataType(data_type).isInt32()) \
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{}'s second argument must be i32", #substrait_name); \
            Int32 field_index = static_cast<Int32>(field.safeGet<Int32>() + 1); \
            const auto * index_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt32>(), field_index); \
            parsed_args.emplace_back(index_node); \
            if (substrait_func.output_type().has_list() \
                && substrait_func.output_type().list().nullability() == substrait::Type_Nullability_NULLABILITY_NULLABLE \
                && !parsed_args[0]->result_type->isNullable()) \
                parsed_args[0] = toFunctionNode(actions_dag, "toNullable", {parsed_args[0]}); \
            const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args); \
            return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag); \
        } \
    }; \
    static FunctionParserRegister<SparkFunctionParser##class_name> register_##substrait_name;

REGISTER_TUPLE_ELEMENT_PARSER(GetStructField, get_struct_field, sparkTupleElement);
REGISTER_TUPLE_ELEMENT_PARSER(GetArrayStructFields, get_array_struct_fields, sparkTupleElement);
}
