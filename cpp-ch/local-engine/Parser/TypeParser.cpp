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
#include <set>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/FunctionParser.h>
#include <Parser/ParserContext.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/TypeParser.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>
#include <Common/QueryContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
using namespace DB;
std::unordered_map<String, String> TypeParser::type_names_mapping
    = {{"BooleanType", "UInt8"},
       {"ByteType", "Int8"},
       {"ShortType", "Int16"},
       {"IntegerType", "Int32"},
       {"LongType", "Int64"},
       {"FloatType", "Float32"},
       {"DoubleType", "Float64"},
       {"StringType", "String"},
       {"DateType", "Date32"},
       {"TimestampType", "DateTime64"}};

String TypeParser::getCHTypeName(const String & spark_type_name)
{
    if (startsWith(spark_type_name, "DecimalType"))
        return "Decimal" + spark_type_name.substr(strlen("DecimalType"));
    else
    {
        auto it = type_names_mapping.find(spark_type_name);
        if (it == type_names_mapping.end())
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported substrait type: {}", spark_type_name);
        return it->second;
    }
}

DB::DataTypePtr TypeParser::getCHTypeByName(const String & spark_type_name)
{
    auto ch_type_name = getCHTypeName(spark_type_name);
    return DB::DataTypeFactory::instance().get(ch_type_name);
}

DB::DataTypePtr TypeParser::parseType(const substrait::Type & substrait_type, std::list<String> * field_names)
{
    DB::DataTypePtr ch_type = nullptr;

    std::string_view field_name;
    if (field_names)
    {
        assert(!field_names->empty());
        field_name = field_names->front();
        field_names->pop_front();
    }

    if (substrait_type.has_bool_())
    {
        ch_type = DB::DataTypeFactory::instance().get("Bool");
        ch_type = tryWrapNullable(substrait_type.bool_().nullability(), ch_type);
    }
    else if (substrait_type.has_i8())
    {
        ch_type = std::make_shared<DB::DataTypeInt8>();
        ch_type = tryWrapNullable(substrait_type.i8().nullability(), ch_type);
    }
    else if (substrait_type.has_i16())
    {
        ch_type = std::make_shared<DB::DataTypeInt16>();
        ch_type = tryWrapNullable(substrait_type.i16().nullability(), ch_type);
    }
    else if (substrait_type.has_i32())
    {
        ch_type = std::make_shared<DB::DataTypeInt32>();
        ch_type = tryWrapNullable(substrait_type.i32().nullability(), ch_type);
    }
    else if (substrait_type.has_i64())
    {
        ch_type = std::make_shared<DB::DataTypeInt64>();
        ch_type = tryWrapNullable(substrait_type.i64().nullability(), ch_type);
    }
    else if (substrait_type.has_string())
    {
        ch_type = std::make_shared<DB::DataTypeString>();
        ch_type = tryWrapNullable(substrait_type.string().nullability(), ch_type);
    }
    else if (substrait_type.has_binary())
    {
        ch_type = std::make_shared<DB::DataTypeString>();
        ch_type = tryWrapNullable(substrait_type.binary().nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_char())
    {
        const auto & fixed_char = substrait_type.fixed_char();
        ch_type = std::make_shared<DB::DataTypeFixedString>(fixed_char.length());
        ch_type = tryWrapNullable(fixed_char.nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_binary())
    {
        const auto & fixed_binary = substrait_type.fixed_binary();
        ch_type = std::make_shared<DB::DataTypeFixedString>(fixed_binary.length());
        ch_type = tryWrapNullable(fixed_binary.nullability(), ch_type);
    }
    else if (substrait_type.has_fp32())
    {
        ch_type = std::make_shared<DB::DataTypeFloat32>();
        ch_type = tryWrapNullable(substrait_type.fp32().nullability(), ch_type);
    }
    else if (substrait_type.has_fp64())
    {
        ch_type = std::make_shared<DB::DataTypeFloat64>();
        ch_type = tryWrapNullable(substrait_type.fp64().nullability(), ch_type);
    }
    else if (substrait_type.has_precision_timestamp_tz())
    {
        ch_type = std::make_shared<DB::DataTypeDateTime64>(6);
        ch_type = tryWrapNullable(substrait_type.precision_timestamp_tz().nullability(), ch_type);
    }
    else if (substrait_type.has_date())
    {
        ch_type = std::make_shared<DB::DataTypeDate32>();
        ch_type = tryWrapNullable(substrait_type.date().nullability(), ch_type);
    }
    else if (substrait_type.has_decimal())
    {
        UInt32 precision = substrait_type.decimal().precision();
        UInt32 scale = substrait_type.decimal().scale();
        if (precision > DB::DataTypeDecimal128::maxPrecision())
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
        ch_type = DB::createDecimal<DB::DataTypeDecimal>(precision, scale);
        ch_type = tryWrapNullable(substrait_type.decimal().nullability(), ch_type);
    }
    else if (substrait_type.has_struct_())
    {
        const auto & types = substrait_type.struct_().types();
        DB::DataTypes struct_field_types(types.size());
        DB::Strings struct_field_names;

        if (field_names)
        {
            /// Construct CH tuple type following the DFS rule.
            /// Refer to NamedStruct in https://github.com/oap-project/gluten/blob/main/cpp-ch/local-engine/proto/substrait/type.proto
            for (int i = 0; i < types.size(); ++i)
            {
                struct_field_names.push_back(field_names->front());
                struct_field_types[i] = parseType(types[i], field_names);
            }
        }
        else
        {
            /// Construct CH tuple type without DFS rule.
            for (int i = 0; i < types.size(); ++i)
                struct_field_types[i] = parseType(types[i]);

            const auto & names = substrait_type.struct_().names();
            for (const auto & name : names)
                if (!name.empty())
                    struct_field_names.push_back(name);
        }

        if (!struct_field_names.empty())
            ch_type = std::make_shared<DB::DataTypeTuple>(struct_field_types, struct_field_names);
        else
            ch_type = std::make_shared<DB::DataTypeTuple>(struct_field_types);

        ch_type = tryWrapNullable(substrait_type.struct_().nullability(), ch_type);
    }
    else if (substrait_type.has_list())
    {
        auto ch_nested_type = parseType(substrait_type.list().type());
        ch_type = std::make_shared<DB::DataTypeArray>(ch_nested_type);
        ch_type = tryWrapNullable(substrait_type.list().nullability(), ch_type);
    }
    else if (substrait_type.has_map())
    {
        if (substrait_type.map().key().has_nothing())
        {
            // special case
            ch_type = std::make_shared<DB::DataTypeMap>(std::make_shared<DB::DataTypeNothing>(), std::make_shared<DB::DataTypeNothing>());
            ch_type = tryWrapNullable(substrait_type.map().nullability(), ch_type);
        }
        else
        {
            auto ch_key_type = parseType(substrait_type.map().key());
            auto ch_val_type = parseType(substrait_type.map().value());
            ch_type = std::make_shared<DB::DataTypeMap>(ch_key_type, ch_val_type);
            ch_type = tryWrapNullable(substrait_type.map().nullability(), ch_type);
        }
    }
    else if (substrait_type.has_nothing())
    {
        ch_type = std::make_shared<DB::DataTypeNothing>();
        ch_type = tryWrapNullable(substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE, ch_type);
    }
    else
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support type {}", substrait_type.DebugString());

    /// TODO(taiyang-li): consider Time/IntervalYear/IntervalDay/TimestampTZ/UUID/VarChar/FixedBinary/UserDefined
    return ch_type;
}

DB::Block TypeParser::buildBlockFromNamedStruct(const substrait::NamedStruct & struct_, const std::string & low_card_cols)
{
    std::unordered_set<std::string> low_card_columns;
    Poco::StringTokenizer tokenizer(low_card_cols, ",");
    for (const auto & token : tokenizer)
        low_card_columns.insert(token);

    DB::ColumnsWithTypeAndName internal_cols;
    internal_cols.reserve(struct_.names_size());
    std::list<std::string> field_names;
    for (int i = 0; i < struct_.names_size(); ++i)
        field_names.emplace_back(struct_.names(i));

    /// Spark permits schemas with duplicate field names, whereas ClickHouse (CH) does not. However,
    /// since Substrait plans reference columns by position rather than name, renaming duplicate
    /// columns is safe and will not affect query execution.
    /// See bug #9317.
    std::set<String> column_names_set;
    for (int i = 0; i < struct_.struct_().types_size(); ++i)
    {
        auto name = field_names.front();
        const auto & substrait_type = struct_.struct_().types(i);
        auto ch_type = parseType(substrait_type, &field_names);

        if (low_card_columns.contains(name))
            ch_type = std::make_shared<DB::DataTypeLowCardinality>(ch_type);

        // This is a partial aggregate data column.
        // It's type is special, must be a struct type contains all arguments types.
        // Notice: there are some coincidence cases in which the type is not a struct type, e.g. name is "_1#913 + _2#914#928". We need to handle it.
        Poco::StringTokenizer name_parts(name, "#");
        if (name_parts.count() >= 4 && !name.contains(' '))
        {
            auto nested_data_type = DB::removeNullable(ch_type);
            const auto * tuple_type = typeid_cast<const DB::DataTypeTuple *>(nested_data_type.get());
            if (!tuple_type)
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Tuple is expected, but got {}", ch_type->getName());

            auto args_types = tuple_type->getElements();
            AggregateFunctionProperties properties;
            auto tmp_ctx = DB::Context::createCopy(QueryContext::globalContext());
            auto parser_context = ParserContext::build(tmp_ctx);
            auto function_parser = AggregateFunctionParserFactory::instance().get(name_parts[3], parser_context);
            /// This may remove elements from args_types, because some of them are used to determine CH function name, but not needed for the following
            /// call `AggregateFunctionFactory::instance().get`
            auto agg_function_name = function_parser->getCHFunctionName(args_types);
            ch_type = RelParser::getAggregateFunction(
                          agg_function_name, args_types, properties, function_parser->getDefaultFunctionParameters())
                          ->getStateType();
        }
        if (column_names_set.contains(name))
        {
            name = name + "_" + std::to_string(i);
        }
        else
        {
            column_names_set.insert(name);
        }
        internal_cols.push_back(ColumnWithTypeAndName(ch_type, name));
    }

    DB::Block res(std::move(internal_cols));
    return res;
}

DB::Block TypeParser::buildBlockFromNamedStructWithoutDFS(const substrait::NamedStruct & named_struct_)
{
    DB::ColumnsWithTypeAndName columns;
    const auto & names = named_struct_.names();
    const auto & types = named_struct_.struct_().types();
    columns.reserve(names.size());
    if (names.size() != types.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Mismatch between names and types in named_struct");

    int size = named_struct_.names_size();
    for (int i = 0; i < size; ++i)
    {
        const auto & name = names[i];
        const auto & type = types[i];
        auto ch_type = parseType(type);
        columns.emplace_back(ColumnWithTypeAndName(ch_type, name));
    }

    DB::Block res(std::move(columns));
    return res;
}

bool TypeParser::isTypeMatched(const substrait::Type & substrait_type, const DataTypePtr & ch_type, bool ignore_nullability)
{
    const auto parsed_ch_type = TypeParser::parseType(substrait_type);
    if (ignore_nullability)
    {
        // if it's only different in nullability, we consider them same.
        // this will be problematic for some functions being not-null in spark but nullable in clickhouse.
        // e.g. murmur3hash
        const auto a = removeNullable(parsed_ch_type);
        const auto b = removeNullable(ch_type);
        return a->equals(*b);
    }
    else
        return parsed_ch_type->equals(*ch_type);
}

DB::DataTypePtr TypeParser::tryWrapNullable(substrait::Type_Nullability nullable, DB::DataTypePtr nested_type)
{
    if (nullable == substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE && !nested_type->isNullable())
        return std::make_shared<DB::DataTypeNullable>(nested_type);
    return nested_type;
}
}
