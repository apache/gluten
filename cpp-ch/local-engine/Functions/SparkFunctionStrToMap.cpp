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
#include <memory>
#include <ranges>
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/Regexps.h>
#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
}
}

namespace local_engine
{

class TrivialCharSplitter
{
public:
    using Pos = const char *;
    TrivialCharSplitter(const String & delimiter_) : delimiter(delimiter_) { }

    void reset(Pos str_begin_, Pos str_end_)
    {
        str_begin = str_begin_;
        str_end = str_end_;
        str_cursor = str_begin;
        delimiter_begin = nullptr;
        delimiter_end = nullptr;
    }

    Pos getDelimiterBegin() const { return delimiter_begin; }
    Pos getDelimiterEnd() const { return delimiter_end; }

    bool next(Pos & token_begin, Pos & token_end)
    {
        if (str_cursor > str_end)
            return false;
        token_begin = str_cursor;
        auto next_token_pos = static_cast<Pos>(memmem(str_cursor, str_end - str_cursor, delimiter.c_str(), delimiter.size()));
        // If delimiter is not found, return the remaining string.
        if (!next_token_pos)
        {
            token_end = str_end;
            str_cursor = str_end + 1;
            delimiter_begin = nullptr;
            delimiter_end = nullptr;
        }
        else
        {
            delimiter_begin = next_token_pos;
            token_end = next_token_pos;
            str_cursor = next_token_pos + delimiter.size();
            delimiter_end = str_cursor;
        }
        return true;
    }

private:
    String delimiter;
    Pos str_begin;
    Pos str_end;
    Pos str_cursor;
    Pos delimiter_begin;
    Pos delimiter_end;
};

struct RegularSplitter
{
public:
    using Pos = const char *;
    RegularSplitter(const String & delimiter_) : delimiter(delimiter_)
    {
        if (!delimiter.empty())
            re = std::make_shared<OptimizedRegularExpression>(DB::Regexps::createRegexp<false, false, false>(delimiter));
    }

    void reset(Pos str_begin_, Pos str_end_)
    {
        str_begin = str_begin_;
        str_end = str_end_;
        str_cursor = str_begin;
        delimiter_begin = nullptr;
        delimiter_end = nullptr;
    }

    Pos getDelimiterBegin() const { return delimiter_begin; }
    Pos getDelimiterEnd() const { return delimiter_end; }

    bool next(Pos & token_begin, Pos & token_end)
    {
        if (str_cursor > str_end)
            return false;
        // If delimiter is empty, return each character as a token.
        if (!re)
        {
            token_begin = str_cursor;
            ++str_cursor;
            delimiter_begin = str_cursor;
            delimiter_end = str_cursor;
            token_end = str_cursor;
        }
        else
        {
            if (!re->match(str_cursor, str_end - str_cursor, matches))
            {
                token_begin = str_cursor;
                token_end = str_end;
                str_cursor = str_end + 1;
                delimiter_begin = nullptr;
                delimiter_end = nullptr;
                return true;
            }
            token_begin = str_cursor;
            token_end = str_cursor + matches[0].offset;
            delimiter_begin = token_end;
            str_cursor = token_end + matches[0].length;
            delimiter_end = str_cursor;
        }
        return true;
    }

private:
    String delimiter;
    DB::Regexps::RegexpPtr re;
    OptimizedRegularExpression::MatchVec matches;
    Pos str_begin;
    Pos str_end;
    Pos str_cursor;
    Pos delimiter_begin;
    Pos delimiter_end;
};


static bool isConstColumn(const DB::IColumn & col)
{
    return col.isConst();
}

template <typename PairGenerator, typename KVGenerator>
class SparkFunctionStrToMap : public DB::IFunction
{
public:
    using Pos = const char *;
    static constexpr auto name = "spark_str_to_map";
    static DB::FunctionPtr create(const DB::ContextPtr) { return std::make_shared<SparkFunctionStrToMap<PairGenerator, KVGenerator>>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DB::ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        DB::FunctionArgumentDescriptors mandatory_args{
            {"string", static_cast<DB::FunctionArgumentDescriptor::TypeValidator>(&DB::isStringOrFixedString), nullptr, "String"},
            {"pair_delimiter",
             static_cast<DB::FunctionArgumentDescriptor::TypeValidator>(&DB::isStringOrFixedString),
             &isConstColumn,
             "String"},
            {"key_value_delimiter",
             static_cast<DB::FunctionArgumentDescriptor::TypeValidator>(&DB::isStringOrFixedString),
             &isConstColumn,
             "String"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args);

        auto map_typ = std::make_shared<DB::DataTypeMap>(
            std::make_shared<DB::DataTypeString>(), makeNullable(std::make_shared<DB::DataTypeString>()));
        if (arguments[0].type->isNullable())
            return std::make_shared<DB::DataTypeNullable>(map_typ);
        else
            return map_typ;
    }

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        String pair_delim = (*arguments[1].column)[0].safeGet<String>();
        String kv_delim = (*arguments[2].column)[0].safeGet<String>();
        PairGenerator pair_generator(pair_delim);
        KVGenerator kv_generator(kv_delim);

        auto col_map = result_type->createColumn();

        const DB::ColumnString * col_str = nullptr;
        const DB::ColumnUInt8 * null_map = nullptr;
        if (arguments[0].column->isNullable())
        {
            const auto * col_null = DB::checkAndGetColumn<DB::ColumnNullable>(arguments[0].column.get());
            col_str = DB::checkAndGetColumn<DB::ColumnString>(col_null->getNestedColumnPtr().get());
            null_map = &(col_null->getNullMapColumn());
        }
        else
        {
            col_str = DB::checkAndGetColumn<DB::ColumnString>(arguments[0].column.get());
        }

        const auto & strs = col_str->getChars();
        const auto & offsets = col_str->getOffsets();

        DB::ColumnString::Offset prev_offset = 0;

        for (size_t i = 0, n = offsets.size(); i < n; ++i)
        {
            if (null_map && (*null_map)[n] != 0)
                col_map->insertDefault();
            else
            {
                DB::Map map;
                Pos str_begin = reinterpret_cast<Pos>(&strs[prev_offset]);
                Pos str_end = reinterpret_cast<Pos>(&strs[offsets[i]]);
                LOG_TRACE(
                    getLogger("SparkFunctionStrToMap"),
                    "str_begin: {}, str_end: {}. {}",
                    0,
                    str_end - str_begin,
                    std::string_view(str_begin, str_end - str_begin));
                pair_generator.reset(str_begin, str_end);
                Pos pair_begin;
                Pos pair_end;
                while (pair_generator.next(pair_begin, pair_end))
                {
                    LOG_TRACE(
                        getLogger("SparkFunctionStrToMap"),
                        "pair_begin: {}, pair_end: {}, {}",
                        pair_begin - str_begin,
                        pair_end - str_begin,
                        std::string_view(pair_begin, pair_end - pair_begin));
                    kv_generator.reset(pair_begin, pair_end);
                    Pos key_begin;
                    Pos key_end;
                    if (kv_generator.next(key_begin, key_end))
                    {
                        DB::Tuple tuple(2);
                        size_t key_len = key_end - key_begin;
                        tuple[0] = key_end == str_end ? std::string_view(key_begin, key_len) : std::string_view(key_begin, key_len);
                        auto delimiter_begin = kv_generator.getDelimiterBegin();
                        auto delimiter_end = kv_generator.getDelimiterEnd();
                        LOG_TRACE(
                            getLogger("SparkFunctionStrToMap"),
                            "key_begin: {}, key_end: {}, delim_begin: {}, delim_end: {}, key:{}",
                            key_begin - str_begin,
                            key_end - str_begin,
                            delimiter_begin - str_begin,
                            delimiter_end - str_begin,
                            std::string_view(key_begin, key_end - key_begin));
                        if (delimiter_begin && delimiter_begin != str_end)
                        {
                            DB::Field value = pair_end == str_end ? std::string_view(delimiter_end, pair_end - delimiter_end)
                                                                 : std::string_view(delimiter_end, pair_end - delimiter_end);
                            tuple[1] = std::move(value);
                        }
                        else
                        {
                            // Not found delimiter, the value should be null
                            tuple[1] = DB::Null();
                        }
                        map.emplace_back(std::move(tuple));
                    }
                    else if (pair_begin == pair_end)
                    {
                        // Empty pair. key is empty string, but value is null.
                        DB::Tuple tuple(2);
                        tuple[0] = std::string();
                        tuple[1] = DB::Null();
                        map.emplace_back(std::move(tuple));
                    }
                    else
                    {
                        LOG_WARNING(getLogger("SparkFunctionStrToMap"), "Split key value failed.");
                    }
                }
                col_map->insert(std::move(map));
            }
            prev_offset = offsets[i];
        }

        return col_map;
    }
};

class SparkFunctionStrToMapOverloadResolver : public DB::IFunctionOverloadResolver
{
public:
    static constexpr auto name = "spark_str_to_map";
    static DB::FunctionOverloadResolverPtr create(const DB::ContextPtr context)
    {
        return std::make_shared<SparkFunctionStrToMapOverloadResolver>(context);
    }

    explicit SparkFunctionStrToMapOverloadResolver(DB::ContextPtr context_)
        : context(context_), trivial_function(SparkFunctionStrToMap<TrivialCharSplitter, TrivialCharSplitter>::create(context))
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isVariadic() const override { return false; }
    DB::FunctionBasePtr buildImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & return_type) const override
    {
        // The delimiter could be a regular expression.
        bool is_trivial_pair_delim = patternIsTrivialChar(arguments[1]);
        bool is_trivial_kv_delim = patternIsTrivialChar(arguments[2]);
        DB::FunctionPtr function_ptr = nullptr;
        if (is_trivial_pair_delim && is_trivial_kv_delim)
            function_ptr = trivial_function;
        else if (is_trivial_pair_delim && !is_trivial_kv_delim)
            function_ptr = SparkFunctionStrToMap<TrivialCharSplitter, RegularSplitter>::create(context);
        else if (!is_trivial_pair_delim && is_trivial_kv_delim)
            function_ptr = SparkFunctionStrToMap<RegularSplitter, TrivialCharSplitter>::create(context);
        else
            function_ptr = SparkFunctionStrToMap<RegularSplitter, RegularSplitter>::create(context);
        return std::make_unique<DB::FunctionToFunctionBaseAdaptor>(
            function_ptr,
            DB::DataTypes{std::from_range_t{}, arguments | std::views::transform([](const auto & elem) { return elem.type; })},
            return_type);
    }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        return trivial_function->getReturnTypeImpl(arguments);
    }

private:
    DB::ContextPtr context;
    DB::FunctionPtr trivial_function;

    bool patternIsTrivialChar(const DB::ColumnWithTypeAndName & argument) const
    {
        const DB::ColumnConst * col = checkAndGetColumnConstStringOrFixedString(argument.column.get());
        if (!col)
            return false;

        String pattern = col->getValue<String>();
        if (pattern.empty())
            return false;
        OptimizedRegularExpression re = DB::Regexps::createRegexp<false, false, false>(pattern);

        std::string required_substring;
        bool is_trivial;
        bool required_substring_is_prefix;
        re.getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);
        return is_trivial && required_substring == pattern;
    }
};

REGISTER_FUNCTION(SparkStrToMap)
{
    factory.registerFunction<SparkFunctionStrToMapOverloadResolver>();
}
}
