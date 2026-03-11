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
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearchToString.h>
#include <Functions/IFunction.h>
#include <Functions/URL/domain.h>
#include <Poco/Logger.h>
#include <Poco/URI.h>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
/// allow to return null.
template <typename Extractor>
struct ExtractNullableSubstringImpl
{
    static void vector(const DB::ColumnString::Chars & data, const DB::ColumnString::Offsets & offsets,
        DB::ColumnString::Chars & res_data, DB::ColumnString::Offsets & res_offsets, DB::IColumn & null_map)
    {
        size_t size = offsets.size();
        res_offsets.resize_exact(size);
        res_data.reserve_exact(size * Extractor::getReserveLengthForElement());
        null_map.reserve(size);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        DB::Pos start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
        {
            String s(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset);
            try
            {
                Poco::URI uri(s, false);
                Extractor::execute(uri, s, start, length);
            } 
            catch (const Poco::SyntaxException &)
            {
                start = nullptr;
                length = 0;
            }
            res_data.resize_exact(res_data.size() + length);
            if (start)
            {
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
                null_map.insert(0);
            }
            else
            {
                null_map.insert(1);
            }
            res_offset += length;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};
template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToNullableString : public DB::IFunction
{
public:
    static constexpr auto name = Name::name;
    static DB::FunctionPtr create(DB::ContextPtr)
    {
        return std::make_shared<FunctionStringToNullableString>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const DB::ColumnsWithTypeAndName &) const override
    {
        return is_injective;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if (!DB::isStringOrFixedString(arguments[0]))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        return DB::makeNullable(arguments[0]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const DB::ColumnPtr column = arguments[0].column;
        auto null_map = DB::DataTypeUInt8().createColumn();
        if (const DB::ColumnString * col = checkAndGetColumn<DB::ColumnString>(column.get()))
        {
            auto col_res = DB::ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), *null_map);
            return DB::ColumnNullable::create(std::move(col_res), std::move(null_map));
        }
        else
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());
    }
};

template <typename Impl, typename Name>
class FunctionsStringSearchToNullableString : public DB::IFunction
{
public:
    static constexpr auto name = Name::name;
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<FunctionsStringSearchToNullableString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    DB::ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[1]->getName(), getName());

        return DB::makeNullable(std::make_shared<DB::DataTypeString>());
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const DB::ColumnPtr column = arguments[0].column;
        const DB::ColumnPtr column_needle = arguments[1].column;

        const DB::ColumnConst * col_needle = typeid_cast<const DB::ColumnConst *>(&*column_needle);
        if (!col_needle)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be constant string", getName());

        if (const DB::ColumnString * col = DB::checkAndGetColumn<DB::ColumnString>(column.get()))
        {
            auto col_res = DB::ColumnString::create();
            auto null_map = DB::ColumnUInt8::create(col->size(), 0);
            Impl::vector(*col, col_needle->getValue<String>(), *col_res, *null_map);

            return DB::ColumnNullable::create(std::move(col_res), std::move(null_map));
        }
        else
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());
    }
};

/// Different from CH extractURLParameters which returns an array result.
struct NameSparkExtractURLQuery
{
    static constexpr auto name = "spark_parse_url_query";
};

struct SparkExtractURLQuery
{
    static size_t getReserveLengthForElement() { return 30; }

    static void execute(const Poco::URI & uri, const String & data, DB::Pos & res_data, size_t & res_size)
    {
        
        const auto & query = uri.getRawQuery();
        res_data = query.data();
        res_size = query.size();
        String protocol_prefix = uri.getScheme() + "://";
        DB::Pos query_string_begin = data.starts_with(protocol_prefix) ? 
            find_first_symbols<'?', '#'>(data.data(), data.data() + data.size()) :
            find_first_symbols<'?', '#', ':'>(data.data(), data.data() + data.size());
        if (query_string_begin && *query_string_begin != '?')
        {
            res_data = nullptr;
            res_size = 0;
        }
    }
};
using SparkFunctionURLQuery = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLQuery>, NameSparkExtractURLQuery>;
REGISTER_FUNCTION(SparkFunctionURLQuery)
{
    factory.registerFunction<SparkFunctionURLQuery>();
}

struct NameSparkExtractURLOneQuery
{
    static constexpr auto name = "spark_parse_url_one_query";
};
struct SparkExtractURLOneQuery
{
    static void vector(const DB::ColumnString & col, std::string pattern, DB::IColumn & res_col, DB::IColumn & null_map)
    {
        DB::ColumnUInt8 & null_map_col = assert_cast<DB::ColumnUInt8 &>(null_map);
        DB::PaddedPODArray<UInt8> & null_map_data = null_map_col.getData();
        for (size_t i = 0; i < col.size(); ++i)
        {
            try 
            {
                const String s = col.getDataAt(i).toString();
                Poco::URI uri(s, false);
                
                String protocol_prefix = uri.getScheme() + "://";
                DB::Pos query_string_begin = s.starts_with(protocol_prefix) ? 
                    find_first_symbols<'?', '#'>(s.data(), s.data() + s.size()) :
                    find_first_symbols<'?', '#', ':'>(s.data(), s.data() + s.size());
                if (query_string_begin && *query_string_begin != '?')
                {
                    res_col.insertDefault();
                    null_map_data[i] = 1;
                    continue;
                }

                const String & query = uri.getRawQuery();
                DB::Pos query_pos = query.data();
                auto getMatchedValue = [&](const DB::Pos & begin_pos, const size_t len) -> bool
                {
                    for (size_t j = 0; j < len; ++j)
                    {
                        if (*(begin_pos + j) == '=')
                        {
                            if (pattern == String(begin_pos, j))
                            {
                                res_col.insertData(begin_pos + j + 1, len - j - 1);
                                return true;
                            }
                        }
                    }
                    return false;
                };

                bool matched = false;
                for (size_t j = 0; j < query.size(); ++j)
                {
                    if (query.at(j) == '&')
                    {
                        if(getMatchedValue(query_pos, query.data() + j - query_pos))
                        {
                            matched = true;
                            break;
                        }
                        else
                            query_pos = query.data() + j + 1;
                    }
                }
                if (!matched && query_pos < query.data() + query.size())
                    matched = getMatchedValue(query_pos, query.data() + query.size() - query_pos);

                if (!matched)
                    res_col.insertDefault();
    
                null_map_data[i] = !matched;
            }
            catch (const Poco::SyntaxException &)
            {
                res_col.insertDefault();
                null_map_data[i] = 1;
            }
        }
    }
};
using SparkFunctionURLOneQuery = FunctionsStringSearchToNullableString<SparkExtractURLOneQuery, NameSparkExtractURLOneQuery>;
REGISTER_FUNCTION(SparkFunctionURLOneQuery)
{
    factory.registerFunction<SparkFunctionURLOneQuery>();
}



struct SparkExtractURLHost
{
    static size_t getReserveLengthForElement() { return 30; }

    static void execute(const Poco::URI & uri, const String &, DB::Pos & res_data, size_t & res_size)
    {
        const auto & host = uri.getHost();
        res_data = host.data();
        res_size = host.size();  
        if (host.empty())
        {
            res_data = nullptr;
            res_size = 0;
        }
    }
};

struct NameSparkExtractURLHost
{
    static constexpr auto name = "spark_parse_url_host";
};
using SparkFunctionURLHost = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLHost>, NameSparkExtractURLHost>;
REGISTER_FUNCTION(SparkFunctionURLHost)
{
    factory.registerFunction<SparkFunctionURLHost>();
}

struct NameSparkExtractURLPath
{
    static constexpr auto name = "spark_parse_url_path";
};
struct SparkExtractURLPath
{
    static size_t getReserveLengthForElement() { return 25; }

    static void execute(const Poco::URI & uri, const String &, DB::Pos & res_data, size_t & res_size)
    {
        const auto & path = uri.getPath();
        res_data = path.data();
        res_size = path.size();
    }
};
using SparkFunctionURLPath = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLPath>, NameSparkExtractURLPath>;
REGISTER_FUNCTION(SparkFunctionURLPath)
{
    factory.registerFunction<SparkFunctionURLPath>();
}

struct NameSparkExtractUserInfo
{
    static constexpr auto name = "spark_parse_url_userinfo";
};
struct SparkExtractURLUserInfo
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(const Poco::URI & uri, const String &, DB::Pos & res_data, size_t & res_size)
    {
        const auto & userinfo = uri.getUserInfo();
        res_data = userinfo.data();
        res_size = userinfo.size();
        if (userinfo.empty())
        {
            res_data = nullptr;
            res_size = 0;
        }
    }
};
using SparkFunctionURLUserInfo = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLUserInfo>, NameSparkExtractUserInfo>;
REGISTER_FUNCTION(SparkFunctionURLUserInfo)
{
    factory.registerFunction<SparkFunctionURLUserInfo>();
}

struct NameSparkExtractURLRef
{
    static constexpr auto name = "spark_parse_url_ref";
};
struct SparkExtractURLRef
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(const Poco::URI & uri, const String & data, DB::Pos & res_data, size_t & res_size)
    {
        const auto & fragment = uri.getFragment();
        res_data = fragment.data();
        res_size = fragment.size();
        if (data.find(fragment) == std::string::npos || fragment.empty())
        {
            const auto * ref_delim_pos = find_first_symbols<'#'>(data.data(),data.data() + data.size());
            if (ref_delim_pos && ref_delim_pos < data.data() + data.size())
            {
                res_data = ref_delim_pos + 1;
                res_size = data.data() + data.size() - res_data;
            }
            else
            {
                res_data = nullptr;
                res_size = 0;
            }
        }
    }
};
using SparkFunctionURLRef = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLRef>, NameSparkExtractURLRef>;
REGISTER_FUNCTION(SparkFunctionURLRef)
{
    factory.registerFunction<SparkFunctionURLRef>();
}

struct NameSparkExtractURLFile
{
    static constexpr auto name = "spark_parse_url_file";
};
struct SparkExtractURLFile
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(const Poco::URI & uri, const String & data, DB::Pos & res_data, size_t & res_size)
    {
        const static String protocol_delim = "://";
        const auto * protocol_delim_pos = static_cast<DB::Pos>(memmem(data.data(), data.size(), protocol_delim.data(), protocol_delim.size()));
        if (!protocol_delim_pos)
        {
            auto colon_pos = find_first_symbols<':'>(data.data(), data.data() + data.size());
            if (colon_pos && colon_pos + 1 < data.data() + data.size())
            {
                res_data = nullptr;
                res_size = 0;
            }
            else
            {
                res_data = data.data();
                res_size = data.size();
            }
            return;
        }
        const String & res = uri.getPath();
        res_data = res.data();
        res_size = res.size();
        DB::Pos query_begin_pos = find_first_symbols<'?'>(protocol_delim_pos + protocol_delim.size(), data.data() + data.size());
        if (query_begin_pos && *query_begin_pos == '?')
        {
            const String & query = uri.getRawQuery();
            String new_res = res.empty() && query.empty() ? "" : res + "?" + query;
            res_data = new_res.data();
            res_size = new_res.size();
        }
    }
};
using SparkFunctionURLFile = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLFile>, NameSparkExtractURLFile>;
REGISTER_FUNCTION(SparkFunctionURLFile)
{
    factory.registerFunction<SparkFunctionURLFile>();
}

struct NameSparkExtractURLAuthority
{
    static constexpr auto name = "spark_parse_url_authority";
};
struct SparkExtractURLAuthority
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(const Poco::URI & uri, const String &, DB::Pos & res_data, size_t & res_size)
    {
        const auto & authority = uri.getAuthority();
        res_data = authority.data();
        res_size = authority.size();
        if (authority.empty())
        {
            res_data = nullptr;
            res_size = 0;
        }
    }
};
using SparkFunctionURLAuthority
    = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLAuthority>, NameSparkExtractURLAuthority>;
REGISTER_FUNCTION(SparkFunctionURLAuthority)
{
    factory.registerFunction<SparkFunctionURLAuthority>();
}

struct NameSparkExtractURLInvalid
{
    static constexpr auto name = "spark_parse_url_invalid";
};

struct SparkExtractURLInvalid
{
    static size_t getReserveLengthForElement() { return 1; }
    static void execute(const Poco::URI &, const String &, DB::Pos & res_data, size_t & res_size)
    {
        res_data = nullptr;
        res_size = 0;
    }
};
using SparkFunctionURLInvalid = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLInvalid>, NameSparkExtractURLInvalid>;
REGISTER_FUNCTION(SparkFunctionURLInvalid)
{
    factory.registerFunction<SparkFunctionURLInvalid>();
}
}
