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


#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <simdjson/implementation_detection.h>
#if SIMDJSON_IMPLEMENTATION_ICELAKE && defined(__AVX512F__) && defined(__AVX512BW__)
#include <simdjson/icelake/simd.h>
namespace simdjson_impl = simdjson::icelake::simd;
#elif SIMDJSON_IMPLEMENTATION_HASWELL && defined(__AVX2__)
#include <simdjson/haswell/simd.h>
namespace simdjson_impl = simdjson::haswell::simd;
#elif SIMDJSON_IMPLEMENTATION_WESTMERE && defined(__SSE4_2__)
#include <simdjson/westmere/simd.h>
namespace simdjson_impl = simdjson::westmere::simd;
#elif SIMDJSON_IMPLEMENTATION_ARM64
#include <simdjson/arm64/simd.h>
namespace simdjson_impl = simdjson::arm64::simd;
#elif SIMDJSON_IMPLEMENTATION_PPC64
#include <simdjson/ppc64/simd.h>
namespace simdjson_impl = simdjson::ppc64::simd;
#elif SIMDJSON_IMPLEMENTATION_LSX
#include <simdjson/lsx/simd.h>
namespace simdjson_impl = simdjson::lsx::simd;
#elif SIMDJSON_IMPLEMENTATION_LASX
#include <simdjson/lasx/simd.h>
namespace simdjson_impl = simdjson::lasx::simd;
#else
#define SIMDJSON_NO_SIMD 1
#endif
#include <boost/iostreams/detail/select.hpp>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
// Since spark 3.3, unix_timestamp support arabic number input, e.g., "٢٠٢١-٠٧-٠١ ١٢:٠٠:٠٠".
// We implement a function to translate arabic indic digits to ascii digits here.
class LocalDigitsToAsciiDigitForDateFunction : public DB::IFunction
{
public:
    static constexpr auto name = "local_digit_to_ascii_digit_for_date";

    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<LocalDigitsToAsciiDigitForDateFunction>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        auto nested_type = DB::removeNullable(arguments[0]);
        if (!DB::WhichDataType(nested_type).isString())
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function {} must be String, but got {}",
                getName(),
                arguments[0]->getName());
        return arguments[0];
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t input_rows_count) const override
    {
        auto data_col = arguments[0].column;
        const DB::ColumnString * col_str = nullptr;
        const DB::ColumnNullable * col_nullable = nullptr;
        const DB::NullMap * null_map = nullptr;
        if (data_col->isConst())
        {
            if (data_col->isNullAt(0))
            {
                return data_col;
            }
            const DB::ColumnConst * col_const = DB::checkAndGetColumn<DB::ColumnConst>(data_col.get());
            data_col = col_const->getDataColumnPtr();
            if (data_col->isNullable())
            {
                col_nullable = DB::checkAndGetColumn<DB::ColumnNullable>(data_col.get());
                null_map = &(col_nullable->getNullMapData());
                col_str = DB::checkAndGetColumn<DB::ColumnString>(&(col_nullable->getNestedColumn()));
            }
            else
            {
                col_str = DB::checkAndGetColumn<DB::ColumnString>(data_col.get());
            }
            if (!col_str)
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument for function {} must be String, but got {}",
                    getName(),
                    data_col->getName());
            auto date_str = col_str->getDataAt(0);
            std::string new_str;
            if (!convertLocalDigitIfNeeded(date_str, new_str))
                return arguments[0].column;
            auto new_data_col = data_col->cloneEmpty();
            new_data_col->insertData(new_str.c_str(), new_str.size());
            return DB::ColumnConst::create(std::move(new_data_col), input_rows_count);
        }

        if (data_col->isNullable())
        {
            col_nullable = DB::checkAndGetColumn<DB::ColumnNullable>(data_col.get());
            null_map = &(col_nullable->getNullMapData());
            col_str = DB::checkAndGetColumn<DB::ColumnString>(&(col_nullable->getNestedColumn()));
        }
        else
        {
            col_str = DB::checkAndGetColumn<DB::ColumnString>(data_col.get());
        }
        if (!col_str)
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function {} must be String, but got {}",
                getName(),
                data_col->getName());

        std::string converted;
        DB::MutableColumnPtr res_col;
        for (size_t row_index = 0; row_index < input_rows_count; ++row_index)
        {
            if (null_map && (*null_map)[row_index])
            {
                if (res_col)
                    res_col->insertDefault();
                continue;
            }
            auto str = col_str->getDataAt(row_index);
            if (convertLocalDigitIfNeeded(str, converted))
            {
                if (!res_col)
                {
                    res_col = data_col->cloneEmpty();
                    if (row_index)
                        res_col->insertManyFrom(*data_col, 0, row_index);
                }
                LOG_DEBUG(
                    getLogger("LocalDigitsToAsciiDigitForDateFunction"),
                    "Converted local digit string {} to ascii digit string: {}",
                    col_str->getDataAt(row_index).toString(),
                    converted);
                res_col->insertData(converted.c_str(), converted.size());
            }
            else if (res_col)
            {
                res_col->insertFrom(*data_col, row_index);
            }
        }
        if (!res_col)
            return arguments[0].column;
        return res_col;
    }

private:
    char toAsciiDigit(char32_t c) const {
        // In Thai and Persian, dates typically do not use the Gregorian calendar.
        // This may cause failures in unix_timestamp parsing.
        if (c >= 0x0660 && c <= 0x0669)
            return static_cast<char>(c - 0x0660 + '0');
        else if (c >= 0x06F0 && c <= 0x06F9)
            return static_cast<char>(c - 0x06F0 + '0');
        else if (c >= 0x0966 && c <= 0x096F)
            return static_cast<char>(c - 0x0966 + '0');
        else if (c >= 0x0E50 && c <= 0x0E59)
            return static_cast<char>(c - 0x0E50 + '0');
        else if (c >= 0x17E0 && c <= 0x17E9)
            return static_cast<char>(c - 0x17E0 + '0');
        else if (c >= 0x09E6 && c <= 0x09EF)
            return static_cast<char>(c - 0x09E6 + '0');
        else
            return 0;
    }

    bool hasNonAsciiSimd(const char * data, size_t size) const
    {
#if SIMDJSON_NO_SIMD
        const unsigned char * bytes = reinterpret_cast<const unsigned char *>(data);
        for (size_t i = 0; i < size; ++i)
        {
            if (bytes[i] & 0x80)
                return true;
        }
        return false;
#else
        using simd8_u8 = simdjson_impl::simd8<uint8_t>;
        constexpr size_t kBlockSize = simd8_u8::SIZE;
        size_t i = 0;
        for (; i + kBlockSize <= size; i += kBlockSize)
        {
            if (!simd8_u8::load(reinterpret_cast<const uint8_t *>(data + i)).is_ascii())
                return true;
        }
        for (; i < size; ++i)
        {
            if (static_cast<unsigned char>(data[i]) & 0x80)
                return true;
        }
        return false;
#endif
    }

    bool convertLocalDigitIfNeeded(StringRef str, std::string & result) const
    {
        if (!str.size)
            return false;
        if (!hasNonAsciiSimd(str.data, str.size))
            return false;
        result.clear();
        result.reserve(str.size);
        bool has_local_digit = false;
        for (size_t i = 0; i < str.size;)
        {
            unsigned char c = str.data[i];
            char32_t cp = 0;
            if ((c & 0x80) == 0) // 1-byte
            {
                result.push_back(c);
                i += 1;
                continue;
            }
            else if ((c & 0xE0) == 0xC0) // 2-byte
            {
                unsigned char b1 = str.data[i + 1];
                if (c == 0xD9 && b1 >= 0xA0 && b1 <= 0xA9) // Arabic-Indic
                {
                    result.push_back(static_cast<char>('0' + (b1 - 0xA0)));
                    has_local_digit = true;
                    i += 2;
                    continue;
                }
                if (c == 0xDB && b1 >= 0xB0 && b1 <= 0xB9) // Eastern Arabic-Indic (Persian)
                {
                    result.push_back(static_cast<char>('0' + (b1 - 0xB0)));
                    has_local_digit = true;
                    i += 2;
                    continue;
                }
                cp = ((c & 0x1F) << 6) | (b1 & 0x3F);
                auto local_digit = toAsciiDigit(cp);
                if (local_digit)
                {
                    result.push_back(local_digit);
                    has_local_digit = true;
                }
                else
                {
                    result.push_back(static_cast<char>(c));
                    result.push_back(static_cast<char>(b1));
                }
                i += 2;
                continue;
            }
            else if ((c & 0xF0) == 0xE0) // 3-byte
            {
                unsigned char b1 = str.data[i + 1];
                unsigned char b2 = str.data[i + 2];
                if (c == 0xE0)
                {
                    if ((b1 == 0xA5 && b2 >= 0xA6 && b2 <= 0xAF) || // Devanagari
                        (b1 == 0xA7 && b2 >= 0xA6 && b2 <= 0xAF))   // Bengali
                    {
                        result.push_back(static_cast<char>('0' + (b2 - 0xA6)));
                        has_local_digit = true;
                        i += 3;
                        continue;
                    }
                    if (b1 == 0xB9 && b2 >= 0x90 && b2 <= 0x99) // Thai
                    {
                        result.push_back(static_cast<char>('0' + (b2 - 0x90)));
                        has_local_digit = true;
                        i += 3;
                        continue;
                    }
                }
                else if (c == 0xE1 && b1 == 0x9F && b2 >= 0xA0 && b2 <= 0xA9) // Khmer
                {
                    result.push_back(static_cast<char>('0' + (b2 - 0xA0)));
                    has_local_digit = true;
                    i += 3;
                    continue;
                }
                cp = ((c & 0x0F) << 12) | ((b1 & 0x3F) << 6) | (b2 & 0x3F);
                auto local_digit = toAsciiDigit(cp);
                if (local_digit)
                {
                    result.push_back(local_digit);
                    has_local_digit = true;
                }
                else
                {
                    result.push_back(static_cast<char>(c));
                    result.push_back(static_cast<char>(b1));
                    result.push_back(static_cast<char>(b2));
                }
                i += 3;
                continue;
            }
            else if ((c & 0xF8) == 0xF0) // 4-byte
            {
                unsigned char b1 = str.data[i + 1];
                unsigned char b2 = str.data[i + 2];
                unsigned char b3 = str.data[i + 3];
                cp = ((c & 0x07) << 18) | ((b1 & 0x3F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F);
                auto local_digit = toAsciiDigit(cp);
                if (local_digit)
                {
                    result.push_back(local_digit);
                    has_local_digit = true;
                }
                else
                {
                    result.push_back(static_cast<char>(c));
                    result.push_back(static_cast<char>(b1));
                    result.push_back(static_cast<char>(b2));
                    result.push_back(static_cast<char>(b3));
                }
                i += 4;
                continue;
            }
        }
        return has_local_digit;
    }
};

using namespace DB;
REGISTER_FUNCTION(LocalDigitToAsciiDigitForDate)
{
    factory.registerFunction<LocalDigitsToAsciiDigitForDateFunction>();
}
}
