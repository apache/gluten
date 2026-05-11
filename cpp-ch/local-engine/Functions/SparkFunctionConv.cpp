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
#include "SparkFunctionConv.h"
#include <string>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Poco/Logger.h>
#include <Poco/Types.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

DB::DataTypePtr SparkFunctionConv::getReturnTypeImpl(const DB::DataTypes & arguments) const
{
    if (arguments.size() != 3)
        throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 3.",
            getName(), arguments.size());

    if (!isInteger(arguments[1]))
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function {} must be Int", getName());
    if (!isInteger(arguments[2]))
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Third argument for function {} must be Int", getName());

    auto arg0_type = DB::removeNullable(arguments[0]);
    return std::make_shared<DB::DataTypeNullable>(arg0_type);
}

/// Taken from mysql-server sql/item_strfunc.cc
static unsigned long long my_strntoull_8bit(const char *nptr,
                                     size_t l, int base, const char **endptr,
                                     int *err)
{
    int negative;
    unsigned long long cutoff = 0;
    unsigned cutlim = 0;
    unsigned long long i = 0;
    const char *save = nullptr;
    int overflow = 0;

    *err = 0; /* Initialize error indicator */

    const char *s = nptr;
    const char *e = nptr + l;

    for (; s < e && std::isspace(*s); s++)
        ;

    if (s == e)
    {
        err[0] = EDOM;
        goto noconv;
    }

    if (*s == '-')
    {
        negative = 1;
        ++s;
    } else if (*s == '+') {
        negative = 0;
        ++s;
    } else
        negative = 0;

    save = s;

    cutoff = (~static_cast<unsigned long long>(0)) / static_cast<unsigned long int>(base);
    cutlim = static_cast<unsigned>(((~static_cast<unsigned long long>(0)) % static_cast<unsigned long int>(base)));

    overflow = 0;
    i = 0;
    for (; s != e; s++) {
        uint8_t c = *s;

        if (c >= '0' && c <= '9')
            c -= '0';
        else if (c >= 'A' && c <= 'Z')
            c = c - 'A' + 10;
        else if (c >= 'a' && c <= 'z')
            c = c - 'a' + 10;
        else
            break;
        if (c >= base) break;
        if (i > cutoff || (i == cutoff && c > cutlim))
            overflow = 1;
        else
        {
            i *= static_cast<unsigned long long>(base);
            i += c;
        }
    }

    if (s == save) goto noconv;

    if (endptr != nullptr) *endptr = s;

    if (overflow)
    {
        err[0] = ERANGE;
        return (~static_cast<unsigned long long>(0));
    }

    return negative ? -i : i;

noconv:
    if (endptr != nullptr) *endptr = nptr;
    return 0L;
}

static char * ll2str(int64_t val, char * dst, int radix, bool upcase)
{
    constexpr std::array<const char, 37> dig_vec_upper{"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"};
    constexpr std::array<const char, 37> dig_vec_lower{"0123456789abcdefghijklmnopqrstuvwxyz"};
    char buffer[65];
    const char *const dig_vec = upcase ? dig_vec_upper.data() : dig_vec_lower.data();
    auto uval = static_cast<uint64_t>(val);

    if (radix < 0)
    {
        if (radix < -36 || radix > -2) return nullptr;
        if (val < 0)
        {
            *dst++ = '-';
            /* Avoid integer overflow in (-val) for LLONG_MIN (BUG#31799). */
            uval = 0ULL - uval;
        }
        radix = -radix;
    }
    else if (radix > 36 || radix < 2)
    {
        return nullptr;
    }

    char *p = std::end(buffer);
    do
    {
        *--p = dig_vec[uval % radix];
        uval /= radix;
    } while (uval != 0);

    const size_t length = std::end(buffer) - p;
    memcpy(dst, p, length);
    dst[length] = '\0';
    return dst + length;
}

DB::ColumnPtr SparkFunctionConv::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const
{
    using longlong = Int64;
    auto from_base = static_cast<int>(arguments[1].column->getInt(0));
    auto to_base = static_cast<int>(arguments[2].column->getInt(0));

    auto result = result_type->createColumn();
    result->reserve(input_rows_count);
    // Note that abs(INT_MIN) is undefined.
    if (from_base == INT_MIN || to_base == INT_MIN || abs(to_base) > 36 || abs(to_base) < 2 || abs(from_base) > 36 || abs(from_base) < 2)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            result->insertData(nullptr, 1);
        return result;
    }

    longlong dec = 0;
    const char * endptr = nullptr;
    int err = 0;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        auto str_data = arguments[0].column->getDataAt(i);
        auto value_str = String(str_data.data(), str_data.size());
        if (from_base < 0)
            dec = my_strntoull_8bit(value_str.data(), value_str.length(), -from_base, &endptr, &err);
        else
            dec = static_cast<longlong>(my_strntoull_8bit(value_str.data(), value_str.length(), from_base, &endptr, &err));
        if (err == EDOM)
        {
            result->insertData(nullptr, 1);
            continue;
        }
        char ans[CONV_MAX_LENGTH + 1U];
        auto ret_ptr = ll2str(dec, ans, to_base, true);
        if (ret_ptr == nullptr)
            result->insertData(nullptr, 1);
        else
            result->insertData(ans, ret_ptr - ans);
    }
    return result;
}

REGISTER_FUNCTION(SparkFunctionConv)
{
    factory.registerFunction<SparkFunctionConv>();
}
}
