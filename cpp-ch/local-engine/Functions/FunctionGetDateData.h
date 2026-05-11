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
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>

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
template<bool get_year, bool get_date, typename T>
class FunctionGetDateData : public DB::IFunction
{
public:
    FunctionGetDateData() = default;
    ~FunctionGetDateData() override = default;

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t) const override
    {
        if (arguments.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 1.", getName());

        const DB::ColumnWithTypeAndName arg1 = arguments[0];
        const auto * src_col = checkAndGetColumn<DB::ColumnString>(arg1.column.get());
        size_t size = src_col->size();

        using ColVecTo = DB::ColumnVector<T>;
        typename ColVecTo::MutablePtr result_column = ColVecTo::create(size, 0);
        typename ColVecTo::Container & result_container = result_column->getData();
        DB::ColumnUInt8::MutablePtr null_map = DB::ColumnUInt8::create(size, 0);
        typename DB::ColumnUInt8::Container & null_container = null_map->getData();
        const DateLUTImpl * local_time_zone = &DateLUT::instance();
        const DateLUTImpl * utc_time_zone = &DateLUT::instance("UTC");

        for (size_t i = 0; i < size; ++i)
        {
            auto str = src_col->getDataAt(i);
            if (str.size() < 4)
            {
                null_container[i] = true;
                continue;
            }
            else
            {
                DB::ReadBufferFromMemory buf(str.data(), str.size());
                while(!buf.eof() && *buf.position() == ' ')
                {
                    buf.position() ++;
                }
                if(buf.buffer().end() - buf.position() < 4)
                {
                    null_container[i] = true;
                    continue;
                }
                bool can_be_parsed = true;
                if (!checkAndGetDateData(buf, buf.buffer().end() - buf.position(), result_container[i], *local_time_zone, can_be_parsed))
                {
                    if (!can_be_parsed)
                        null_container[i] = true;
                    else
                    {
                        time_t tmp = 0;
                        bool parsed = tryParseDateTimeBestEffort(tmp, buf, *local_time_zone, *utc_time_zone);
                        if (get_date)
                            result_container[i] = local_time_zone->toDayNum<time_t>(tmp);
                        null_container[i] = !parsed;
                    }
                }
            }
        }
        return DB::ColumnNullable::create(std::move(result_column), std::move(null_map));
    }

private:
    bool checkAndGetDateData(DB::ReadBuffer & buf, size_t buf_size, T &x, const DateLUTImpl & date_lut, bool & can_be_parsed) const
    {
        auto checkNumbericASCII = [&](DB::ReadBuffer & rb, size_t start, size_t length) -> bool
        {
            for (size_t i = start; i < start + length; ++i)
            {
                if (i >= buf_size || !isNumericASCII(*(rb.position() + i)))
                {
                    return false;
                }
            }
            return true;
        };
        auto checkDelimiter = [&](DB::ReadBuffer & rb, size_t pos) -> bool
        {
            if (pos >= buf_size || *(rb.position() + pos) != '-')
                return false;
            else
                return true;
        };
        bool yearNumberCanbeParsed = checkNumbericASCII(buf, 0, 4) && (buf_size == 4 || checkDelimiter(buf, 4));
        Int16 year = 0;
        if (yearNumberCanbeParsed)
        {
            year = (*(buf.position() + 0) - '0') * 1000 +
                    (*(buf.position() + 1) - '0') * 100 +
                    (*(buf.position() + 2) - '0') * 10 +
                    (*(buf.position() + 3) - '0');
            x = get_year ? year : 0;
        }
        if (!yearNumberCanbeParsed
            || !checkNumbericASCII(buf, 5, 2)
            || !checkDelimiter(buf, 7)
            || !checkNumbericASCII(buf, 8, 2))
        {
            can_be_parsed = yearNumberCanbeParsed;
            return false;
        }
        else
        {
            UInt8 month = (*(buf.position() + 5) - '0') * 10 + (*(buf.position() + 6) - '0');
            if (month <= 0 || month > 12)
                return false;
            UInt8 day = (*(buf.position() + 8) - '0') * 10 + (*(buf.position() + 9) - '0');
            if (day <= 0 || day > 31)
                return false;
            else if (day == 31 && (month == 2 || month == 4 || month == 6 || month == 9 || month == 11))
                return false;
            else if (day == 30 && month == 2)
                return false;
            else
            {
                if (day == 29 && month == 2 && year % 4 != 0)
                    return false;
                else
                {
                    if (get_date)
                        x = date_lut.makeDayNum(year, month, day, -static_cast<Int32>(date_lut.getDayNumOffsetEpoch()));
                    return true;
                }
            }
        }
    }
};
}
