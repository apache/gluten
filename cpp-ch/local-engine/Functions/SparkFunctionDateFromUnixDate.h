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
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
class SparkFunctionDateFromUnixDate : public DB::IFunction
{
public:
    static constexpr auto name = "sparkDateFromUnixDate";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionDateFromUnixDate>(); }

    SparkFunctionDateFromUnixDate() = default;
    ~SparkFunctionDateFromUnixDate() override = default;

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName &) const override { return std::make_shared<DB::DataTypeDate32>(); }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows) const override
    {
        if (arguments.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 1 argument", name);

        const DB::ColumnWithTypeAndName & first_arg = arguments[0];
        DB::WhichDataType which(first_arg.type);
        if (which.isInt32())
            return executeInternal<Int32>(first_arg.column, input_rows);
        else
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires Int32 argument", name);
    }

    template<typename T>
    DB::ColumnPtr executeInternal(const DB::ColumnPtr & col, size_t input_rows) const
    {
        const DB::ColumnVector<T> * col_src = checkAndGetColumn<DB::ColumnVector<T>>(col.get());
        DB::MutableColumnPtr res = DB::ColumnVector<Int32>::create(col->size());
        DB::PaddedPODArray<Int32> & data = assert_cast<DB::ColumnVector<Int32> *>(res.get())->getData();

        for (size_t i = 0; i < input_rows; ++i)
        {
            // Spark's date_from_unix_date returns a Date that is the given number of days since 1970-01-01.
            // CH Date32 is also stored as days since 1970-01-01, so the value passes through unchanged.
            const T unix_date = col_src->getElement(i);
            data[i] = static_cast<Int32>(unix_date);
        }
        return res;
    }
};
} 