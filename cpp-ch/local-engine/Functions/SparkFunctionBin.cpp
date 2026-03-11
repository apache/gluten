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
#include <cmath>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}
}

using namespace DB;
namespace local_engine
{
namespace
{
    class SparkFunctionBin : public IFunction
    {
    public:
        static constexpr auto name = "sparkBin";

        static FunctionPtr create(ContextPtr) { return std::make_shared<SparkFunctionBin>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (!isInt64(arguments[0].type) && !isInt32(arguments[0].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}, expected Int64 or Int32.",
                    arguments[0].type->getName(),
                    getName());

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
        {
            const IColumn * col = arguments[0].column.get();
            ColumnPtr res_column;

            if (tryExecute<Int32>(col, res_column) ||
                tryExecute<Int64>(col, res_column))
                return res_column;

            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                    arguments[0].column->getName(), getName());
        }

        template <typename T>
        bool tryExecute(const IColumn * col, ColumnPtr & col_res) const
        {
            const ColumnVector<T> * col_vec = checkAndGetColumn<ColumnVector<T>>(col);
            if (!col_vec)
                return false;

            auto col_str = ColumnString::create();
            ColumnString::Chars & out_chars = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const typename ColumnVector<T>::Container & in_vec = col_vec->getData();
            size_t size = in_vec.size();
            out_offsets.resize_exact(size);

            size_t tot_len = 0;
            for (size_t i = 0; i < size; ++i)
            {
                auto len = std::max(1, static_cast<int>(64 - getLeadingZeroBits(static_cast<Int64>(in_vec[i]))));
                tot_len += len + 1;
            }
            out_chars.resize_exact(tot_len);

            size_t pos = 0;
            for (size_t i = 0; i < size; ++i)
            {
                auto val = static_cast<Int64>(in_vec[i]);
                auto len = std::max(1, static_cast<int>(64 - getLeadingZeroBits(val)));
                char * begin = reinterpret_cast<char *>(&out_chars[pos]);
                int char_pos = len;
                do
                {
                    *(begin + (--char_pos)) = (val & 1) ? '1' : '0';
                    val >>= 1;
                } while (val != 0 && char_pos > 0);

                pos += len;
                out_offsets[i] = pos;
            }

            col_res = std::move(col_str);
            return true;
        }
    };
}

REGISTER_FUNCTION(SparkFunctionBin)
{
    factory.registerFunction<SparkFunctionBin>();
}

}
