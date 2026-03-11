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
#include <bitset>
#include <memory>
#include <string>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
struct TrimModeLeft
{
    static constexpr auto name = "trimLeftSpark";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = false;
};

struct TrimModeRight
{
    static constexpr auto name = "trimRightSpark";
    static constexpr bool trim_left = false;
    static constexpr bool trim_right = true;
};

struct TrimModeBoth
{
    static constexpr auto name = "trimBothSpark";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = true;
};


namespace
{
    template <typename TrimMode>
    class TrimSparkFunction : public IFunction
    {
    public:
        static constexpr auto name = TrimMode::name;

        static FunctionPtr create(ContextPtr) { return std::make_shared<TrimSparkFunction>(); }

        String getName() const override { return name; }

        bool useDefaultImplementationForConstants() const override { return true; }

        size_t getNumberOfArguments() const override { return 2; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 2)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be  2.",
                    getName(),
                    toString(arguments.size()));

            if (!isString(arguments[0]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type ({}) of first argument of function {}. Should be String.",
                    arguments[0]->getName(),
                    getName());

            if (!isString(arguments[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type ({}) of second argument of function {}. Should be String.",
                    arguments[0]->getName(),
                    getName());

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
        {
            const ColumnString * src_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
            const ColumnConst * src_const_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
            const ColumnString * trim_col = checkAndGetColumn<ColumnString>(arguments[1].column.get());
            const ColumnConst * trim_const_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());

            String src_const_str;
            String trim_const_str;
            if (src_const_col)
                src_const_str = src_const_col->getValue<String>();
            if (trim_const_col)
            {
                trim_const_str = trim_const_col->getValue<String>();
                if (trim_const_str.empty())
                {
                    return arguments[0].column;
                }
            }

            // If both arguments are constants, it will be simplified to a constant. Skipped here.

            auto res_col = ColumnString::create();
            ColumnString::Chars & res_data = res_col->getChars();
            ColumnString::Offsets & res_offsets = res_col->getOffsets();
            res_offsets.resize_exact(input_rows_count);

            // Source column is constant and trim column is not constant
            if (src_const_col)
            {
                res_data.reserve_exact(src_const_str.size() * input_rows_count);
                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    StringRef trim_str_ref = trim_col->getDataAt(row);
                    std::unique_ptr<std::bitset<256>> trim_set = buildTrimSet(trim_str_ref.data, trim_str_ref.size);
                    executeRow(src_const_str.c_str(), src_const_str.size(), res_data, res_offsets, row, *trim_set);
                }
                return std::move(res_col);
            }

            // Source column is not constant and trim column is constant
            if (trim_const_col)
            {
                res_data.reserve_exact(src_col->getChars().size());
                std::unique_ptr<std::bitset<256>> trim_set = buildTrimSet(trim_const_str.c_str(), trim_const_str.size());
                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    StringRef src_str_ref = src_col->getDataAt(row);
                    executeRow(src_str_ref.data, src_str_ref.size, res_data, res_offsets, row, *trim_set);
                }
                return std::move(res_col);
            }

            // Both columns are not constant
            res_data.reserve(src_col->getChars().size());
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                StringRef src_str_ref = src_col->getDataAt(row);
                StringRef trim_str_ref = trim_col->getDataAt(row);
                std::unique_ptr<std::bitset<256>> trim_set = buildTrimSet(trim_str_ref.data, trim_str_ref.size);
                executeRow(src_str_ref.data, src_str_ref.size, res_data, res_offsets, row, *trim_set);
            }
            return std::move(res_col);
        }

    private:
        void executeRow(
            const char * src,
            size_t src_size,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            size_t row,
            const std::bitset<256> & trim_set) const
        {
            const char * dst;
            size_t dst_size;
            trim(src, src_size, dst, dst_size, trim_set);
            size_t res_offset = row > 0 ? res_offsets[row - 1] : 0;
            res_data.resize_exact(res_data.size() + dst_size + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], dst, dst_size);
            res_offset += dst_size;
            res_offsets[row] = res_offset;
        }

        std::unique_ptr<std::bitset<256>> buildTrimSet(const char* data, const size_t size) const
        {
            auto trim_set = std::make_unique<std::bitset<256>>();
            for (size_t i = 0; i < size; ++i)
                trim_set->set((unsigned char)data[i]);
            return trim_set;
        }

        void trim(const char * src, const size_t src_size, const char *& dst, size_t & dst_size, const std::bitset<256> & trim_set) const
        {
            const char * src_end = src + src_size;
            if constexpr (TrimMode::trim_left)
                while (src < src_end && trim_set.test((unsigned char)*src))
                    ++src;

            if constexpr (TrimMode::trim_right)
                while (src < src_end && trim_set.test((unsigned char)*(src_end - 1)))
                    --src_end;

            dst = src;
            dst_size = src_end - src;
        }
    };

    using FunctionTrimBothSpark = TrimSparkFunction<TrimModeBoth>;
    using FunctionTrimLeftSpark = TrimSparkFunction<TrimModeLeft>;
    using FunctionTrimRightSpark = TrimSparkFunction<TrimModeRight>;
}

REGISTER_FUNCTION(TrimSpark)
{
    factory.registerFunction<FunctionTrimBothSpark>();
    factory.registerFunction<FunctionTrimLeftSpark>();
    factory.registerFunction<FunctionTrimRightSpark>();
}

}
