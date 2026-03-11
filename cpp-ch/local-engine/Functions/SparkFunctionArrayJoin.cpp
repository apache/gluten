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
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <base/StringRef.h>

using namespace DB;

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
class SparkFunctionArrayJoin : public IFunction
{
public:
    static constexpr auto name = "sparkArrayJoin";
    static FunctionPtr create(ContextPtr) { return std::make_shared<SparkFunctionArrayJoin>(); }
    SparkFunctionArrayJoin() = default;
    ~SparkFunctionArrayJoin() override = default;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        auto data_type = std::make_shared<DataTypeString>();
        return makeNullable(data_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must have 2 or 3 arguments", getName());
        auto res_col = ColumnString::create();
        auto null_col = ColumnUInt8::create(input_rows_count, 0);
        PaddedPODArray<UInt8> & null_result = null_col->getData();
        if (input_rows_count == 0)
            return ColumnNullable::create(std::move(res_col), std::move(null_col));
        
        const ColumnArray * array_col = array_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());;
        if (!array_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 1st argument must be array type", getName());
        
        const ColumnNullable * array_nested_col = checkAndGetColumn<ColumnNullable>(&array_col->getData());
        const ColumnString * string_col;
        if (array_nested_col)
            string_col = checkAndGetColumn<ColumnString>(array_nested_col->getNestedColumnPtr().get());
        else
            string_col = checkAndGetColumn<ColumnString>(&array_col->getData());
        const ColumnArray::Offsets & array_offsets = array_col->getOffsets();
        const ColumnString::Offsets & string_offsets = string_col->getOffsets();
        const ColumnString::Chars & string_data = string_col->getChars();

        auto extractColumnString = [&](const ColumnPtr & col) -> const ColumnString *
        {
            const ColumnString * res = nullptr;
            if (col->isConst())
            {
                const ColumnConst * const_col = checkAndGetColumn<ColumnConst>(col.get());
                if (const_col)
                    res = checkAndGetColumn<ColumnString>(const_col->getDataColumnPtr().get());
            }
            else
                res = checkAndGetColumn<ColumnString>(col.get());
            return res;
        };
        bool const_delim_col = arguments[1].column->isConst();
        bool const_null_replacement_col = false;
        const ColumnString * delim_col = extractColumnString(arguments[1].column);
        const ColumnString * null_replacement_col = nullptr;
        if (arguments.size() == 3)
        {
            const_null_replacement_col = arguments[2].column->isConst();
            null_replacement_col = extractColumnString(arguments[2].column);
        }
        size_t current_offset = 0, array_pos = 0;
        for (size_t i = 0; i < array_col->size(); ++i)
        {
            String res;
            const StringRef delim = const_delim_col ? delim_col->getDataAt(0) : delim_col->getDataAt(i);
            StringRef null_replacement = StringRef(nullptr, 0);
            if (null_replacement_col)
            {
                null_replacement = const_null_replacement_col ? null_replacement_col->getDataAt(0) : null_replacement_col->getDataAt(i);
            }
            size_t array_size = array_offsets[i] - current_offset;
            size_t data_pos = array_pos == 0 ? 0 : string_offsets[array_pos - 1];
            size_t last_not_null_pos = 0;
            for (size_t j = 0; j < array_size; ++j)
            {
                if (array_nested_col && array_nested_col->isNullAt(j + array_pos))
                {
                    if (null_replacement.data)
                    {
                        res += null_replacement.toString();
                        if (j != array_size - 1)
                            res += delim.toString();
                    }
                    else if (j == array_size - 1)
                        res = res.substr(0, last_not_null_pos);
                }
                else
                {
                    const StringRef s(&string_data[data_pos], string_offsets[j + array_pos] - data_pos);
                    res += s.toString();
                    last_not_null_pos = res.size();
                    if (j != array_size - 1)
                        res += delim.toString();
                }
                data_pos = string_offsets[j + array_pos];
            }
            array_pos += array_size;
            res_col->insertData(res.data(), res.length());
            current_offset = array_offsets[i];
        }
        return ColumnNullable::create(std::move(res_col), std::move(null_col));
    }
};

REGISTER_FUNCTION(SparkArrayJoin)
{
    factory.registerFunction<SparkFunctionArrayJoin>();
}
}