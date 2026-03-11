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

#include <base/TypeList.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/castTypeToEither.h>
#include <Functions/FunctionsConversion.h>

using namespace DB;

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

class SparkFunctionCastFloatToString : public IFunction
{
public:
    static constexpr auto name = "sparkCastFloatToString";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionCastFloatToString>(); }

    SparkFunctionCastFloatToString() = default;
    ~SparkFunctionCastFloatToString() override = default;

    size_t getNumberOfArguments() const override { return 1; }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<const DataTypeString>();
    }

    template <typename F>
    requires is_floating_point<F>
    inline void writeFloatEnd(F x, WriteBuffer & buf) const
    {
        if constexpr (std::is_same_v<F, Float64>)
        {
            if (DecomposedFloat64(x).isIntegerInRepresentableRange())
            {
                writeChar('.', buf);
                writeChar('0', buf);
            }
        }
        else if constexpr (std::is_same_v<F, Float32>)
        {
            if (DecomposedFloat32(x).isIntegerInRepresentableRange())
            {
                writeChar('.', buf);
                writeChar('0', buf);
            } 
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 1", name);

        if (!isFloat(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument must be float type", name);

        auto res_col = ColumnString::create();
        ColumnString::Chars & res_data = res_col->getChars();
        ColumnString::Offsets & res_offsets = res_col->getOffsets();
        size_t size = arguments[0].column->size();
        res_data.resize_exact(size * 3);
        res_offsets.resize_exact(size);
        using Types = TypeList<DataTypeFloat32, DataTypeFloat64>;
        castTypeToEither(Types{}, arguments[0].type.get(), [&](const auto & arg_type)
        {
            using F = typename std::decay_t<decltype(arg_type)>::FieldType;
            const ColumnVector<F> * src_col = checkAndGetColumn<ColumnVector<F>>(arguments[0].column.get());
            WriteBufferFromVector<ColumnString::Chars> write_buffer(res_data);

            for (size_t i = 0 ; i < src_col->size(); ++i)
            {
                writeFloatText(src_col->getElement(i), write_buffer);
                writeFloatEnd<F>(src_col->getElement(i), write_buffer);
                res_offsets[i] = write_buffer.count();
            }
            return true;
        });
        return std::move(res_col);
    }
};

REGISTER_FUNCTION(SparkFunctionCastFloatToString)
{
    factory.registerFunction<SparkFunctionCastFloatToString>();
}

}
