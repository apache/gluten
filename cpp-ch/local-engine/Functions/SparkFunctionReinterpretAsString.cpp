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
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

}

using namespace DB;
namespace local_engine
{
namespace
{
    /// The difference between reinterpretAsStringSpark and reinterpretAsString is that reinterpretAsStringSpark:
    /// 1. Does not cut trailing zeros
    /// 2. Output reinterpreted bytes in big-endian order for integer type. e.g. input: 0x1234, output: [0x00 0x00 0x12 0x34]
    class FunctionReinterpretAsStringSpark : public IFunction
    {
    public:
        static constexpr auto name = "reinterpretAsStringSpark";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReinterpretAsStringSpark>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            DataTypePtr from_type = arguments[0].type;
            if (!from_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as String because it is not contiguous in memory",
                    from_type->getName());

            DataTypePtr to_type = std::make_shared<DataTypeString>();
            return to_type;
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
        {
            auto is_string_type = isString(arguments[0].type);

            ColumnPtr result;

            const IColumn & src = *arguments[0].column;
            MutableColumnPtr dst = result_type->createColumn();
            ColumnString * dst_concrete = assert_cast<ColumnString *>(dst.get());

            size_t rows = src.size();
            ColumnString::Chars & data_to = dst_concrete->getChars();
            ColumnString::Offsets & offsets_to = dst_concrete->getOffsets();
            offsets_to.resize_exact(rows);

            ColumnString::Offset offset = 0;
            for (size_t i = 0; i < rows; ++i)
            {
                /// Transform little-endian in input to big-endian in output
                /// NOTE: We don't need do the transform for string type
                String data = src.getDataAt(i).toString();
                if (!is_string_type)
                    std::reverse(data.begin(), data.end());

                data_to.resize(offset + data.size());
                memcpy(&data_to[offset], data.data(), data.size());
                offset += data.size();
                offsets_to[i] = offset;
            }

            result = std::move(dst);
            return result;
        }
    };
}

REGISTER_FUNCTION(ReinterpretAsStringSpark)
{
    factory.registerFunction<FunctionReinterpretAsStringSpark>();
}

}
