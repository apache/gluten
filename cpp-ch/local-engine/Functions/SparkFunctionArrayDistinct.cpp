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
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/HashTable/ClearableHashSet.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Columns/IColumn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// copied from src/Functions/array/arrayDistinct.cpp
/// but have little differences in implementation detail:
/// We shoud remain the first null value in the array
/// e.g. select arrayDistinct([1, null, 2, null, 3, 3]) should return [1, null, 2, 3]
class FunctionArrayDistinctSpark : public IFunction
{
public:
    static constexpr auto name = "arrayDistinctSpark";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionArrayDistinctSpark>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be array but it  has type {}.",
                getName(), arguments[0]->getName());
        /// difference: we can return Array(Nullable()) type
        return std::make_shared<DataTypeArray>(array_type->getNestedType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    template <typename T>
    static bool executeNumber(
        const IColumn & src_data,
        const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        ColumnArray::Offsets & res_offsets,
        const ColumnNullable * nullable_col);

    static bool executeString(
        const IColumn & src_data,
        const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        ColumnArray::Offsets & res_offsets,
        const ColumnNullable * nullable_col);

    static void executeHashed(
        const IColumn & src_data,
        const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        ColumnArray::Offsets & res_offsets,
        const ColumnNullable * nullable_col);
};


ColumnPtr FunctionArrayDistinctSpark::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    ColumnPtr array_ptr = arguments[0].column;
    const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

    const auto & return_type = result_type;

    auto res_ptr = return_type->createColumn();
    ColumnArray & res = assert_cast<ColumnArray &>(*res_ptr);

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & offsets = array->getOffsets();

    IColumn & res_data = res.getData();
    ColumnArray::Offsets & res_offsets = res.getOffsets();

    const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(&src_data);

    const IColumn * inner_col;

    if (nullable_col)
    {
        inner_col = &nullable_col->getNestedColumn();
    }
    else
    {
        inner_col = &src_data;
    }

    if (!(executeNumber<UInt8>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<UInt16>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<UInt32>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<UInt64>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int8>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int16>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int32>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int64>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Float32>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Float64>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeString(*inner_col, offsets, res_data, res_offsets, nullable_col)))
        executeHashed(*inner_col, offsets, res_data, res_offsets, nullable_col);

    return res_ptr;
}

template <typename T>
bool FunctionArrayDistinctSpark::executeNumber(
    const IColumn & src_data,
    const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    ColumnArray::Offsets & res_offsets,
    const ColumnNullable * nullable_col)
{
    const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);

    if (!src_data_concrete)
    {
        return false;
    }

    const PaddedPODArray<T> & values = src_data_concrete->getData();

    const PaddedPODArray<UInt8> * src_null_map = nullptr;

    if (nullable_col)
        src_null_map = &nullable_col->getNullMapData();

    using Set = ClearableHashSetWithStackMemory<T, DefaultHash<T>,
        INITIAL_SIZE_DEGREE>;

    Set set;

    ColumnArray::Offset prev_src_offset = 0;
    ColumnArray::Offset res_offset = 0;

    for (auto curr_src_offset : src_offsets)
    {
        set.clear();
        bool has_null = false;

        for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j)
        {
            /// difference: we should remain the first null value
            if (nullable_col && (*src_null_map)[j])
            {
                if (has_null)
                    continue;
                res_data_col.insertDefault();
                has_null = true;
                continue;
            }

            if (!set.find(values[j]))
            {
                res_data_col.insert(values[j]);
                set.insert(values[j]);
            }
        }

        res_offset += set.size() + has_null;
        res_offsets.emplace_back(res_offset);

        prev_src_offset = curr_src_offset;
    }
    return true;
}

bool FunctionArrayDistinctSpark::executeString(
    const IColumn & src_data,
    const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    ColumnArray::Offsets & res_offsets,
    const ColumnNullable * nullable_col)
{
    const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data);

    if (!src_data_concrete)
        return false;

    using Set = ClearableHashSetWithStackMemory<std::string_view, StringViewHash,
        INITIAL_SIZE_DEGREE>;

    const PaddedPODArray<UInt8> * src_null_map = nullptr;

    if (nullable_col)
        src_null_map = &nullable_col->getNullMapData();

    Set set;

    ColumnArray::Offset prev_src_offset = 0;
    ColumnArray::Offset res_offset = 0;

    for (auto curr_src_offset : src_offsets)
    {
        set.clear();
        bool has_null = false;

        for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j)
        {
            /// difference: we should remain the first null value
            if (nullable_col && (*src_null_map)[j])
            {
                if (has_null)
                    continue;
                res_data_col.insertDefault();
                has_null = true;
                continue;
            }

            std::string_view str_ref = src_data_concrete->getDataAt(j);

            if (!set.find(str_ref))
            {
                set.insert(str_ref);
                res_data_col.insertData(str_ref.data(), str_ref.size());
            }
        }

        res_offset += set.size() + has_null;
        res_offsets.emplace_back(res_offset);

        prev_src_offset = curr_src_offset;
    }
    return true;
}

void FunctionArrayDistinctSpark::executeHashed(
    const IColumn & src_data,
    const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    ColumnArray::Offsets & res_offsets,
    const ColumnNullable * nullable_col)
{
    using Set = ClearableHashSetWithStackMemory<UInt128, UInt128TrivialHash,
        INITIAL_SIZE_DEGREE>;

    const PaddedPODArray<UInt8> * src_null_map = nullptr;

    if (nullable_col)
        src_null_map = &nullable_col->getNullMapData();

    Set set;

    ColumnArray::Offset prev_src_offset = 0;
    ColumnArray::Offset res_offset = 0;

    for (auto curr_src_offset : src_offsets)
    {
        set.clear();
        bool has_null = false;

        for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j)
        {
            /// difference: we should remain the first null value
            if (nullable_col && (*src_null_map)[j])
            {
                if (has_null)
                    continue;
                res_data_col.insertDefault();
                has_null = true;
                continue;
            }

            SipHash hash_function;
            src_data.updateHashWithValue(j, hash_function);
            UInt128 hash = hash_function.get128();

            if (!set.find(hash))
            {
                set.insert(hash);
                if (nullable_col)
                    res_data_col.insertFrom(*nullable_col, j);
                else
                    res_data_col.insertFrom(src_data, j);
            }
        }

        res_offset += set.size() + has_null;
        res_offsets.emplace_back(res_offset);

        prev_src_offset = curr_src_offset;
    }
}


REGISTER_FUNCTION(ArrayDistinctSpark)
{
    factory.registerFunction<FunctionArrayDistinctSpark>();
}

}
