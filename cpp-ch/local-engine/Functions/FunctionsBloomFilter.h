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

#include <cstddef>
#include <cstring>
#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBloomFilter.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/castColumn.h>
#include <base/types.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
class FunctionBloomFilterContains : public DB::IFunction
{
public:
    static constexpr auto name = "bloomFilterContains";

    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<FunctionBloomFilterContains>(); }

    ~FunctionBloomFilterContains() override
    {
        if (allocated_bytes_for_bloom_filter_state != nullptr)
        {
            agg_func->destroy(allocated_bytes_for_bloom_filter_state);
            delete[] allocated_bytes_for_bloom_filter_state;
        }
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        const auto * bloom_filter_type0 = typeid_cast<const DB::DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bloom_filter_type0 && bloom_filter_type0->getFunctionName() == "groupBloomFilter"))
        {
            if (arguments[0]->getTypeId() != DB::TypeIndex::String && arguments[0]->getTypeId() != DB::TypeIndex::AggregateFunction)
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be a groupBloomFilterState or its binary form, but it has type {}",
                    getName(),
                    arguments[0]->getName());
        }

        DB::WhichDataType which(arguments[1].get());
        if (!which.isInt64() && !which.isUInt64())
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be an INT64 or UINT64 type but it has type {}",
                getName(),
                arguments[1]->getName());

        return std::make_shared<DB::DataTypeNumber<UInt8>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = DB::ColumnVector<UInt8>::create(input_rows_count);
        typename DB::ColumnVector<UInt8>::Container & vec_to = col_to->getData();
        execute(arguments, input_rows_count, vec_to);

        return col_to;
    }

private:
    // For Gluten use.
    mutable char * allocated_bytes_for_bloom_filter_state = nullptr;
    // For Gluten use.
    // Why not make it a static member? Because functions are registered prior to aggregate functions (groupBloomFilter), so static initialization of static agg_func will fail.
    mutable DB::AggregateFunctionPtr agg_func;

    template <typename T>
    std::enable_if_t<std::is_same_v<T, Int64> || std::is_same_v<T, UInt64>, void> internalExecute(
        const DB::ColumnsWithTypeAndName & arguments,
        size_t input_rows_count,
        typename DB::ColumnVector<UInt8>::Container & vec_to,
        DB::AggregateDataPtr bloom_filter_state) const
    {
        using ColumnType = DB::ColumnVector<T>;
        const typename ColumnType::Container * container_of_int;
        const auto * column_ptr = arguments[1].column.get();
        auto second_arg_const = isColumnConst(*column_ptr);

        AggregateFunctionGroupBloomFilterData & bloom_filter_data_0
                = *reinterpret_cast<AggregateFunctionGroupBloomFilterData *>(bloom_filter_state);
        if (second_arg_const)
        {
            vec_to[0] = bloom_filter_data_0.bloom_filter.find(typeid_cast<const DB::ColumnConst &>(*column_ptr).getDataAt(0).data(), sizeof(T));
            // copy to all rows, better use constant column
            std::memcpy(&vec_to[1], &vec_to[0], (input_rows_count - 1) * sizeof(UInt8));

            return;
        }

        container_of_int = &typeid_cast<const ColumnType &>(*column_ptr).getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const T v = (*container_of_int)[i];
            vec_to[i] = bloom_filter_data_0.bloom_filter.find(reinterpret_cast<const char *>(&v), sizeof(T));
        }
    }

    void execute(const DB::ColumnsWithTypeAndName & arguments, size_t input_rows_count, typename DB::ColumnVector<UInt8>::Container & vec_to) const
    {
        DB::AggregateDataPtr bloom_filter_state = nullptr;

        const auto * first_column_ptr = arguments[0].column.get();

        if (arguments[0].type->getTypeId() == DB::TypeIndex::AggregateFunction)
        {
            const auto & column_agg_function = typeid_cast<const DB::ColumnAggregateFunction &>(*first_column_ptr);
            // When argument is nullable, AggregateFunctionNull is inserted in front of AggregateFunctionState.
            bool has_null_prefix = column_agg_function.getAggregateFunction()->getArgumentTypes().at(0)->isNullable();
            bloom_filter_state
                = column_agg_function.getData()[0] + (has_null_prefix ? column_agg_function.getAggregateFunction()->alignOfData() : 0);
        }
        else if (arguments[0].type->getTypeId() == DB::TypeIndex::String)
        {
            if (!agg_func)
            {
                DB::AggregateFunctionProperties properties;
                auto action = DB::NullsAction::EMPTY;
                agg_func = DB::AggregateFunctionFactory::instance().get(
                    "groupBloomFilter", action, DB::DataTypes{std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeInt64>())}, {}, properties);
            }
            // Gluten serialized the AggregateFunction into a String.
            if (allocated_bytes_for_bloom_filter_state == nullptr)
            {
                if (isColumnConst(*first_column_ptr))
                    first_column_ptr = &typeid_cast<const DB::ColumnConst &>(*first_column_ptr).getDataColumn();
                std::string_view sr = typeid_cast<const DB::ColumnString &>(*first_column_ptr).getDataAt(0);

                size_t size_of_state = agg_func->sizeOfData();
                allocated_bytes_for_bloom_filter_state = new char[size_of_state];
                agg_func->create(allocated_bytes_for_bloom_filter_state);
                if (!sr.empty())
                {
                    DB::ReadBufferFromMemory read_buffer(sr.data(), sr.size());
                    agg_func->deserialize((allocated_bytes_for_bloom_filter_state), read_buffer);
                }
            }

            // In Gluten , argument of groupBloomFilter is always nullable, so always add prefix.
            bloom_filter_state = allocated_bytes_for_bloom_filter_state + agg_func->alignOfData();
        }
        else
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a groupBloomFilterState or its binary form, but it has type {}",
                getName(),
                arguments[0].type->getName());
        }

        const DB::IColumn * second_column_ptr = arguments[1].column.get();
        if (isColumnNullable(*second_column_ptr))
            second_column_ptr = &typeid_cast<const DB::ColumnNullable &>(*second_column_ptr).getNestedColumn();
        if (isColumnConst(*second_column_ptr))
            second_column_ptr = &typeid_cast<const DB::ColumnConst &>(*second_column_ptr).getDataColumn();

        if (checkColumn<DB::ColumnInt64>(second_column_ptr))
        {
            internalExecute<Int64>(arguments, input_rows_count, vec_to, bloom_filter_state);
        }
        else if (checkColumn<DB::ColumnUInt64>(second_column_ptr))
        {
            internalExecute<UInt64>(arguments, input_rows_count, vec_to, bloom_filter_state);
        }
        else
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be an INT64 or UINT64 type but it has type {}",
                getName(),
                arguments[1].type->getName());
        }
    }
};
}
