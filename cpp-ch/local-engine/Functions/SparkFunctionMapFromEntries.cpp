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
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <utility>
#include <unordered_map>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <bool last_win>
class SparkFunctionMapFromEntries : public IFunction
{
public:
    static constexpr auto name = last_win ? "sparkMapFromEntriesLastWin" : "sparkMapFromEntries";

    static FunctionPtr create(ContextPtr) { return std::make_shared<SparkFunctionMapFromEntries>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[0]).get());
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function {} must be Array, but it has type {}",
                getName(),
                arguments[0]->getName());

        const auto & entry_type = array_type->getNestedType();
        const auto entry_type_without_nullable = removeNullable(entry_type);
        if (isNothing(entry_type_without_nullable))
        {
            auto map_type = std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeNothing>(),
                std::make_shared<DataTypeNothing>());
            if (arguments[0]->isNullable() || entry_type->isNullable())
                return makeNullable(map_type);
            return map_type;
        }

        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(entry_type_without_nullable.get());
        if (!tuple_type || tuple_type->getElements().size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function {} must be Array of pair Tuple, but it has nested type {}",
                getName(),
                entry_type->getName());

        const auto & elements = tuple_type->getElements();
        auto map_type = std::make_shared<DataTypeMap>(removeNullableOrLowCardinalityNullable(elements[0]), elements[1]);
        if (arguments[0]->isNullable() || entry_type->isNullable())
            return makeNullable(map_type);
        return map_type;
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const override
    {
        ColumnPtr holder = arguments[0].column->convertToFullColumnIfConst();

        const PaddedPODArray<UInt8> * input_null_map = nullptr;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(holder.get()))
        {
            input_null_map = &nullable->getNullMapData();
            holder = nullable->getNestedColumnPtr();
        }

        const auto * entries_array = checkAndGetColumn<ColumnArray>(holder.get());
        if (!entries_array)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Argument column for function {} must be Array, but it is {}",
                getName(),
                holder->getName());

        const auto & entries_offsets = entries_array->getOffsets();
        const IColumn * entries_data = &entries_array->getData();
        const PaddedPODArray<UInt8> * entry_null_map = nullptr;
        if (const auto * nullable_entries = checkAndGetColumn<ColumnNullable>(entries_data))
        {
            entry_null_map = &nullable_entries->getNullMapData();
            entries_data = &nullable_entries->getNestedColumn();
        }

        const auto & result_map_type = assert_cast<const DataTypeMap &>(*removeNullable(result_type));
        if (isNothing(entries_data->getDataType()))
        {
            auto result_key_column = result_map_type.getKeyType()->createColumn();
            auto result_value_column = result_map_type.getValueType()->createColumn();
            auto result_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count, 0);

            ColumnUInt8::MutablePtr result_null_map;
            PaddedPODArray<UInt8> * result_null_map_data = nullptr;
            if (result_type->isNullable())
            {
                result_null_map = ColumnUInt8::create(input_rows_count, 0);
                result_null_map_data = &result_null_map->getData();
            }

            size_t previous_entry_offset = 0;
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const auto current_entry_offset = entries_offsets[row];
                const bool input_map_null = input_null_map && (*input_null_map)[row];
                bool entry_map_null = false;
                if (!input_map_null && entry_null_map)
                {
                    for (size_t entry = previous_entry_offset; entry < current_entry_offset; ++entry)
                    {
                        if ((*entry_null_map)[entry])
                        {
                            entry_map_null = true;
                            break;
                        }
                    }
                }
                if (result_null_map_data)
                    (*result_null_map_data)[row] = input_map_null || entry_map_null;
                previous_entry_offset = current_entry_offset;
            }

            auto nested_column = ColumnArray::create(
                ColumnTuple::create(
                    Columns{std::move(result_key_column), std::move(result_value_column)}),
                std::move(result_offsets_column));
            auto result_column = ColumnMap::create(std::move(nested_column));
            if (result_type->isNullable())
                return ColumnNullable::create(std::move(result_column), std::move(result_null_map));
            return result_column;
        }

        const auto * entries_tuple = checkAndGetColumn<ColumnTuple>(entries_data);
        if (!entries_tuple || entries_tuple->tupleSize() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Nested column for function {} must be Tuple with 2 elements, but it is {}",
                getName(),
                entries_data->getName());

        const auto & key_column = entries_tuple->getColumn(0);
        const auto & value_column = entries_tuple->getColumn(1);
        ColumnPtr key_insert_holder;
        const IColumn * key_insert_column = &key_column;
        if (const auto * nullable_key_column = checkAndGetColumn<ColumnNullable>(&key_column))
            key_insert_column = &nullable_key_column->getNestedColumn();
        else if (const auto * low_cardinality_key_column = checkAndGetColumn<ColumnLowCardinality>(&key_column);
                 low_cardinality_key_column && low_cardinality_key_column->nestedIsNullable())
        {
            key_insert_holder = low_cardinality_key_column->cloneWithDefaultOnNull();
            key_insert_column = key_insert_holder.get();
        }

        auto result_key_column = result_map_type.getKeyType()->createColumn();
        auto result_value_column = result_map_type.getValueType()->createColumn();
        auto result_offsets_column = ColumnArray::ColumnOffsets::create();
        auto & result_offsets = result_offsets_column->getData();
        result_offsets.reserve(input_rows_count);

        ColumnUInt8::MutablePtr result_null_map;
        PaddedPODArray<UInt8> * result_null_map_data = nullptr;
        if (result_type->isNullable())
        {
            result_null_map = ColumnUInt8::create(input_rows_count, 0);
            result_null_map_data = &result_null_map->getData();
        }

        size_t previous_entry_offset = 0;
        size_t result_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto current_entry_offset = entries_offsets[row];

            if (input_null_map && (*input_null_map)[row])
            {
                appendNullMap(result_null_map_data, row, result_offsets, result_offset);
                previous_entry_offset = current_entry_offset;
                continue;
            }

            if (hasNullEntry(entry_null_map, previous_entry_offset, current_entry_offset))
            {
                appendNullMap(result_null_map_data, row, result_offsets, result_offset);
                previous_entry_offset = current_entry_offset;
                continue;
            }

            auto selected_entries =
                selectEntriesForRow(key_column, previous_entry_offset, current_entry_offset);
            appendSelectedEntries(
                selected_entries,
                *key_insert_column,
                value_column,
                *result_key_column,
                *result_value_column,
                result_offset);

            result_offsets.push_back(result_offset);
            previous_entry_offset = current_entry_offset;
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(result_key_column), std::move(result_value_column)}),
            std::move(result_offsets_column));
        auto result_column = ColumnMap::create(std::move(nested_column));
        if (result_type->isNullable())
            return ColumnNullable::create(std::move(result_column), std::move(result_null_map));
        return result_column;
    }

private:
    struct UInt128Hash
    {
        size_t operator()(const UInt128 & value) const
        {
            return std::hash<UInt64>{}(value.items[0])
                ^ (std::hash<UInt64>{}(value.items[1]) << 1);
        }
    };

    using SelectedEntry = std::pair<size_t, size_t>;
    using SelectedEntries = std::vector<SelectedEntry>;

    static bool hasNullEntry(
        const PaddedPODArray<UInt8> * entry_null_map,
        size_t previous_entry_offset,
        size_t current_entry_offset)
    {
        if (!entry_null_map)
            return false;

        for (size_t entry = previous_entry_offset; entry < current_entry_offset; ++entry)
        {
            if ((*entry_null_map)[entry])
                return true;
        }
        return false;
    }

    static void appendNullMap(
        PaddedPODArray<UInt8> * result_null_map_data,
        size_t row,
        ColumnArray::Offsets & result_offsets,
        size_t result_offset)
    {
        if (result_null_map_data)
            (*result_null_map_data)[row] = 1;
        result_offsets.push_back(result_offset);
    }

    static SelectedEntries selectEntriesForRow(
        const IColumn & key_column,
        size_t previous_entry_offset,
        size_t current_entry_offset)
    {
        SelectedEntries selected_entries;
        selected_entries.reserve(current_entry_offset - previous_entry_offset);

        std::unordered_map<UInt128, size_t, UInt128Hash> first_selected_index_by_hash;
        first_selected_index_by_hash.reserve(current_entry_offset - previous_entry_offset);
        std::unordered_map<UInt128, std::vector<size_t>, UInt128Hash>
            collision_selected_indices_by_hash;

        for (size_t entry = previous_entry_offset; entry < current_entry_offset; ++entry)
        {
            if (key_column.isNullAt(entry))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot use NULL as map key in function {}", name);

            SipHash hash_function;
            key_column.updateHashWithValue(entry, hash_function);
            const UInt128 hash = hash_function.get128();

            bool has_duplicate_key = false;
            size_t duplicate_selected_index = 0;
            const auto first_selected_index_it = first_selected_index_by_hash.find(hash);
            if (first_selected_index_it != first_selected_index_by_hash.end())
            {
                const auto first_selected_index = first_selected_index_it->second;
                if (key_column.compareAt(
                        entry,
                        selected_entries[first_selected_index].first,
                        key_column,
                        1) == 0)
                {
                    has_duplicate_key = true;
                    duplicate_selected_index = first_selected_index;
                }
                else
                {
                    const auto collision_selected_indices_it =
                        collision_selected_indices_by_hash.find(hash);
                    if (collision_selected_indices_it != collision_selected_indices_by_hash.end())
                    {
                        for (const auto selected_index : collision_selected_indices_it->second)
                        {
                            if (key_column.compareAt(
                                    entry,
                                    selected_entries[selected_index].first,
                                    key_column,
                                    1) == 0)
                            {
                                has_duplicate_key = true;
                                duplicate_selected_index = selected_index;
                                break;
                            }
                        }
                    }
                }
            }

            if (has_duplicate_key)
            {
                if constexpr (last_win)
                {
                    selected_entries[duplicate_selected_index].second = entry;
                    continue;
                }
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Duplicate map key is found in function {}",
                    name);
            }

            if (first_selected_index_it == first_selected_index_by_hash.end())
            {
                first_selected_index_by_hash.emplace(hash, selected_entries.size());
            }
            else
            {
                collision_selected_indices_by_hash[hash].push_back(selected_entries.size());
            }
            selected_entries.emplace_back(entry, entry);
        }

        return selected_entries;
    }

    static void appendSelectedEntries(
        const SelectedEntries & selected_entries,
        const IColumn & key_insert_column,
        const IColumn & value_column,
        IColumn & result_key_column,
        IColumn & result_value_column,
        size_t & result_offset)
    {
        for (const auto & selected_entry : selected_entries)
        {
            result_key_column.insertFrom(key_insert_column, selected_entry.first);
            result_value_column.insertFrom(value_column, selected_entry.second);
            ++result_offset;
        }
    }
};

REGISTER_FUNCTION(SparkMapFromEntries)
{
    factory.registerFunction<SparkFunctionMapFromEntries<false>>();
    factory.registerFunction<SparkFunctionMapFromEntries<true>>();
}

}
