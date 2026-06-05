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
#include <vector>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/VarInt.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

namespace local_engine
{

struct SortOrderField
{
    size_t pos = 0;
    Int8 direction = 0;
    Int8 nulls_direction = 0;
};
using SortOrderFields = std::vector<SortOrderField>;

struct RowNumGroupArraySortedData
{
public:
    using Data = DB::Tuple;
    std::vector<Data> values;

    static bool compare(const Data & lhs, const Data & rhs, const SortOrderFields & sort_orders)
    {
        for (const auto & sort_order : sort_orders)
        {
            const auto & pos = sort_order.pos;
            const auto & asc = sort_order.direction;
            const auto & nulls_first = sort_order.nulls_direction;
            bool l_is_null = lhs[pos].isNull();
            bool r_is_null = rhs[pos].isNull();
            if (l_is_null && r_is_null)
                continue;
            else if (l_is_null)
                return nulls_first;
            else if (r_is_null)
                return !nulls_first;
            else if (lhs[pos] < rhs[pos])
                return asc;
            else if (lhs[pos] > rhs[pos])
                return !asc;
        }
        return false;
    }

    ALWAYS_INLINE void heapReplaceTop(const SortOrderFields & sort_orders)
    {
        size_t size = values.size();
        if (size < 2)
            return;
        size_t child_index = 1;
        if (size > 2 && compare(values[1], values[2], sort_orders))
            ++child_index;

        if (compare(values[child_index], values[0], sort_orders))
            return;

        size_t current_index = 0;
        auto current = values[current_index];
        do
        {
            values[current_index] = values[child_index];
            current_index = child_index;

            child_index = 2 * child_index + 1;

            if (child_index >= size)
                break;

            if ((child_index + 1) < size && compare(values[child_index], values[child_index + 1], sort_orders))
                ++child_index;
        } while (!compare(values[child_index], current, sort_orders));

        values[current_index] = current;
    }

    ALWAYS_INLINE void addElement(const Data && data, const SortOrderFields & sort_orders, size_t max_elements)
    {
        if (values.size() >= max_elements)
        {
            if (!compare(data, values[0], sort_orders))
                return;
            values[0] = data;
            heapReplaceTop(sort_orders);
            return;
        }
        values.emplace_back(std::move(data));
        auto cmp = [&sort_orders](const Data & a, const Data & b) { return compare(a, b, sort_orders); };
        std::push_heap(values.begin(), values.end(), cmp);
    }

    ALWAYS_INLINE void sortAndLimit(size_t max_elements, const SortOrderFields & sort_orders)
    {
        ::sort(values.begin(), values.end(), [&sort_orders](const Data & a, const Data & b) { return compare(a, b, sort_orders); });
        if (values.size() > max_elements)
            values.resize(max_elements);
    }

    ALWAYS_INLINE void insertResultInto(DB::IColumn & to, size_t max_elements, const SortOrderFields & sort_orders)
    {
        auto & result_array = assert_cast<DB::ColumnArray &>(to);
        auto & result_array_offsets = result_array.getOffsets();

        sortAndLimit(max_elements, sort_orders);

        const auto previous_offset = result_array_offsets.empty() ? 0 : result_array_offsets.back();
        result_array_offsets.push_back(previous_offset + values.size());

        if (values.empty())
            return;
        auto & result_array_data = result_array.getData();
        for (int i = 0, sz = static_cast<int>(values.size()); i < sz; ++i)
        {
            auto & value = values[i];
            value.push_back(i + 1);
            result_array_data.insert(value);
        }
    }
};

static DB::DataTypePtr getRowNumReultDataType(DB::DataTypePtr data_type)
{
    const auto * tuple_type = typeid_cast<const DB::DataTypeTuple *>(data_type.get());
    if (!tuple_type)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Tuple type is expected, but got: {}", data_type->getName());
    DB::DataTypes element_types = tuple_type->getElements();
    std::vector<String> element_names = tuple_type->getElementNames();
    element_types.push_back(std::make_shared<DB::DataTypeInt32>());
    element_names.push_back("row_num");
    auto nested_tuple_type = std::make_shared<DB::DataTypeTuple>(element_types, element_names);
    return std::make_shared<DB::DataTypeArray>(nested_tuple_type);
}

// usage: rowNumGroupArraySorted(1, "a asc nulls first, b desc nulls last")(tuple(a,b))
class RowNumGroupArraySorted final : public DB::IAggregateFunctionDataHelper<RowNumGroupArraySortedData, RowNumGroupArraySorted>
{
public:
    explicit RowNumGroupArraySorted(DB::DataTypePtr data_type, const DB::Array & parameters_)
        : DB::IAggregateFunctionDataHelper<RowNumGroupArraySortedData, RowNumGroupArraySorted>(
              {data_type}, parameters_, getRowNumReultDataType(data_type))
    {
        if (parameters_.size() != 2)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{} needs two parameters: limit and order clause", getName());
        const auto * tuple_type = typeid_cast<const DB::DataTypeTuple *>(data_type.get());
        if (!tuple_type)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Tuple type is expected, but got: {}", data_type->getName());

        limit = parameters_[0].safeGet<UInt64>();

        String order_by_clause = parameters_[1].safeGet<String>();
        sort_order_fields = parseSortOrderFields(order_by_clause);

        serialization = data_type->getDefaultSerialization();
    }

    String getName() const override { return "rowNumGroupArraySorted"; }

    void add(DB::AggregateDataPtr __restrict place, const DB::IColumn ** columns, size_t row_num, DB::Arena * /*arena*/) const override
    {
        auto & data = this->data(place);
        DB::Tuple data_tuple = (*columns[0])[row_num].safeGet<DB::Tuple>();
        this->data(place).addElement(std::move(data_tuple), sort_order_fields, limit);
    }

    void merge(DB::AggregateDataPtr __restrict place, DB::ConstAggregateDataPtr rhs, DB::Arena * /*arena*/) const override
    {
        auto & rhs_values = this->data(rhs).values;
        for (auto & rhs_element : rhs_values)
            this->data(place).addElement(std::move(rhs_element), sort_order_fields, limit);
    }

    void serialize(DB::ConstAggregateDataPtr __restrict place, DB::WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & values = this->data(place).values;
        size_t size = values.size();
        DB::writeVarUInt(size, buf);

        for (const auto & value : values)
            serialization->serializeBinary(value, buf, {});
    }

    void deserialize(
        DB::AggregateDataPtr __restrict place, DB::ReadBuffer & buf, std::optional<size_t> /* version */, DB::Arena *) const override
    {
        size_t size = 0;
        DB::readVarUInt(size, buf);

        auto & values = this->data(place).values;
        values.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            DB::Field data;
            serialization->deserializeBinary(data, buf, {});
            values.emplace_back(data.safeGet<DB::Tuple>());
        }
    }

    void insertResultInto(DB::AggregateDataPtr __restrict place, DB::IColumn & to, DB::Arena * /*arena*/) const override
    {
        this->data(place).insertResultInto(to, limit, sort_order_fields);
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    size_t limit = 0;
    SortOrderFields sort_order_fields;
    DB::SerializationPtr serialization;

    SortOrderFields parseSortOrderFields(const String & order_by_clause) const
    {
        DB::ParserOrderByExpressionList order_by_parser;
        auto order_by_ast = DB::parseQuery(order_by_parser, order_by_clause, 1000, 1000, 1000);
        SortOrderFields fields;
        const auto expression_list_ast = assert_cast<const DB::ASTExpressionList *>(order_by_ast.get());
        const auto & tuple_element_names = assert_cast<const DB::DataTypeTuple *>(argument_types[0].get())->getElementNames();
        for (const auto & child : expression_list_ast->children)
        {
            const auto * order_by_element_ast = assert_cast<const DB::ASTOrderByElement *>(child.get());
            const auto * ident_ast = assert_cast<const DB::ASTIdentifier *>(order_by_element_ast->children[0].get());
            const auto & ident_name = ident_ast->shortName();


            SortOrderField field;
            field.direction = order_by_element_ast->direction == 1;
            field.nulls_direction
                = field.direction ? order_by_element_ast->nulls_direction == -1 : order_by_element_ast->nulls_direction == 1;

            auto name_pos = std::find(tuple_element_names.begin(), tuple_element_names.end(), ident_name);
            if (name_pos == tuple_element_names.end())
            {
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS, "Not found column {} in tuple {}", ident_name, argument_types[0]->getName());
            }
            field.pos = std::distance(tuple_element_names.begin(), name_pos);
            fields.push_back(field);
        }
        return fields;
    }
};


DB::AggregateFunctionPtr createAggregateFunctionRowNumGroupArray(
    const std::string & name, const DB::DataTypes & argument_types, const DB::Array & parameters, const DB::Settings *)
{
    if (argument_types.size() != 1 || !typeid_cast<const DB::DataTypeTuple *>(argument_types[0].get()))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, " {} Nees only one tuple argument", name);
    return std::make_shared<RowNumGroupArraySorted>(argument_types[0], parameters);
}

void registerAggregateFunctionRowNumGroup(DB::AggregateFunctionFactory & factory)
{
    DB::AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = false};

    factory.registerFunction("rowNumGroupArraySorted", {createAggregateFunctionRowNumGroupArray, properties});
}
}
