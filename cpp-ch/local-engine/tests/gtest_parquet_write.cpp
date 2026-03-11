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
#include "config.h"

#if USE_PARQUET && USE_ARROW
#include <iostream>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <arrow/table.h>
#include <gtest/gtest.h>

using namespace DB;

template <typename Column>
static void printColumns(const std::vector<Column> & columns)
{
    for (size_t col_i = 0; col_i < columns.size(); ++col_i)
        for (size_t row_i = 0; row_i < columns[col_i]->size(); ++row_i)
            std::cout << "col:" << col_i << ",row:" << row_i << "," << toString((*columns[col_i])[row_i]) << std::endl;
}


template <typename Column>
static bool haveEqualColumns(const Column & lhs, const Column & rhs)
{
    if (lhs->size() != rhs->size())
        return false;

    for (size_t row_i = 0; row_i < lhs->size(); ++row_i)
        if ((*lhs)[row_i] != (*rhs)[row_i])
            return false;
    return true;
}

TEST(ParquetWrite, ComplexTypes)
{
    Block header;

    /// map field
    {
        ColumnWithTypeAndName col;
        col.name = "map_field";
        String str_type = "Nullable(Map(Int32, Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// map field with null
    {
        ColumnWithTypeAndName col;
        col.name = "map_field_with_null";
        String str_type = "Nullable(Map(Int32, Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// map field with empty map
    {
        ColumnWithTypeAndName col;
        col.name = "map_field_with_empty_map";
        String str_type = "Nullable(Map(Int32, Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// array field
    {
        ColumnWithTypeAndName col;
        col.name = "array_field";
        String str_type = "Nullable(Array(Nullable(Int32)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// array field with null
    {
        ColumnWithTypeAndName col;
        col.name = "array_field_with_null";
        String str_type = "Nullable(Array(Nullable(Int32)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// array field with empty array
    {
        ColumnWithTypeAndName col;
        col.name = "array_field_with_empty_array";
        String str_type = "Nullable(Array(Nullable(Int32)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// struct_field
    {
        ColumnWithTypeAndName col;
        col.name = "struct_field";
        String str_type = "Nullable(Tuple(Nullable(Int32), Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// struct field with null
    {
        ColumnWithTypeAndName col;
        col.name = "struct_field_with_null";
        String str_type = "Nullable(Tuple(Nullable(Int32), Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    auto mutable_columns = header.mutateColumns();

    size_t rows = 10;
    for (size_t i = 0; i < rows; i++)
    {
        /// map_field
        {
            Map map;

            Tuple tuple1{Int32(i), Int64(i + 1)};
            map.emplace_back(std::move(tuple1));

            Tuple tuple2{Int32(i + 2), Int64(i + 3)};
            map.emplace_back(std::move(tuple2));

            mutable_columns[0]->insert(std::move(map));
        }

        /// map_field_with_null
        {
            mutable_columns[1]->insert({});
        }

        /// map_field_with_empty_map
        {
            mutable_columns[2]->insert(Map{});
        }

        /// array_field
        {
            Array array;
            array.emplace_back(Int32(i));
            array.emplace_back(Int32(i + 1));
            mutable_columns[3]->insert(std::move(array));
        }

        /// array_field_with_null
        {
            mutable_columns[4]->insert({});
        }

        /// array_field_with_empty_array
        {
            mutable_columns[5]->insert(Array{});
        }

        /// struct_field
        {
            Tuple tuple;
            tuple.emplace_back(Int32(i));
            tuple.emplace_back(Int64(i + 1));
            mutable_columns[6]->insert(std::move(tuple));
        }

        /// struct_field_with_null
        {
            mutable_columns[7]->insert({});
        }
    }

    /// Convert CH Block to Arrow Table
    std::shared_ptr<arrow::Table> arrow_table;

    FormatSettings format_settings;
    CHColumnToArrowColumn ch2arrow(
        header,
        "Parquet",
        CHColumnToArrowColumn::Settings{
            format_settings.arrow.output_string_as_string,
            format_settings.arrow.output_fixed_string_as_fixed_byte_array,
            format_settings.arrow.low_cardinality_as_dictionary,
            format_settings.arrow.use_signed_indexes_for_dictionary,
            format_settings.arrow.use_64_bit_indexes_for_dictionary});
    Chunk input_chunk{std::move(mutable_columns), rows};
    std::vector<Chunk> input_chunks;
    input_chunks.push_back(std::move(input_chunk));
    ch2arrow.chChunkToArrowTable(arrow_table, input_chunks, header.columns());

    /// Convert Arrow Table to CH Block
    ArrowColumnToCHColumn arrow2ch(
        header, "Parquet", format_settings, std::nullopt, std::nullopt, true, true, FormatSettings::DateTimeOverflowBehavior::Ignore, false);
    Chunk output_chunk = arrow2ch.arrowTableToCHChunk(arrow_table, arrow_table->num_rows(), nullptr, nullptr);

    /// Compare input and output columns
    const auto & input_columns = input_chunks.back().getColumns();
    const auto & output_columns = output_chunk.getColumns();
    for (size_t col_i = 0; col_i < header.columns(); ++col_i)
        EXPECT_TRUE(haveEqualColumns(input_columns[col_i], output_columns[col_i]));
}

#endif
