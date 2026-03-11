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

#if USE_PARQUET

#include <ranges>
#include <incbin.h>
#include <Columns/ColumnNullable.h>
#include <Core/Range.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
#include <Parser/LocalExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/Parquet/ArrowUtils.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/level_conversion.h>
#include <tests/utils/gluten_test_util.h>
#include <Common/BlockTypeUtils.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

using namespace DB;
using namespace local_engine;

void readSchema(const String & path)
{
    auto name_and_types = test::readParquetSchema(test::gtest_data(path.c_str()));
    auto & factory = DataTypeFactory::instance();

    auto check_type = [&name_and_types, &factory](const String & column, const String & expect_str_type)
    {
        auto expect_type = factory.get(expect_str_type);

        auto name_and_type = name_and_types.tryGetByName(column);
        EXPECT_TRUE(name_and_type);

        EXPECT_TRUE(name_and_type->type->equals(*expect_type))
            << "real_type:" << name_and_type->type->getName() << ", expect_type:" << expect_type->getName();
    };

    check_type("f_bool", "Nullable(UInt8)");
    check_type("f_byte", "Nullable(Int8)");
    check_type("f_short", "Nullable(Int16)");
    check_type("f_int", "Nullable(Int32)");
    check_type("f_long", "Nullable(Int64)");
    check_type("f_float", "Nullable(Float32)");
    check_type("f_double", "Nullable(Float64)");
    check_type("f_string", "Nullable(String)");
    check_type("f_binary", "Nullable(String)");
    check_type("f_decimal", "Nullable(Decimal(10, 2))");
    check_type("f_date", "Nullable(Date32)");
    check_type("f_timestamp", "Nullable(DateTime64(9))");
    //    check_type("f_array", "Nullable(Array(Nullable(String)))");
    //    check_type("f_array_array", "Nullable(Array(Nullable(Array(Nullable(String)))))");
    //    check_type("f_array_map", "Nullable(Array(Nullable(Map(String, Nullable(Int64)))))");
    //    check_type("f_array_struct", "Nullable(Array(Nullable(Tuple(a Nullable(String), b Nullable(Int64)))))");
    //    check_type("f_map", "Nullable(Map(String, Nullable(Int64)))");
    //    check_type("f_map_map", "Nullable(Map(String, Nullable(Map(String, Nullable(Int64)))))");
    //    check_type("f_map_array", "Nullable(Map(String, Nullable(Array(Nullable(Int64)))))");
    //    check_type("f_map_struct", "Nullable(Map(String, Nullable(Tuple(a Nullable(String), b Nullable(Int64)))))");
    //    check_type("f_struct", "Nullable(Tuple(a Nullable(String), b Nullable(Int64)))");
    //    check_type(
    //        "f_struct_struct",
    //        "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Tuple(x Nullable(String), y Nullable(Int64)))))");
    //    check_type("f_struct_array", "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Array(Nullable(Int64)))))");
    //    check_type("f_struct_map", "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Map(String, Nullable(Int64)))))");
}


BlockRowType createColumn(const String & full_path, const std::map<String, Field> & fields)
{
    const auto name_and_types = test::readParquetSchema(full_path);
    auto is_selected = [&fields](const auto & name_and_type) { return fields.contains(name_and_type.name); };
    return toBlockRowType(name_and_types, is_selected);
}

template <class InputFormat>
void readData(const String & path, const std::map<String, Field> & fields)
{
    String full_path = test::gtest_data(path.c_str());
    FormatSettings settings;
    auto header = toShared(createColumn(full_path, fields));
    ReadBufferFromFile in(full_path);

    InputFormatPtr format;
    auto parser_group
        = std::make_shared<FormatFilterInfo>(nullptr, QueryContext::globalContext(), nullptr);
    auto parser_shared_resources
        = std::make_shared<FormatParserSharedResources>(QueryContext::globalContext()->getSettingsRef(), /*num_streams_=*/1);
    if constexpr (std::is_same_v<InputFormat, DB::ParquetBlockInputFormat>)
        format = std::make_shared<InputFormat>(in, header, settings, parser_shared_resources, parser_group, 8192);
    else
        format = std::make_shared<InputFormat>(in, header, settings);

    QueryPipeline pipeline(format);
    PullingPipelineExecutor reader(pipeline);

    Block block;
    EXPECT_TRUE(reader.pull(block));
    EXPECT_EQ(block.rows(), 1);

    const auto & columns = header->getColumnsWithTypeAndName();
    for (const auto & column_header : columns)
    {
        const auto & name = column_header.name;
        auto it = fields.find(name);
        if (it != fields.end())
        {
            const auto & column = block.getByName(name);
            auto field = (*column.column)[0];
            auto expect_field = it->second;
            EXPECT_TRUE(field == expect_field) << "field:" << toString(field) << ", expect_field:" << toString(expect_field);
        }
    }
}

TEST(ParquetRead, ReadSchema)
{
    readSchema("alltypes/alltypes_notnull.parquet");
    readSchema("alltypes/alltypes_null.parquet");
}

TEST(ParquetRead, VerifyPageindexReaderSupport)
{
    EXPECT_FALSE(
        ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("alltypes/alltypes_notnull.parquet")))));
    EXPECT_FALSE(
        ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("alltypes/alltypes_null.parquet")))));


    EXPECT_FALSE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("array.parquet")))));
    EXPECT_TRUE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("date.parquet")))));
    EXPECT_TRUE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("datetime64.parquet")))));
    EXPECT_TRUE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("decimal.parquet")))));
    EXPECT_TRUE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("iris.parquet")))));
    EXPECT_FALSE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("map.parquet")))));
    EXPECT_TRUE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("sample.parquet")))));
    EXPECT_FALSE(ParquetFormatFile::onlyHasFlatType(toSampleBlock(test::readParquetSchema(test::gtest_data("struct.parquet")))));
}

TEST(ParquetRead, ReadDataNotNull)
{
    const std::map<String, Field> fields{
        {"f_array", Array{"hello", "world"}},
        {"f_bool", UInt8(1)},
        {"f_byte", Int8(1)},
        {"f_short", Int16(2)},
        {"f_int", Int32(3)},
        {"f_long", Int64(4)},
        {"f_float", Float32(5.5)},
        {"f_double", Float64(6.6)},
        {"f_string", "hello world"},
        {"f_binary", "hello world"},
        {"f_decimal", DecimalField<Decimal64>(777, 2)},
        {"f_date", Int32(18262)},
        {"f_timestamp", DecimalField<DateTime64>(1666162060000000L, 6)},
        {"f_array", Array{"hello", "world"}},
        {
            "f_array_array",
            []() -> Field
            {
                Array res;
                res.push_back(Array{"hello"});
                res.push_back(Array{"world"});
                return std::move(res);
            }(),
        },
        {
            "f_array_map",
            []() -> Field
            {
                Array res;

                Map map;
                map.push_back(Tuple{"hello", Int64(1)});
                res.push_back(map);

                map.clear();
                map.push_back(Tuple{"world", Int64(2)});
                res.push_back(map);

                return std::move(res);
            }(),
        },
        {
            "f_array_struct",
            []() -> Field
            {
                Array res;
                res.push_back(Tuple{"hello", Int64(1)});
                res.push_back(Tuple{"world", Int64(2)});

                return std::move(res);
            }(),
        },
        {
            "f_map",
            []() -> Field
            {
                Map res;
                res.push_back(Tuple{"hello", Int64(1)});
                res.push_back(Tuple{"world", Int64(2)});
                return std::move(res);
            }(),
        },
        {
            "f_map_map",
            []() -> Field
            {
                Map nested_map;
                nested_map.push_back(Tuple{"world", Int64(3)});

                Map res;
                res.push_back(Tuple{"hello", std::move(nested_map)});
                return std::move(res);
            }(),
        },
        {
            "f_map_array",
            []() -> Field
            {
                Array array{Int64(1), Int64(2), Int64(3)};

                Map res;
                res.push_back(Tuple{"hello", std::move(array)});
                return std::move(res);
            }(),
        },
        {
            "f_map_struct",
            []() -> Field
            {
                Tuple tuple{"world", Int64(4)};

                Map res;
                res.push_back(Tuple{"hello", std::move(tuple)});
                return std::move(res);
            }(),
        },
        {
            "f_struct",
            []() -> Field
            {
                Tuple res{"hello world", Int64(5)};
                return std::move(res);
            }(),
        },
        {
            "f_struct_struct",
            []() -> Field
            {
                Tuple tuple{"world", Int64(6)};
                Tuple res{"hello", Int64(6), std::move(tuple)};
                return std::move(res);
            }(),
        },
        {
            "f_struct_array",
            []() -> Field
            {
                Array array{Int64(1), Int64(2), Int64(3)};
                Tuple res{"hello", Int64(7), std::move(array)};
                return std::move(res);
            }(),
        },
        {
            "f_struct_map",
            []() -> Field
            {
                Map map;
                map.push_back(Tuple{"world", Int64(9)});

                Tuple res{"hello", Int64(8), std::move(map)};
                return std::move(res);
            }(),
        },
    };

    const std::map<String, Field> primitive_fields{
        {"f_bool", UInt8(1)},
        {"f_byte", Int8(1)},
        {"f_short", Int16(2)},
        {"f_int", Int32(3)},
        {"f_long", Int64(4)},
        {"f_float", Float32(5.5)},
        {"f_double", Float64(6.6)},
        {"f_string", "hello world"},
        {"f_binary", "hello world"},
        {"f_decimal", DecimalField<Decimal64>(777, 2)},
        {"f_date", Int32(18262)},
        {"f_timestamp", DecimalField<DateTime64>(1666162060000000L, 6)}};

    readData<ParquetBlockInputFormat>("alltypes/alltypes_notnull.parquet", fields);
}


TEST(ParquetRead, ReadDataNull)
{
    GTEST_SKIP();
    std::map<String, Field> fields{
        {"f_array", Null{}},        {"f_bool", Null{}},   {"f_byte", Null{}},          {"f_short", Null{}},
        {"f_int", Null{}},          {"f_long", Null{}},   {"f_float", Null{}},         {"f_double", Null{}},
        {"f_string", Null{}},       {"f_binary", Null{}}, {"f_decimal", Null{}},       {"f_date", Null{}},
        {"f_timestamp", Null{}},    {"f_array", Null{}},  {"f_array_array", Null{}},   {"f_array_map", Null{}},
        {"f_array_struct", Null{}}, {"f_map", Null{}},    {"f_map_map", Null{}},       {"f_map_array", Null{}},
        {"f_map_struct", Null{}},   {"f_struct", Null{}}, {"f_struct_struct", Null{}}, {"f_struct_array", Null{}},
        {"f_struct_map", Null{}},
    };

    readData<ParquetBlockInputFormat>("alltypes/alltypes_null.parquet", fields);
}

TEST(ParquetRead, ArrowRead)
{
    // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
    // 20 rows (10 rows per group). Group offsets are 153 and 614.
    // Data is in plain uncompressed format:
    //   a: [1..20]
    //   b: [1.0..20.0]

    const std::string sample(test::gtest_data("sample.parquet"));
    ReadBufferFromFile in(sample);
    const FormatSettings format_settings{};
    auto arrow_file = test::asArrowFileForParquet(in, format_settings);

    // std::shared_ptr<parquet::FileMetaData> metadata = parquet::ReadMetaData(arrow_file);
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(arrow_file, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    EXPECT_EQ(table->num_rows(), 20);
    EXPECT_EQ(table->num_columns(), 2);

    auto columns = toSampleBlock(test::readParquetSchema(sample));

    Block header(columns);
    ArrowColumnToCHColumn converter(
        header,
        "Parquet",
        format_settings,
        std::nullopt,
        std::nullopt,
        format_settings.parquet.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.case_insensitive_column_matching);

    Chunk chunk = converter.arrowTableToCHChunk(table, table->num_rows(), nullptr, nullptr);
    Block res = header.cloneWithColumns(chunk.detachColumns());
    EXPECT_EQ(res.rows(), 20);

    ASSERT_TRUE(res.getByName("a").column != nullptr);
    const auto & col_a = checkAndGetColumn<ColumnNullable>(res.getByName("a").column.get())->getNestedColumn();
    for (size_t i = 0; i < res.rows(); i++)
        EXPECT_EQ(col_a.get64(i), i + 1);

    ASSERT_TRUE(res.getByName("b").column != nullptr);
    const auto & col_b = checkAndGetColumn<ColumnNullable>(res.getByName("b").column.get())->getNestedColumn();
    for (size_t i = 0; i < res.rows(); i++)
        EXPECT_EQ(col_b.getFloat64(i), i + 1);
}

TEST(ParquetRead, LowLevelRead)
{
    const std::string sample(test::gtest_data("sample.parquet"));
    // Create a ParquetReader instance
    const std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(sample, false);

    // Get the File MetaData
    const std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    // Get the number of RowGroups
    const int num_row_groups = file_metadata->num_row_groups();
    EXPECT_EQ(num_row_groups, 2);
    // Get the number of Columns
    const int num_columns = file_metadata->num_columns();
    EXPECT_EQ(num_columns, 2);

    constexpr int col_a = 0;
    const parquet::SchemaDescriptor & schema = *(file_metadata->schema());
    const parquet::ColumnDescriptor & column_a_descr = *(schema.Column(col_a));
    EXPECT_EQ(column_a_descr.name(), "a");
    const parquet::internal::LevelInfo level_info = computeLevelInfo(&column_a_descr);
    const auto reader = parquet::internal::RecordReader::Make(&column_a_descr, level_info);

    // Iterate over all the RowGroups in the file
    for (int r = 0; r < num_row_groups; ++r)
    {
        // Get the RowGroup Reader
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);
        reader->SetPageReader(row_group_reader->GetColumnPageReader(col_a));
        int64_t records_read = reader->ReadRecords(10);
        EXPECT_EQ(records_read, 10);
        const auto read_values = reinterpret_cast<const Int64 *>(reader->values());
        ASSERT_TRUE(read_values != nullptr);
    }
}

TEST(ParquetRead, VectorizedColumnReader)
{
    const std::string sample(test::gtest_data("sample.parquet"));
    const FormatSettings format_settings{};
    Block blockHeader({{DOUBLE(), "b"}, {BIGINT(), "a"}});

    ReadBufferFromFile in(sample);

    ParquetMetaBuilder metaBuilder{.collectPageIndex = true};
    metaBuilder.build(in, blockHeader);
    ColumnIndexRowRangesProvider provider{metaBuilder};
    VectorizedParquetRecordReader recordReader(blockHeader, format_settings);

    auto arrow_file = test::asArrowFileForParquet(in, format_settings);
    recordReader.initialize(arrow_file, provider);

    auto chunk{recordReader.nextBatch()};
    ASSERT_EQ(chunk.getNumColumns(), 2);
    ASSERT_EQ(chunk.getNumRows(), 20);

    // const auto & col_a = checkAndGetColumn<ColumnNullable>(*(chunk.getColumns()[1]))->getNestedColumn();

    /// TODO: nullable
    const auto & col_a = *(chunk.getColumns()[1]);
    for (size_t i = 0; i < chunk.getNumRows(); i++)
        EXPECT_EQ(col_a.get64(i), i + 1);

    const auto & col_b = *(chunk.getColumns()[0]);
    for (size_t i = 0; i < chunk.getNumRows(); i++)
        EXPECT_EQ(col_b.getFloat64(i), i + 1);
}

INCBIN(_upper_col_parquet_, SOURCE_DIR "/utils/extern-local-engine/tests/json/upper_col_parquet.json");
TEST(ParquetRead, UpperColRead)
{
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"459","parquet":{},"schema":{},"metadataColumns":[{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"459"},{"key":"input_file_block_start","value":"0"}],"properties":{"fileSize":"459","modificationTime":"1735012863732"}}]})";
    const std::string file{test::gtest_uri("upper_case_col.parquet")};
    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_upper_col_parquet_), split_template, file, {});
    EXPECT_TRUE(local_executor->hasNext());
    const Block & block = *local_executor->nextColumnar();

    debug::headBlock(block);
}
#endif
