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
#include <string>
#include <vector>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <base/types.h>
#include <benchmark/benchmark.h>
#include <Common/BlockTypeUtils.h>
#include <Common/QueryContext.h>

using namespace DB;
using namespace local_engine;

struct NameType
{
    String name;
    String type;
};

using NameTypes = std::vector<NameType>;

static Block getLineitemHeader(const NameTypes & name_types)
{
    auto & factory = DataTypeFactory::instance();
    ColumnsWithTypeAndName columns(name_types.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns[i].name = name_types[i].name;
        columns[i].type = factory.get(name_types[i].type);
    }
    return std::move(Block(columns));
}

static void readParquetFile(const SharedHeader & header, const String & file, Block & block)
{
    auto in = std::make_unique<ReadBufferFromFile>(file);
    FormatSettings format_settings;
    auto global_context = QueryContext::globalContext();
    auto parser_group = std::make_shared<FormatFilterInfo>(nullptr, global_context, nullptr);
    auto parser_shared_resources
        = std::make_shared<FormatParserSharedResources>(global_context->getSettingsRef(), /*num_streams_=*/1);
    auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, format_settings, parser_shared_resources, std::move(parser_group), 8192);
    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
    while (reader->pull(block))
        return;
}

static void BM_CHColumnToSparkRow_Lineitem(benchmark::State & state)
{
    const NameTypes name_types = {
        {"l_orderkey", "Nullable(Int64)"},
        {"l_partkey", "Nullable(Int64)"},
        {"l_suppkey", "Nullable(Int64)"},
        {"l_linenumber", "Nullable(Int64)"},
        {"l_quantity", "Nullable(Float64)"},
        {"l_extendedprice", "Nullable(Float64)"},
        {"l_discount", "Nullable(Float64)"},
        {"l_tax", "Nullable(Float64)"},
        {"l_returnflag", "Nullable(String)"},
        {"l_linestatus", "Nullable(String)"},
        {"l_shipdate", "Nullable(Date32)"},
        {"l_commitdate", "Nullable(Date32)"},
        {"l_receiptdate", "Nullable(Date32)"},
        {"l_shipinstruct", "Nullable(String)"},
        {"l_shipmode", "Nullable(String)"},
        {"l_comment", "Nullable(String)"},
    };

    auto header = toShared(getLineitemHeader(name_types));
    const String file = "/data1/liyang/cppproject/gluten/gluten-core/src/test/resources/tpch-data/lineitem/"
                        "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    Block block;
    readParquetFile(header, file, block);
    // std::cerr << "read_rows:" << block.rows() << std::endl;
    CHColumnToSparkRow converter;
    for (auto _ : state)
    {
        auto spark_row_info = converter.convertCHColumnToSparkRow(block);
        converter.freeMem(spark_row_info->getBufferAddress(), spark_row_info->getTotalBytes());
    }
}


static void BM_SparkRowToCHColumn_Lineitem(benchmark::State & state)
{
    const NameTypes name_types = {
        {"l_orderkey", "Nullable(Int64)"},
        {"l_partkey", "Nullable(Int64)"},
        {"l_suppkey", "Nullable(Int64)"},
        {"l_linenumber", "Nullable(Int64)"},
        {"l_quantity", "Nullable(Float64)"},
        {"l_extendedprice", "Nullable(Float64)"},
        {"l_discount", "Nullable(Float64)"},
        {"l_tax", "Nullable(Float64)"},
        {"l_returnflag", "Nullable(String)"},
        {"l_linestatus", "Nullable(String)"},
        {"l_shipdate", "Nullable(Date32)"},
        {"l_commitdate", "Nullable(Date32)"},
        {"l_receiptdate", "Nullable(Date32)"},
        {"l_shipinstruct", "Nullable(String)"},
        {"l_shipmode", "Nullable(String)"},
        {"l_comment", "Nullable(String)"},
    };

    auto header = toShared(getLineitemHeader(name_types));
    const String file = "/data1/liyang/cppproject/gluten/gluten-core/src/test/resources/tpch-data/lineitem/"
                        "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    Block in_block;
    readParquetFile(header, file, in_block);

    CHColumnToSparkRow spark_row_converter;
    auto spark_row_info = spark_row_converter.convertCHColumnToSparkRow(in_block);
    for (auto _ : state) [[maybe_unused]]
        auto out_block = SparkRowToCHColumn::convertSparkRowInfoToCHColumn(*spark_row_info, *header);
}

BENCHMARK(BM_CHColumnToSparkRow_Lineitem)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_SparkRowToCHColumn_Lineitem)->Unit(benchmark::kMillisecond)->Iterations(10);
