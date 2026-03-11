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
#include <random>
#include <Core/Block.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/JoinInfo.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Output/NormalFileWriter.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/SubstraitSource/Iceberg/IcebergMetadataColumn.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/substrait_fwd.h>
#include <benchmark/benchmark.h>
#include <substrait/plan.pb.h>
#include <tests/utils/TempFilePath.h>
#include <tests/utils/gluten_test_util.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/BlockTypeUtils.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

namespace
{

void BM_ColumnIndexRead_NoFilter(benchmark::State & state)
{
    using namespace DB;

    std::string file = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    auto header = local_engine::toShared(local_engine::toSampleBlock(local_engine::test::readParquetSchema(file)));
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        local_engine::ParquetMetaBuilder metaBuilder{
            .format_settings = format_settings,
            .collectPageIndex = true,
            .collectSkipRowGroup = false,
            .case_insensitive = format_settings.parquet.case_insensitive_column_matching,
            .allow_missing_columns = format_settings.parquet.allow_missing_columns};
        ReadBufferFromFilePRead fileReader(file);
        metaBuilder.build(fileReader, *header);
        local_engine::ColumnIndexRowRangesProvider provider{metaBuilder};
        auto format = std::make_shared<local_engine ::VectorizedParquetBlockInputFormat>(fileReader, header, provider, format_settings);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_ColumnIndexRead_Old(benchmark::State & state)
{
    using namespace DB;

    std::string file = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    auto header = local_engine::toShared(local_engine::toSampleBlock(local_engine::test::readParquetSchema(file)));
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        ReadBufferFromFilePRead fileReader(file);
        auto global_context = local_engine::QueryContext::globalContext();
        auto parser_group = std::make_shared<FormatFilterInfo>(nullptr, global_context, nullptr);
        auto parser_shared_resources
            = std::make_shared<FormatParserSharedResources>(global_context->getSettingsRef(), /*num_streams_=*/1);
        auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, format_settings, parser_shared_resources, parser_group, 8192);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_ParquetReadDate32(benchmark::State & state)
{
    using namespace DB;
    auto header = local_engine::toShared(Block{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")});
    std::string file{GLUTEN_SOURCE_TPCH_DIR("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        auto in = std::make_unique<ReadBufferFromFile>(file);
        auto global_context = local_engine::QueryContext::globalContext();
        auto parser_group = std::make_shared<FormatFilterInfo>(nullptr, global_context, nullptr);
        auto parser_shared_resources
            = std::make_shared<FormatParserSharedResources>(global_context->getSettingsRef(), /*num_streams_=*/1);
        auto format = std::make_shared<ParquetBlockInputFormat>(fileReader, header, format_settings, parser_shared_resources, parser_group, 8192);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_OptimizedParquetReadString(benchmark::State & state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_returnflag"),
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_linestatus")};
    std::string file{GLUTEN_SOURCE_TPCH_URI("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};
    Block res;

    for (auto _ : state)
    {
        substrait::ReadRel::LocalFiles files;
        substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
        file_item->set_uri_file(file);
        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        file_item->mutable_parquet()->CopyFrom(parquet_format);

        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(
            Pipe(std::make_shared<local_engine::SubstraitFileSource>(local_engine::QueryContext::globalContext(), header, files)));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto reader = PullingPipelineExecutor(pipeline);
        while (reader.pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_OptimizedParquetReadDate32(benchmark::State & state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")};
    std::string file{GLUTEN_SOURCE_TPCH_URI("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};
    Block res;

    for (auto _ : state)
    {
        substrait::ReadRel::LocalFiles files;
        substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
        file_item->set_uri_file(file);
        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        file_item->mutable_parquet()->CopyFrom(parquet_format);

        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(
            Pipe(std::make_shared<local_engine::SubstraitFileSource>(local_engine::QueryContext::globalContext(), header, files)));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto reader = PullingPipelineExecutor(pipeline);
        while (reader.pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

substrait::ReadRel::LocalFiles createLocalFiles(const std::string & filename, const bool use_local_format)
{
    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
    file_item->set_uri_file("file://" + filename);
    file_item->set_start(0);
    file_item->set_length(std::filesystem::file_size(filename));
    const substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    file_item->mutable_parquet()->CopyFrom(parquet_format);

    auto config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    config->setBool("use_local_format", use_local_format);
    local_engine::QueryContext::globalMutableContext()->setConfig(config);

    return files;
}

void doRead(const substrait::ReadRel::LocalFiles & files, const std::shared_ptr<const DB::ActionsDAG> & pushDown, const DB::Block & header)
{
    const auto builder = std::make_unique<DB::QueryPipelineBuilder>();
    const auto source = std::make_shared<local_engine::SubstraitFileSource>(local_engine::QueryContext::globalContext(), header, files);
    source->setKeyCondition(pushDown, local_engine::QueryContext::globalContext());
    builder->init(DB::Pipe(source));
    auto pipeline = DB::QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto reader = DB::PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    while (reader.pull(result))
    {
#ifndef NDEBUG
        debug::headBlock(result);
#endif
        total_rows += result.rows();
    }
#ifndef NDEBUG
    std::cerr << "rows:" << total_rows << std::endl;
#endif
}

void BM_ColumnIndexRead_Filter_ReturnAllResult(benchmark::State & state)
{
    using namespace DB;

    const std::string filename = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    const std::string filter1 = "l_shipdate is not null AND l_shipdate <= toDate32('1998-09-01')";
    const substrait::ReadRel::LocalFiles files = createLocalFiles(filename, true);
    const local_engine::RowType schema = local_engine::test::readParquetSchema(filename);
    auto pushDownOpt = local_engine::test::parseFilter(filter1, schema);
    auto pushDown = pushDownOpt ? std::make_shared<const ActionsDAG>(std::move(*pushDownOpt)) : nullptr;

    const Block header = {local_engine::toSampleBlock(schema)};

    for (auto _ : state)
        doRead(files, pushDown, header);
    local_engine::QueryContext::globalMutableContext()->setConfig(Poco::AutoPtr(new Poco::Util::MapConfiguration()));
}

void BM_ColumnIndexRead_Filter_ReturnHalfResult(benchmark::State & state)
{
    using namespace DB;

    const std::string filename = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    const std::string filter1 = "l_orderkey is not null AND l_orderkey > 300977829";
    const substrait::ReadRel::LocalFiles files = createLocalFiles(filename, true);
    const local_engine::RowType schema = local_engine::test::readParquetSchema(filename);
    auto pushDownOpt = local_engine::test::parseFilter(filter1, schema);
    auto pushDown = pushDownOpt ? std::make_shared<const ActionsDAG>(std::move(*pushDownOpt)) : nullptr;
    const Block header = {local_engine::toSampleBlock(schema)};

    for (auto _ : state)
        doRead(files, pushDown, header);
    local_engine::QueryContext::globalMutableContext()->setConfig(Poco::AutoPtr(new Poco::Util::MapConfiguration()));
}

//// Iceberg perf test
///

size_t readFileWithDeletesAndGetRowCount(
    const std::string & file, const DB::Block & header, const local_engine::SubstraitIcebergDeleteFile * delete_file)
{
    using namespace DB;
    using namespace local_engine;
    using namespace local_engine::test;

    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
    file_item->set_uri_file("file://" + file);
    file_item->set_start(0);
    file_item->set_length(std::filesystem::file_size(file));

    substrait::ReadRel::LocalFiles::FileOrFiles::IcebergReadOptions iceberg_options;
    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    iceberg_options.mutable_parquet()->CopyFrom(parquet_format);

    if (delete_file)
        iceberg_options.add_delete_files()->CopyFrom(*delete_file);

    file_item->mutable_iceberg()->CopyFrom(iceberg_options);


    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe(std::make_shared<SubstraitFileSource>(QueryContext::globalContext(), header, files)));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto reader = PullingPipelineExecutor(pipeline);


    size_t total_read_rows = 0;
    DB::Block res;
    while (reader.pull(res))
        total_read_rows += res.rows();
    return total_read_rows;
}

std::pair<size_t, int64_t> calculateRowsAndDeleteCount(benchmark::State & state, const std::string & file_path, const DB::Block & header)
{
    using namespace DB;
    using namespace local_engine;

    FormatSettings format_settings;

    ParquetMetaBuilder metaBuilder{
        .format_settings = format_settings,
        .case_insensitive = format_settings.parquet.case_insensitive_column_matching,
        .allow_missing_columns = format_settings.parquet.allow_missing_columns};
    ReadBufferFromFilePRead fileReader(file_path);
    metaBuilder.build(fileReader, header);

    size_t total_rows = std::ranges::fold_left(
        metaBuilder.readRowGroups, static_cast<size_t>(0), [](size_t sum, const auto & row_group) { return sum + row_group.num_rows; });

    // Calculate delete count based on percentage
    int64_t delete_percentage = state.range(0);
    int64_t delete_count = total_rows * delete_percentage / 100;

    return {total_rows, delete_count};
}

// Helper function to write a Parquet file with position deletes
std::shared_ptr<local_engine::test::TempFilePath>
writePositionDeleteFile(const std::string & base_file_path, const std::vector<int64_t> & delete_positions)
{
    using namespace local_engine;
    using namespace local_engine::test;
    using namespace local_engine::iceberg;

    auto delete_file_path = TempFilePath::tmp("parquet");

    // Create the file path column with base file path repeated
    std::string uri_file_path = "file://" + base_file_path;
    auto file_path_vector = createColumn<std::string>(delete_positions.size(), [&](size_t /*row*/) { return uri_file_path; });

    // Create the position column with delete positions
    auto deletePosVector = createColumn<int64_t>(delete_positions);

    // Create the block with both columns
    DB::Block delete_block{
        {file_path_vector,
         IcebergMetadataColumn::icebergDeleteFilePathColumn()->type,
         IcebergMetadataColumn::icebergDeleteFilePathColumn()->name},
        {deletePosVector, IcebergMetadataColumn::icebergDeletePosColumn()->type, IcebergMetadataColumn::icebergDeletePosColumn()->name}};

    // Write the block to the delete file
    const DB::ContextPtr context = QueryContext::globalContext();
    const Poco::Path file{delete_file_path->string()};
    const Poco::URI fileUri{file};

    auto writer = NormalFileWriter::create(context, fileUri.toString(), delete_block, "parquet");
    writer->write(delete_block);
    writer->close();

    return delete_file_path;
}
std::pair<local_engine::SubstraitIcebergDeleteFile, std::shared_ptr<local_engine::test::TempFilePath>>
createPositionDeleteFile(int64_t delete_count, size_t total_rows, const std::string & base_file_path)
{
    if (delete_count == 0)
        return {{}, nullptr};

    assert(delete_count > 0);
    using namespace local_engine;
    using namespace local_engine::test;

    std::vector<int64_t> delete_positions;
    delete_positions.reserve(delete_count);

    std::vector<int64_t> all_positions(total_rows);
    std::iota(all_positions.begin(), all_positions.end(), 0);

    std::mt19937 g(std::random_device{}());
    std::ranges::shuffle(all_positions, g);

    delete_positions.assign(all_positions.begin(), all_positions.begin() + delete_count);
    std::ranges::sort(delete_positions);

    std::shared_ptr<TempFilePath> delete_file_path = writePositionDeleteFile(base_file_path, delete_positions);

    SubstraitIcebergDeleteFile delete_file;
    delete_file.set_filecontent(IcebergReadOptions::POSITION_DELETES);
    delete_file.set_filepath("file://" + delete_file_path->string());
    delete_file.set_recordcount(delete_count);
    delete_file.set_filesize(std::filesystem::file_size(delete_file_path->string()));

    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    delete_file.mutable_parquet()->CopyFrom(parquet_format);

    return {delete_file, delete_file_path};
}

// Helper function to write a Parquet file with equality deletes
std::shared_ptr<local_engine::test::TempFilePath>
writeEqualityDeleteFile(const std::vector<int64_t> & delete_values, const std::string & column_name = "l_shipdate")
{
    using namespace local_engine;
    using namespace local_engine::test;

    auto delete_file_path = TempFilePath::tmp("parquet");

    // Create the column with values to be deleted
    auto delete_values_vector = createColumn<int64_t>(delete_values);

    // Create the block with the delete values
    DB::Block delete_block{{delete_values_vector, std::make_shared<DB::DataTypeInt64>(), column_name}};

    // Write the block to the delete file
    const DB::ContextPtr context = QueryContext::globalContext();
    const Poco::Path file{delete_file_path->string()};
    const Poco::URI fileUri{file};

    auto writer = NormalFileWriter::create(context, fileUri.toString(), delete_block, "parquet");
    writer->write(delete_block);
    writer->close();

    return delete_file_path;
}

std::pair<local_engine::SubstraitIcebergDeleteFile, std::shared_ptr<local_engine::test::TempFilePath>>
createEqualityDeleteFile(int64_t delete_count)
{
    if (delete_count == 0)
        return {{}, nullptr};

    assert(delete_count > 0);
    using namespace local_engine;
    using namespace local_engine::test;

    std::vector<int64_t> delete_values;
    delete_values.reserve(delete_count);

    // For simplicity in the benchmark, we uniformly select deletion values from the minimum and maximum values.
    // Therefore, some deletion values do not exist, and we cannot determine whether the benchmarking is correct
    // based on the number of deletions.
    //
    // This is acceptable for identifying performance issues with EqualityDeletes.

    // +-------+-------------------------+---------------+---------------+
    // |count()|countDistinct(l_orderkey)|min(l_orderkey)|max(l_orderkey)|
    // +-------+-------------------------+---------------+---------------+
    // |8333867|                  8105793|              7|      599999972|
    // +-------+-------------------------+---------------+---------------+
    constexpr int64_t min_orderkey = 7;
    constexpr int64_t max_orderkey = 599999972;
    std::uniform_int_distribution<int64_t> distrib(min_orderkey, max_orderkey);

    std::mt19937 gen(std::random_device{}());
    for (int64_t i = 0; i < delete_count; ++i)
        delete_values.push_back(distrib(gen));
    std::ranges::sort(delete_values);


    std::shared_ptr<TempFilePath> delete_file_path = writeEqualityDeleteFile(delete_values, "l_orderkey");

    SubstraitIcebergDeleteFile delete_file;
    delete_file.set_filecontent(IcebergReadOptions::EQUALITY_DELETES);
    delete_file.set_filepath("file://" + delete_file_path->string());
    delete_file.set_recordcount(delete_count);
    delete_file.set_filesize(std::filesystem::file_size(delete_file_path->string()));
    delete_file.add_equalityfieldids(1); // l_orderkey

    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    delete_file.mutable_parquet()->CopyFrom(parquet_format);

    return {delete_file, delete_file_path};
}

template <bool is_position_delete>
void BM_IcebergReadWithDeletes(benchmark::State & state)
{
    using namespace DB;
    using namespace local_engine;
    using namespace local_engine::test;

    std::string file
        = third_party_data("benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    Block header{ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate")};
    auto [total_rows, delete_count] = calculateRowsAndDeleteCount(state, file, header);
    Block res;

    auto [delete_file, delete_file_path]
        = is_position_delete ? createPositionDeleteFile(delete_count, total_rows, file) : createEqualityDeleteFile(delete_count);

    for (auto _ : state)
    {
        size_t total_read_rows = readFileWithDeletesAndGetRowCount(file, header, delete_count ? &delete_file : nullptr);

        if (is_position_delete && total_read_rows != total_rows - delete_count)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expected {}, but got {} ", total_rows - delete_count, total_read_rows);

        // see `createEqualityDeleteFile` for `total_read_rows < total_rows - delete_count`
        if (!is_position_delete && total_read_rows < total_rows - delete_count)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Read at least {}, but got {} ", total_rows - delete_count, total_read_rows);
    }
}

void BM_IcebergReadWithPositionDeletes(benchmark::State & state)
{
    return BM_IcebergReadWithDeletes<true>(state);
}
void BM_IcebergReadWithEqualityDeletes(benchmark::State & state)
{
    return BM_IcebergReadWithDeletes<false>(state);
}

}

BENCHMARK(BM_ColumnIndexRead_Old)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ColumnIndexRead_NoFilter)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ColumnIndexRead_Filter_ReturnAllResult)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ColumnIndexRead_Filter_ReturnHalfResult)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(200);
BENCHMARK(BM_IcebergReadWithPositionDeletes)->Unit(benchmark::kMillisecond)->Iterations(10)->Arg(0)->Arg(1)->Arg(10)->Arg(50)->Arg(100);
BENCHMARK(BM_IcebergReadWithEqualityDeletes)->Unit(benchmark::kMillisecond)->Iterations(10)->Arg(0)->Arg(1)->Arg(5)->Arg(10)->Arg(50);
