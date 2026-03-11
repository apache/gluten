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
#include "ParquetFormatFile.h"

#if USE_PARQUET

#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/Parquet/VirtualColumnRowIndexReader.h>
#include <Storages/SubstraitSource/Delta/DeltaMeta.h>
#include <Common/BlockTypeUtils.h>

namespace DB
{
namespace Setting
{
extern const SettingsMaxThreads max_download_threads;
extern const SettingsMaxThreads max_parsing_threads;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TYPE;
}
}

using namespace DB;

namespace local_engine
{
struct ShouldIncludeRowGroup
{
    const SubstraitInputFile & file_info;

    explicit ShouldIncludeRowGroup(const SubstraitInputFile & file_info) : file_info(file_info) { }

    bool operator()(UInt64 midpoint_offset) const
    {
        return file_info.start() <= midpoint_offset && midpoint_offset < file_info.start() + file_info.length();
    }
};

namespace
{
ParquetMetaBuilder collectRequiredRowGroups(ReadBuffer & read_buffer, const SubstraitInputFile & file_info)
{
    ParquetMetaBuilder result;
    ShouldIncludeRowGroup should_include_row_group{file_info};
    result.build(read_buffer, should_include_row_group);
    return result;
}
}

using namespace ParquetVirtualMeta;
class ParquetInputFormat : public FormatFile::InputFormat
{
    const Block readHeader;
    const Block outputHeader;
    std::unique_ptr<ColumnIndexRowRangesProvider> rowRangesProvider;
    std::optional<VirtualColumnRowIndexReader> row_index_reader;

public:
    ParquetInputFormat(
        std::unique_ptr<ReadBuffer> read_buffer_,
        const InputFormatPtr & input_,
        std::unique_ptr<ColumnIndexRowRangesProvider> provider,
        const Block & readHeader_,
        const Block & outputHeader_)
        : InputFormat(std::move(read_buffer_), input_)
        , readHeader(readHeader_)
        , outputHeader(outputHeader_)
        , rowRangesProvider(std::move(provider))
        , row_index_reader(
              outputHeader.columns() > readHeader.columns()
                  ? std::make_optional<VirtualColumnRowIndexReader>(*rowRangesProvider, getMetaColumnType(outputHeader))
                  : std::nullopt)
    {
    }

    Chunk generate() override
    {
        if (readHeader.columns() == 0)
        {
            assert(outputHeader.columns());
            assert(row_index_reader);
            // TODO: format_settings_.parquet.max_block_size
            Columns cols{row_index_reader->readBatch(8192)};
            size_t rows = cols[0]->size();
            return Chunk(std::move(cols), rows);
        }
        auto chunk = input->generate();
        size_t num_rows = chunk.getNumRows();
        if (row_index_reader && num_rows)
        {
            auto row_index_column = row_index_reader->readBatch(num_rows);
            assert(outputHeader.columns() > readHeader.columns());
            size_t column_pos = outputHeader.getPositionByName(TMP_ROWINDEX);
            if (column_pos < chunk.getNumColumns())
                chunk.addColumn(column_pos, std::move(row_index_column));
            else
                chunk.addColumn(std::move(row_index_column));
        }
        return chunk;
    }
};

ParquetFormatFile::ParquetFormatFile(
    const ContextPtr & context_,
    const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_,
    const ReadBufferBuilderPtr & read_buffer_builder_,
    bool use_local_format_)
    : FormatFile(context_, file_info_, read_buffer_builder_), use_pageindex_reader{use_local_format_}
{
}

void ParquetFormatFile::initialize(const ColumnIndexFilterPtr & filter)
{
    column_index_filter_ = filter;
    read_buffer_ = read_buffer_builder->build(file_info);
    if (file_info.has_iceberg())
        file_schema = ParquetMetaBuilder::collectFileSchema(context, *read_buffer_);
}

FormatFile::InputFormatPtr
ParquetFormatFile::createInputFormat(const Block & header, const std::shared_ptr<const DB::ActionsDAG> & filter_actions_dag)
{
    assert(read_buffer_);

    bool readRowIndex = hasMetaColumns(header);
    bool usePageIndexReader = (use_pageindex_reader || readRowIndex) && onlyHasFlatType(header);

    auto format_settings = getFormatSettings(context);
    auto read_header = toShared(DeltaVirtualMeta::removeMetaColumns(removeMetaColumns(header)));

    ParquetMetaBuilder metaBuilder{
        .format_settings = format_settings,
        .collectPageIndex = usePageIndexReader || readRowIndex,
        .collectSkipRowGroup = !usePageIndexReader,
        .case_insensitive = format_settings.parquet.case_insensitive_column_matching,
        .allow_missing_columns = format_settings.parquet.allow_missing_columns};

    ShouldIncludeRowGroup should_include_row_group{file_info};
    if (auto * seekable_in = dynamic_cast<SeekableReadBuffer *>(read_buffer_.get()))
    {
        // reuse the read_buffer to avoid opening the file twice.
        // especially，the cost of opening a hdfs file is large.
        metaBuilder.build(*seekable_in, *read_header, column_index_filter_.get(), should_include_row_group);
        seekable_in->seek(0, SEEK_SET);
    }
    else
    {
        const auto in = read_buffer_builder->build(file_info);
        metaBuilder.build(*in, *read_header, column_index_filter_.get(), should_include_row_group);
    }

    column_index_filter_.reset();

    if (metaBuilder.readRowGroups.empty())
        return nullptr;

    auto provider = usePageIndexReader || readRowIndex ? std::make_unique<ColumnIndexRowRangesProvider>(metaBuilder) : nullptr;

    auto createVectorizedFormat = [&]() -> InputFormatPtr
    {
        auto input = std::make_shared<VectorizedParquetBlockInputFormat>(*read_buffer_, read_header, *provider, format_settings);
        return std::make_shared<ParquetInputFormat>(std::move(read_buffer_), input, std::move(provider), *read_header, header);
    };

    auto createParquetBlockInputFormat = [&]() -> InputFormatPtr
    {
        const Settings & settings = context->getSettingsRef();
        format_settings.parquet.skip_row_groups
            = std::unordered_set<int>(metaBuilder.skipRowGroups.begin(), metaBuilder.skipRowGroups.end());

        if (readRowIndex)
        {
            assert(provider);

            // In the case of readRowIndex, we need to preserve the order of the rows
            format_settings.parquet.preserve_order = true;

            // TODO: enable filter push down again
            // We need to disable fiter push down and read all row groups, so that we can get correct row index.
            format_settings.parquet.filter_push_down = false;
        }
        auto parser_group = std::make_shared<FormatFilterInfo>(filter_actions_dag, context, nullptr);
        auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), /*num_streams_=*/1);
        auto input = std::make_shared<ParquetBlockInputFormat>(*read_buffer_, read_header, format_settings, parser_shared_resources, parser_group, 8192);
        return std::make_shared<ParquetInputFormat>(std::move(read_buffer_), input, std::move(provider), *read_header, header);
    };

    return usePageIndexReader ? createVectorizedFormat() : createParquetBlockInputFormat();
}

std::optional<size_t> ParquetFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }

    auto in = read_buffer_builder->build(file_info);
    auto result = collectRequiredRowGroups(*in, file_info);

    size_t rows = std::ranges::fold_left(
        result.readRowGroups, static_cast<size_t>(0), [](size_t sum, const auto & row_group) { return sum + row_group.num_rows; });

    {
        std::lock_guard lock(mutex);
        total_rows = rows;
        return total_rows;
    }
}

bool ParquetFormatFile::onlyHasFlatType(const Block & header)
{
    return std::ranges::all_of(
        header,
        [](ColumnWithTypeAndName const & col)
        {
            const DataTypePtr type_not_nullable = removeNullable(col.type);
            const WhichDataType which(type_not_nullable);
            return !isArray(which) && !isMap(which) && !isTuple(which);
        });
}

}
#endif
