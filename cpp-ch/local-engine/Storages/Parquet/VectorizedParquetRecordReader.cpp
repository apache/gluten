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
#include "VectorizedParquetRecordReader.h"

#if USE_PARQUET
#include <numeric>
#include <optional>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>
#include <Storages/Parquet/ArrowUtils.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <arrow/io/memory.h>
#include <arrow/util/int_util_overflow.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/page_index.h>

namespace
{
bool extend(arrow::io::ReadRange & read_range, const int64_t offset, const int64_t length)
{
    if (read_range.offset + read_range.length == offset)
    {
        read_range.length += length;
        return true;
    }
    return false;
}
void emplaceReadRange(local_engine::ReadRanges & read_ranges, const int64_t offset, const int64_t length)
{
    if (read_ranges.empty() || !extend(read_ranges.back(), offset, length))
        read_ranges.emplace_back(arrow::io::ReadRange{offset, length});
}

/// Compute the section of the file that should be read for the given
/// row group and column chunk.
::arrow::io::ReadRange computeColumnChunkRange(
    const parquet::FileMetaData & file_metadata, const parquet::ColumnChunkMetaData & column_metadata, const int64_t source_size)
{
    // For PARQUET-816
    static constexpr int64_t kMaxDictHeaderSize = 100;

    int64_t col_start = column_metadata.data_page_offset();
    if (column_metadata.has_dictionary_page() && column_metadata.dictionary_page_offset() > 0
        && col_start > column_metadata.dictionary_page_offset())
        col_start = column_metadata.dictionary_page_offset();

    int64_t col_length = column_metadata.total_compressed_size();
    int64_t col_end;
    if (col_start < 0 || col_length < 0)
        throw parquet::ParquetException("Invalid column metadata (corrupt file?)");

    if (arrow::internal::AddWithOverflow(col_start, col_length, &col_end) || col_end > source_size)
        throw parquet::ParquetException("Invalid column metadata (corrupt file?)");

    // PARQUET-816 workaround for old files created by older parquet-mr
    const parquet::ApplicationVersion & version = file_metadata.writer_version();
    if (version.VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION()))
    {
        // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
        // dictionary page header size in total_compressed_size and total_uncompressed_size
        // (see IMPALA-694). We add padding to compensate.
        const int64_t bytes_remaining = source_size - col_end;
        const int64_t padding = std::min<int64_t>(kMaxDictHeaderSize, bytes_remaining);
        col_length += padding;
    }
    return {col_start, col_length};
}

std::string lowerColumnNameIfNeed(const std::string & column_name, const DB::FormatSettings & settings)
{
    std::string result = column_name;
    if (settings.parquet.case_insensitive_column_matching)
        boost::to_lower(result);
    return result;
}
}

namespace local_engine
{
using namespace ParquetVirtualMeta;

VectorizedColumnReader::VectorizedColumnReader(
    const parquet::arrow::SchemaField & field, ParquetFileReaderExt * reader, const std::vector<Int32> & row_groups)
    : arrow_field_(field.field)
    , input_(field.column_index, reader, row_groups)
    , record_reader_(parquet::internal::RecordReader::Make(
          input_.descr(), computeLevelInfo(input_.descr()), defaultArrowPool(), arrow_field_->type()->id() == ::arrow::Type::DICTIONARY))
{
    nextRowGroup();
}

void VectorizedColumnReader::nextRowGroup()
{
    input_.nextRowGroup().and_then(
        [&](ColumnChunkPageRead && read) -> std::optional<int64_t>
        {
            setPageReader(std::move(read.first), read.second);
            return std::nullopt;
        });
}

void VectorizedColumnReader::setPageReader(std::unique_ptr<parquet::PageReader> reader, const ReadSequence & read_sequence)
{
    if (read_state_)
    {
        read_state_->hasLastSkip().and_then(
            [&](const int64_t skip) -> std::optional<int64_t>
            {
                assert(skip < 0);
                record_reader_->SkipRecords(-skip);
                return std::nullopt;
            });
    }
    record_reader_->SetPageReader(std::move(reader));
    read_state_ = std::make_unique<ParquetReadState>(read_sequence);
}

std::shared_ptr<arrow::ChunkedArray> VectorizedColumnReader::readBatch(int64_t batch_size)
{
    record_reader_->Reset();
    record_reader_->Reserve(batch_size);

    while (read_state_->hasMoreRead() && batch_size > 0)
    {
        const int64_t readNumber = read_state_->currentRead();
        if (readNumber < 0)
        {
            const int64_t records_skipped = record_reader_->SkipRecords(-readNumber);
            assert(records_skipped == -readNumber);
            read_state_->skip(records_skipped);
            assert(hasMoreRead());
        }
        else
        {
            const int64_t readBatch = std::min(batch_size, readNumber);
            const int64_t records_read = record_reader_->ReadRecords(readBatch);
            assert(records_read == readBatch);
            batch_size -= records_read;
            read_state_->read(records_read);
            if (!read_state_->hasMoreRead())
                nextRowGroup();
        }
    }

    std::shared_ptr<arrow::ChunkedArray> result;
    THROW_ARROW_NOT_OK(
        parquet::arrow::TransferColumnData(record_reader_.get(), arrow_field_, input_.descr(), local_engine::defaultArrowPool(), &result));
    return result;
}

parquet::arrow::SchemaManifest VectorizedParquetRecordReader::createSchemaManifest(const parquet::FileMetaData & metadata)
{
    const parquet::SchemaDescriptor * parquet_schema = metadata.schema();
    const auto & keyValueMetadata = metadata.key_value_metadata();
    const parquet::ArrowReaderProperties properties;
    parquet::arrow::SchemaManifest manifest;
    THROW_ARROW_NOT_OK(parquet::arrow::SchemaManifest::Make(parquet_schema, keyValueMetadata, properties, &manifest));
    return manifest;
}


VectorizedParquetRecordReader::VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings)
    : parquet_header_(header)
    , format_settings_(format_settings)
    , arrow_column_to_ch_column_(
          parquet_header_,
          "Parquet",
          format_settings_,
          std::nullopt,
          std::nullopt,
          format_settings_.parquet.allow_missing_columns,
          format_settings_.null_as_default,
          format_settings_.date_time_overflow_behavior,
          format_settings_.parquet.case_insensitive_column_matching)
{
}

bool VectorizedParquetRecordReader::initialize(
    const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    const ColumnIndexRowRangesProvider & row_ranges_provider,
    const std::shared_ptr<parquet::FileMetaData> & metadata)
{
    auto file_reader = parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties(), metadata);
    const parquet::FileMetaData & file_metadata = *file_reader->metadata();


    const std::vector<Int32> & column_indices = row_ranges_provider.getReadColumns();
    const std::vector<Int32> & row_groups = row_ranges_provider.getReadRowGroups();
    if (row_groups.empty())
        return false;

    // initialize File Reader
    parquet::arrow::SchemaManifest manifest = createSchemaManifest(file_metadata);
    THROW_ARROW_NOT_OK_OR_ASSIGN(std::vector<int> field_indices, manifest.GetFieldIndices(column_indices));
    file_reader_ = std::make_unique<ParquetFileReaderExt>(arrow_file, std::move(file_reader), row_ranges_provider, format_settings_);
    column_readers_.reserve(field_indices.size());

    for (auto const & column_index : field_indices)
    {
        auto const & field = manifest.schema_fields[column_index];
        assert(field.column_index >= 0);
        assert(column_index == field.column_index);
        column_readers_.emplace_back(field, file_reader_.get(), row_groups);
        if (!column_readers_.back().hasMoreRead())
        {
            assert(column_readers_.size() == 1);
            return false;
        }
    }
    return true;
}

DB::Chunk VectorizedParquetRecordReader::nextBatch()
{
    assert(initialized());
    ::arrow::ChunkedArrayVector columns(column_readers_.size());
    DB::ArrowColumnToCHColumn::NameToArrowColumn name_to_column_ptr;
    for (auto & vectorized_column_reader : column_readers_)
        name_to_column_ptr[lowerColumnNameIfNeed(vectorized_column_reader.columnName(), format_settings_)]
            = {vectorized_column_reader.readBatch(format_settings_.parquet.max_block_size), vectorized_column_reader.arrowField()};

    if (const size_t num_rows = name_to_column_ptr.begin()->second.column->length(); num_rows > 0)
        return arrow_column_to_ch_column_.arrowColumnsToCHChunk(
            name_to_column_ptr, num_rows, file_reader_->fileMeta()->key_value_metadata(), nullptr);
    return {};
}

ParquetFileReaderExt::ParquetFileReaderExt(
    const std::shared_ptr<arrow::io::RandomAccessFile> & source,
    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
    const ColumnIndexRowRangesProvider & row_ranges_provider,
    const DB::FormatSettings & format_settings)
    : source_(source)
    , file_reader_(std::move(parquetFileReader))
    , format_settings_(format_settings)
    , row_ranges_provider_(row_ranges_provider)
{
    THROW_ARROW_NOT_OK_OR_ASSIGN(const int64_t source_size, source_->GetSize());
    source_size_ = source_size;
}

std::optional<ColumnChunkPageRead> PageIterator::nextRowGroup()
{
    while (!row_groups_.empty())
    {
        const Int32 row_group_index = row_groups_.front();
        auto result = reader_ext_->nextRowGroup(row_group_index, column_index_, descr()->name());
        row_groups_.pop_front();
        if (result)
            return result;
    }
    return std::nullopt;
}

std::optional<ColumnChunkPageRead>
ParquetFileReaderExt::nextRowGroup(int32_t row_group_index, int32_t column_index, const std::string & column_name) const
{
    return row_ranges_provider_.getRowRanges(row_group_index)
        .transform(
            [&](const RowRanges & row_ranges)
            {
                const auto rg = fileMeta()->RowGroup(row_group_index);
                const auto rg_count = rg->num_rows();
                const BuildRead readAll = [&](const arrow::io::ReadRange & col_range) { return buildAllRead(rg_count, col_range); };
                const BuildRead read = row_ranges.rowCount() == rg_count ? readAll : [&](const arrow::io::ReadRange & col_range)
                {
                    const ColumnIndexStore & column_index_store = row_ranges_provider_.getColumnIndexStore(row_group_index);
                    const ColumnIndex & index = *column_index_store.find(lowerColumnNameIfNeed(column_name, format_settings_))->second;
                    return buildRead(rg_count, col_range, index.offsetIndex().page_locations(), row_ranges);
                };

                return readColumnChunkPageBase(*rg, column_index, read);
            });
}

ColumnChunkPageRead ParquetFileReaderExt::readColumnChunkPageBase(
    const parquet::RowGroupMetaData & rg, const Int32 column_index, const BuildRead & build_read) const
{
    const auto file_metadata = file_reader_->metadata();

    // Prior to Arrow 3.0.0, is_compressed was always set to false in column headers,
    // even if compression was used. See ARROW-17100.
    const bool always_compressed
        = file_metadata->writer_version().VersionLt(parquet::ApplicationVersion::PARQUET_CPP_10353_FIXED_VERSION());
    const auto column_metadata = rg.ColumnChunk(column_index);
    const auto col_range = computeColumnChunkRange(*file_metadata, *column_metadata, source_size_);
    const parquet::ReaderProperties properties;
    auto [read_ranges, read_sequence] = build_read(col_range);
    const auto input_stream = getStream(*source_, read_ranges);
    return std::make_pair(
        parquet::PageReader::Open(
            input_stream, column_metadata->num_values(), column_metadata->compression(), properties, always_compressed),
        read_sequence);
}

/// input format
VectorizedParquetBlockInputFormat::VectorizedParquetBlockInputFormat(
    DB::ReadBuffer & in_,
    const DB::SharedHeader & header_,
    const ColumnIndexRowRangesProvider & row_ranges_provider,
    const DB::FormatSettings & format_settings)
    : DB::IInputFormat(header_, &in_), record_reader_(getPort().getHeader(), format_settings), row_ranges_provider_(row_ranges_provider)
{
}

void VectorizedParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    record_reader_.reset();
}

DB::Chunk VectorizedParquetBlockInputFormat::read()
{
    if (is_stopped != 0)
        return {};

    if (!record_reader_.initialized())
    {
        const auto arrow_file = DB::asArrowFile(*in, record_reader_.formatSettings(), is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
        if (is_stopped != 0)
            return {};
        if (!record_reader_.initialize(arrow_file, row_ranges_provider_))
            return {};
    }
    return record_reader_.nextBatch();
}
ColumnReadState buildAllRead(const int64_t rg_count, const arrow::io::ReadRange & chunk_range)
{
    return std::make_pair(ReadRanges{chunk_range}, ReadSequence{rg_count});
}

ColumnReadState buildRead(
    const int64_t rg_count,
    const arrow::io::ReadRange & chunk_range,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & row_ranges)
{
    if (rg_count == row_ranges.rowCount())
        return buildAllRead(rg_count, chunk_range);

    auto skip = [](ReadSequence & read_sequence, const int64_t number) -> void
    {
        assert(number > 0);
        if (read_sequence.empty() || read_sequence.back() > 0)
            read_sequence.push_back(-number);
        else
            read_sequence.back() -= number;
    };

    auto read = [](ReadSequence & read_sequence, const int64_t number) -> void
    {
        assert(number > 0);
        if (read_sequence.empty() || read_sequence.back() < 0)
            read_sequence.push_back(number);
        else
            read_sequence.back() += number;
    };

    ColumnReadState result;
    const RowRangesBuilder rg_builder{rg_count, page_locations};
    ReadRanges & read_ranges = result.first;

    // Add a range for dictionary page if required
    if (chunk_range.offset < page_locations[0].offset)
        emplaceReadRange(read_ranges, chunk_range.offset, page_locations[0].offset - chunk_range.offset);

    std::vector<Range> page_row_ranges;
    const size_t pageSize = page_locations.size();
    for (size_t pageIndex = 0; pageIndex < pageSize; ++pageIndex)
    {
        size_t from = rg_builder.firstRowIndex(pageIndex);
        size_t to = rg_builder.lastRowIndex(pageIndex);
        if (row_ranges.isOverlapping(from, to))
        {
            emplaceReadRange(read_ranges, page_locations[pageIndex].offset, page_locations[pageIndex].compressed_page_size);
            page_row_ranges.push_back({from, to});
        }
    }

    result.second.resize(1);
    std::vector<int64_t> & read_sequence = result.second;

    auto row_range_begin = row_ranges.getRanges().begin();
    const auto row_range_end = row_ranges.getRanges().end();
    size_t rowIndex = row_range_begin->from;

    for (size_t i = 0; i < page_row_ranges.size() && row_range_begin != row_range_end; ++i)
    {
        const size_t lastRowIndexInPage = page_row_ranges[i].to;
        size_t readRowIndexInPage = page_row_ranges[i].from;

        /// [readRowIndexInPage, rowIndex-1] - [rowIndex, rowIndex+readNumber-1] - [rowIndex+readNumber, lastRowIndexInPage]
        if (rowIndex <= lastRowIndexInPage)
        {
            assert(rowIndex >= readRowIndexInPage);
            do
            {
                if (rowIndex > readRowIndexInPage)
                    skip(read_sequence, rowIndex - readRowIndexInPage);
                const size_t readNumber = std::min(row_range_begin->to, lastRowIndexInPage) - rowIndex + 1;
                read(read_sequence, readNumber);
                rowIndex += readNumber;
                readRowIndexInPage = rowIndex;

                /// we already read current page, so we need to read next page.
                if (row_range_begin->to > lastRowIndexInPage)
                {
                    assert(readRowIndexInPage > lastRowIndexInPage);
                    break;
                }

                /// we already read current range so we need read next range
                ++row_range_begin;
                if (row_range_begin != row_range_end)
                    rowIndex = row_range_begin->from;
            } while (row_range_begin != row_range_end && rowIndex <= lastRowIndexInPage);

            /// skip read remain records in current page.
            if (readRowIndexInPage <= lastRowIndexInPage)
                skip(read_sequence, lastRowIndexInPage - readRowIndexInPage + 1);
        }
        assert(!read_sequence.empty());
    }
    return result;
}

std::shared_ptr<parquet::ArrowInputStream> getStream(arrow::io::RandomAccessFile & reader, const ReadRanges & ranges)
{
    const int64_t nbytes = std::accumulate(
        ranges.begin(), ranges.end(), 0, [](const int64_t total, const arrow::io::ReadRange & range) { return total + range.length; });
    THROW_ARROW_NOT_OK_OR_ASSIGN(std::shared_ptr<arrow::Buffer> buffer, arrow::AllocateResizableBuffer(nbytes));

    int64_t readPos = 0;
    std::ranges::for_each(
        ranges,
        [&](const arrow::io::ReadRange & range)
        {
            const int64_t offset = range.offset;
            const int64_t length = range.length;
            THROW_ARROW_NOT_OK_OR_ASSIGN(const int64_t bytes_read, reader.ReadAt(offset, length, buffer->mutable_data() + readPos));
            assert(bytes_read == length);
            readPos += bytes_read;
        });
    assert(nbytes == readPos);

    return std::make_shared<::arrow::io::BufferReader>(buffer);
}

}
#endif
