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

#include "PaimonReader.h"
#include "HdfsPaimonFileSystem.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/read_context.h>
#include <paimon/reader/batch_reader.h>
#include <paimon/reader/count_reader.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_read.h>

#include <algorithm>
#include <map>
#include <utility>
#include <Poco/URI.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace local_engine::paimon
{
namespace
{
template <typename T>
T takeResult(::paimon::Result<T> && result)
{
    if (!result.ok())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon reader: {}", result.status().ToString());
    return std::move(result).value();
}

class PaimonCountInputFormat final : public DB::IInputFormat
{
    std::unique_ptr<::paimon::CountReader> reader_;
    String table_path_;
    int64_t remaining_rows_ = -1;

public:
    PaimonCountInputFormat(
        DB::ReadBuffer * in_,
        const DB::SharedHeader & header_,
        std::unique_ptr<::paimon::CountReader> reader,
        String table_path)
        : DB::IInputFormat(header_, in_)
        , reader_(std::move(reader))
        , table_path_(std::move(table_path))
    {
    }

    String getName() const override { return "PaimonCountInputFormat"; }

protected:
    DB::Chunk read() override
    {
        if (remaining_rows_ < 0)
        {
            remaining_rows_ = takeResult(reader_->CountRows());
            if (remaining_rows_ < 0)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Paimon CountReader returned a negative row count");
            LOG_DEBUG(&Poco::Logger::get("PaimonReader"), "Native Paimon count finished: table_path={}, rows={}", table_path_, remaining_rows_);
            reader_.reset();
        }
        if (remaining_rows_ == 0)
            return {};

        constexpr int64_t count_batch_size = 8192;
        const auto rows = std::min(remaining_rows_, count_batch_size);
        remaining_rows_ -= rows;
        return BlockUtil::buildRowCountChunk(rows);
    }
};

class PaimonBlockInputFormat final : public DB::IInputFormat
{
    std::unique_ptr<::paimon::BatchReader> reader_;
    String table_path_;
    DB::ArrowColumnToCHColumn arrow_to_ch_;
    bool eof_ = false;
    size_t batches_read_ = 0;
    size_t rows_read_ = 0;

public:
    PaimonBlockInputFormat(
        DB::ReadBuffer * in_,
        const DB::SharedHeader & header_,
        std::unique_ptr<::paimon::BatchReader> reader,
        String table_path,
        DB::ContextPtr context)
        : DB::IInputFormat(header_, in_)
        , reader_(std::move(reader))
        , table_path_(std::move(table_path))
        , arrow_to_ch_(
              [&]
              {
                  const DB::FormatSettings format_settings = getFormatSettings(context);
                  return DB::ArrowColumnToCHColumn(
                      *header_,
                      "Paimon",
                      format_settings,
                      std::nullopt,
                      std::nullopt,
                      format_settings.parquet.allow_missing_columns,
                      format_settings.null_as_default,
                      format_settings.date_time_overflow_behavior,
                      format_settings.parquet.allow_geoparquet_parser,
                      format_settings.parquet.case_insensitive_column_matching,
                      false,
                      format_settings.parquet.enable_json_parsing);
              }())
    {
    }

    String getName() const override { return "PaimonBlockInputFormat"; }

protected:
    DB::Chunk read() override
    {
        if (eof_ || !reader_)
            return {};

        while (true)
        {
            auto paimon_batch = takeResult(reader_->NextBatch());
            if (::paimon::BatchReader::IsEofBatch(paimon_batch))
            {
                LOG_INFO(
                    &Poco::Logger::get("PaimonReader"),
                    "Native Paimon read finished: table_path={}, batches={}, rows={}",
                    table_path_,
                    batches_read_,
                    rows_read_);
                eof_ = true;
                reader_->Close();
                reader_.reset();
                return {};
            }

            auto import_result = arrow::ImportArray(paimon_batch.first.get(), paimon_batch.second.get());
            if (!import_result.ok())
            {
                if (paimon_batch.first && paimon_batch.first->release)
                    paimon_batch.first->release(paimon_batch.first.get());
                if (paimon_batch.second && paimon_batch.second->release)
                    paimon_batch.second->release(paimon_batch.second.get());
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon Arrow import: {}", import_result.status().ToString());
            }

            const auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(import_result.ValueOrDie());
            if (!struct_array)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon reader expects a struct Arrow batch");

            const auto struct_type = std::static_pointer_cast<arrow::StructType>(struct_array->type());
            std::vector<std::shared_ptr<arrow::Field>> fields;
            std::vector<std::shared_ptr<arrow::Array>> columns;
            for (int i = 0; i < struct_type->num_fields(); ++i)
            {
                if (struct_type->field(i)->name() == "_VALUE_KIND")
                    continue;
                fields.push_back(struct_type->field(i));
                columns.push_back(struct_array->field(i));
            }
            if (fields.empty())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon batch has no data columns");

            auto batch = arrow::RecordBatch::Make(arrow::schema(fields), struct_array->length(), columns);
            auto table_result = arrow::Table::FromRecordBatches({batch});
            if (!table_result.ok())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon Arrow table: {}", table_result.status().ToString());
            auto table = table_result.ValueOrDie();
            DB::Chunk chunk = arrow_to_ch_.arrowTableToCHChunk(table, table->num_rows(), nullptr, nullptr);
            if (chunk.getNumRows() != 0)
            {
                ++batches_read_;
                rows_read_ += chunk.getNumRows();
                if (batches_read_ == 1 || batches_read_ % 1000 == 0)
                    LOG_DEBUG(
                        &Poco::Logger::get("PaimonReader"),
                        "Native Paimon reader emitted batch: table_path={}, batch={}, rows={}",
                        table_path_,
                        batches_read_,
                        chunk.getNumRows());
                return chunk;
            }
        }
    }
};

}

FormatFile::InputFormatPtr createPaimonInputFormat(const FormatFile & file, const DB::Block & header)
{
    const auto & file_info = file.getFileInfo();
    if (!file_info.has_paimon())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Paimon input format requires paimon read options in LocalFiles");

    const auto & cpp_split = file_info.paimon().cpp_split();
    const auto & serialized_split = cpp_split.serialized_data_split();
    const auto & table_path = cpp_split.table_path();

    LOG_INFO(
        &Poco::Logger::get("PaimonReader"),
        "Opening native Paimon reader: table_path={}, serialized_split_bytes={}",
        table_path,
        serialized_split.size());

    auto split = takeResult(::paimon::Split::Deserialize(
        serialized_split.data(), serialized_split.size(), ::paimon::GetDefaultPool()));
    ::paimon::ReadContextBuilder builder(table_path);
    const std::map<std::string, std::string> read_options(
        cpp_split.read_options().begin(), cpp_split.read_options().end());
    builder.SetOptions(read_options);
    builder.WithBranch(cpp_split.branch());
    const bool count_only = header.columns() == 1 && header.has(BlockUtil::VIRTUAL_ROW_COUNT_COLUMN);
    std::vector<std::string> read_fields;
    read_fields.reserve(header.columns());
    for (const auto & column : header)
        if (column.name != BlockUtil::VIRTUAL_ROW_COUNT_COLUMN)
            read_fields.push_back(column.name);
    if (!read_fields.empty())
        builder.SetReadFieldNames(read_fields);
    const auto & config = file.getContext()->getConfigRef();
    if (config.has("paimon.read_batch_size"))
    {
        const int batch_size = config.getInt("paimon.read_batch_size");
        if (batch_size <= 0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "paimon.read_batch_size must be positive, got {}", batch_size);
        builder.AddOption("read.batch-size", std::to_string(batch_size));
    }
    if (Poco::URI(table_path).getScheme() == "hdfs")
        builder.WithFileSystem(createHdfsPaimonFileSystem(table_path, file.getContext()));
    auto read_context = takeResult(builder.Finish());
    auto table_read = takeResult(::paimon::TableRead::Create(std::move(read_context)));

    static const std::string empty_read_buffer;
    auto read_buffer = std::make_unique<DB::ReadBufferFromString>(empty_read_buffer);
    DB::InputFormatPtr input;
    if (count_only)
    {
        auto count_reader = takeResult(table_read->CreateCountReader({split}));
        input = std::make_shared<PaimonCountInputFormat>(read_buffer.get(), toShared(header), std::move(count_reader), table_path);
        LOG_DEBUG(&Poco::Logger::get("PaimonReader"), "Native Paimon count reader created: table_path={}", table_path);
    }
    else
    {
        auto reader = takeResult(table_read->CreateReader(split));
        input = std::make_shared<PaimonBlockInputFormat>(
            read_buffer.get(), toShared(header), std::move(reader), table_path, file.getContext());
        LOG_INFO(&Poco::Logger::get("PaimonReader"), "Native Paimon reader created: table_path={}", table_path);
    }
    return std::make_shared<FormatFile::InputFormat>(std::move(read_buffer), input);
}

}
