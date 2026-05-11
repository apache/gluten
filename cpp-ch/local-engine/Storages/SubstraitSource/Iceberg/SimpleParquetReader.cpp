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
#include "SimpleParquetReader.h"
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilterHelper.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Poco/URI.h>

using namespace DB;
namespace local_engine
{

namespace iceberg
{
SubstraitInputFile fromDeleteFile(const SubstraitIcebergDeleteFile & deleteFile)
{
    assert(
        deleteFile.filecontent() == IcebergReadOptions::EQUALITY_DELETES
        || deleteFile.filecontent() == IcebergReadOptions::POSITION_DELETES);
    assert(deleteFile.has_parquet());
    SubstraitInputFile file;
    file.set_uri_file(deleteFile.filepath());
    file.set_start(0);
    file.set_length(deleteFile.filesize());
    return file;
}
}

SimpleParquetReader::SimpleParquetReader(
    const ContextPtr & context, const SubstraitInputFile & file_info, Block header, const std::optional<ActionsDAG> & filter)
{
    const Poco::URI file_uri{file_info.uri_file()};
    ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);

    read_buffer_arrow_ = read_buffer_builder->build(file_info);
    if (!header.columns())
        header = ParquetMetaBuilder::collectFileSchema(context, *read_buffer_arrow_);

    FormatSettings format_settings = getFormatSettings(context);
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    {
        std::atomic<int> is_stopped{0};
        arrow_file = asArrowFile(*read_buffer_arrow_, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
    }

    // TODO: set min_bytes_for_seek
    ParquetReader::Settings settings{
        .arrow_properties = parquet::ArrowReaderProperties(),
        .reader_properties = parquet::ReaderProperties(arrow::default_memory_pool()),
        .format_settings = format_settings};

    read_buffer_reader_ = read_buffer_builder->build(file_info);

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(read_buffer_reader_.get());
    assert(seekable != nullptr);
    assert(header.columns());

    reader_ = std::make_shared<ParquetReader>(std::move(header), *seekable, arrow_file, settings);
    reader_->setSourceArrowFile(arrow_file);
    if (filter.has_value())
        pushFilterToParquetReader(*filter, *reader_);
}

SimpleParquetReader::SimpleParquetReader(
    const ContextPtr & context, const SubstraitIcebergDeleteFile & file_info, Block header, const std::optional<ActionsDAG> & filter)
    : SimpleParquetReader(context, iceberg::fromDeleteFile(file_info), std::move(header), filter)
{
}

SimpleParquetReader::~SimpleParquetReader() = default;

Block SimpleParquetReader::next() const
{
    return reader_->read();
}

}