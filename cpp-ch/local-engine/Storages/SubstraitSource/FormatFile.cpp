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
#include "FormatFile.h"
#include <Core/Settings.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/SubstraitSource/JSONFormatFile.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/GlutenConfig.h>
#include <Common/GlutenStringUtils.h>
#include <Common/logger_useful.h>

#if GLUTEN_ENABLE_NATIVE_PAIMON
#include <Storages/SubstraitSource/Paimon/PaimonFormatFile.h>
#endif

#if USE_PARQUET
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#endif

#if USE_ORC
#include <Storages/SubstraitSource/ORCFormatFile.h>
#endif

#if USE_HIVE
#include <Storages/SubstraitSource/ExcelTextFormatFile.h>
#include <Storages/SubstraitSource/TextFormatFile.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}
namespace local_engine
{
using namespace DB;
// Initialize the static variable outside the class definition
std::map<std::string, std::function<Field(const std::string &)>> FileMetaColumns::BASE_METADATA_EXTRACTORS
    = {{FILE_PATH, [](const std::string & metadata) { return metadata; }},
       {FILE_NAME, [](const std::string & metadata) { return metadata; }},
       {FILE_SIZE, [](const std::string & value) { return std::strtoll(value.c_str(), nullptr, 10); }},
       {FILE_BLOCK_START, [](const std::string & value) { return std::strtoll(value.c_str(), nullptr, 10); }},
       {FILE_BLOCK_LENGTH, [](const std::string & value) { return std::strtoll(value.c_str(), nullptr, 10); }},
       {FILE_MODIFICATION_TIME,
        [](const std::string & metadata)
        {
            DB::ReadBufferFromString in(metadata);
            DateTime64 time = 0;
            readDateTime64Text(time, 6, in, DateLUT::instance("UTC"));
            return DecimalField(time, 6);
        }}};

// Initialize the static variable outside the class definition
std::map<std::string, std::function<DB::Field(const SubstraitInputFile &)>> FileMetaColumns::INPUT_FUNCTION_EXTRACTORS
    = {{INPUT_FILE_NAME, [](const SubstraitInputFile & file) { return file.uri_file(); }},
       {INPUT_FILE_BLOCK_START, [](const SubstraitInputFile & file) { return file.start(); }},
       {INPUT_FILE_BLOCK_LENGTH, [](const SubstraitInputFile & file) { return file.length(); }}};

FileMetaColumns::FileMetaColumns(const SubstraitInputFile & file)
{
    for (const auto & column : file.metadata_columns())
    {
        if (!BASE_METADATA_EXTRACTORS.contains(column.key()))
            continue;

        assert(BASE_METADATA_EXTRACTORS.contains(column.key()));
        metadata_columns_map[column.key()] = BASE_METADATA_EXTRACTORS[column.key()](column.value());
    }

    for (const auto & inputExtractor : INPUT_FUNCTION_EXTRACTORS)
    {
        assert(!metadata_columns_map.contains(inputExtractor.first));
        metadata_columns_map[inputExtractor.first] = inputExtractor.second(file);
    }
}

DB::ColumnPtr FileMetaColumns::createMetaColumn(const String & columnName, const DB::DataTypePtr & type, size_t rows) const
{
    assert(metadata_columns_map.contains(columnName));
    const auto field = metadata_columns_map.at(columnName);

    if (INPUT_FILE_COLUMNS_SET.contains(columnName))
    {
        /// copied from InputFileNameParser::addInputFileColumnsToChunk()
        /// TODO: check whether using const column is correct or not.
        return type->createColumnConst(rows, field);
    }
    auto mutable_column = type->createColumn();
    mutable_column->insertMany(field, rows);
    return mutable_column;
}

FormatFile::FormatFile(DB::ContextPtr context_, const SubstraitInputFile & file_info_, const ReadBufferBuilderPtr & read_buffer_builder_)
    : context(context_), file_info(file_info_), read_buffer_builder(read_buffer_builder_), meta_columns(file_info_)
{
    /// Escaping rules for file_info:
    /// 1. file_info.partition_columns() and file_info.partition_values() had been decoded in jvm. We don't need to decode them again in c++.
    /// e.g. The raw partition value is "%s", and the decoded value is "%s".
    ///
    /// 2. the file path in file_info.uri_file() and file_info.metadata_columns() are not decoded.
    /// e.g. The raw partition value is "%s", the raw file path is "file:/spark-warehouse/sales/year=%25s/part-00000"
    /// the file path in file_info.uri_file() and file_info.metadata_columns() is encoded based on raw file path: "file:/spark-warehouse/sales/year=%2525s/part-00000"
    /// To get the raw file path, we need to decode file_info.uri_file(). Currently it is implemented in [[ReadBufferBuilder::build()]].
    ///
    /// 3. input_file_name() returns file_info.uri_file() directly instead of decoding it.

    if (file_info.partition_columns_size())
    {
        for (size_t i = 0; i < file_info.partition_columns_size(); ++i)
        {
            const auto & partition_column = file_info.partition_columns(i);
            partition_values[partition_column.key()] = partition_column.value();

            auto normalized_key = boost::to_lower_copy(partition_column.key());
            normalized_partition_values[normalized_key] = partition_column.value();
        }
    }

    LOG_INFO(
        &Poco::Logger::get("FormatFile"),
        "Reading File path: {}, format: {}, range: {}, partition_index: {}, partition_values: {}",
        file_info.uri_file(),
        file_info.file_format_case(),
        std::to_string(file_info.start()) + "-" + std::to_string(file_info.start() + file_info.length()),
        file_info.partition_index(),
        GlutenStringUtils::mkString(partition_values));
}

FormatFilePtr FormatFileUtil::createFile(
    DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file)
{
    if (file.has_paimon())
    {
#if GLUTEN_ENABLE_NATIVE_PAIMON
        return std::make_shared<PaimonFormatFile>(context, file, read_buffer_builder);
#else
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Paimon native read is disabled in this libch build (GLUTEN_ENABLE_NATIVE_PAIMON=OFF)");
#endif
    }
#if USE_PARQUET
    if (file.has_parquet() || (file.has_iceberg() && file.iceberg().has_parquet()))
    {
        auto config = ExecutorConfig::loadFromContext(context);
        return std::make_shared<ParquetFormatFile>(context, file, read_buffer_builder, config.use_local_format);
    }
#endif

#if USE_ORC
    if (file.has_orc() || (file.has_iceberg() && file.iceberg().has_orc()))
        return std::make_shared<ORCFormatFile>(context, file, read_buffer_builder);
#endif

#if USE_HIVE
    if (file.has_text())
    {
        if (ExcelTextFormatFile::useThis(context))
            return std::make_shared<ExcelTextFormatFile>(context, file, read_buffer_builder);
        else
            return std::make_shared<TextFormatFile>(context, file, read_buffer_builder);
    }
#endif

    if (file.has_json())
        return std::make_shared<JSONFormatFile>(context, file, read_buffer_builder);
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported:{}", file.DebugString());
}
}
