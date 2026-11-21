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

#include "FileReader.h"

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromString.h>
#include <Parser/SubstraitParserUtils.h>
#include <Storages/SubstraitSource/Delta/DeltaMeta.h>
#include <Storages/SubstraitSource/Delta/DeltaReader.h>
#include <Storages/SubstraitSource/Iceberg/IcebergReader.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/GlutenStringUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
DB::Columns BaseReader::addVirtualColumn(DB::Chunk dataChunk, size_t rowNum) const
{
    // dataChunk may be empty
    const size_t rows = dataChunk.empty() ? rowNum : dataChunk.getNumRows();
    assert(rows && "read 0 rows from file");

    auto read_columns = dataChunk.detachColumns();
    const auto & columns = getHeader().getColumnsWithTypeAndName();
    const auto & normalized_partition_values = file->getFileNormalizedPartitionValues();

    DB::Columns res_columns;
    res_columns.reserve(columns.size());
    std::ranges::transform(
        columns,
        std::back_inserter(res_columns),
        [&](const auto & column) -> DB::ColumnPtr
        {
            if (auto it = normalized_partition_values.find(boost::to_lower_copy(column.name)); it != normalized_partition_values.end())
                return createPartitionColumn(it->second, column.type, rows);
            if (file->fileMetaColumns().virtualColumn(column.name))
                return file->fileMetaColumns().createMetaColumn(column.name, column.type, rows);
            if (readHeader.has(column.name))
                return read_columns[readHeader.getPositionByName(column.name)];
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Not found column = {} when reading file: {}.", column.name, file->getURIPath());
        });
    return res_columns;
}

DB::ColumnPtr BaseReader::createConstColumn(DB::DataTypePtr data_type, const DB::Field & field, size_t rows)
{
    auto nested_type = DB::removeNullable(data_type);
    auto column = nested_type->createColumnConst(rows, field);

    if (data_type->isNullable())
        column = DB::ColumnNullable::create(column, DB::ColumnUInt8::create(rows, 0));
    return column;
}

DB::ColumnPtr BaseReader::createPartitionColumn(const String & value, const DB::DataTypePtr & type, size_t rows)
{
    if (GlutenStringUtils::isNullPartitionValue(value))
    {
        if (!type->isNullable())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Partition column is null value,but column data type is not nullable.");
        auto nested_type = static_cast<const DB::DataTypeNullable &>(*type).getNestedType();
        auto column = nested_type->createColumnConstWithDefaultValue(rows);
        return DB::ColumnNullable::create(column, DB::ColumnUInt8::create(rows, 1));
    }
    else
    {
        auto field = buildFieldFromString(value, type);
        return createConstColumn(type, field, rows);
    }
}

#define BUILD_INT_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    { \
        type value = 0; \
        DB::readIntText(value, in); \
        return DB::Field(value); \
    }

#define BUILD_FP_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    { \
        type value = 0.0; \
        DB::readFloatText(value, in); \
        return DB::Field(value); \
    }

DB::Field BaseReader::buildFieldFromString(const String & str_value, DB::DataTypePtr type)
{
    using FieldBuilder = std::function<DB::Field(DB::ReadBuffer &, const String &)>;
    static std::map<std::string, FieldBuilder> field_builders
        = {{"Int8", BUILD_INT_FIELD(Int8)},
           {"Int16", BUILD_INT_FIELD(Int16)},
           {"Int32", BUILD_INT_FIELD(Int32)},
           {"Int64", BUILD_INT_FIELD(Int64)},
           {"Float32", BUILD_FP_FIELD(Float32)},
           {"Float64", BUILD_FP_FIELD(Float64)},
           {"String", [](DB::ReadBuffer &, const String & val) { return DB::Field(val); }},
           {"Date",
            [](DB::ReadBuffer & in, const String &)
            {
                DayNum value;
                readDateText(value, in);
                return DB::Field(value);
            }},
           {"Date32",
            [](DB::ReadBuffer & in, const String &)
            {
                ExtendedDayNum value;
                readDateText(value, in);
                return DB::Field(value.toUnderType());
            }},
           {"Bool",
            [](DB::ReadBuffer & in, const String &)
            {
                bool value;
                readBoolTextWord(value, in, true);
                return DB::Field(value);
            }},
           {"DateTime64(6)",
            [](DB::ReadBuffer &, const String & s)
            {
                std::string decoded; // s: "2023-07-12 05%3A05%3A33.798" (spark encoded it) => decoded: "2023-07-12 05:05:33.798"
                Poco::URI::decode(s, decoded);

                std::string to_read;
                if (decoded.length() > 23) // we see cases when spark mistakely? encode the URI twice, so we need to decode twice
                    Poco::URI::decode(decoded, to_read);
                else
                    to_read = decoded;

                DB::ReadBufferFromString read_buffer(to_read);
                DB::DateTime64 value;
                DB::readDateTime64Text(value, 6, read_buffer);
                return DB::Field(value);
            }}

        };

    auto nested_type = DB::removeNullable(type);
    DB::ReadBufferFromString read_buffer(str_value);
    auto it = field_builders.find(nested_type->getName());
    if (it == field_builders.end())
    {
        DB::WhichDataType which(nested_type->getTypeId());
        if (which.isDecimal32())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal32> &>(*nested_type);
            DB::Decimal32 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal32>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal64())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal64> &>(*nested_type);
            DB::Decimal64 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal64>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal128())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal128> &>(*nested_type);
            DB::Decimal128 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal128>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal256> &>(*nested_type);
            DB::Decimal256 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal256>(value, dataTypeDecimal.getScale());
        }

        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported data type {}", nested_type->getName());
    }
    return it->second(read_buffer, str_value);
}

ConstColumnsFileReader::ConstColumnsFileReader(const FormatFilePtr & file_, const DB::Block & header_, size_t blockSize)
    : BaseReader(file_, {}, header_), remained_rows(file->getTotalRows().value()), block_size(blockSize)
{
}

bool ConstColumnsFileReader::pull(DB::Chunk & chunk)
{
    if (isCancelled())
        return false;

    if (!remained_rows)
        return false;

    size_t to_read_rows = 0;
    if (remained_rows < block_size)
    {
        to_read_rows = remained_rows;
        remained_rows = 0;
    }
    else
    {
        to_read_rows = block_size;
        remained_rows -= block_size;
    }

    /// If the original output header is empty, build a block to represent the row count.
    DB::Columns res_columns
        = getHeader().columns() > 0 ? addVirtualColumn({}, to_read_rows) : BlockUtil::buildRowCountChunk(to_read_rows).detachColumns();

    chunk = DB::Chunk(std::move(res_columns), to_read_rows);
    return true;
}


NormalFileReader::NormalFileReader(
    const FormatFilePtr & file_,
    const DB::Block & to_read_header_,
    const DB::Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_)
    : BaseReader(file_, to_read_header_, output_header_), input_format(input_format_)
{
    assert(input_format);
}

bool NormalFileReader::pull(DB::Chunk & chunk)
{
    if (isCancelled())
        return false;

    /// read read real data chunk from input.
    DB::Chunk dataChunk = doPull();
    const size_t rows = dataChunk.getNumRows();
    if (!rows)
        return false;

    chunk = DB::Chunk(addVirtualColumn(std::move(dataChunk)), rows);
    return true;
}

DB::Block BaseReader::buildRowCountHeader(const DB::Block & header)
{
    return !header.empty() ? header : BlockUtil::buildRowCountHeader();
}

namespace
{
/// Factory method to create a reader for normal file, iceberg file or delta file
///
std::unique_ptr<NormalFileReader> createNormalFileReader(
    const FormatFilePtr & file,
    const DB::Block & to_read_header_,
    const DB::Block & output_header_,
    const std::shared_ptr<const DB::ActionsDAG> & filter_actions_dag = nullptr,
    const ColumnIndexFilterPtr & column_index_filter = nullptr)
{
    file->initialize(column_index_filter);
    auto createInputFormat = [&](const DB::Block & new_read_header_) -> FormatFile::InputFormatPtr
    { return file->createInputFormat(new_read_header_, filter_actions_dag); };

    if (file->getFileInfo().has_iceberg())
        return iceberg::IcebergReader::create(file, to_read_header_, output_header_, createInputFormat);

    auto input_format = createInputFormat(to_read_header_);

    if (!input_format)
        return nullptr;

    // when there is a '__delta_internal_is_row_deleted' column, it needs to use DeltaReader to read data and add column
    if (!file->getFileInfo().has_paimon() && DeltaVirtualMeta::hasMetaColumns(to_read_header_))
    {
        String row_index_ids_encoded;
        String row_index_filter_type;
        if (file->getFileInfo().other_const_metadata_columns_size())
        {
            for (const auto & column : file->getFileInfo().other_const_metadata_columns())
            {
                if (column.key() == DeltaVirtualMeta::DeltaDVBitmapConfig::DELTA_ROW_INDEX_FILTER_ID_ENCODED)
                    row_index_ids_encoded = toString(column.value());
                if (column.key() == DeltaVirtualMeta::DeltaDVBitmapConfig::DELTA_ROW_INDEX_FILTER_TYPE)
                    row_index_filter_type = toString(column.value());
            }
        }
        return delta::DeltaReader::create(
            file, to_read_header_, output_header_, input_format, row_index_ids_encoded, row_index_filter_type);
    }

    return std::make_unique<NormalFileReader>(file, to_read_header_, output_header_, input_format);
}
}

/// TODO Remove ColumnIndexFilterPtr
std::unique_ptr<BaseReader> BaseReader::create(
    const FormatFilePtr & current_file,
    const DB::Block & readHeader,
    const DB::Block & outputHeader,
    const std::shared_ptr<const DB::ActionsDAG> & filter_actions_dag,
    const ColumnIndexFilterPtr & column_index_filter)
{
    if (readHeader.empty())
    {
        if (auto totalRows = current_file->getTotalRows())
            return std::make_unique<ConstColumnsFileReader>(current_file, outputHeader, *totalRows);
        else
        {
            /// If we can't get total rows from file metadata (i.e. text/json format file), adding a dummy column to
            /// indicate the number of rows.
            return createNormalFileReader(current_file, buildRowCountHeader(readHeader), buildRowCountHeader(outputHeader));
        }
    }

    return createNormalFileReader(current_file, readHeader, outputHeader, filter_actions_dag, column_index_filter);
}


}
