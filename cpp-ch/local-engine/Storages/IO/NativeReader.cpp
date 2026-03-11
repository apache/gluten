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
#include "NativeReader.h"

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Storages/IO/AggregateSerializationUtils.h>
#include <Storages/IO/NativeWriter.h>
#include <jni/jni_common.h>
#include <Common/Arena.h>
#include <Common/JNIUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_INDEX;
extern const int LOGICAL_ERROR;
extern const int CANNOT_READ_ALL_DATA;
extern const int INCORRECT_DATA;
extern const int TOO_LARGE_ARRAY_SIZE;
}
}

using namespace DB;

namespace local_engine
{

Block NativeReader::getHeader() const
{
    return header;
}

DB::Block NativeReader::read()
{
    DB::Block result_block;
    if (istr.eof())
        return result_block;

    if (columns_parse_util.empty())
    {
        result_block = prepareByFirstBlock();
        if (result_block.empty())
            return {};
    }
    else
    {
        result_block = header.cloneEmpty();
    }

    /// Append small blocks into a large one, could reduce memory allocations overhead.
    while (result_block.rows() < max_block_size && result_block.bytes() < max_block_bytes)
        if (!appendNextBlock(result_block))
            break;

    if (result_block.rows())
    {
        for (size_t i = 0; i < result_block.columns(); ++i)
        {
            auto & column_parse_util = columns_parse_util[i];
            auto & column = result_block.getByPosition(i);
            column_parse_util.avg_value_size_hint = column.column->byteSize() / column.column->size();
        }
    }
    return result_block;
}

static void
readFixedSizeAggregateData(DB::ReadBuffer & in, DB::ColumnPtr & column, size_t rows, NativeReader::ColumnParseUtil & column_parse_util)
{
    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(*column->assumeMutable());
    auto & arena = real_column.createOrGetArena();
    ColumnAggregateFunction::Container & vec = real_column.getData();
    size_t initial_size = vec.size();
    vec.reserve_exact(initial_size + rows);
    for (size_t i = 0; i < rows; ++i)
    {
        AggregateDataPtr place = arena.alignedAlloc(column_parse_util.aggregate_state_size, column_parse_util.aggregate_state_align);
        column_parse_util.aggregate_function->create(place);
        auto n = in.read(place, column_parse_util.aggregate_state_size);
        chassert(n == column_parse_util.aggregate_state_size);
        vec.push_back(place);
    }
}

static void
readVarSizeAggregateData(DB::ReadBuffer & in, DB::ColumnPtr & column, size_t rows, NativeReader::ColumnParseUtil & column_parse_util)
{
    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(*column->assumeMutable());
    auto & arena = real_column.createOrGetArena();
    ColumnAggregateFunction::Container & vec = real_column.getData();
    size_t initial_size = vec.size();
    vec.reserve_exact(initial_size + rows);
    for (size_t i = 0; i < rows; ++i)
    {
        AggregateDataPtr place = arena.alignedAlloc(column_parse_util.aggregate_state_size, column_parse_util.aggregate_state_align);
        column_parse_util.aggregate_function->create(place);
        column_parse_util.aggregate_function->deserialize(place, in, std::nullopt, &arena);
        vec.push_back(place);
    }
}

static void
readNormalSimpleData(DB::ReadBuffer & in, DB::ColumnPtr & column, size_t rows, NativeReader::ColumnParseUtil & column_parse_util)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &in; };
    settings.avg_value_size_hint = column_parse_util.avg_value_size_hint;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;

    column_parse_util.serializer->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    column_parse_util.serializer->deserializeBinaryBulkWithMultipleStreams(column, 0, rows, settings, state, nullptr);
}

// May not efficient.
static void
readNormalComplexData(DB::ReadBuffer & in, DB::ColumnPtr & column, size_t rows, NativeReader::ColumnParseUtil & column_parse_util)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &in; };
    settings.avg_value_size_hint = column_parse_util.avg_value_size_hint;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;

    DB::ColumnPtr new_col = column->cloneResized(0);
    column_parse_util.serializer->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    column_parse_util.serializer->deserializeBinaryBulkWithMultipleStreams(new_col, 0, rows, settings, state, nullptr);
    column->assumeMutable()->insertRangeFrom(*new_col, 0, new_col->size());
}

DB::Block NativeReader::prepareByFirstBlock()
{
    if (istr.eof())
        return {};

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    DB::Block result_block;

    size_t columns = 0;
    size_t rows = 0;
    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    if (columns > 1'000'000uz)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many columns in Native format: {}", columns);
    if (rows > 1'000'000'000'000uz)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many rows in Native format: {}", rows);

    if (columns == 0 && header.empty() && rows != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Zero columns but {} rows in Native format.", rows);

    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column;
        ColumnParseUtil column_parse_util;

        column.name = "col_" + std::to_string(i);

        /// Type
        String type_name;
        readBinary(type_name, istr);
        bool agg_opt_column = false;
        String real_type_name = type_name;
        if (type_name.ends_with(NativeWriter::AGG_STATE_SUFFIX))
        {
            agg_opt_column = true;
            real_type_name = type_name.substr(0, type_name.length() - NativeWriter::AGG_STATE_SUFFIX.length());
        }
        column.type = data_type_factory.get(real_type_name);
        auto nested_type = DB::removeNullable(column.type);
        bool is_agg_state_type = WhichDataType(column.type).isAggregateFunction();
        SerializationPtr serialization = column.type->getDefaultSerialization();

        column_parse_util.type = column.type;
        column_parse_util.name = column.name;
        column_parse_util.serializer = serialization;

        /// Data
        ColumnPtr read_column = column.type->createColumn(*serialization);

        if (rows) /// If no rows, nothing to read.
        {
            if (is_agg_state_type && agg_opt_column)
            {
                const DataTypeAggregateFunction * agg_type = checkAndGetDataType<DataTypeAggregateFunction>(column.type.get());
                auto aggregate_function = agg_type->getFunction();

                column_parse_util.aggregate_function = aggregate_function;
                column_parse_util.aggregate_state_size = aggregate_function->sizeOfData();
                column_parse_util.aggregate_state_align = aggregate_function->alignOfData();

                bool fixed = isFixedSizeAggregateFunction(aggregate_function);
                if (fixed)
                {
                    readFixedSizeAggregateData(istr, read_column, rows, column_parse_util);
                    column_parse_util.parse = readFixedSizeAggregateData;
                }
                else
                {
                    readVarSizeAggregateData(istr, read_column, rows, column_parse_util);
                    column_parse_util.parse = readVarSizeAggregateData;
                }
            }
            else if (DB::isNativeNumber(*nested_type) || DB::isStringOrFixedString(*nested_type))
            {
                readNormalSimpleData(istr, read_column, rows, column_parse_util);
                column_parse_util.parse = readNormalSimpleData;
            }
            else
            {
                readNormalComplexData(istr, read_column, rows, column_parse_util);
                column_parse_util.parse = readNormalComplexData;
            }
        }
        column.column = std::move(read_column);
        result_block.insert(std::move(column));
        columns_parse_util.emplace_back(column_parse_util);
    }

    header = result_block.cloneEmpty();
    return result_block;
}

bool NativeReader::appendNextBlock(DB::Block & result_block)
{
    if (istr.eof())
        return false;

    size_t columns = 0;
    size_t rows = 0;
    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    for (size_t i = 0; i < columns; ++i)
    {
        // Not actually read type name.
        size_t type_name_len = 0;
        readVarUInt(type_name_len, istr);
        istr.ignore(type_name_len);

        if (!rows) [[unlikely]]
            continue;

        auto & column_parse_util = columns_parse_util[i];
        auto & column = result_block.getByPosition(i);
        column_parse_util.parse(istr, column.column, rows, column_parse_util);
    }

    return true;
}

jclass ReadBufferFromJavaInputStream::input_stream_class = nullptr;
jmethodID ReadBufferFromJavaInputStream::input_stream_read = nullptr;

bool ReadBufferFromJavaInputStream::nextImpl()
{
    int count = readFromJava();
    if (count > 0)
        working_buffer.resize(count);
    return count > 0;
}

int ReadBufferFromJavaInputStream::readFromJava() const
{
    GET_JNIENV(env)
    jint count = safeCallIntMethod(env, input_stream, input_stream_read, buffer);

    if (count > 0)
        env->GetByteArrayRegion(buffer, 0, count, reinterpret_cast<jbyte *>(internal_buffer.begin()));

    CLEAN_JNIENV
    return count;
}

ReadBufferFromJavaInputStream::ReadBufferFromJavaInputStream(jobject input_stream_, jbyteArray buffer_, const size_t buffer_size_)
    : input_stream(input_stream_), buffer(buffer_), buffer_size(buffer_size_)
{
}

ReadBufferFromJavaInputStream::~ReadBufferFromJavaInputStream()
{
}

}
