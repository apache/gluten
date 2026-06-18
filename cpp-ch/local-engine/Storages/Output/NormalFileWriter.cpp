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
#include "NormalFileWriter.h"

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Interpreters/castColumn.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Poco/URI.h>
#include <Common/DebugUtils.h>
#include <Common/logger_useful.h>

namespace local_engine
{

using namespace DB;

const std::string WriteStatsBase::NO_PARTITION_ID{"__NO_PARTITION_ID__"};
const std::string SparkPartitionedBaseSink::DEFAULT_PARTITION_NAME{"__HIVE_DEFAULT_PARTITION__"};
const std::string SparkPartitionedBaseSink::BUCKET_COLUMN_NAME{"__bucket_value__"};
const std::vector<std::string> FileNameGenerator::SUPPORT_PLACEHOLDERS{"{id}", "{bucket}"};

/// For Nullable(Map(K, V)) or Nullable(Array(T)), if the i-th row is null, we must make sure its nested data is empty.
/// It is for ORC/Parquet writing compatiability. For more details, refer to
/// https://github.com/apache/gluten/issues/8022 and https://github.com/apache/gluten/issues/8021
static ColumnPtr truncateNestedDataIfNull(const ColumnPtr & column)
{
    if (const auto * col_const = checkAndGetColumn<ColumnConst>(column.get()))
    {
        size_t s = col_const->size();
        auto new_data = truncateNestedDataIfNull(col_const->getDataColumnPtr());
        return ColumnConst::create(std::move(new_data), s);
    }
    else if (const auto * col_array = checkAndGetColumn<ColumnArray>(column.get()))
    {
        auto new_data = truncateNestedDataIfNull(col_array->getDataPtr());
        return ColumnArray::create(std::move(new_data), col_array->getOffsetsPtr());
    }
    else if (const auto * col_map = checkAndGetColumn<ColumnMap>(column.get()))
    {
        auto new_nested = truncateNestedDataIfNull(col_map->getNestedColumnPtr());
        return ColumnMap::create(std::move(new_nested));
    }
    else if (const auto * col_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
    {
        Columns new_columns;
        for (size_t i = 0; i < col_tuple->tupleSize(); ++i)
            new_columns.emplace_back(truncateNestedDataIfNull(col_tuple->getColumnPtr(i)));
        return ColumnTuple::create(std::move(new_columns));
    }
    else if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        const auto & null_map = col_nullable->getNullMapData();
        auto nested = truncateNestedDataIfNull(col_nullable->getNestedColumnPtr());
        const auto * nested_array = checkAndGetColumn<ColumnArray>(nested.get());
        const auto * nested_map = checkAndGetColumn<ColumnMap>(nested.get());
        const auto * nested_tuple = checkAndGetColumn<ColumnTuple>(nested.get());

        if (!memoryIsZero(null_map.data(), 0, null_map.size()) && (nested_array || nested_map || nested_tuple))
        {
            /// Process Nullable(Array) or Nullable(Map)
            if (nested_array || nested_map)
            {
                if (!nested_array)
                    nested_array = checkAndGetColumn<ColumnArray>(&nested_map->getNestedColumn());

                const auto & offsets = nested_array->getOffsets();
                size_t total_data_size = 0;
                for (size_t i = 0; i < null_map.size(); ++i)
                    total_data_size += (offsets[i] - offsets[i - 1]) * (!null_map[i]);

                auto new_nested_array = nested_array->cloneEmpty();
                new_nested_array->reserve(nested_array->size());
                auto & new_nested_array_data = assert_cast<ColumnArray &>(*new_nested_array).getData();
                new_nested_array_data.reserve(total_data_size);

                for (size_t i = 0; i < null_map.size(); ++i)
                    if (null_map[i])
                        new_nested_array->insertDefault();
                    else
                        new_nested_array->insertFrom(*nested_array, i);

                if (nested_map)
                {
                    auto new_nested_map = ColumnMap::create(std::move(new_nested_array));
                    return ColumnNullable::create(std::move(new_nested_map), col_nullable->getNullMapColumnPtr());
                }
                else
                {
                    return ColumnNullable::create(std::move(new_nested_array), col_nullable->getNullMapColumnPtr());
                }
            }
            else
            {
                /// Process Nullable(Tuple)
                const auto & nested_columns = nested_tuple->getColumns();
                Columns new_nested_columns(nested_columns.size());
                for (size_t i = 0; i < nested_columns.size(); ++i)
                {
                    const auto & nested_column = nested_columns[i];
                    TypeIndex type_index = nested_column->getDataType();
                    if (const auto * nullable_nested_column = checkAndGetColumn<ColumnNullable>(nested_column.get()))
                        type_index = nullable_nested_column->getNestedColumnPtr()->getDataType();

                    bool should_truncate = type_index == TypeIndex::Array || type_index == TypeIndex::Map || type_index == TypeIndex::Tuple;
                    if (should_truncate)
                    {
                        auto new_nested_column = nested_column->cloneEmpty();
                        new_nested_column->reserve(nested_column->size());
                        for (size_t j = 0; j < null_map.size(); ++j)
                        {
                            if (null_map[j])
                                new_nested_column->insertDefault();
                            else
                                new_nested_column->insertFrom(*nested_column, j);
                        }
                        new_nested_columns[i] = std::move(new_nested_column);
                    }
                    else
                    {
                        new_nested_columns[i] = nested_column;
                    }
                }

                auto new_nested_tuple = ColumnTuple::create(std::move(new_nested_columns));
                return ColumnNullable::create(std::move(new_nested_tuple), col_nullable->getNullMapColumnPtr());
            }
        }
        else
        {
            auto new_nested = truncateNestedDataIfNull(nested);
            return ColumnNullable::create(std::move(new_nested), col_nullable->getNullMapColumnPtr());
        }
    }
    else
        return column;
}

NormalFileWriter::NormalFileWriter(const OutputFormatFilePtr & file_, const DB::ContextPtr & context_) : file(file_), context(context_)
{
}

DB::Block NormalFileWriter::castBlock(const DB::Block & block) const
{
    if (block.empty())
        return block;

    Block res = block;

    /// In case input block didn't have the same types as the preferred schema, we cast the input block to the preferred schema.
    /// Notice that preferred_schema is the actual file schema, which is also the data schema of current inserted table.
    /// Refer to issue: https://github.com/apache/gluten/issues/6588
    size_t index = 0;
    const auto & preferred_schema = file->getPreferredSchema();
    for (auto & column : res)
    {
        if (column.name.starts_with(SparkPartitionedBaseSink::BUCKET_COLUMN_NAME))
            continue;

        const auto & preferred_column = preferred_schema.getByPosition(index++);
        /// Make sure nested array or map data is empty when the row is null in Nullable(Map(K, V)) or Nullable(Array(T)).
        column.column = truncateNestedDataIfNull(column.column);
        column.column = DB::castColumn(column, preferred_column.type);
        column.name = preferred_column.name;
        column.type = preferred_column.type;
    }
    return res;
}

void NormalFileWriter::write(const DB::Block & block)
{
    if (!writer) [[unlikely]]
    {
        // init the writer at first block
        output_format = file->createOutputFormat(block.cloneEmpty());
        pipeline = std::make_unique<DB::QueryPipeline>(output_format->output);
        writer = std::make_unique<DB::PushingPipelineExecutor>(*pipeline);
    }

    /// Although gluten will append MaterializingTransform to the end of the pipeline before native insert in most cases, there are some cases in which MaterializingTransform won't be appended.
    /// e.g. https://github.com/oap-project/gluten/issues/2900
    /// So we need to do materialize here again to make sure all blocks passed to native writer are all materialized.
    /// Note: duplicate materialization on block doesn't has any side affect.
    writer->push(materializeBlock(castBlock(block)));
}

void NormalFileWriter::close()
{
    /// When insert into a table with empty dataset, NormalFileWriter::consume would be never called.
    /// So we need to skip when writer is nullptr.
    if (writer)
    {
        writer->finish();
        assert(output_format);
        output_format->finalizeOutput();
    }
}

OutputFormatFilePtr createOutputFormatFile(
    const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint)
{
    // the passed in file_uri is exactly what is expected to see in the output folder
    // e.g /xxx/中文/timestamp_field=2023-07-13 03%3A00%3A17.622/abc.parquet
    LOG_INFO(&Poco::Logger::get("FileWriterWrappers"), "Create native writer, format_hint: {}, file: {}", format_hint, file_uri);
    std::string encoded;
    Poco::URI::encode(file_uri, "", encoded); // encode the space and % seen in the file_uri
    Poco::URI poco_uri(encoded);
    auto write_buffer_builder = WriteBufferBuilderFactory::instance().createBuilder(poco_uri.getScheme(), context);
    return OutputFormatFileUtil::createFile(context, write_buffer_builder, encoded, preferred_schema, format_hint);
}

std::unique_ptr<NativeOutputWriter> NormalFileWriter::create(
    const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint)
{
    assert(context);
    return std::make_unique<NormalFileWriter>(createOutputFormatFile(context, file_uri, preferred_schema, format_hint), context);
}

}
