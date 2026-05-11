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
#include "FillingDeltaInternalRowDeletedStep.h"

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/SubstraitSource/Delta/DeltaMeta.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Common/BlockTypeUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

FillingDeltaInternalRowDeletedStep::FillingDeltaInternalRowDeletedStep(const DB::SharedHeader & input_header, const MergeTreeTableInstance & _merge_tree_table, const DB::ContextPtr _context)
    : ITransformingStep(input_header, input_header, getTraits()), merge_tree_table(_merge_tree_table), context(_context)
{
}

DB::Block FillingDeltaInternalRowDeletedStep::transformHeader(const DB::Block & input)
{
    DB::Block output;
    for (int i = 0; i < input.columns(); i++)
    {
        output.insert(input.getByPosition(i));
    }
    return output;
}

void FillingDeltaInternalRowDeletedStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    pipeline.addSimpleTransform([&](const DB::SharedHeader & header) { return std::make_shared<FillingDeltaInternalRowDeletedTransform>(header, merge_tree_table, context); });
}

void FillingDeltaInternalRowDeletedStep::updateOutputHeader()
{
    output_header = toShared(transformHeader(*input_headers.front()));
}

FillingDeltaInternalRowDeletedTransform::FillingDeltaInternalRowDeletedTransform(const DB::SharedHeader & input_header_, const MergeTreeTableInstance & merge_tree_table, const DB::ContextPtr context)
    : ISimpleTransform(input_header_, input_header_, true), read_header(*input_header_)
{
    for (const auto part : merge_tree_table.parts)
    {
        if (!part.row_index_filter_type.empty() && !part.row_index_filter_id_encoded.empty())
        {
            std::shared_ptr<DeltaVirtualMeta::DeltaDVBitmapConfig> bitmap_config =
                            DeltaVirtualMeta::DeltaDVBitmapConfig::parse_config(part.row_index_filter_id_encoded);
            std::unique_ptr<DeltaDVRoaringBitmapArray> bitmap_array = std::make_unique<DeltaDVRoaringBitmapArray>();
            bitmap_array->rb_read(bitmap_config->path_or_inline_dv, bitmap_config->offset, bitmap_config->size_in_bytes, context);
            std::string part_path_key;
            part_path_key.append(merge_tree_table.absolute_path).append("/").append(part.name);
            dv_map.emplace(part_path_key, std::move(bitmap_array));
        }
    }
}

void FillingDeltaInternalRowDeletedTransform::transform(DB::Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    size_t deleted_row_pos = read_header.getPositionByName(DeltaVirtualMeta::DELTA_INTERNAL_IS_ROW_DELETED);
    size_t part_path_key_pos = read_header.getPositionByName(FileMetaColumns::FILE_PATH);
    size_t row_index_pos = read_header.getPositionByName(ParquetVirtualMeta::TMP_ROWINDEX);

    const auto & input_columns = chunk.getColumns();

    auto deleted_row_column_nest = DB::ColumnUInt8::create(num_rows);
    auto & vec = deleted_row_column_nest->getData();

    const DB::ColumnPtr & part_path_key_column = input_columns[part_path_key_pos];
    const auto & part_path_key_column_data = assert_cast<const DB::ColumnLowCardinality&>(*part_path_key_column);
    const DB::ColumnPtr & row_index_column = input_columns[row_index_pos];
    const auto & row_index_column_data = assert_cast<const DB::ColumnUInt64&>(*row_index_column);

    for (size_t i = 0; i < num_rows; ++i)
    {
        const std::string_view str_data = part_path_key_column_data.getDataAt(i);
        std::string part_name = std::string(str_data.data(), str_data.size());
        if (dv_map.contains(part_name))
        {
            vec[i] = dv_map.at(part_name)->rb_contains(row_index_column_data.get64(i));
        }
        else
        {
            vec[i] = false;
        }
    }

    auto nullmap_column = DB::ColumnUInt8::create(deleted_row_column_nest->size(), 0);
    DB::ColumnPtr deleted_row_column = DB::ColumnNullable::create(std::move(deleted_row_column_nest), std::move(nullmap_column));

    DB::Columns output_columns = input_columns;
    output_columns[deleted_row_pos] = std::move(deleted_row_column);
    chunk.setColumns(std::move(output_columns), num_rows);
}
}
