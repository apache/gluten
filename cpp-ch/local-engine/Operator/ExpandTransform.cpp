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
#include "ExpandTransform.h"

#include <memory>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
ExpandTransform::ExpandTransform(const DB::SharedHeader & input_, const DB::SharedHeader & output_, const ExpandField & project_set_exprs_)
    : DB::IProcessor({input_}, {output_}), project_set_exprs(project_set_exprs_)
{
}

ExpandTransform::Status ExpandTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished() || isCancelled())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_chunk = input.pull(true);
        has_input = true;
        expand_expr_iterator = 0;
    }

    return Status::Ready;
}

void ExpandTransform::work()
{
    if (expand_expr_iterator >= project_set_exprs.getExpandRows())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "expand_expr_iterator >= project_set_exprs.getExpandRows()");

    const auto & input_header = getInputs().front().getHeader();
    const auto & input_columns = input_chunk.getColumns();
    const auto & types = project_set_exprs.getTypes();
    const auto & kinds = project_set_exprs.getKinds()[expand_expr_iterator];
    const auto & fields = project_set_exprs.getFields()[expand_expr_iterator];
    size_t rows = input_chunk.getNumRows();

    DB::Columns columns(types.size());
    for (size_t col_i = 0; col_i < types.size(); ++col_i)
    {
        const auto & type = types[col_i];
        const auto & kind = kinds[col_i];
        const auto & field = fields[col_i];

        if (kind == EXPAND_FIELD_KIND_SELECTION)
        {
            auto index = field.safeGet<Int32>();
            const auto & input_column = input_columns[index];

            DB::ColumnWithTypeAndName input_arg;
            input_arg.column = input_column;
            input_arg.type = input_header.getByPosition(index).type;
            /// input_column maybe non-Nullable
            columns[col_i] = DB::castColumn(input_arg, type)->convertToFullColumnIfConst();
        }
        else if (kind == EXPAND_FIELD_KIND_LITERAL)
        {
            /// Add const column with field value
            auto column = type->createColumnConst(rows, field)->convertToFullColumnIfConst();
            columns[col_i] = std::move(column);
        }
        else
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown ExpandFieldKind {}", magic_enum::enum_name(kind));
    }

    output_chunk = DB::Chunk(std::move(columns), rows);
    has_output = true;

    ++expand_expr_iterator;
    has_input = expand_expr_iterator < project_set_exprs.getExpandRows();
}
}
