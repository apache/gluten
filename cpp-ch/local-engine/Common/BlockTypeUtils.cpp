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

#include "BlockTypeUtils.h"

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

namespace local_engine
{
DB::Block toSampleBlock(const RowType & type)
{
    DB::ColumnsWithTypeAndName result;
    result.reserve(type.size());
    for (const auto & field : type)
        result.emplace_back(toColumnType(field));
    return result;
}

RowType blockToRowType(const DB::Block & header)
{
    RowType types;
    for (const auto & name : header.getNames())
    {
        const auto * column = header.findByName(name);
        types.push_back(DB::NameAndTypePair(column->name, column->type));
    }
    return types;
}

DB::DataTypePtr wrapNullableType(bool nullable, DB::DataTypePtr nested_type)
{
    if (nullable && !nested_type->isNullable())
    {
        if (nested_type->isLowCardinalityNullable())
            return nested_type;
        if (nested_type->lowCardinality())
            return std::make_shared<DB::DataTypeLowCardinality>(
                std::make_shared<DB::DataTypeNullable>(dynamic_cast<const DB::DataTypeLowCardinality &>(*nested_type).getDictionaryType()));
        return std::make_shared<DB::DataTypeNullable>(nested_type);
    }
    return nested_type;
}

bool onlyHasFlatType(const DB::Block & header)
{
    return std::ranges::all_of(
        header,
        [](DB::ColumnWithTypeAndName const & col)
        {
            const DB::DataTypePtr type_not_nullable = removeNullable(col.type);
            const DB::WhichDataType which(type_not_nullable);
            return !isArray(which) && !isMap(which) && !isTuple(which);
        });
}

}