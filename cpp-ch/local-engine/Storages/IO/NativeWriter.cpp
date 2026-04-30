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
#include "NativeWriter.h"
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Storages/IO/AggregateSerializationUtils.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>


using namespace DB;

namespace local_engine
{

const String NativeWriter::AGG_STATE_SUFFIX= "#optagg";

void NativeWriter::flush()
{
    ostr.next();
}

static void writeData(const ISerialization & serialization, const ColumnPtr & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column = column->convertToFullColumnIfReplicated()->convertToFullColumnIfConst();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0;

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization.serializeBinaryBulkStatePrefix(*full_column, settings, state);
    serialization.serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    serialization.serializeBinaryBulkStateSuffix(settings, state);
}

size_t NativeWriter::write(const DB::Block & block)
{
    size_t written_before = ostr.count();

    block.checkNumberOfRows();

    /// Dimensions
    size_t columns = block.columns();
    size_t rows = block.rows();

    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    for (size_t i = 0; i < columns; ++i)
    {
        auto column = block.safeGetByPosition(i);
        /// agg state will convert to fixedString, need write actual agg state type
        auto original_type = header.safeGetByPosition(i).type;
        original_type = recursiveRemoveLowCardinality(original_type);
        /// Type
        String type_name = original_type->getName();
        bool is_agg_opt = WhichDataType(original_type).isAggregateFunction()
            && header.safeGetByPosition(i).column->getDataType() != block.safeGetByPosition(i).column->getDataType();
        if (is_agg_opt)
        {
            writeStringBinary(type_name + AGG_STATE_SUFFIX, ostr);
        }
        else
        {
            writeStringBinary(type_name, ostr);
        }

        column.column = recursiveRemoveSparse(column.column);
        column.column = recursiveRemoveLowCardinality(column.column);
        SerializationPtr serialization = recursiveRemoveLowCardinality(column.type)->getDefaultSerialization();
        /// Data
        if (rows)    /// Zero items of data is always represented as zero number of bytes.
        {
            const auto * agg_type = checkAndGetDataType<DataTypeAggregateFunction>(original_type.get());
            if (is_agg_opt && agg_type && !isFixedSizeAggregateFunction(agg_type->getFunction()))
            {
                const auto * str_col = static_cast<const ColumnString *>(column.column.get());
                const PaddedPODArray<UInt8> & column_chars = str_col->getChars();
                ostr.write(column_chars.raw_data(), str_col->getOffsets().back());
            }
            else
            {
                writeData(*serialization, column.column, ostr, 0, 0);
            }
        }
    }

    size_t written_after = ostr.count();
    size_t written_size = written_after - written_before;
    return written_size;
}
}
