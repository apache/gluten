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
#pragma once
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Formats/FormatSettings.h>

namespace local_engine
{

template <typename Reader>
static inline void excelRead(DB::IColumn & column, Reader && reader)
{
    auto & column_string = assert_cast<DB::ColumnString &>(column);
    DB::ColumnString::Chars & data = column_string.getChars();
    DB::ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        reader(data);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

template <typename Vector, bool include_quotes = false>
void readExcelCSVQuoteString(Vector & s, DB::ReadBuffer & buf, const char delimiter, const String & escape_value, const char & quote);
template <typename Vector>
void readExcelCSVStringInto(Vector & s, DB::ReadBuffer & buf, const DB::FormatSettings::CSV & settings, const String & escape_value);


void deserializeExcelStringTextCSV(
    DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const String & escape_value);


}
