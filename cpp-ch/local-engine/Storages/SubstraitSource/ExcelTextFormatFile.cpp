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
#include "ExcelTextFormatFile.h"
#include <memory>
#include <string>
#include <Columns/ColumnNullable.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/PeekableReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <Storages/Serializations/ExcelDecimalSerialization.h>
#include <Storages/Serializations/ExcelSerialization.h>
#include <Storages/Serializations/ExcelStringReader.h>
#include <Common/BlockTypeUtils.h>
#include <Common/GlutenSettings.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}
}

namespace local_engine
{
using namespace DB;
void skipErrorChars(DB::ReadBuffer & buf, bool has_quote, char quote, String & escape, const DB::FormatSettings & settings)
{
    if (has_quote)
    {
        ColumnString::Chars data;
        readExcelCSVQuoteString(data, buf, settings.csv.delimiter, escape, quote);
    }
    else
        /// skip all chars before quote/delimiter exclude line delimiter
        while (!buf.eof() && *buf.position() != settings.csv.delimiter && *buf.position() != '\n' && *buf.position() != '\r')
            ++buf.position();
}

bool ExcelTextFormatFile::useThis(const DB::ContextPtr & context)
{
    return settingsEqual(context->getSettingsRef(), USE_EXCEL_PARSER, "true");
}

FormatFile::InputFormatPtr
ExcelTextFormatFile::createInputFormat(const DB::Block & header, const std::shared_ptr<const DB::ActionsDAG> & /*filter_actions_dag*/)
{
    auto read_buffer = read_buffer_builder->build(file_info);

    DB::FormatSettings format_settings = createFormatSettings();
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams params = {.max_block_size = max_block_size};

    std::shared_ptr<DB::PeekableReadBuffer> buffer = std::make_unique<DB::PeekableReadBuffer>(*read_buffer);
    DB::Names column_names;
    column_names.reserve(file_info.schema().names_size());
    for (const auto & item : file_info.schema().names())
        column_names.push_back(item);

    auto txt_input_format
        = std::make_shared<ExcelRowInputFormat>(toShared(header), buffer, params, format_settings, column_names, file_info.text().escape());
    return std::make_shared<InputFormat>(std::move(read_buffer), txt_input_format);
}

DB::FormatSettings ExcelTextFormatFile::createFormatSettings() const
{
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.csv.trim_whitespaces = true;
    format_settings.with_names_use_header = true;
    format_settings.with_types_use_header = false;
    format_settings.skip_unknown_fields = true;
    std::string delimiter = file_info.text().field_delimiter();
    format_settings.csv.delimiter = *delimiter.data();

    if (file_info.start() == 0)
        format_settings.csv.skip_first_lines = file_info.text().header_lines_to_skip();

    if (delimiter == "\t" || delimiter == " ")
        format_settings.csv.allow_whitespace_or_tab_as_delimiter = true;

    /// `value_treated_as_null` is an `optional` field, so presence -- not emptiness -- says whether
    /// the producer asked for a null representation at all. An explicitly set empty string means
    /// "an empty field is NULL", which is not the same as the field being absent.
    if (file_info.text().has_value_treated_as_null())
        format_settings.csv.null_representation = file_info.text().value_treated_as_null();

    bool empty_as_null = true;
    if (context->getSettingsRef().has(EXCEL_EMPTY_AS_NULL))
        empty_as_null = settingsEqual(context->getSettingsRef(), EXCEL_EMPTY_AS_NULL, "true");

    format_settings.try_infer_integers = false;
    if (!context->getSettingsRef().has(EXCEL_NUMBER_FORCE))
        format_settings.try_infer_integers = true;

    if (settingsEqual(context->getSettingsRef(), EXCEL_NUMBER_FORCE, "true"))
        format_settings.try_infer_integers = true;

    if (format_settings.csv.null_representation.empty() || empty_as_null)
        format_settings.csv.empty_as_default = true;
    else
        format_settings.csv.empty_as_default = false;

    char quote = *file_info.text().quote().data();

    if (quote == '\'')
    {
        format_settings.csv.allow_single_quotes = true;
        format_settings.csv.allow_double_quotes = false;
    }
    else if (quote == '\"')
    {
        /// quote == '"' and default
        format_settings.csv.allow_single_quotes = false;
        format_settings.csv.allow_double_quotes = true;
    }
    else
    {
        format_settings.csv.allow_single_quotes = false;

        if (settingsEqual(context->getSettingsRef(), EXCEL_QUOTE_STRICT, "true"))
            format_settings.csv.allow_double_quotes = false;
        else
            format_settings.csv.allow_double_quotes = true;
    }

    return format_settings;
}


ExcelRowInputFormat::ExcelRowInputFormat(
    const DB::SharedHeader & header_,
    std::shared_ptr<DB::PeekableReadBuffer> & buf_,
    const DB::RowInputFormatParams & params_,
    const DB::FormatSettings & format_settings_,
    DB::Names & input_field_names_,
    String escape_)
    : CSVRowInputFormat(
        header_,
        buf_,
        params_,
        true,
        false,
        format_settings_,
        std::make_unique<ExcelTextFormatReader>(*buf_, input_field_names_, escape_, format_settings_))
    , escape(escape_)
{
    DB::Serializations gluten_serializations;
    for (const auto & item : data_types)
    {
        const DataTypePtr nest_type = item->isNullable() ? static_cast<const DataTypeNullable &>(*item).getNestedType() : item;
        SerializationPtr nest_serialization;
        WhichDataType which(nest_type->getTypeId());
        if (which.isDecimal32())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal32> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal32>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal64())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal64> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal64>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal128())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal128> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal128>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal256> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal256>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else
            nest_serialization = std::make_shared<ExcelSerialization>(nest_type->getDefaultSerialization(), escape);


        if (item->isNullable())
            gluten_serializations.insert(gluten_serializations.end(), std::make_shared<SerializationNullable>(nest_serialization));
        else
            gluten_serializations.insert(gluten_serializations.end(), nest_serialization);
    }

    serializations = gluten_serializations;
}


ExcelTextFormatReader::ExcelTextFormatReader(
    DB::PeekableReadBuffer & buf_, DB::Names & input_field_names_, String escape_, const DB::FormatSettings & format_settings_)
    : CSVFormatReader(buf_, format_settings_), input_field_names(input_field_names_), escape(escape_)
{
}


std::vector<String> ExcelTextFormatReader::readNames()
{
    return input_field_names;
}

std::vector<String> ExcelTextFormatReader::readTypes()
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "ExcelTextRowInputFormat::readTypes is not implemented");
}

bool ExcelTextFormatReader::readField(
    DB::IColumn & column,
    const DB::DataTypePtr & type,
    const DB::SerializationPtr & serialization,
    bool is_last_file_column,
    const String &)
{
    if (isEndOfLine() && format_settings.csv.empty_as_default)
    {
        column.insertDefault();
        return false;
    }

    preSkipNullValue();
    size_t column_size = column.size();

    if (format_settings.csv.trim_whitespaces && isNumber(removeNullable(type)))
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    const bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
    const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n' || *buf->position() == '\r');

    /// Note: Tuples are serialized in CSV as separate columns, but with empty_as_default or null_as_default
    /// only one empty or NULL column will be expected
    if ((at_delimiter || at_last_column_line_end) && (format_settings.csv.empty_as_default || !isStringOrFixedString(removeNullable(type))))
    {
        /// Treat empty unquoted column value as default value, if
        /// specified in the settings. Tuple columns might seem
        /// problematic, because they are never quoted but still contain
        /// commas, which might be also used as delimiters. However,
        /// they do not contain empty unquoted fields, so this check
        /// works for tuples as well.
        column.insertDefault();
        return false;
    }

    char maybe_quote = *buf->position();
    bool has_quote = false;
    if ((format_settings.csv.allow_single_quotes && maybe_quote == '\'')
        || (format_settings.csv.allow_double_quotes && maybe_quote == '\"'))
        has_quote = true;

    auto column_back_func = [&column_size](DB::IColumn & column_back) -> void
    {
        if (column_back.isNullable())
        {
            ColumnNullable & col = assert_cast<ColumnNullable &>(column_back);
            if (col.getNullMapData().size() == column_size + 1)
                col.getNullMapData().pop_back();
            if (col.getNestedColumn().size() == column_size + 1)
                col.getNestedColumn().popBack(1);
        }
    };

    try
    {
        /// Read the column normally.
        serialization->deserializeTextCSV(column, *buf, format_settings);
    }
    catch (Exception & e)
    {
        /// Logic for possible skipping of errors.
        if (!isParseError(e.code()))
            throw;

        skipErrorChars(*buf, has_quote, maybe_quote, escape, format_settings);
        column_back_func(column);
        column.insertDefault();

        return false;
    }

    // See https://github.com/ClickHouse/ClickHouse/pull/60556
    // In case of failing to parse, we will always push element into nullmap.
    // so, we need using nestedColumn to check if error occurs.
    /// FIXME:  move it to ExcelSerialization ???
    const auto nestedColumn = DB::removeNullable(column.getPtr());
    if (column_size == nestedColumn->size())
    {
        skipErrorChars(*buf, has_quote, maybe_quote, escape, format_settings);
        column_back_func(column);
        column.insertDefault();
        return false;
    }

    return true;
}

void ExcelTextFormatReader::skipField()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    ColumnString::Chars data;
    readExcelCSVStringInto(data, *buf, format_settings.csv, escape);
}

void ExcelTextFormatReader::preSkipNullValue()
{
    /// null_representation is empty and value is "" or '' in spark return null
    if(((format_settings.csv.allow_single_quotes && *buf->position() == '\'')
            || (format_settings.csv.allow_double_quotes && *buf->position() == '\"')))
    {
        PeekableReadBufferCheckpoint checkpoint{*buf, false};
        char maybe_quote = *buf->position();
        ++buf->position();

        if (!buf->eof() && *buf->position() == maybe_quote)
            ++buf->position();
        else
        {
            buf->rollbackToCheckpoint();
            return;
        }

        bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
        bool at_line_end = buf->eof() || *buf->position() == '\n' || *buf->position() == '\r';

        if (!at_delimiter && !at_line_end)
            buf->rollbackToCheckpoint();
    }
}

void ExcelTextFormatReader::skipFieldDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    if (!isEndOfLine())
        assertChar(format_settings.csv.delimiter, *buf);
}

bool ExcelTextFormatReader::isEndOfLine()
{
    return buf->eof() || *buf->position() == '\r' || *buf->position() == '\n';
}


void ExcelTextFormatReader::skipRowEndDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    if (buf->eof())
        return;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv.delimiter)
        ++buf->position();

    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    if (buf->eof())
        return;

    if (!isEndOfLine())
    {
        // remove unused chars
        skipField();
        skipRowEndDelimiter();
    }
    else
        skipEndOfLine(*buf);
}

void ExcelTextFormatReader::skipEndOfLine(DB::ReadBuffer & readBuffer)
{
    /// \n (Unix) or \r\n (DOS/Windows) or \n\r (Mac OS Classic)

    if (*readBuffer.position() == '\n')
    {
        ++readBuffer.position();
        if (!readBuffer.eof() && *readBuffer.position() == '\r')
            ++readBuffer.position();
    }
    else if (*readBuffer.position() == '\r')
    {
        ++readBuffer.position();
        if (!readBuffer.eof() && *readBuffer.position() == '\n')
            ++readBuffer.position();
        /// Different with CH master:
        /// removed \r check
    }
    else if (!readBuffer.eof())
        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Expected end of line");
}

inline void ExcelTextFormatReader::skipWhitespacesAndTabs(ReadBuffer & readBuffer, bool allow_whitespace_or_tab_as_delimiter)
{
    if (allow_whitespace_or_tab_as_delimiter)
    {
        return;
    }
    /// Skip `whitespace` symbols allowed readBuffer CSV.
    while (!readBuffer.eof() && (*readBuffer.position() == ' ' || *readBuffer.position() == '\t'))
        ++readBuffer.position();
}


}
