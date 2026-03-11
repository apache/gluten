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
#include <cerrno>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionSQLJSON.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Interpreters/Context.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <base/find_symbols.h>
#include <base/range.h>
#include <Poco/Logger.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool allow_simdjson;
extern const SettingsUInt64 max_parser_depth;
extern const SettingsUInt64 max_parser_backtracks;
}
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}
namespace local_engine
{
// We notice that, `get_json_object` have different behavior with `JSON_VALUE/JSON_QUERY`.
// - ('{"x":[{"y":1},{"y":2}]}' '$.x[*].y'), `json_value` return only one element, but `get_json_object` return
//   return a list.
// - ('{"x":[{"y":1}]}' '$.x[*].y'), `json_query`'s result is '[1]',
//   but `get_json_object`'s result is '1'
//

struct GetJsonObject
{
    static constexpr auto name{"get_json_object"};
};

class JSONTextNormalizer
{
public:
    // simd json will fail to parse the json text on some cases, see #7014, #3750, #3337, #5303
    // To keep the result same with vanilla, we normalize the json string when simd json fails.
    // It returns null when normalize the json text fail, otherwise returns a position among `pos`
    // and `end` which points to the whole json object end.
    // `dst` refer to a memory buffer that is used to store the normalization result.
    static const char * normalize(const char * pos, const char * end, char *& dst)
    {
        pos = normalizeWhitespace(pos, end, dst);
        if (!pos || pos >= end)
            return nullptr;
        if (*pos == '[')
            return normalizeArray(pos, end, dst);
        else if (*pos == '{')
            return normalizeObject(pos, end, dst);
        return nullptr;
    }

private:
    inline static void copyToDst(char *& p, char c)
    {
        *p = c;
        p++;
    }

    inline static void copyToDst(char *& p, const char * src, size_t len)
    {
        memcpy(p, src, len);
        p += len;
    }

    inline static bool isExpectedChar(char c, const char * pos, const char * end) { return pos && pos < end && *pos == c; }

    inline static const char * normalizeWhitespace(const char * pos, const char * end, char *& dst)
    {
        const auto * start_pos = pos;
        while (pos && pos < end)
        {
            if (isWhitespaceASCII(*pos))
                pos++;
            else
                break;
        }
        if (pos != start_pos)
            copyToDst(dst, start_pos, pos - start_pos);
        return pos;
    }

    inline static const char * normalizeComma(const char * pos, const char * end, char *& dst)
    {
        pos = normalizeWhitespace(pos, end, dst);
        if (!isExpectedChar(',', pos, end)) [[unlikely]]
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeComma. not ,");
            return nullptr;
        }
        pos += 1;
        copyToDst(dst, ',');
        return normalizeWhitespace(pos, end, dst);
    }

    inline static const char * normalizeColon(const char * pos, const char * end, char *& dst)
    {
        pos = normalizeWhitespace(pos, end, dst);
        if (!isExpectedChar(':', pos, end))
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeColon. not :");
            return nullptr;
        }
        pos += 1;
        copyToDst(dst, ':');
        return normalizeWhitespace(pos, end, dst);
    }

    inline static const char * normalizeField(const char * pos, const char * end, char *& dst)
    {
        const auto * start_pos = pos;
        pos = find_first_symbols<',', '}', ']'>(pos, end);
        if (pos >= end) [[unlikely]]
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeField. not field");
            return nullptr;
        }
        if (*start_pos == '"' || *start_pos == '\'')
        {
            copyToDst(dst, start_pos, pos - start_pos);
        }
        else
        {
            // If it's a too large number, replace it with "Infinity".
            const char * inf_str = "\"\\\"Infinity\\\"\"";
            size_t inf_str_len = 14;
            const char * large_e = "308";
            const auto * ep = find_first_symbols<'e', 'E'>(start_pos, pos);
            if (pos - ep < 3)
                copyToDst(dst, start_pos, pos - start_pos);
            else if (pos - ep > 4 || (pos - ep == 4 and memcmp(ep + 1, large_e, 3) >= 0))
            {
                if (isTooLargeNumber(start_pos, pos))
                {
                    copyToDst(dst, inf_str, inf_str_len);
                }
                else
                {
                    copyToDst(dst, start_pos, pos - start_pos);
                }
            }
            else
            {
                copyToDst(dst, start_pos, pos - start_pos);
            }
        }
        return pos;
    }

    inline static bool isTooLargeNumber(const char * start, const char * end)
    {
        bool res = false;
        try
        {
            double num2 = std::stod(String(start, end));
        }
        catch (const std::invalid_argument & e)
        {
            res = false;
        }
        catch (const std::out_of_range & e)
        {
            res = true;
        }
        return res;
    }

    inline static const char * normalizeString(const char * pos, const char * end, char *& dst)
    {
        const auto * start_pos = pos;
        if (!isExpectedChar('"', pos, end)) [[unlikely]]
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeString. not \"");
            return nullptr;
        }
        pos += 1;

        do
        {
            pos = find_first_symbols<'\\', '"'>(pos, end);
            if (pos != end && *pos == '\\')
            {
                // escape charaters. e.g. '\"', '\\'
                pos += 2;
                if (pos >= end)
                    return nullptr;
            }
            else
                break;
        } while (pos != end);

        pos = find_first_symbols<'"'>(pos, end);
        if (!isExpectedChar('"', pos, end))
            return nullptr;
        pos += 1;

        size_t n = 0;
        for (; start_pos != pos; ++start_pos)
        {
            if ((*start_pos >= 0x00 && *start_pos <= 0x1f) || *start_pos == 0x7f)
            {
                if (n)
                {
                    copyToDst(dst, start_pos - n, n);
                    n = 0;
                }
                continue;
            }
            else
            {
                n += 1;
            }
        }
        if (n)
            copyToDst(dst, start_pos - n, n);

        return normalizeWhitespace(pos, end, dst);
    }

    /// To use simdjson, we need to convert single quotes to double quotes.
    /// FIXME: It will be OK if we just return a leaf value, but it will have different result for
    /// returning a object with strings which are wrapped by single quotes.
    inline static const char * normalizeSingleQuotesString(const char * pos, const char * end, char *& dst)
    {
        if (!isExpectedChar('\'', pos, end)) [[unlikely]]
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeSingleQuotesString. not '");
            return nullptr;
        }
        pos += 1;
        const auto * start_pos = pos;
        copyToDst(dst, '\"');
        do
        {
            pos = find_first_symbols<'\\', '\''>(pos, end);
            if (pos < end && *pos == '\\')
            {
                // escape charaters. e.g. '\\', '\''
                pos += 2;
                if (pos >= end)
                    return nullptr;
            }
            else
                break;
        } while (pos != end);
        pos = find_first_symbols<'\''>(pos, end);
        if (!isExpectedChar('\'', pos, end))
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeSingleQuotesString. not '");
            return nullptr;
        }
        pos += 1;
        size_t n = 0;
        for (; start_pos != pos; ++start_pos)
        {
            if ((*start_pos >= 0x00 && *start_pos <= 0x1f) || *start_pos == 0x7f)
            {
                if (n)
                {
                    copyToDst(dst, start_pos - n, n);
                    n = 0;
                }
                continue;
            }
            else
            {
                n += 1;
            }
        }
        if (n && n - 1)
            copyToDst(dst, start_pos - n, n - 1);
        copyToDst(dst, '\"');

        return normalizeWhitespace(pos, end, dst);
    }

    static const char * normalizeArray(const char * pos, const char * end, char *& dst)
    {
        if (!isExpectedChar('[', pos, end)) [[unlikely]]
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeArray. not [");
            return nullptr;
        }
        pos += 1;
        copyToDst(dst, '[');

        pos = normalizeWhitespace(pos, end, dst);

        bool has_more = false;
        while (pos && pos < end && *pos != ']')
        {
            has_more = false;
            switch (*pos)
            {
                case '{': {
                    pos = normalizeObject(pos, end, dst);
                    break;
                }
                case '"': {
                    pos = normalizeString(pos, end, dst);
                    break;
                }
                case '\'': {
                    pos = normalizeSingleQuotesString(pos, end, dst);
                    break;
                }
                case '[': {
                    pos = normalizeArray(pos, end, dst);
                    break;
                }
                default: {
                    pos = normalizeField(pos, end, dst);
                    break;
                }
            }
            if (!isExpectedChar(',', pos, end))
                break;
            pos = normalizeComma(pos, end, dst);
            has_more = true;
        }

        if (!isExpectedChar(']', pos, end) || has_more)
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeArray. not ]");
            return nullptr;
        }
        pos += 1;
        copyToDst(dst, ']');
        return normalizeWhitespace(pos, end, dst);
    }

    static const char * normalizeObject(const char * pos, const char * end, char *& dst)
    {
        if (!isExpectedChar('{', pos, end)) [[unlikely]]
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeObject. not object start");
            return nullptr;
        }
        pos += 1;
        copyToDst(dst, '{');

        bool has_more = false;
        while (pos && pos < end && *pos != '}')
        {
            has_more = false;
            pos = normalizeWhitespace(pos, end, dst);
            if (pos != end)
            {
                if (*pos == '\'')
                    pos = normalizeSingleQuotesString(pos, end, dst);
                else if (*pos == '"')
                    pos = normalizeString(pos, end, dst);
                else if (*pos == '}')
                    continue;
                else
                    return nullptr;
            }

            pos = normalizeColon(pos, end, dst);
            if (!pos)
            {
                // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeObject. not :");
                break;
            }

            switch (*pos)
            {
                case '{': {
                    pos = normalizeObject(pos, end, dst);
                    break;
                }
                case '"': {
                    pos = normalizeString(pos, end, dst);
                    break;
                }
                case '\'': {
                    pos = normalizeSingleQuotesString(pos, end, dst);
                    break;
                }
                case '[': {
                    pos = normalizeArray(pos, end, dst);
                    break;
                }
                default: {
                    pos = normalizeField(pos, end, dst);
                    break;
                }
            }

            if (!isExpectedChar(',', pos, end))
                break;
            pos = normalizeComma(pos, end, dst);
            has_more = true;
        }

        if (!isExpectedChar('}', pos, end) || has_more)
        {
            // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalizeObject. not object end");
            return nullptr;
        }
        pos += 1;
        copyToDst(dst, '}');
        return normalizeWhitespace(pos, end, dst);
    }
};

template <typename JSONParser, typename JSONStringSerializer>
class GetJsonObjectImpl
{
public:
    using Element = typename JSONParser::Element;

    static DB::DataTypePtr getReturnType(const char *, const DB::ColumnsWithTypeAndName &, bool)
    {
        auto nested_type = std::make_shared<DB::DataTypeString>();
        return std::make_shared<DB::DataTypeNullable>(nested_type);
    }

    static size_t getNumberOfIndexArguments(const DB::ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    bool insertResultToColumn(DB::IColumn & dest, const Element & root, DB::GeneratorJSONPath<JSONParser> & generator_json_path, bool path_has_asterisk)
    {   
        Element current_element = root;
        DB::VisitorStatus status;
        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        /// Create json array of results: [res1, res2, ...]
        bool success = false;
        std::vector<Element> elements;
        while ((status = generator_json_path.getNextItem(current_element)) != DB::VisitorStatus::Exhausted)
        {
            if (status == DB::VisitorStatus::Ok)
            {
                success = true;
                elements.push_back(current_element);
            }
            else if (status == DB::VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = root;
        }
        if (!success)
        {
            return false;
        }
        DB::ColumnNullable & nullable_col_str = assert_cast<DB::ColumnNullable &>(dest);
        DB::ColumnString * col_str = assert_cast<DB::ColumnString *>(&nullable_col_str.getNestedColumn());
        JSONStringSerializer serializer(*col_str);
        if (elements.size() == 1) [[likely]]
        {
            if (elements[0].isNull())
                return false;
            nullable_col_str.getNullMapData().push_back(0);

            if (elements[0].isString())
            {
                auto str = elements[0].getString();
                if (path_has_asterisk)
                {
                    str = "\"" + std::string(str) + "\"";
                }
                serializer.addRawString(str);
            }
            else
            { 
                serializer.addElement(elements[0]);
            }
        }
        else
        {
            const char * array_begin = "[";
            const char * array_end = "]";
            const char * comma = ",";
            bool flag = false;
            serializer.addRawData(array_begin, 1);
            nullable_col_str.getNullMapData().push_back(0);
            for (auto & element : elements)
            {
                if (flag)
                {
                    serializer.addRawData(comma, 1);
                }
                serializer.addElement(element);
                flag = true;
            }
            serializer.addRawData(array_end, 1);
        }
        serializer.commit();
        return true;
    }
};

/// CH uses the lexer to parse the json path, it's not a good idea.
/// If a json field containt spaces, we wrap it by double quotes.
/// FIXME: If it contains \t, \n, simdjson cannot parse.
class JSONPathNormalizer
{
public:
    static String normalize(const String & json_path_)
    {
        DB::Tokens tokens(json_path_.data(), json_path_.data() + json_path_.size());
        DB::IParser::Pos iter(tokens, 0, 0);
        String res;
        while (iter->type != DB::TokenType::EndOfStream)
        {
            if (isSubPathBegin(iter))
            {
                if (iter->type == DB::TokenType::Number)
                {
                    normalizeOnNumber(iter, res);
                }
                else
                {
                    // It may begins with '=', '==' and so on.
                    res += ".";
                    ++iter;
                    normalizeOnBareWord(iter, res);
                }
            }
            else
                normalizeOnOtherTokens(iter, res);
        }
        return res;
    }

private:
    static std::pair<DB::TokenType, StringRef> prevToken(DB::IParser::Pos & iter, size_t n = 1);

    static std::pair<DB::TokenType, StringRef> nextToken(DB::IParser::Pos & iter, size_t n = 1);

    static bool isSubPathBegin(DB::IParser::Pos & iter);

    static void normalizeOnNumber(DB::IParser::Pos & iter, String & res);

    static void normalizeOnBareWord(DB::IParser::Pos & iter, String & res);
    static void normalizeOnOtherTokens(DB::IParser::Pos & iter, String & res);
};

/// Flatten a json string into a tuple.
/// Not use JSONExtract here, since the json path is a complicated expression.
class FlattenJSONStringOnRequiredFunction : public DB::IFunction
{
public:
    static constexpr auto name = "flattenJSONStringOnRequired";

    static DB::FunctionPtr create(const DB::ContextPtr & context) { return std::make_shared<FlattenJSONStringOnRequiredFunction>(context); }
    explicit FlattenJSONStringOnRequiredFunction(DB::ContextPtr context_) : context(context_) { }
    ~FlattenJSONStringOnRequiredFunction() override = default;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        String json_fields;
        if (const auto * json_fields_col = typeid_cast<const DB::ColumnConst *>(arguments[1].column.get()))
        {
            json_fields = json_fields_col->getDataAt(0).toString();
        }
        else
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must be a non-constant column", getName());
        }

        Poco::StringTokenizer tokenizer(json_fields, "|");
        std::vector<String> names;
        DB::DataTypes types;
        DB::DataTypePtr str_type = std::make_shared<DB::DataTypeString>();
        str_type = DB::makeNullable(str_type);
        for (const auto & field : tokenizer)
        {
            names.push_back(field);
            types.push_back(str_type);
        }
        return std::make_shared<DB::DataTypeTuple>(types, names);
    }

    /// The second argument is required json fields sperated by '|'.
    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
#if USE_SIMDJSON
        if (context->getSettingsRef()[DB::Setting::allow_simdjson])
        {
            return innerExecuteImpl<
                DB::SimdJSONParser,
                GetJsonObjectImpl<DB::SimdJSONParser, DB::JSONStringSerializer<DB::SimdJSONParser::Element, DB::SimdJSONElementFormatter>>>(
                arguments);
        }
#endif
        return innerExecuteImpl<
            DB::DummyJSONParser,
            GetJsonObjectImpl<DB::DummyJSONParser, DB::DefaultJSONStringSerializer<DB::DummyJSONParser::Element>>>(arguments);
    }

private:
    DB::ContextPtr context;
    /// If too many rows cannot be parsed by simdjson directly, we will normalize the json text at first;
    mutable bool is_most_normal_json_text = true;
    mutable size_t total_parsed_rows = 0;
    mutable size_t total_normalized_rows = 0;

    template <typename JSONParser>
    bool safeParseJson(std::string_view str, JSONParser & parser, JSONParser::Element & doc) const
    {
        total_parsed_rows++;
        if (total_parsed_rows > 10000 && total_normalized_rows * 100 / total_parsed_rows > 90)
        {
            is_most_normal_json_text = false;
        }

        bool is_doc_ok = false;
        if (is_most_normal_json_text)
        {
            is_doc_ok = parser.parse(str, doc);
        }
        if (!is_doc_ok && str.size() > 0)
        {
            total_normalized_rows++;
            std::vector<char> buf;
            buf.resize(str.size(), 0);
            char * buf_pos = buf.data();
            const char * pos = JSONTextNormalizer::normalize(str.data(), str.data() + str.size(), buf_pos);
            if (pos)
            {
                std::string n_str(buf.data(), buf_pos - buf.data());
                // LOG_DEBUG(getLogger("GetJsonObject"), "xxx normalize {} to {}", str, n_str);
                is_doc_ok = parser.parse(n_str, doc);
            }
        }
        return is_doc_ok;
    }

    template <typename JSONParser, typename Impl>
    DB::ColumnPtr innerExecuteImpl(const DB::ColumnsWithTypeAndName & arguments) const
    {
        DB::DataTypePtr str_type = std::make_shared<DB::DataTypeString>();
        str_type = DB::makeNullable(str_type);
        DB::MutableColumns tuple_columns;
        std::vector<DB::ASTPtr> json_path_asts;

        std::vector<String> required_fields;
        std::vector<bool> path_has_asterisk;
        const auto & first_column = arguments[0];
        if (const auto * required_fields_col = typeid_cast<const DB::ColumnConst *>(arguments[1].column.get()))
        {
            std::string json_fields = required_fields_col->getDataAt(0).toString();
            Poco::StringTokenizer tokenizer(json_fields, "|");
            bool path_parsed = true;
            for (const auto & field : tokenizer)
            {
                auto normalized_field = JSONPathNormalizer::normalize(field);
                // LOG_ERROR(getLogger("JSONPatch"), "xxx field {} -> {}", field, normalized_field);
                if(normalized_field.find("[*]") != std::string::npos)
                    path_has_asterisk.emplace_back(true);
                else
                    path_has_asterisk.emplace_back(false);

                required_fields.push_back(normalized_field);
                tuple_columns.emplace_back(str_type->createColumn());

                const char * query_begin = reinterpret_cast<const char *>(required_fields.back().c_str());
                const char * query_end = required_fields.back().c_str() + required_fields.back().size();
                DB::Tokens tokens(query_begin, query_end);
                UInt32 max_parser_depth = static_cast<UInt32>(context->getSettingsRef()[DB::Setting::max_parser_depth]);
                UInt32 max_parser_backtracks = static_cast<UInt32>(context->getSettingsRef()[DB::Setting::max_parser_backtracks]);
                DB::IParser::Pos token_iterator(tokens, max_parser_depth, max_parser_backtracks);
                DB::ASTPtr json_path_ast;
                DB::ParserJSONPath path_parser;
                DB::Expected expected;
                if (!path_parser.parse(token_iterator, json_path_ast, expected))
                {
                    path_parsed = false;
                }
                json_path_asts.push_back(json_path_ast);
            }
            if (!path_parsed)
            {
                for (size_t i = 0; i < first_column.column->size(); ++i)
                {
                    for (size_t j = 0; j < tuple_columns.size(); ++j)
                        tuple_columns[j]->insertDefault();
                }
                return DB::ColumnTuple::create(std::move(tuple_columns));
            }
        }
        else
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must be a non-constant column", getName());
        }

        if (!isString(first_column.type))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing JSON, illegal type: "
                "{}",
                String(name),
                first_column.type->getName());

        const DB::ColumnPtr & arg_json = first_column.column;
        const auto * col_json_const = typeid_cast<const DB::ColumnConst *>(arg_json.get());
        const auto * col_json_string
            = typeid_cast<const DB::ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());
        if (!col_json_string)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}", arg_json->getName());
        const DB::ColumnString::Chars & chars = col_json_string->getChars();
        const DB::ColumnString::Offsets & offsets = col_json_string->getOffsets();

        Impl impl;
        JSONParser parser;
        using Element = typename JSONParser::Element;
        Element document;
        bool document_ok = false;
        if (col_json_const)
        {
            std::string_view json{reinterpret_cast<const char *>(chars.data()), offsets[0]};
            document_ok = safeParseJson(json, parser, document);
        }

        size_t tuple_size = tuple_columns.size();
        std::vector<std::shared_ptr<DB::GeneratorJSONPath<JSONParser>>> generator_json_paths;
        std::transform(
            json_path_asts.begin(),
            json_path_asts.end(),
            std::back_inserter(generator_json_paths),
            [](const auto & ast) { return std::make_shared<DB::GeneratorJSONPath<JSONParser>>(ast); });

        for (const auto i : collections::range(0, arguments[0].column->size()))
        {
            if (!col_json_const)
            {
                std::string_view json{reinterpret_cast<const char *>(&chars[offsets[i - 1]]), offsets[i] - offsets[i - 1]};
                document_ok = safeParseJson(json, parser, document);
            }
            if (document_ok)
            {
                for (size_t j = 0; j < tuple_size; ++j)
                {
                    generator_json_paths[j]->reinitialize();
                    if (!impl.insertResultToColumn(*tuple_columns[j], document, *generator_json_paths[j], path_has_asterisk[j]))
                    {
                        tuple_columns[j]->insertDefault();
                    }
                }
            }
            else
            {
                for (size_t j = 0; j < tuple_size; ++j)
                {
                    tuple_columns[j]->insertDefault();
                }
            }
        }

        return DB::ColumnTuple::create(std::move(tuple_columns));
    }
};

}
