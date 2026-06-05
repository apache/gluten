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
#include "SparkFunctionGetJsonObject.h"
#include <Functions/FunctionFactory.h>


namespace local_engine
{

std::pair<DB::TokenType, std::string_view> JSONPathNormalizer::prevToken(DB::IParser::Pos & iter, size_t n)
{
    size_t i = 0;
    for (; i < n && iter->type != DB::TokenType::DollarSign; ++i)
    {
        --iter;
    }
    std::pair<DB::TokenType, std::string_view> res = {iter->type, std::string_view(iter->begin, iter->end - iter->begin)};
    for (; i > 0; --i)
    {
        ++iter;
    }
    return res;
}

std::pair<DB::TokenType, std::string_view> JSONPathNormalizer::nextToken(DB::IParser::Pos & iter, size_t n)
{
    size_t i = 0;
    for (; i < n && iter->type != DB::TokenType::EndOfStream; ++i)
    {
        ++iter;
    }
    std::pair<DB::TokenType, std::string_view> res = {iter->type, std::string_view(iter->begin, iter->end - iter->begin)};
    for (; i > 0; --i)
    {
        --iter;
    }
    return res;
}

bool JSONPathNormalizer::isSubPathBegin(DB::IParser::Pos & iter)
{
    if (iter->type == DB::TokenType::Dot || (iter->type == DB::TokenType::Number && *iter->begin == '.'))
    {
        return true;
    }
    return false;
}


void JSONPathNormalizer::normalizeOnNumber(DB::IParser::Pos & iter, String & res)
{
    if (*iter->begin == '.')
    {
        res += ".\"";
        res += String(iter->begin + 1, iter->end);
        ++iter;
        auto token_type = iter->type;
        while (token_type != DB::TokenType::Dot && token_type != DB::TokenType::OpeningSquareBracket
               && token_type != DB::TokenType::EndOfStream)
        {
            auto [_, prev_iter_str] = prevToken(iter);
            // may contains spaces
            if (prev_iter_str.data() + prev_iter_str.size() != iter->begin)
            {
                res += String(prev_iter_str.data() + prev_iter_str.size(), iter->begin);
            }
            res += String(iter->begin, iter->end);
            ++iter;
            token_type = iter->type;
        }
        auto [_, prev_iter_str] = prevToken(iter);
        res += String(prev_iter_str.data() + prev_iter_str.size(), iter->begin);
        res += "\"";
    }
    else
        normalizeOnOtherTokens(iter, res);
}

void JSONPathNormalizer::normalizeOnBareWord(DB::IParser::Pos & iter, String & res)
{
    auto [prev_iter_type_2, _] = nextToken(iter);
    /// e.g. $.data.forecast[?(@.aqi>65)]
    if (prev_iter_type_2 == DB::TokenType::At)
    {
        normalizeOnOtherTokens(iter, res);
    }
    else
    {
        auto token_type = iter->type;
        res += "\"";
        size_t i = 0;
        while (token_type != DB::TokenType::Dot && token_type != DB::TokenType::OpeningSquareBracket
               && token_type != DB::TokenType::EndOfStream)
        {
            if (i)
            {
                auto [_, prev_iter_str] = prevToken(iter);
                // may contains spaces
                if (prev_iter_str.data() + prev_iter_str.size() != iter->begin)
                {
                    res += String(prev_iter_str.data() + prev_iter_str.size(), iter->begin);
                }
            }
            res += String(iter->begin, iter->end);
            ++iter;
            i += 1;
            token_type = iter->type;
        }
        auto [_, prev_iter_str] = prevToken(iter);
        res += String(prev_iter_str.data() + prev_iter_str.size(), iter->begin);
        res += "\"";
    }
}


void JSONPathNormalizer::normalizeOnOtherTokens(DB::IParser::Pos & iter, String & res)
{
    res += String(iter->begin, iter->end);
    ++iter;
}


REGISTER_FUNCTION(GetJsonObject)
{
    factory.registerFunction<DB::FunctionSQLJSON<GetJsonObject, GetJsonObjectImpl>>();
}
REGISTER_FUNCTION(FlattenJSONStringOnRequiredFunction)
{
    factory.registerFunction<FlattenJSONStringOnRequiredFunction>();
}
}
