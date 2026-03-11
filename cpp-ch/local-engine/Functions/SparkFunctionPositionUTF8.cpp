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
#include <string>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/PositionImpl.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

}

namespace local_engine
{
using namespace DB;

// Spark-specific version of PositionImpl
template <typename Name, typename Impl>
struct PositionSparkImpl
{
    static constexpr bool use_default_implementation_for_constants = false;
    static constexpr bool supports_start_pos = true;
    static constexpr auto name = Name::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {}; }

    using ResultType = UInt64;

    /// Find one substring in many strings.
    static void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        [[maybe_unused]] size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// Current index in the array of strings.
        size_t i = 0;

        typename Impl::SearcherInBigHaystack searcher = Impl::createSearcherInBigHaystack(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it refers to.
            while (begin + offsets[i] <= pos)
            {
                res[i] = 0;
                ++i;
            }
            auto start = start_pos != nullptr ? start_pos->getUInt(i) : 0;

            /// We check that the entry does not pass through the boundaries of strings.
            // The result is 0 if start_pos is 0, in compliance with Spark semantics
            if (start != 0 && pos + needle.size() < begin + offsets[i])
            {
                auto res_pos
                    = 1 + Impl::countChars(reinterpret_cast<const char *>(begin + offsets[i - 1]), reinterpret_cast<const char *>(pos));
                if (res_pos < start)
                {
                    pos = reinterpret_cast<const UInt8 *>(Impl::advancePos(
                        reinterpret_cast<const char *>(pos), reinterpret_cast<const char *>(begin + offsets[i]), start - res_pos));
                    continue;
                }
                // The result is 1 if needle is empty, in compliance with Spark semantics
                res[i] = needle.empty() ? 1 : res_pos;
            }
            else
            {
                res[i] = 0;
            }
            pos = begin + offsets[i];
            ++i;
        }

        if (i < res.size())
            memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    /// Search for substring in string.
    static void constantConstantScalar(std::string data, const std::string & needle, UInt64 start_pos, UInt64 & res)
    {
        size_t start_byte = Impl::advancePos(data.data(), data.data() + data.size(), start_pos - 1) - data.data();
        res = data.find(needle, start_byte);
        if (res == std::string::npos)
            res = 0;
        else
            res = 1 + Impl::countChars(data.data(), data.data() + res);
    }

    /// Search for substring in string starting from different positions.
    static void constantConstant(
        std::string data,
        std::string needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        Impl::toLowerIfNeed(data);
        Impl::toLowerIfNeed(needle);

        if (start_pos == nullptr)
        {
            res[0] = 0;
            return;
        }

        size_t haystack_size = Impl::countChars(data.data(), data.data() + data.size());

        size_t size = start_pos != nullptr ? start_pos->size() : 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto start = start_pos->getUInt(i);

            if (start == 0 || start > haystack_size + 1)
            {
                res[i] = 0;
                continue;
            }
            if (needle.empty())
            {
                res[0] = 1;
                continue;
            }
            constantConstantScalar(data, needle, start, res[i]);
        }
    }

    /// Search each time for a different single substring inside each time different string.
    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;


        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset;

            auto start = start_pos != nullptr ? start_pos->getUInt(i) : UInt64(0);

            if (start == 0 || start > haystack_size + 1)
            {
                res[i] = 0;
            }
            else if (0 == needle_size)
            {
                /// An empty string is always 1 in compliance with Spark semantics.
                res[i] = 1;
            }
            else
            {
                /// It is assumed that the StringSearcher is not very difficult to initialize.
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                    needle_offsets[i] - prev_needle_offset);

                const char * beg = Impl::advancePos(
                    reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]),
                    reinterpret_cast<const char *>(&haystack_data[haystack_offsets[i]]),
                    start - 1);
                /// searcher returns a pointer to the found substring or to the end of `haystack`.
                size_t pos = searcher.search(reinterpret_cast<const UInt8 *>(beg), &haystack_data[haystack_offsets[i]])
                    - &haystack_data[prev_haystack_offset];

                if (pos != haystack_size)
                {
                    res[i] = 1
                        + Impl::countChars(
                                 reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]),
                                 reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset + pos]));
                }
                else
                    res[i] = 0;
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
        }
    }

    /// Find many substrings in single string.
    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        /// NOTE You could use haystack indexing. But this is a rare case.
        ColumnString::Offset prev_needle_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset;

            auto start = start_pos != nullptr ? start_pos->getUInt(i) : UInt64(0);

            if (start == 0 || start > haystack.size() + 1)
            {
                res[i] = 0;
            }
            else if (0 == needle_size)
            {
                res[i] = 1;
            }
            else
            {
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]), needle_offsets[i] - prev_needle_offset);

                const char * beg = Impl::advancePos(haystack.data(), haystack.data() + haystack.size(), start - 1);
                size_t pos = searcher.search(
                                 reinterpret_cast<const UInt8 *>(beg), reinterpret_cast<const UInt8 *>(haystack.data()) + haystack.size())
                    - reinterpret_cast<const UInt8 *>(haystack.data());

                if (pos != haystack.size())
                {
                    res[i] = 1 + Impl::countChars(haystack.data(), haystack.data() + pos);
                }
                else
                    res[i] = 0;
            }

            prev_needle_offset = needle_offsets[i];
        }
    }

    template <typename... Args>
    static void vectorFixedConstant(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }

    template <typename... Args>
    static void vectorFixedVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }
};

struct NamePositionUTF8Spark
{
    static constexpr auto name = "positionUTF8Spark";
};


using FunctionPositionUTF8Spark = FunctionsStringSearch<PositionSparkImpl<NamePositionUTF8Spark, PositionCaseSensitiveUTF8>>;


REGISTER_FUNCTION(PositionUTF8Spark)
{
    factory.registerFunction<FunctionPositionUTF8Spark>();
}

}
