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

#include <Common/SparkRegexpToRe2.h>
#include <gtest/gtest.h>
#include <Common/re2.h>

using local_engine::spark_regexp::re2::translatePattern;

TEST(SparkRegexpToRe2, EscapesAndCaptures)
{
    EXPECT_EQ(
        translatePattern("(a\\Q.+\\E)$"),
        "(a\\Q.+\\E)(?:$|\\r?\\n\\z|\\r\\z|\\x{85}\\z|\\x{2028}\\z|\\x{2029}\\z)");
    EXPECT_EQ(translatePattern("."), "[^\\r\\n\\x{85}\\x{2028}\\x{2029}]");
    EXPECT_EQ(translatePattern("$(a)"), "(?:$|\\r?\\n\\z|\\r\\z|\\x{85}\\z|\\x{2028}\\z|\\x{2029}\\z)(a)");
    EXPECT_EQ(translatePattern("(?i)abc"), "(?i)abc");
    EXPECT_EQ(
        translatePattern("(?:a[bc])(d)$"),
        "(?:a[bc])(d)(?:$|\\r?\\n\\z|\\r\\z|\\x{85}\\z|\\x{2028}\\z|\\x{2029}\\z)");
}

TEST(SparkRegexpToRe2, NestedPositiveClass)
{
    EXPECT_EQ(translatePattern("[0-9][[\\.][0-9]]*"), "[0-9](?:[\\.]|[0-9])*");
    EXPECT_EQ(translatePattern("[a-[b]]"), "(?:[a-]|[b])");
    EXPECT_EQ(translatePattern("[a[b]-c]"), "(?:[a]|[b]|[-c])");
    EXPECT_EQ(translatePattern("[^a[b]]"), "[^ab]");
    EXPECT_EQ(translatePattern("[a-z]foo[bar]"), "[a-z]foo[bar]");
    EXPECT_EQ(translatePattern("[\\Q-]\\E]"), "[\\-\\]]");
}

TEST(SparkRegexpToRe2, PreservesCaptureIndexAndLineEnd)
{
    const ::re2::RE2 regex(translatePattern("(a)$"));
    ASSERT_TRUE(regex.ok());
    std::string capture;
    EXPECT_TRUE(::re2::RE2::PartialMatch("a\n", regex, &capture));
    EXPECT_EQ(capture, "a");
    EXPECT_FALSE(::re2::RE2::PartialMatch("a\nb", regex, &capture));
}

TEST(SparkRegexpToRe2, DotAllScope)
{
    const ::re2::RE2 scoped(translatePattern("(?s:.)x."));
    const ::re2::RE2 toggled(translatePattern("(?s).(?-s)."));
    const ::re2::RE2 nested(translatePattern("((?s).)."));
    ASSERT_TRUE(scoped.ok());
    ASSERT_TRUE(toggled.ok());
    ASSERT_TRUE(nested.ok());
    EXPECT_TRUE(::re2::RE2::FullMatch("\nxA", scoped));
    EXPECT_FALSE(::re2::RE2::FullMatch("\nx\n", scoped));
    EXPECT_TRUE(::re2::RE2::FullMatch("\na", toggled));
    EXPECT_FALSE(::re2::RE2::FullMatch("\n\n", toggled));
    EXPECT_TRUE(::re2::RE2::FullMatch("\na", nested));
    EXPECT_FALSE(::re2::RE2::FullMatch("\n\n", nested));
    EXPECT_EQ(translatePattern("(?s:.)"), "(?:(?s:.))");
}

TEST(SparkRegexpToRe2, RejectsUnsupported)
{
    EXPECT_THROW(translatePattern("(?!x)"), DB::Exception);
    EXPECT_THROW(translatePattern("(a)\\1"), DB::Exception);
    EXPECT_THROW(translatePattern("[a&&b]"), DB::Exception);
    EXPECT_THROW(translatePattern("[a-z&&[^bc]]"), DB::Exception);
    EXPECT_EQ(translatePattern("[a[^b]]"), "(?:[a]|[^b])");
    EXPECT_THROW(translatePattern("(?m:.)"), DB::Exception);
}
