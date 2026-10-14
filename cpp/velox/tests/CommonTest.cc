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

#include "utils/Common.h"

#include <gtest/gtest.h>

namespace gluten {

// ---------------------------------------------------------------------------
// translateJavaUnicodeToRe2
// ---------------------------------------------------------------------------

TEST(CommonTest, translateJavaUnicodeToRe2_singleEscape) {
  EXPECT_EQ(translateJavaUnicodeToRe2("\\u4e00"), "\\x{4e00}");
}

TEST(CommonTest, translateJavaUnicodeToRe2_rangeInBrackets) {
  EXPECT_EQ(translateJavaUnicodeToRe2("[\\u4e00-\\u9fa5]"), "[\\x{4e00}-\\x{9fa5}]");
}

TEST(CommonTest, translateJavaUnicodeToRe2_noEscape) {
  EXPECT_EQ(translateJavaUnicodeToRe2("[a-z]+"), "[a-z]+");
}

TEST(CommonTest, translateJavaUnicodeToRe2_idempotent) {
  // Already in RE2 syntax — must pass through unchanged.
  EXPECT_EQ(translateJavaUnicodeToRe2("\\x{4e00}"), "\\x{4e00}");
}

TEST(CommonTest, translateJavaUnicodeToRe2_mixedContent) {
  EXPECT_EQ(translateJavaUnicodeToRe2("hello\\u0041world"), "hello\\x{0041}world");
}

TEST(CommonTest, translateJavaUnicodeToRe2_uppercaseHex) {
  EXPECT_EQ(translateJavaUnicodeToRe2("\\uABCD"), "\\x{ABCD}");
}

TEST(CommonTest, translateJavaUnicodeToRe2_shortEscapeLeftAlone) {
  // \u followed by fewer than 4 hex digits must not be translated.
  EXPECT_EQ(translateJavaUnicodeToRe2("\\u41"), "\\u41");
}

// ---------------------------------------------------------------------------
// validatePattern — Phase 2 safeguard
// ---------------------------------------------------------------------------

TEST(CommonTest, validatePattern_unicodeEscapeTranslatedBeforeRe2) {
  // \u4e00 would cause RE2 compile failure without translation.
  std::string error;
  EXPECT_TRUE(validatePattern("\\u4e00", error)) << "error: " << error;
}

TEST(CommonTest, validatePattern_cjkRangeTranslated) {
  std::string error;
  EXPECT_TRUE(validatePattern("[\\u4e00-\\u9fa5]+", error)) << "error: " << error;
}

TEST(CommonTest, validatePattern_lookaheadRejected) {
  std::string error;
  EXPECT_FALSE(validatePattern("(?=x)", error));
}

TEST(CommonTest, validatePattern_lookbehindRejected) {
  std::string error;
  EXPECT_FALSE(validatePattern("(?<=x)y", error));
}

TEST(CommonTest, validatePattern_simplePatternAccepted) {
  std::string error;
  EXPECT_TRUE(validatePattern("[a-z]+", error)) << "error: " << error;
}

TEST(CommonTest, validatePattern_characterClassUnionRejected) {
  std::string error;
  EXPECT_FALSE(validatePattern("[a[b]]", error));
}

} // namespace gluten
