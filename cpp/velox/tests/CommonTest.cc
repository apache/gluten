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

TEST(CommonTest, translateJavaUnicodeToRe2_escapedBackslashNotTranslated) {
  // "\\u0041" is a literal backslash followed by the text u0041; it must stay as-is.
  EXPECT_EQ(translateJavaUnicodeToRe2("\\\\u0041"), "\\\\u0041");
}

TEST(CommonTest, translateJavaUnicodeToRe2_escapedBackslashThenRealEscape) {
  // "\\A" is an escaped backslash followed by a real A escape.
  EXPECT_EQ(translateJavaUnicodeToRe2("\\\\\\u0041"), "\\\\\\x{0041}");
}

TEST(CommonTest, translateJavaUnicodeToRe2_surrogatePair) {
  EXPECT_EQ(translateJavaUnicodeToRe2("\\uD83D\\uDE00"), "\\x{1F600}");
}

TEST(CommonTest, translateJavaUnicodeToRe2_surrogatePairRange) {
  EXPECT_EQ(translateJavaUnicodeToRe2("[\\uD83D\\uDE00-\\uD83D\\uDE4F]"), "[\\x{1F600}-\\x{1F64F}]");
}

TEST(CommonTest, translateJavaUnicodeToRe2_loneSurrogateLeftAlone) {
  EXPECT_EQ(translateJavaUnicodeToRe2("\\uD83Dx"), "\\uD83Dx");
  EXPECT_EQ(translateJavaUnicodeToRe2("\\uDE00"), "\\uDE00");
}

// ---------------------------------------------------------------------------
// validatePattern — validates the pattern exactly as Velox will compile it (no translation)
// ---------------------------------------------------------------------------

TEST(CommonTest, validatePattern_rawUnicodeEscapeRejected) {
  // A raw \uXXXX would be accepted here but fail in Velox at runtime, so it must fall back.
  std::string error;
  EXPECT_FALSE(validatePattern("\\u4e00", error));
  EXPECT_FALSE(validatePattern("[\\u4e00-\\u9fa5]+", error));
  EXPECT_FALSE(validatePattern("[\\uD83D\\uDE00-\\uD83D\\uDE4F]", error));
}

TEST(CommonTest, validatePattern_translatedUnicodeEscapeAccepted) {
  // What the Scala side sends after translation.
  std::string error;
  EXPECT_TRUE(validatePattern("\\x{4e00}", error)) << "error: " << error;
  EXPECT_TRUE(validatePattern("[\\x{4e00}-\\x{9fa5}]+", error)) << "error: " << error;
  EXPECT_TRUE(validatePattern("[\\x{1F600}-\\x{1F64F}]", error)) << "error: " << error;
}

TEST(CommonTest, validatePattern_translatorOutputAccepted) {
  // translateJavaUnicodeToRe2 output must always pass the validator.
  std::string error;
  for (const char* p : {"\\u4e00", "[\\u4e00-\\u9fa5]+", "[\\uD83D\\uDE00-\\uD83D\\uDE4F]"}) {
    EXPECT_TRUE(validatePattern(translateJavaUnicodeToRe2(p), error)) << p << " error: " << error;
  }
}

TEST(CommonTest, validatePattern_escapedBackslashThenUAccepted) {
  // "\\\\u0041" is a literal backslash followed by text, not a unicode escape.
  std::string error;
  EXPECT_TRUE(validatePattern("\\\\u0041", error)) << "error: " << error;
}

TEST(CommonTest, validatePattern_errorMessageEscapedAndTruncated) {
  std::string error;
  EXPECT_FALSE(validatePattern("(a\n", error));
  EXPECT_NE(error.find("'(a\\n'"), std::string::npos) << error;
  error.clear();
  EXPECT_FALSE(validatePattern("(" + std::string(500, 'x'), error));
  EXPECT_NE(error.find("truncated, 501 chars total"), std::string::npos) << error;
  EXPECT_LT(error.size(), 400u) << error;
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

TEST(CommonTest, validatePattern_loneSurrogateRejected) {
  std::string error;
  EXPECT_FALSE(validatePattern("\\uD83D", error));
}

TEST(CommonTest, validatePattern_characterClassUnionRejected) {
  std::string error;
  EXPECT_FALSE(validatePattern("[a[b]]", error));
}

} // namespace gluten
