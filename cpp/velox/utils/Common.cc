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

#include "Common.h"

#include <cctype>
#include <cstdio>
#include <string>

namespace gluten {

// Note: This method is mostly copied from velox/functions/sparksql/RegexFunctions.cpp
//
// Blocks patterns that contain character class union, intersection, or
// difference because these are not understood by RE2 and will be parsed as a
// different pattern than in java.util.regex.
bool ensureRegexIsCompatible(const std::string& pattern, std::string& error) {
  // If in a character class, points to the [ at the beginning of that class.
  auto charClassStart = pattern.cend();
  // This minimal regex parser looks just for the class begin/end markers.
  for (auto c = pattern.cbegin(); c < pattern.cend(); ++c) {
    if (*c == '\\') {
      ++c;
    } else if (*c == '[') {
      if (charClassStart != pattern.cend()) {
        error =
            "Not support character class union, intersection, "
            "or difference ([a[b]], [a&&[b]], [a&&[^b]])";
        return false;
      }
      charClassStart = c;
      // A ] immediately after a [ does not end the character class, and is
      // instead adds the character ].
    } else if (*c == ']' && charClassStart != pattern.cend() && charClassStart + 1 != c) {
      charClassStart = pattern.cend();
    }
  }
  return true;
}

std::unique_ptr<re2::RE2> compilePattern(const std::string& pattern) {
  return std::make_unique<re2::RE2>(re2::StringPiece(pattern), RE2::Quiet);
}

namespace {
// Parses a Java \uXXXX escape at pattern[i]; returns the code unit, or -1 if there is none.
int parseJavaUnicodeEscape(const std::string& pattern, size_t i) {
  if (i + 6 > pattern.size() || pattern[i] != '\\' || pattern[i + 1] != 'u') {
    return -1;
  }
  for (size_t k = 2; k < 6; ++k) {
    if (!std::isxdigit(static_cast<unsigned char>(pattern[i + k]))) {
      return -1;
    }
  }
  return static_cast<int>(std::stoul(pattern.substr(i + 2, 4), nullptr, 16));
}
} // namespace

std::string translateJavaUnicodeToRe2(const std::string& pattern) {
  std::string result;
  result.reserve(pattern.size());
  size_t i = 0;
  while (i < pattern.size()) {
    if (pattern[i] != '\\') {
      result += pattern[i++];
      continue;
    }
    const int cu = parseJavaUnicodeEscape(pattern, i);
    if (cu < 0) {
      // Any other escape (including an escaped backslash) is copied verbatim as a pair, so that
      // "\\\\u0041" (literal backslash followed by text) is not mistaken for a unicode escape.
      result += pattern[i++];
      if (i < pattern.size()) {
        result += pattern[i++];
      }
      continue;
    }
    if (cu >= 0xD800 && cu <= 0xDBFF) {
      const int lo = parseJavaUnicodeEscape(pattern, i + 6);
      if (lo >= 0xDC00 && lo <= 0xDFFF) {
        const unsigned cp =
            0x10000 + ((static_cast<unsigned>(cu) - 0xD800) << 10) + (static_cast<unsigned>(lo) - 0xDC00);
        char buf[16];
        std::snprintf(buf, sizeof(buf), "\\x{%X}", cp);
        result += buf;
        i += 12;
        continue;
      }
    }
    if (cu >= 0xD800 && cu <= 0xDFFF) {
      // Unpaired surrogate: leave untouched so that RE2 rejects it and the caller falls back.
      result.append(pattern, i, 6);
    } else {
      result += "\\x{";
      result.append(pattern, i + 2, 4);
      result += '}';
    }
    i += 6;
  }
  return result;
}

namespace {

// Renders a pattern for error messages: quoted, control characters escaped, and truncated so that
// very long patterns do not bloat fallback reasons and logs.
std::string describePattern(const std::string& pattern) {
  constexpr size_t kMaxLen = 100;
  std::string out = "'";
  size_t i = 0;
  for (; i < pattern.size() && out.size() < kMaxLen; ++i) {
    const unsigned char ch = static_cast<unsigned char>(pattern[i]);
    switch (ch) {
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (ch < 0x20 || ch == 0x7F) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\x%02X", ch);
          out += buf;
        } else {
          out += static_cast<char>(ch);
        }
    }
  }
  if (i < pattern.size()) {
    out += "...(truncated, " + std::to_string(pattern.size()) + " chars total)";
  }
  out += "'";
  return out;
}

} // namespace

bool validatePattern(const std::string& pattern, std::string& error) {
  // Validate the pattern exactly as Velox will compile it at execution time. The plan keeps the
  // original literal, so translating a private copy here would approve patterns (e.g. a raw
  // \uXXXX) that Velox then fails to compile at runtime instead of falling back to Spark. Java
  // \uXXXX escapes are rewritten on the Scala side before the plan is built.
  auto re2 = compilePattern(pattern);
  if (!re2->ok()) {
    // RE2's own message usually repeats the whole pattern, so cap it as well.
    std::string reason = re2->error();
    constexpr size_t kMaxReasonLen = 100;
    if (reason.size() > kMaxReasonLen) {
      reason = reason.substr(0, kMaxReasonLen) + "...(truncated)";
    }
    error = "Pattern " + describePattern(pattern) + " compilation failed in RE2. Reason: " + reason;
    return false;
  }
  return ensureRegexIsCompatible(pattern, error);
}

} // namespace gluten
