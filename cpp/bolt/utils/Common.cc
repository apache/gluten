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

namespace gluten {

// Note: This method is mostly copied from bolt/functions/sparksql/RegexFunctions.cpp
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

bool validatePattern(const std::string& pattern, std::string& error) {
  auto re2 = compilePattern(pattern);
  if (!re2->ok()) {
    error = "Pattern " + pattern + " compilation failed in RE2. Reason: " + re2->error();
    return false;
  }
  return ensureRegexIsCompatible(pattern, error);
}

} // namespace gluten
