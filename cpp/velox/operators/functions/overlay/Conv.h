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

#include <folly/Likely.h>

#include <charconv>
#include <cstdint>
#include <system_error>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/String.h"

namespace gluten {

/// conv(num, fromBase, toBase) -> varchar
///
/// Overrides Velox's conv, which always lets the conversion overflow. Spark
/// only does that with ANSI mode off; with ANSI mode on, an input whose digits
/// do not fit in an unsigned 64-bit integer raises an error instead of being
/// saturated. Everything else, including the conversion itself, is delegated to
/// Velox's implementation.
template <typename T>
struct ConvFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  void initialize(
      const std::vector<facebook::velox::TypePtr>& /*inputTypes*/,
      const facebook::velox::core::QueryConfig& config,
      const arg_type<facebook::velox::Varchar>* /*num*/,
      const int32_t* /*fromBase*/,
      const int32_t* /*toBase*/) {
    ansiEnabled_ = facebook::velox::functions::sparksql::SparkQueryConfig{config}.ansiEnabled();
  }

  bool call(
      out_type<facebook::velox::Varchar>& result,
      const arg_type<facebook::velox::Varchar>& num,
      int32_t fromBase,
      int32_t toBase) {
    if (FOLLY_UNLIKELY(ansiEnabled_) && overflows(num, fromBase, toBase)) {
      // Same wording as Spark's QueryExecutionErrors.overflowInConvError. The
      // simple function framework turns this into a per-row error.
      VELOX_USER_FAIL("Overflow in function conv()");
    }
    return delegate_.call(result, num, fromBase, toBase);
  }

 private:
  using VeloxConvFunction = facebook::velox::functions::sparksql::ConvFunction<T>;

  /// Returns true when the digits of 'num' do not fit in an unsigned 64-bit
  /// integer. That is exactly when Spark's NumberConverter.encode() reports an
  /// overflow: its two checks together detect that accumulating the next digit
  /// would pass 2^64 - 1. Locates and parses the digits the same way Velox's
  /// conv does, so the two agree on where the digits end.
  static bool overflows(const arg_type<facebook::velox::Varchar>& num, int32_t fromBase, int32_t toBase) {
    if (!VeloxConvFunction::checkInput(num, fromBase, toBase)) {
      // An empty input or an out-of-range base gives NULL, in ANSI mode too.
      return false;
    }
    auto position = static_cast<size_t>(VeloxConvFunction::skipLeadingSpaces(num));
    if (position == num.size()) {
      // All spaces.
      return false;
    }
    // Skips the negative symbol, std::from_chars does not accept one for an
    // unsigned type. Spark applies the sign after the digits are accumulated,
    // so it does not affect whether the digits overflow.
    if (num.data()[position] == '-') {
      ++position;
    }
    uint64_t value;
    const auto status = std::from_chars(num.data() + position, num.data() + num.size(), value, fromBase);
    return status.ec == std::errc::result_out_of_range;
  }

  VeloxConvFunction delegate_;
  bool ansiEnabled_{false};
};

} // namespace gluten
