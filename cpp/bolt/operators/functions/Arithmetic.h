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
#include <folly/CPortability.h>
#include <stdint.h>
#include <cmath>
#include <limits>
#include <type_traits>

namespace gluten {
template <typename T>
/// Round function
/// When AlwaysRoundNegDec is true, spark semantics is followed which
/// rounds negative decimals for integrals and does not round it otherwise
/// Note that is is likely techinically impossible for this function to return
/// expected results in all cases as the loss of precision plagues it on both
/// paths: factor multiplication for large numbers and addition of truncated
/// number to the rounded fraction for small numbers.
/// We are trying to minimize the loss of precision by using the best path for
/// the number, but the journey is likely not over yet.
struct RoundFunction {
  template <typename TNum, typename TDecimals, bool alwaysRoundNegDec = true>
  FOLLY_ALWAYS_INLINE TNum round(const TNum& number, const TDecimals& decimals = 0) {
    static_assert(!std::is_same_v<TNum, bool> && "round not supported for bool");

    if constexpr (std::is_integral_v<TNum>) {
      if constexpr (alwaysRoundNegDec) {
        if (decimals >= 0)
          return number;
      } else {
        return number;
      }
    }
    if (!std::isfinite(number)) {
      return number;
    }

    long double factor = std::pow(10.0L, static_cast<long double>(decimals));
    static const TNum kInf = std::numeric_limits<TNum>::infinity();

    if (number < 0) {
      return static_cast<TNum>((std::round(std::nextafter(number, -kInf) * factor * -1) / factor) * -1);
    }
    return static_cast<TNum>(std::round(std::nextafter(number, kInf) * factor) / factor);
  }
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a, const int32_t b = 0) {
    result = round(a, b);
  }
};
} // namespace gluten
