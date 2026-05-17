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

#include <re2/re2.h>
#include <memory>
#include <string>

#include "utils/Macros.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/time/CpuWallTimer.h"

namespace gluten {

// Compile the given pattern and return the RE2 object.
inline std::unique_ptr<re2::RE2> compilePattern(const std::string& pattern);

bool validatePattern(const std::string& pattern, std::string& error);

static inline void fastCopy(void* dst, const void* src, size_t n) {
  facebook::velox::simd::memcpy(dst, src, n);
}

// SCOPED_TIMER — RAII-style timer that lives until the end of the
// enclosing scope and reports elapsed CPU+wall time to `timing` on
// destruction.
//
// Implementation note: a `do { … } while (0)` wrapper would destroy
// the DeltaCpuWallTimer immediately at the end of the do-block,
// recording essentially zero. Instead we declare uniquely-named
// locals (via GLUTEN_CONCAT(..., __LINE__)) so the timer survives
// until the enclosing scope closes — which is what every call site
// already expects.
//
// Note: we capture the timing target by pointer in a unique local
// variable rather than directly in the lambda, because `timing` may
// be an arbitrary expression (e.g. `array_[idx]`) that is not a valid
// lambda capture name.
#define SCOPED_TIMER(timing)                                                                      \
  auto GLUTEN_CONCAT(_glutenScopedTimerTarget_, __LINE__) = &(timing);                            \
  facebook::velox::DeltaCpuWallTimer GLUTEN_CONCAT(_glutenScopedTimer_, __LINE__) {               \
    [_glutenScopedTimerTargetPtr = GLUTEN_CONCAT(_glutenScopedTimerTarget_, __LINE__)](           \
        const facebook::velox::CpuWallTiming& delta) { _glutenScopedTimerTargetPtr->add(delta); } \
  }

} // namespace gluten
