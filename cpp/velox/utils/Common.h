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

#include "velox/common/base/SimdUtil.h"
#include "velox/common/time/CpuWallTimer.h"

namespace gluten {

// Compile the given pattern and return the RE2 object.
inline std::unique_ptr<re2::RE2> compilePattern(const std::string& pattern);

bool validatePattern(const std::string& pattern, std::string& error);

static inline void fastCopy(void* dst, const void* src, size_t n) {
  facebook::velox::simd::memcpy(dst, src, n);
}

#define SCOPED_TIMER(timing)                                                              \
  do {                                                                                    \
    auto ptiming = &timing;                                                               \
    facebook::velox::DeltaCpuWallTimer timer{                                             \
        [ptiming](const facebook::velox::CpuWallTiming& delta) { ptiming->add(delta); }}; \
  } while (0)

} // namespace gluten
