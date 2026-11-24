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

#include <utility>

#include "jni/JavaException.h"
#include "velox/common/base/VeloxException.h"

namespace gluten {

template <typename F>
decltype(auto) wrapJavaException(F&& callback) {
  try {
    return std::forward<F>(callback)();
  } catch (const JavaException& error) {
    // Driver's std::exception catch keeps only what(); VeloxException keeps the exception_ptr.
    // Use a runtime error so TRY expressions cannot suppress a failed Java callback.
    // Standard nesting also lets other JNI boundaries recover the callback without knowing about Velox.
    std::throw_with_nested(facebook::velox::VeloxException(
        std::current_exception(),
        error.what(),
        facebook::velox::error_source::kErrorSourceRuntime,
        false,
        facebook::velox::VeloxException::Type::kSystem,
        facebook::velox::VeloxRuntimeError::kDefaultName));
  }
}

inline std::exception_ptr unwrapVeloxException(const std::exception& error) noexcept {
  const auto* veloxError = dynamic_cast<const facebook::velox::VeloxException*>(&error);
  return veloxError ? veloxError->wrappedException() : nullptr;
}

} // namespace gluten
