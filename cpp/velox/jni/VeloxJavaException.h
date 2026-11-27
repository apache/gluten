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

#include <new>
#include <utility>

#include "jni/JavaException.h"
#include "velox/common/base/VeloxException.h"

namespace gluten {

inline facebook::velox::VeloxException makeVeloxJavaException(const std::exception_ptr& cause, const char* message) {
  return facebook::velox::VeloxException(
      cause,
      message,
      facebook::velox::error_source::kErrorSourceRuntime,
      false,
      facebook::velox::VeloxException::Type::kSystem,
      facebook::velox::VeloxRuntimeError::kDefaultName);
}

template <typename F>
decltype(auto) wrapJavaException(F&& callback, const facebook::velox::VeloxException* fallback = nullptr) {
  try {
    return std::forward<F>(callback)();
  } catch (const JavaException& error) {
    // Driver's std::exception catch keeps only what(); VeloxException keeps the exception_ptr.
    // Use a runtime error so TRY expressions cannot suppress a failed Java callback.
    // Standard nesting also lets other JNI boundaries recover the callback without knowing about Velox.
    try {
      std::throw_with_nested(makeVeloxJavaException(std::current_exception(), error.what()));
    } catch (const std::bad_alloc&) {
      if (fallback == nullptr) {
        throw;
      }
    }
    // Outside the bad_alloc handler, the nested cause is the original JavaException.
    std::throw_with_nested(*fallback);
  }
}

inline std::exception_ptr unwrapVeloxException(const std::exception& error) noexcept {
  const auto* veloxError = dynamic_cast<const facebook::velox::VeloxException*>(&error);
  return veloxError ? veloxError->wrappedException() : nullptr;
}

} // namespace gluten
