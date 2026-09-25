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
    throw facebook::velox::VeloxRuntimeError(std::current_exception(), error.what(), false);
  }
}

// Rethrow only a transported Java throwable. The caller must rethrow the original error otherwise.
inline void rethrowJavaException(std::exception_ptr error) {
  if (error == nullptr) {
    return;
  }
  try {
    std::rethrow_exception(error);
  } catch (const JavaException&) {
    throw;
  } catch (const facebook::velox::VeloxException& e) {
    rethrowJavaException(e.wrappedException());
    if (const auto* nested = dynamic_cast<const std::nested_exception*>(&e)) {
      rethrowJavaException(nested->nested_ptr());
    }
  } catch (const std::nested_exception& e) {
    rethrowJavaException(e.nested_ptr());
  } catch (...) {
    // No transported Java throwable. The enclosing JNI boundary retains its native-error handling.
  }
}

} // namespace gluten
