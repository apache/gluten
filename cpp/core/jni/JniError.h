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

#include <stdexcept>

#include "JniCommon.h"
#include "utils/Exception.h"

#ifndef JNI_METHOD_START
#define JNI_METHOD_START                     \
  const gluten::JniEntryGuard jniEntryGuard; \
  try {
// macro ended
#endif

#ifndef JNI_METHOD_END
#define JNI_METHOD_END(fallback_expr)   \
  }                                     \
  catch (const std::exception& e) {     \
    gluten::handleJniException(env, e); \
    return fallback_expr;               \
  }
#endif

namespace gluten {

// Register for the lifetime of the backend library. Called under the registry lock, the
// callback must only inspect its own wrapper and return its immediate cause (otherwise nullptr).
class JniExceptionUnwrapperRegistration final {
 public:
  explicit JniExceptionUnwrapperRegistration(ExceptionUnwrapper unwrap);
  ~JniExceptionUnwrapperRegistration();
  JniExceptionUnwrapperRegistration(const JniExceptionUnwrapperRegistration&) = delete;
  JniExceptionUnwrapperRegistration& operator=(const JniExceptionUnwrapperRegistration&) = delete;

 private:
  ExceptionUnwrapper unwrap_;
};

void handleJniException(JNIEnv* env, const std::exception& error);

class JniErrorState {
 public:
  virtual ~JniErrorState() = default;

  void ensureInitialized(JNIEnv* env);

  void assertInitialized();

  void close();

  jclass runtimeExceptionClass();

  jclass illegalAccessExceptionClass();

  jclass glutenExceptionClass();

 private:
  void initialize(JNIEnv* env);

  jclass ioExceptionClass_ = nullptr;
  jclass runtimeExceptionClass_ = nullptr;
  jclass unsupportedOperationExceptionClass_ = nullptr;
  jclass illegalAccessExceptionClass_ = nullptr;
  jclass illegalArgumentExceptionClass_ = nullptr;
  jclass glutenExceptionClass_ = nullptr;
  JavaVM* vm_;
  bool initialized_{false};
  bool closed_{false};
  std::mutex mtx_;
};

inline JniErrorState* getJniErrorState() {
  static JniErrorState jniErrorState;
  return &jniErrorState;
}

} // namespace gluten
