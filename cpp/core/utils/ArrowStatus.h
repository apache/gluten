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

#include <exception>

namespace gluten {

class ArrowException final : public std::runtime_error {
 public:
  explicit ArrowException(const std::string& arg) : runtime_error(arg) {}
};

static inline void throwArrowException(const std::string& message) {
  throw ArrowException(message);
}

template <typename T>
inline T arrowGetOrThrow(arrow::Result<T> result) {
  if (!result.status().ok()) {
    throwArrowException(result.status().message());
  }
  return std::move(result).ValueOrDie();
}

static inline void arrowAssertOkOrThrow(arrow::Status status) {
  if (!status.ok()) {
    throwArrowException(status.message());
  }
}

static inline void arrowAssertOkOrThrow(arrow::Status status, const std::string& message) {
  if (!status.ok()) {
    throwArrowException(message + " - " + status.ToString());
  }
}
} // namespace gluten
