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

#include <arrow/status.h>

#define ASSERT_NOT_OK(status)                  \
  do {                                         \
    arrow::Status __s = (status);              \
    if (!__s.ok()) {                           \
      throw std::runtime_error(__s.message()); \
    }                                          \
  } while (false);

#define ARROW_ASSIGN_OR_THROW_IMPL(status_name, lhs, rexpr) \
  do {                                                      \
    auto status_name = (rexpr);                             \
    auto __s = status_name.status();                        \
    if (!__s.ok()) {                                        \
      throw std::runtime_error(__s.message());              \
    }                                                       \
    lhs = std::move(status_name).ValueOrDie();              \
  } while (false);

#define ARROW_ASSIGN_OR_THROW_NAME(x, y) ARROW_CONCAT(x, y)

#define ARROW_ASSIGN_OR_THROW(lhs, rexpr) \
  ARROW_ASSIGN_OR_THROW_IMPL(ARROW_ASSIGN_OR_THROW_NAME(_error_or_value, __COUNTER__), lhs, rexpr);
