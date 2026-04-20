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

#include "utils/tac/TypeAwareCompressCodec.h"
#include "velox/type/Type.h"

namespace gluten {

/// Convert a Velox TypeKind to a TAC data type for type-aware compression.
/// Returns tac::kUnsupported for types that cannot be compressed by TAC.
inline int8_t veloxTypeToTacType(facebook::velox::TypeKind kind) {
  switch (kind) {
    case facebook::velox::TypeKind::BIGINT:
      return tac::kUInt64;
    default:
      return tac::kUnsupported;
  }
}

} // namespace gluten
