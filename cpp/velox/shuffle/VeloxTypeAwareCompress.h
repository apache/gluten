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
      // BIGINT covers signed/unsigned int64, double (reinterpreted), date64 and
      // ShortDecimal(p<=18) since Velox stores ShortDecimal as DecimalType<BIGINT>.
      return tac::kUInt64;
    case facebook::velox::TypeKind::HUGEINT:
      // HUGEINT (int128_t) covers LongDecimal(p>18) — Velox stores it as
      // DecimalType<HUGEINT>. Compressed via split-lane FFOR(uint64) on the
      // low / high 64 bits independently.
      return tac::kUInt128;
    case facebook::velox::TypeKind::TIMESTAMP:
      // TIMESTAMP is a 16-byte struct { int64_t seconds_; uint64_t nanos_; }.
      // Layout matches kUInt128: seconds lane has dense int64 values with
      // strong locality (low FFOR bit-width); nanos lane is typically 0 or a
      // small set of recurring values (tiny FFOR bit-width).
      return tac::kUInt128;
    case facebook::velox::TypeKind::INTEGER:
      // INTEGER covers signed/unsigned int32. Velox's DateType also derives
      // from IntegerType (TypeKind::INTEGER), so date32 columns flow through
      // the same path. Compressed via FFOR(uint64) over a zero-extended view.
      return tac::kUInt32;
    default:
      return tac::kUnsupported;
  }
}

} // namespace gluten
