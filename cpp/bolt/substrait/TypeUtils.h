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

#include "SubstraitParser.h"
#include "bolt/type/Filter.h"
#include "bolt/type/Type.h"

using namespace bytedance::bolt;

namespace gluten {

#ifndef RANGETRAITS_H
#define RANGETRAITS_H

// Traits used to map type kind to the range used in Filter.
template <TypeKind KIND>
struct RangeTraits {};

template <>
struct RangeTraits<TypeKind::TINYINT> {
  using RangeType = common::BigintRange;
  using MultiRangeType = common::BigintMultiRange;
  using NativeType = int8_t;
};

template <>
struct RangeTraits<TypeKind::SMALLINT> {
  using RangeType = common::BigintRange;
  using MultiRangeType = common::BigintMultiRange;
  using NativeType = int16_t;
};

template <>
struct RangeTraits<TypeKind::INTEGER> {
  using RangeType = common::BigintRange;
  using MultiRangeType = common::BigintMultiRange;
  using NativeType = int32_t;
};

template <>
struct RangeTraits<TypeKind::BIGINT> {
  using RangeType = common::BigintRange;
  using MultiRangeType = common::BigintMultiRange;
  using NativeType = int64_t;
};

template <>
struct RangeTraits<TypeKind::REAL> {
  using RangeType = common::FloatRange;
  using MultiRangeType = common::MultiRange;
  using NativeType = float;
};

template <>
struct RangeTraits<TypeKind::DOUBLE> {
  using RangeType = common::DoubleRange;
  using MultiRangeType = common::MultiRange;
  using NativeType = double;
};

template <>
struct RangeTraits<TypeKind::BOOLEAN> {
  using RangeType = common::BigintRange;
  using MultiRangeType = common::BigintMultiRange;
  using NativeType = bool;
};

template <>
struct RangeTraits<TypeKind::VARCHAR> {
  using RangeType = common::BytesRange;
  using MultiRangeType = common::MultiRange;
  using NativeType = std::string;
};

template <>
struct RangeTraits<TypeKind::HUGEINT> {
  using NativeType = int128_t;
};

#endif /* RANGETRAITS_H */

} // namespace gluten
