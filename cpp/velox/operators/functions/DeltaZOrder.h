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

#include "velox/functions/Udf.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

namespace gluten {

namespace {
constexpr size_t kInterleaveBitsStackInputCapacity = 16;
constexpr size_t kRangePartitionLinearSearchBoundCount = 128;
} // namespace

template <typename T>
struct DeltaInterleaveBitsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<facebook::velox::Varbinary>& result,
      const arg_type<facebook::velox::Variadic<int32_t>>& inputs) {
    interleave(result, inputs);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<facebook::velox::Varbinary>& result,
      const arg_type<facebook::velox::Variadic<int32_t>>* inputs) {
    interleave(result, inputs);
    return true;
  }

 private:
  FOLLY_ALWAYS_INLINE void
  writeInterleavedBits(out_type<facebook::velox::Varbinary>& result, const int32_t* values, size_t valueCount) {
    const auto byteCount = valueCount * sizeof(int32_t);
    result.resize(byteCount);
    if (byteCount == 0) {
      return;
    }
    std::memset(result.data(), 0, byteCount);

    size_t outputBit = 0;
    for (int bit = 31; bit >= 0; --bit) {
      for (size_t i = 0; i < valueCount; ++i) {
        if ((static_cast<uint32_t>(values[i]) >> bit) & 1U) {
          result.data()[outputBit >> 3] |= static_cast<char>(1U << (7 - (outputBit & 7)));
        }
        ++outputBit;
      }
    }
  }

  template <typename TInputs>
  FOLLY_ALWAYS_INLINE void interleave(out_type<facebook::velox::Varbinary>& result, const TInputs* inputs) {
    if (inputs == nullptr) {
      result.resize(0);
      return;
    }

    const auto inputCount = inputs->size();
    if (inputCount == 0) {
      result.resize(0);
      return;
    }

    if (inputCount <= kInterleaveBitsStackInputCapacity) {
      std::array<int32_t, kInterleaveBitsStackInputCapacity> values;
      for (size_t i = 0; i < inputCount; ++i) {
        const auto input = inputs->at(i);
        values[i] = input.has_value() ? input.value() : 0;
      }
      writeInterleavedBits(result, values.data(), inputCount);
      return;
    }

    std::vector<int32_t> values(inputCount);
    for (size_t i = 0; i < inputCount; ++i) {
      const auto input = inputs->at(i);
      values[i] = input.has_value() ? input.value() : 0;
    }
    writeInterleavedBits(result, values.data(), inputCount);
  }

  template <typename TInputs>
  FOLLY_ALWAYS_INLINE void interleave(out_type<facebook::velox::Varbinary>& result, const TInputs& inputs) {
    interleave(result, &inputs);
  }
};

template <typename TInputView>
FOLLY_ALWAYS_INLINE void deltaRangePartitionId(int32_t& result, const TInputView* inputs) {
  result = 0;
  if (inputs == nullptr) {
    return;
  }
  const auto inputCount = inputs->size();
  if (inputCount == 0) {
    return;
  }

  const auto valueArg = inputs->at(0);
  if (!valueArg.has_value()) {
    return;
  }

  const auto value = valueArg.value();
  const auto boundCount = inputCount - 1;
  if (boundCount <= kRangePartitionLinearSearchBoundCount) {
    for (size_t i = 1; i < inputCount; ++i) {
      const auto bound = inputs->at(i);
      if (!bound.has_value() || value <= bound.value()) {
        return;
      }
      ++result;
    }
    return;
  }

  size_t lower = 0;
  size_t upper = boundCount;
  while (lower < upper) {
    const auto mid = lower + (upper - lower) / 2;
    const auto bound = inputs->at(mid + 1);
    if (!bound.has_value() || value <= bound.value()) {
      upper = mid;
    } else {
      lower = mid + 1;
    }
  }
  result = static_cast<int32_t>(lower);
}

template <typename TValue, typename TBoundsView>
FOLLY_ALWAYS_INLINE void
deltaRangePartitionIdFromBounds(int32_t& result, const TValue& value, const TBoundsView* bounds) {
  result = 0;
  if (bounds == nullptr) {
    return;
  }

  const auto boundCount = bounds->size();
  if (boundCount <= kRangePartitionLinearSearchBoundCount) {
    for (size_t i = 0; i < boundCount; ++i) {
      const auto bound = bounds->at(i);
      if (!bound.has_value() || value <= bound.value()) {
        return;
      }
      ++result;
    }
    return;
  }

  size_t lower = 0;
  size_t upper = boundCount;
  while (lower < upper) {
    const auto mid = lower + (upper - lower) / 2;
    const auto bound = bounds->at(mid);
    if (!bound.has_value() || value <= bound.value()) {
      upper = mid;
    } else {
      lower = mid + 1;
    }
  }
  result = static_cast<int32_t>(lower);
}

template <typename T>
struct DeltaRangePartitionIdTinyintFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<facebook::velox::Variadic<int8_t>>& inputs) {
    deltaRangePartitionId(result, &inputs);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(int32_t& result, const arg_type<facebook::velox::Variadic<int8_t>>* inputs) {
    deltaRangePartitionId(result, inputs);
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdTinyintArrayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int8_t value, const arg_type<facebook::velox::Array<int8_t>>& bounds) {
    deltaRangePartitionIdFromBounds(result, value, &bounds);
  }

  FOLLY_ALWAYS_INLINE bool
  callNullable(int32_t& result, const int8_t* value, const arg_type<facebook::velox::Array<int8_t>>* bounds) {
    result = 0;
    if (value != nullptr) {
      deltaRangePartitionIdFromBounds(result, *value, bounds);
    }
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdSmallintFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<facebook::velox::Variadic<int16_t>>& inputs) {
    deltaRangePartitionId(result, &inputs);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(int32_t& result, const arg_type<facebook::velox::Variadic<int16_t>>* inputs) {
    deltaRangePartitionId(result, inputs);
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdSmallintArrayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(int32_t& result, int16_t value, const arg_type<facebook::velox::Array<int16_t>>& bounds) {
    deltaRangePartitionIdFromBounds(result, value, &bounds);
  }

  FOLLY_ALWAYS_INLINE bool
  callNullable(int32_t& result, const int16_t* value, const arg_type<facebook::velox::Array<int16_t>>* bounds) {
    result = 0;
    if (value != nullptr) {
      deltaRangePartitionIdFromBounds(result, *value, bounds);
    }
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdIntegerFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<facebook::velox::Variadic<int32_t>>& inputs) {
    deltaRangePartitionId(result, &inputs);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(int32_t& result, const arg_type<facebook::velox::Variadic<int32_t>>* inputs) {
    deltaRangePartitionId(result, inputs);
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdIntegerArrayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(int32_t& result, int32_t value, const arg_type<facebook::velox::Array<int32_t>>& bounds) {
    deltaRangePartitionIdFromBounds(result, value, &bounds);
  }

  FOLLY_ALWAYS_INLINE bool
  callNullable(int32_t& result, const int32_t* value, const arg_type<facebook::velox::Array<int32_t>>* bounds) {
    result = 0;
    if (value != nullptr) {
      deltaRangePartitionIdFromBounds(result, *value, bounds);
    }
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdBigintFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<facebook::velox::Variadic<int64_t>>& inputs) {
    deltaRangePartitionId(result, &inputs);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(int32_t& result, const arg_type<facebook::velox::Variadic<int64_t>>* inputs) {
    deltaRangePartitionId(result, inputs);
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdBigintArrayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(int32_t& result, int64_t value, const arg_type<facebook::velox::Array<int64_t>>& bounds) {
    deltaRangePartitionIdFromBounds(result, value, &bounds);
  }

  FOLLY_ALWAYS_INLINE bool
  callNullable(int32_t& result, const int64_t* value, const arg_type<facebook::velox::Array<int64_t>>* bounds) {
    result = 0;
    if (value != nullptr) {
      deltaRangePartitionIdFromBounds(result, *value, bounds);
    }
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<facebook::velox::Variadic<facebook::velox::Date>>& inputs) {
    deltaRangePartitionId(result, &inputs);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      int32_t& result,
      const arg_type<facebook::velox::Variadic<facebook::velox::Date>>* inputs) {
    deltaRangePartitionId(result, inputs);
    return true;
  }
};

template <typename T>
struct DeltaRangePartitionIdDateArrayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<facebook::velox::Date>& value,
      const arg_type<facebook::velox::Array<facebook::velox::Date>>& bounds) {
    deltaRangePartitionIdFromBounds(result, value, &bounds);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      int32_t& result,
      const arg_type<facebook::velox::Date>* value,
      const arg_type<facebook::velox::Array<facebook::velox::Date>>* bounds) {
    result = 0;
    if (value != nullptr) {
      deltaRangePartitionIdFromBounds(result, *value, bounds);
    }
    return true;
  }
};

} // namespace gluten
