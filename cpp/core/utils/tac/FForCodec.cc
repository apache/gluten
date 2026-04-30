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

#include "utils/tac/FForCodec.h"
#include "utils/tac/ffor.hpp"

namespace gluten {

int64_t FForCodec::maxCompressedLength(int64_t inputSize) {
  size_t numValues = inputSize / sizeof(uint64_t);
  return static_cast<int64_t>(ffor::compress64Bound(numValues));
}

arrow::Result<int64_t>
FForCodec::compress(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize) {
  if (inputSize == 0) {
    return 0;
  }
  if (inputSize % sizeof(uint64_t) != 0) {
    return arrow::Status::Invalid("FForCodec: input size ", inputSize, " is not a multiple of 8.");
  }

  size_t numValues = inputSize / sizeof(uint64_t);
  auto maxLen = static_cast<int64_t>(ffor::compress64Bound(numValues));
  if (outputSize < maxLen) {
    return arrow::Status::Invalid("FForCodec: output buffer too small.");
  }

  auto written = ffor::compress64(reinterpret_cast<const uint64_t*>(input), numValues, output);
  return static_cast<int64_t>(written);
}

arrow::Result<int64_t>
FForCodec::decompress(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize) {
  if (outputSize == 0) {
    return 0;
  }
  if (outputSize % sizeof(uint64_t) != 0) {
    return arrow::Status::Invalid("FForCodec: output size ", outputSize, " is not a multiple of 8.");
  }

  auto nDecoded = ffor::decompress64(input, inputSize, reinterpret_cast<uint64_t*>(output));
  return static_cast<int64_t>(nDecoded);
}

} // namespace gluten
