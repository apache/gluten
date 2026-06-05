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

#include "utils/tac/TypeAwareCompressCodec.h"
#include "utils/tac/FForCodec.h"

namespace gluten {

bool TypeAwareCompressCodec::support(int8_t tacType) {
  return tacType == tac::kUInt64;
}

int64_t TypeAwareCompressCodec::maxCompressedLen(int64_t inputLen, int8_t tacType) {
  if (!support(tacType)) {
    return 0;
  }
  return kPayloadHeaderSize + FForCodec::maxCompressedLength(inputLen);
}

arrow::Result<int64_t> TypeAwareCompressCodec::compress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen,
    int8_t tacType) {
  if (!support(tacType)) {
    return arrow::Status::Invalid("Type-aware compression not supported for tac type: ", static_cast<int>(tacType));
  }
  if (inputLen == 0) {
    return 0;
  }
  if (outputLen < kPayloadHeaderSize) {
    return arrow::Status::Invalid("Output buffer too small for type-aware compression.");
  }

  auto* out = output;
  *out++ = static_cast<uint8_t>(CodecId::kFFor);
  *out++ = static_cast<uint8_t>(tacType);

  auto availableOutput = outputLen - kPayloadHeaderSize;
  ARROW_ASSIGN_OR_RAISE(auto compressedLen, FForCodec::compress(input, inputLen, out, availableOutput));

  return kPayloadHeaderSize + compressedLen;
}

arrow::Result<int64_t>
TypeAwareCompressCodec::decompress(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen) {
  if (inputLen < kPayloadHeaderSize) {
    return arrow::Status::Invalid("Input too small for type-aware decompress header.");
  }

  auto* in = input;
  auto codecId = static_cast<CodecId>(*in++);
  [[maybe_unused]] auto tacType = *in++;
  auto dataLen = inputLen - kPayloadHeaderSize;

  switch (codecId) {
    case CodecId::kFFor: {
      ARROW_ASSIGN_OR_RAISE(auto nDecoded, FForCodec::decompress(in, dataLen, output, outputLen));
      (void)nDecoded;
      return inputLen;
    }
    default:
      return arrow::Status::Invalid("Unknown type-aware codec ID: ", static_cast<int>(codecId));
  }
}

} // namespace gluten
