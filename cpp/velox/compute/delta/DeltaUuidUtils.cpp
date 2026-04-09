/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "compute/delta/DeltaUuidUtils.h"

#include <cstring>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string_view>

#include "velox/common/base/Exceptions.h"

namespace gluten::delta {

namespace {

// Thread-local random number generator for UUID generation
thread_local std::random_device rd;
thread_local std::mt19937_64 gen(rd());
thread_local std::uniform_int_distribution<uint64_t> dis;

} // namespace

DeltaUuidUtils::Uuid DeltaUuidUtils::generateUuid() {
  Uuid uuid;
  
  // Generate 16 random bytes
  uint64_t* ptr = reinterpret_cast<uint64_t*>(uuid.data());
  ptr[0] = dis(gen);
  ptr[1] = dis(gen);
  
  // Set version to 4 (random UUID)
  // Version is in bits 12-15 of the time_hi_and_version field (byte 6)
  uuid[6] = (uuid[6] & 0x0F) | 0x40;
  
  // Set variant to RFC 4122
  // Variant is in bits 6-7 of the clock_seq_hi_and_reserved field (byte 8)
  uuid[8] = (uuid[8] & 0x3F) | 0x80;
  
  return uuid;
}

std::string DeltaUuidUtils::uuidToString(const Uuid& uuid) {
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  
  // Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  for (size_t i = 0; i < 16; ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      oss << '-';
    }
    oss << std::setw(2) << static_cast<int>(uuid[i]);
  }
  
  return oss.str();
}

std::string DeltaUuidUtils::encodeUuidToZ85(const Uuid& uuid) {
  // Z85 encodes 4 bytes into 5 characters
  // UUID is 16 bytes, so we get 20 characters
  std::string result(20, '\0');
  
  // Encode in 4-byte blocks
  for (size_t i = 0; i < 4; ++i) {
    encodeZ85Block(
        uuid.data() + (i * 4),
        &result[i * 5]);
  }
  
  return result;
}

std::string DeltaUuidUtils::encodeBytesToBase85(std::string_view data) {
  if (data.empty()) {
    return "";
  }

  const size_t paddedSize = ((data.size() + 3) / 4) * 4;
  std::string result((paddedSize / 4) * 5, '\0');
  std::array<uint8_t, 4> block{};

  for (size_t blockIndex = 0; blockIndex < paddedSize / 4; ++blockIndex) {
    block.fill(0);
    const size_t inputOffset = blockIndex * 4;
    const size_t bytesToCopy =
        inputOffset < data.size() ? std::min<size_t>(4, data.size() - inputOffset) : 0;
    if (bytesToCopy > 0) {
      std::memcpy(block.data(), data.data() + inputOffset, bytesToCopy);
    }
    encodeZ85Block(block.data(), result.data() + (blockIndex * 5));
  }

  return result;
}

DeltaUuidUtils::Uuid DeltaUuidUtils::decodeZ85ToUuid(const std::string& z85) {
  VELOX_CHECK_EQ(
      z85.length(),
      20,
      "Z85 encoded UUID must be exactly 20 characters");
  
  Uuid uuid;
  
  // Decode in 5-character blocks
  for (size_t i = 0; i < 4; ++i) {
    decodeZ85Block(
        z85.data() + (i * 5),
        uuid.data() + (i * 4));
  }
  
  return uuid;
}

std::string DeltaUuidUtils::decodeBase85ToBytes(
    std::string_view encoded,
    size_t decodedSize) {
  if (encoded.empty()) {
    VELOX_CHECK_EQ(
        decodedSize,
        0,
        "Expected decoded Base85 size to be 0 for empty input");
    return "";
  }

  VELOX_CHECK_EQ(
      encoded.size() % 5,
      0,
      "Base85 encoded payload must be aligned to 5-character blocks");

  const size_t maxDecodedSize = (encoded.size() / 5) * 4;
  VELOX_CHECK_LE(
      decodedSize,
      maxDecodedSize,
      "Requested decoded size {} exceeds Base85 payload capacity {}",
      decodedSize,
      maxDecodedSize);

  std::string decoded(maxDecodedSize, '\0');
  for (size_t blockIndex = 0; blockIndex < encoded.size() / 5; ++blockIndex) {
    decodeZ85Block(
        encoded.data() + (blockIndex * 5),
        reinterpret_cast<uint8_t*>(decoded.data() + (blockIndex * 4)));
  }
  decoded.resize(decodedSize);
  return decoded;
}

void DeltaUuidUtils::encodeZ85Block(
    const uint8_t* input,
    char* output) {
  // Convert 4 bytes to a 32-bit value (big-endian)
  uint32_t value = 
      (static_cast<uint32_t>(input[0]) << 24) |
      (static_cast<uint32_t>(input[1]) << 16) |
      (static_cast<uint32_t>(input[2]) << 8) |
      static_cast<uint32_t>(input[3]);
  
  // Encode as 5 base-85 digits (most significant first)
  for (int i = 4; i >= 0; --i) {
    output[i] = Z85_ALPHABET[value % 85];
    value /= 85;
  }
}

void DeltaUuidUtils::decodeZ85Block(
    const char* input,
    uint8_t* output) {
  // Build reverse lookup table
  static const auto buildReverseLookup = []() {
    std::array<int8_t, 256> table;
    table.fill(-1);
    for (size_t i = 0; i < 85; ++i) {
      table[static_cast<uint8_t>(Z85_ALPHABET[i])] = static_cast<int8_t>(i);
    }
    return table;
  };
  static const auto reverseLookup = buildReverseLookup();
  
  // Decode 5 characters to a 32-bit value
  uint32_t value = 0;
  for (int i = 0; i < 5; ++i) {
    int8_t digit = reverseLookup[static_cast<uint8_t>(input[i])];
    VELOX_CHECK_GE(
        digit,
        0,
        "Invalid Z85 character: {}",
        input[i]);
    value = value * 85 + static_cast<uint32_t>(digit);
  }
  
  // Convert to 4 bytes (big-endian)
  output[0] = static_cast<uint8_t>((value >> 24) & 0xFF);
  output[1] = static_cast<uint8_t>((value >> 16) & 0xFF);
  output[2] = static_cast<uint8_t>((value >> 8) & 0xFF);
  output[3] = static_cast<uint8_t>(value & 0xFF);
}

std::pair<std::string, DeltaUuidUtils::Uuid>
DeltaUuidUtils::extractUuidFromZ85(const std::string& z85) {
  // Z85-encoded UUID is always 20 characters
  // Any characters before that are the random prefix
  if (z85.length() < 20) {
    VELOX_FAIL("Z85 string too short to contain UUID: {}", z85);
  }
  
  std::string randomPrefix;
  std::string uuidZ85;
  
  if (z85.length() == 20) {
    // No random prefix
    uuidZ85 = z85;
  } else {
    // Extract random prefix and UUID
    randomPrefix = z85.substr(0, z85.length() - 20);
    uuidZ85 = z85.substr(z85.length() - 20);
  }
  
  Uuid uuid = decodeZ85ToUuid(uuidZ85);
  return {randomPrefix, uuid};
}

std::string DeltaUuidUtils::reconstructUuidPath(
    const std::string& tableDir,
    const std::string& randomPrefix,
    const Uuid& uuid) {
  std::ostringstream oss;
  oss << tableDir;
  
  if (!randomPrefix.empty()) {
    oss << "/" << randomPrefix;
  }
  
  oss << "/deletion_vector_" << uuidToString(uuid) << ".bin";
  return oss.str();
}

} // namespace gluten::delta
