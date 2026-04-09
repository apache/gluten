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

#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <string_view>

namespace gluten::delta {

/// Utilities for UUID generation and encoding for Delta Deletion Vectors.
/// Implements UUID v4 generation and Z85 encoding as per Delta spec.
class DeltaUuidUtils {
 public:
  /// UUID represented as 16 bytes
  using Uuid = std::array<uint8_t, 16>;

  /// Generate a random UUID v4.
  /// Format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
  /// where x is any hexadecimal digit and y is one of 8, 9, A, or B
  static Uuid generateUuid();

  /// Convert UUID to canonical string format.
  /// Example: "550e8400-e29b-41d4-a716-446655440000"
  static std::string uuidToString(const Uuid& uuid);

  /// Encode UUID to Z85 format (20 characters).
  /// Z85 is a Base85 encoding that is JSON-friendly.
  /// Used in Delta's 'u' storage type for DV descriptors.
  static std::string encodeUuidToZ85(const Uuid& uuid);

  /// Decode Z85 string back to UUID.
  static Uuid decodeZ85ToUuid(const std::string& z85);

  /// Encode arbitrary bytes using Delta's Base85-compatible alphabet.
  static std::string encodeBytesToBase85(std::string_view data);

  /// Decode arbitrary bytes encoded with Delta's Base85-compatible alphabet.
  static std::string decodeBase85ToBytes(
      std::string_view encoded,
      size_t decodedSize);

  /// Extract UUID and random prefix from Z85-encoded string.
  /// Returns pair of (randomPrefix, uuid).
  /// The Z85 string may have a random prefix before the UUID.
  static std::pair<std::string, Uuid> extractUuidFromZ85(const std::string& z85);

  /// Reconstruct the full path to a DV file from components.
  /// Format: <tableDir>/<randomPrefix>/deletion_vector_<uuid>.bin
  static std::string reconstructUuidPath(
      const std::string& tableDir,
      const std::string& randomPrefix,
      const Uuid& uuid);

 private:
  /// Z85 encoding alphabet (85 printable ASCII characters)
  static constexpr const char* Z85_ALPHABET =
      "0123456789"
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      ".-:+=^!/*?&<>()[]{}@%$#";

  /// Encode 4 bytes to 5 Z85 characters
  static void encodeZ85Block(
      const uint8_t* input,
      char* output);

  /// Decode 5 Z85 characters to 4 bytes
  static void decodeZ85Block(
      const char* input,
      uint8_t* output);
};

} // namespace gluten::delta
