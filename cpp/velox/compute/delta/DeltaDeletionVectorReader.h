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

#include "velox/common/base/BitUtil.h"
#include "velox/vector/ComplexVector.h"

#include <memory>
#include <optional>
#include <roaring/roaring64map.hh>
#include <string>
#include <string_view>

namespace gluten::delta {

using namespace facebook::velox;

/// Reads and manages Delta Lake deletion vectors for filtering deleted rows
/// during table scans.
///
/// The JVM Delta side materializes the deletion vector and hands the serialized
/// bitmap payload to native. This reader only deserializes that payload and
/// applies row filtering during scan.
///
/// Usage example:
/// @code
///   auto reader = std::make_unique<DeltaDeletionVectorReader>();
///   reader->loadSerializedDeletionVector(serializedPayload, expectedCardinality);
///   if (reader->isRowDeleted(42)) {
///     // Skip this row during scan
///   }
/// @endcode
class DeltaDeletionVectorReader {
 public:
  DeltaDeletionVectorReader() = default;

  /// Loads a deletion vector from an already decoded serialized Delta payload.
  /// @param serializedPayload Materialized Delta DV bitmap payload.
  /// @param expectedCardinality Optional number of deleted row positions from
  /// Delta metadata. When set, the reader verifies this value against the
  /// deserialized bitmap cardinality and fails loading on mismatch.
  void loadSerializedDeletionVector(
      std::string_view serializedPayload,
      std::optional<uint64_t> expectedCardinality = std::nullopt);

  /// Checks if a specific row position is marked as deleted.
  /// Note: This method is not const because it may update internal caching
  /// state.
  /// @param rowPosition 0-based row position in the data file
  /// @return true if the row is deleted, false otherwise
  bool isRowDeleted(uint64_t rowPosition);

  /// Applies deletion filter to a batch of rows, updating the deletion bitmap.
  /// This is called during scan to mark deleted rows in the output bitmap.
  /// @param baseReadOffset Starting row position for this batch (absolute)
  /// @param size Number of rows in the batch
  /// @param deleteBitmap Output bitmap marking deleted rows (1 = deleted, 0 =
  /// keep)
  void applyDeletionFilter(uint64_t baseReadOffset, uint64_t size, BufferPtr deleteBitmap);

  /// Returns true if no deletion vector is loaded.
  bool empty() const {
    return !deletionBitmap_.has_value();
  }

  /// Returns the approximate number of deleted rows in the loaded DV.
  /// Note: This is an approximation based on bitmap size.
  uint64_t estimatedDeletedRowCount() const;

 private:
  void loadSerializedDeletionVectorInternal(
      std::string_view serializedPayload,
      const std::string& debugName,
      std::optional<uint64_t> expectedCardinality);

  // The loaded deletion vector bitmap
  std::optional<roaring::Roaring64Map> deletionBitmap_;
};

} // namespace gluten::delta
