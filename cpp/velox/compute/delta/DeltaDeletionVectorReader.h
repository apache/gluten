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
#include "velox/common/file/FileSystems.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"

#include <memory>
#include <optional>
#include <roaring/roaring64map.hh>
#include <string>

namespace gluten::delta {

using namespace facebook::velox;

/// Reads and manages Delta Lake deletion vectors for filtering deleted rows
/// during table scans.
///
/// Delta deletion vectors are stored as roaring bitmaps in binary files under
/// the _delta_log/_deletion_vectors/ directory. Each DV file contains a
/// serialized RoaringBitmapArray marking deleted row positions (0-based).
///
/// The deletion vector format follows the Delta Lake protocol specification:
/// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format
///
/// Storage types:
/// - 'u': UUID-based path in _deletion_vectors/ directory
/// - 'p': Absolute path to DV file
/// - 'i': Inline base64-encoded data (for small DVs)
///
/// Usage example:
/// @code
///   auto reader = std::make_unique<DeltaDeletionVectorReader>(
///       fileSystem, pool, ioStats);
///   reader->loadDeletionVector("s3://bucket/_delta_log/_deletion_vectors/dv_uuid.bin");
///   if (reader->isRowDeleted(42)) {
///     // Skip this row during scan
///   }
/// @endcode
class DeltaDeletionVectorReader {
 public:
  /// Constructs a DV reader.
  /// @param fileSystem File system for reading DV files
  /// @param pool Memory pool for allocations
  /// @param ioStats I/O statistics tracker
  DeltaDeletionVectorReader(
      std::shared_ptr<filesystems::FileSystem> fileSystem,
      memory::MemoryPool* pool,
      std::shared_ptr<io::IoStatistics> ioStats);

  /// Loads a deletion vector from an external file.
  /// @param dvPath Full path to the DV file
  /// @param offset Optional byte offset inside the DV file
  /// @param sizeInBytes Optional number of bytes to read from the DV file
  /// @param expectedCardinality Optional expected cardinality for validation
  /// @throws VeloxException if file cannot be read or format is invalid
  void loadDeletionVector(
      const std::string& dvPath,
      std::optional<uint64_t> offset = std::nullopt,
      std::optional<uint64_t> sizeInBytes = std::nullopt,
      std::optional<uint64_t> expectedCardinality = std::nullopt);

  /// Loads a deletion vector from inline Base85-encoded data.
  /// Used for small DVs stored directly in the Delta log.
  /// @param inlineData Base85-encoded serialized roaring bitmap
  /// @param sizeInBytes Optional decoded payload size required by the Delta inline format
  /// @param expectedCardinality Optional expected cardinality for validation
  /// @throws VeloxException if data cannot be decoded or format is invalid
  void loadInlineDeletionVector(
      const std::string& inlineData,
      std::optional<uint64_t> sizeInBytes = std::nullopt,
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
  std::shared_ptr<filesystems::FileSystem> fileSystem_;
  memory::MemoryPool* pool_;
  std::shared_ptr<io::IoStatistics> ioStats_;

  // The loaded deletion vector bitmap
  std::optional<roaring::Roaring64Map> deletionBitmap_;
};

} // namespace gluten::delta
