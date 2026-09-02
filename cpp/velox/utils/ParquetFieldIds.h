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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/type/Type.h"

namespace gluten {

class IcebergNestedField;
class ParquetFieldIdSchema;

/// Backend-neutral representation of one node of a Parquet field-id tree,
/// decoded from either wire format before validation.
struct FieldIdNode {
  enum class Kind { kPrimitive, kStruct, kArray, kMap };

  /// Empty for synthetic array-element / map-key / map-value nodes, and for any
  /// producer whose wire format does not carry names (IcebergNestedField).
  std::string name;
  Kind kind{Kind::kPrimitive};
  /// Absent means "this node legitimately has no field id". Distinct from any
  /// sentinel: see kNoParquetFieldId.
  std::optional<int32_t> id;
  std::vector<FieldIdNode> children;
};

/// The value velox::parquet uses to mean "no field id".
///
/// It must be strictly negative. velox/dwio/parquet/writer/arrow/ArrowSchema.cpp
/// fieldIdMetadata() emits the PARQUET:field_id metadata for any id >= 0, so 0 is
/// a real field id that would be written to the footer, not an absence marker.
///
/// This is the one and only place a Gluten FieldIdNode's absent id is lowered to
/// a sentinel. If velox::dwio::common::ParquetFieldId ever grows an
/// std::optional<int32_t>, this constant and its single use site disappear.
constexpr int32_t kNoParquetFieldId = -1;

/// Decode the tree carried by the Delta / generic Parquet write path.
std::vector<FieldIdNode> fromProto(const ParquetFieldIdSchema& proto);

/// Decode the tree carried by the Iceberg write path.
///
/// IcebergNestedField carries no names and no field presence, so the resulting
/// nodes have empty names and always-present ids, and must be resolved with
/// checkNames = false. Its root node is synthetic (it wraps the top-level fields
/// in one struct), so this returns that root's children.
std::vector<FieldIdNode> fromProto(const IcebergNestedField& proto);

/// Validate a decoded field-id tree against the Velox write schema it annotates,
/// and lower it to the vector velox::parquet::ParquetWriterOptions expects.
///
/// Throws GlutenException, naming the offending dotted path, when the tree does
/// not describe the schema. Checks performed at EVERY depth, including inside
/// arrays and maps:
///
///   - arity: struct children == row size, array children == 1, map children == 2,
///     primitive children == 0
///   - kind agrees with the Velox TypeKind
///   - name agrees with the Velox field name (when checkNames)
///   - every present id is strictly positive
///   - ids are unique across the WHOLE tree, not merely per level: Iceberg requires
///     table-global unique ids, and two sibling structs each carrying id 5 would
///     otherwise produce a well-formed footer that resolves to the wrong column
///
/// Velox performs none of these except a shallow arity check, and its own
/// recursion stops at non-ROW array elements, so ARRAY<ARRAY<T>> and ARRAY<MAP<K,V>>
/// reach an unguarded .at(0) instead of a diagnostic.
std::vector<facebook::velox::parquet::ParquetFieldId> resolveParquetFieldIds(
    const std::vector<FieldIdNode>& nodes,
    const facebook::velox::RowTypePtr& rowType,
    bool checkNames);

} // namespace gluten
