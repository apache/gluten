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

#include <string_view>
#include <vector>

#include "ParquetFieldIds.pb.h"
#include "velox/dwio/common/ParquetFieldId.h"
#include "velox/type/Type.h"

namespace gluten {

class IcebergNestedField;

// Parquet field IDs are passed as one velox ParquetFieldId tree per column, in the shape
// velox::parquet::Writer takes: a struct node has one child per field, an array node one child
// for its element, a map node two, for its key and value, and any other node none. An ID of 0 or
// more is written to the file footer; a negative ID means the node has none.

/// The field IDs of the Iceberg write path. The root of IcebergNestedField wraps the columns in a
/// struct of its own and has no ID, so the columns are its children.
std::vector<facebook::velox::parquet::ParquetFieldId> parquetFieldIdsFromProto(const IcebergNestedField& root);

/// The field IDs of one ParquetFieldIdNode per column.
std::vector<facebook::velox::parquet::ParquetFieldId> parquetFieldIdsFromProto(
    const google::protobuf::RepeatedPtrField<ParquetFieldIdNode>& columns);

/// The field IDs of the Parquet writer option kParquetFieldIds: a Base64-encoded ParquetFieldIdNode
/// whose children are the columns.
std::vector<facebook::velox::parquet::ParquetFieldId> parquetFieldIdsFromWriteOption(std::string_view value);

/// Checks that `fieldIds` describes `rowType` at every depth. Throws a GlutenException naming the
/// dotted path of the first node that:
///   - has a number of children other than its type's;
///   - has no ID, unless `allowMissingIds`;
///   - has the ID of another node of the tree. Iceberg requires IDs to be unique in the whole
///     schema, so two sibling structs that each use ID 5 would otherwise give a well-formed footer
///     that resolves to the wrong column.
///
/// velox::parquet::Writer checks the number of children of columns and struct fields only, and
/// does not look below an array or map whose element, key or value is not a struct. A short tree
/// there fails inside the writer, and extra children are ignored.
void validateParquetFieldIds(
    const std::vector<facebook::velox::parquet::ParquetFieldId>& fieldIds,
    const facebook::velox::RowType& rowType,
    bool allowMissingIds);

} // namespace gluten
