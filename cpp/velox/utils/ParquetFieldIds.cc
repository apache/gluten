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

#include "utils/ParquetFieldIds.h"

#include <string>
#include <unordered_map>

#include "IcebergNestedField.pb.h"
#include "utils/Exception.h"

using namespace facebook::velox;

namespace gluten {

namespace {

parquet::ParquetFieldId fromProto(const IcebergNestedField& field) {
  parquet::ParquetFieldId fieldId{field.id(), {}};
  fieldId.children.reserve(field.children_size());
  for (const auto& child : field.children()) {
    fieldId.children.push_back(fromProto(child));
  }
  return fieldId;
}

class Validator {
 public:
  explicit Validator(bool allowMissingIds) : allowMissingIds_(allowMissingIds) {}

  void validateColumns(const std::vector<parquet::ParquetFieldId>& fieldIds, const RowType& rowType) {
    checkChildren(fieldIds.size(), rowType.size(), "<root>", "one per column");
    for (column_index_t i = 0; i < rowType.size(); ++i) {
      validate(fieldIds[i], rowType.childAt(i), rowType.nameOf(i));
    }
  }

 private:
  void validate(const parquet::ParquetFieldId& fieldId, const TypePtr& type, const std::string& path) {
    checkId(fieldId.fieldId, path);
    const auto& children = fieldId.children;
    switch (type->kind()) {
      case TypeKind::ROW: {
        const auto& rowType = type->asRow();
        checkChildren(children.size(), rowType.size(), path, "one per struct field");
        for (column_index_t i = 0; i < rowType.size(); ++i) {
          validate(children[i], rowType.childAt(i), path + "." + rowType.nameOf(i));
        }
        break;
      }
      case TypeKind::ARRAY:
        checkChildren(children.size(), 1, path, "the array element");
        validate(children[0], type->childAt(0), path + ".element");
        break;
      case TypeKind::MAP:
        checkChildren(children.size(), 2, path, "the map key and value");
        validate(children[0], type->childAt(0), path + ".key");
        validate(children[1], type->childAt(1), path + ".value");
        break;
      default:
        checkChildren(children.size(), 0, path, "none for a " + type->toString());
        break;
    }
  }

  void checkId(int32_t id, const std::string& path) {
    if (id < 0) {
      if (!allowMissingIds_) {
        throw GlutenException("Parquet field ID of '" + path + "' is missing (" + std::to_string(id) + ").");
      }
      return;
    }
    const auto [it, inserted] = paths_.emplace(id, path);
    if (!inserted) {
      throw GlutenException(
          "Parquet field ID " + std::to_string(id) + " of '" + path + "' is also the ID of '" + it->second +
          "'. Field IDs must be unique in the whole schema.");
    }
  }

  static void checkChildren(size_t actual, size_t expected, const std::string& path, const std::string& what) {
    if (actual != expected) {
      throw GlutenException(
          "Parquet field IDs do not match the write schema at '" + path + "': expected " + std::to_string(expected) +
          " children (" + what + "), got " + std::to_string(actual) + ".");
    }
  }

  const bool allowMissingIds_;
  // The path of each ID seen so far, over the whole tree.
  std::unordered_map<int32_t, std::string> paths_;
};

} // namespace

std::vector<parquet::ParquetFieldId> parquetFieldIdsFromProto(const IcebergNestedField& root) {
  return fromProto(root).children;
}

void validateParquetFieldIds(
    const std::vector<parquet::ParquetFieldId>& fieldIds,
    const RowType& rowType,
    bool allowMissingIds) {
  Validator(allowMissingIds).validateColumns(fieldIds, rowType);
}

} // namespace gluten
