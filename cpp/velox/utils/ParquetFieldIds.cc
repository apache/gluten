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

#include <unordered_map>

#include "IcebergNestedField.pb.h"
#include "ParquetFieldIds.pb.h"
#include "utils/Exception.h"

using namespace facebook::velox;

namespace gluten {

namespace {

FieldIdNode::Kind kindFromProto(ParquetFieldIdNode::Kind kind) {
  switch (kind) {
    case ParquetFieldIdNode::STRUCT:
      return FieldIdNode::Kind::kStruct;
    case ParquetFieldIdNode::ARRAY:
      return FieldIdNode::Kind::kArray;
    case ParquetFieldIdNode::MAP:
      return FieldIdNode::Kind::kMap;
    default:
      return FieldIdNode::Kind::kPrimitive;
  }
}

FieldIdNode nodeFromProto(const ParquetFieldIdNode& proto) {
  FieldIdNode node;
  node.name = proto.name();
  node.kind = kindFromProto(proto.kind());
  if (proto.has_id()) {
    node.id = proto.id();
  }
  node.children.reserve(proto.children_size());
  for (const auto& child : proto.children()) {
    node.children.push_back(nodeFromProto(child));
  }
  return node;
}

FieldIdNode nodeFromIcebergProto(const IcebergNestedField& proto) {
  FieldIdNode node;
  // IcebergNestedField carries neither a name nor field presence.
  node.id = proto.id();
  node.children.reserve(proto.children_size());
  for (const auto& child : proto.children()) {
    node.children.push_back(nodeFromIcebergProto(child));
  }
  // Kind is not on the wire; infer it structurally so the arity check below still
  // has something to compare against. resolveParquetFieldIds re-derives the
  // authoritative kind from the Velox type, so this only affects the message text.
  node.kind = node.children.empty() ? FieldIdNode::Kind::kPrimitive : FieldIdNode::Kind::kStruct;
  return node;
}

const char* kindName(FieldIdNode::Kind kind) {
  switch (kind) {
    case FieldIdNode::Kind::kStruct:
      return "STRUCT";
    case FieldIdNode::Kind::kArray:
      return "ARRAY";
    case FieldIdNode::Kind::kMap:
      return "MAP";
    default:
      return "PRIMITIVE";
  }
}

FieldIdNode::Kind kindOf(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::ROW:
      return FieldIdNode::Kind::kStruct;
    case TypeKind::ARRAY:
      return FieldIdNode::Kind::kArray;
    case TypeKind::MAP:
      return FieldIdNode::Kind::kMap;
    default:
      return FieldIdNode::Kind::kPrimitive;
  }
}

std::string childPath(const std::string& parent, const std::string& child) {
  if (parent.empty()) {
    return child;
  }
  return parent + "." + child;
}

struct Resolver {
  bool checkNames;
  // Whole-tree, not per-level: Iceberg requires table-global unique ids.
  std::unordered_map<int32_t, std::string> seenIds;

  parquet::ParquetFieldId resolve(const FieldIdNode& node, const TypePtr& type, const std::string& path) {
    const auto expectedKind = kindOf(type);
    // Iceberg's wire format has no kind, so only reject a genuine disagreement
    // between a kind-bearing producer and the schema.
    if (checkNames && node.kind != expectedKind) {
      throw GlutenException(
          "Parquet field id tree does not match the write schema at '" + path + "': tree says " + kindName(node.kind) +
          " but the schema says " + kindName(expectedKind) + ".");
    }

    parquet::ParquetFieldId result;
    result.fieldId = kNoParquetFieldId;
    if (node.id.has_value()) {
      const auto id = node.id.value();
      if (id <= 0) {
        throw GlutenException(
            "Parquet field id at '" + path + "' must be a positive number, got " + std::to_string(id) +
            ". Zero is a valid Parquet field id and would be written to the footer; "
            "a node with no field id must omit it rather than encode a sentinel.");
      }
      const auto [it, inserted] = seenIds.emplace(id, path);
      if (!inserted) {
        throw GlutenException(
            "Duplicate Parquet field id " + std::to_string(id) + " at '" + path + "': already used by '" + it->second +
            "'. Field ids must be unique across the whole schema.");
      }
      result.fieldId = id;
    }

    switch (expectedKind) {
      case FieldIdNode::Kind::kStruct: {
        const auto& rowType = type->asRow();
        checkArity(node, rowType.size(), path, "one child per struct field");
        result.children.reserve(rowType.size());
        for (auto i = 0; i < rowType.size(); ++i) {
          const auto& childName = rowType.nameOf(i);
          if (checkNames && node.children[i].name != childName) {
            throw GlutenException(
                "Parquet field id tree does not match the write schema at '" + childPath(path, childName) +
                "': tree says field " + std::to_string(i) + " is named '" + node.children[i].name +
                "' but the schema says '" + childName + "'.");
          }
          result.children.push_back(resolve(node.children[i], rowType.childAt(i), childPath(path, childName)));
        }
        break;
      }
      case FieldIdNode::Kind::kArray: {
        checkArity(node, 1, path, "exactly one child, the array element");
        result.children.push_back(resolve(node.children[0], type->childAt(0), childPath(path, "element")));
        break;
      }
      case FieldIdNode::Kind::kMap: {
        checkArity(node, 2, path, "exactly two children, the map key then the map value");
        result.children.push_back(resolve(node.children[0], type->childAt(0), childPath(path, "key")));
        result.children.push_back(resolve(node.children[1], type->childAt(1), childPath(path, "value")));
        break;
      }
      case FieldIdNode::Kind::kPrimitive:
        checkArity(node, 0, path, "no children");
        break;
    }
    return result;
  }

  void checkArity(const FieldIdNode& node, size_t expected, const std::string& path, const char* what) {
    // Deliberately '!=', not '<='. velox::parquet::Writer accepts an over-long
    // child list and silently ignores the extras.
    if (node.children.size() != expected) {
      throw GlutenException(
          "Parquet field id tree does not match the write schema at '" + (path.empty() ? std::string("<root>") : path) +
          "': expected " + what + " (" + std::to_string(expected) + "), got " + std::to_string(node.children.size()) +
          ".");
    }
  }
};

} // namespace

std::vector<FieldIdNode> fromProto(const ParquetFieldIdSchema& proto) {
  std::vector<FieldIdNode> nodes;
  nodes.reserve(proto.fields_size());
  for (const auto& field : proto.fields()) {
    nodes.push_back(nodeFromProto(field));
  }
  return nodes;
}

std::vector<FieldIdNode> fromProto(const IcebergNestedField& proto) {
  // The Iceberg producer wraps the top-level fields in a synthetic root struct
  // whose own id is unset; the caller wants the columns.
  auto root = nodeFromIcebergProto(proto);
  return std::move(root.children);
}

std::vector<parquet::ParquetFieldId>
resolveParquetFieldIds(const std::vector<FieldIdNode>& nodes, const RowTypePtr& rowType, bool checkNames) {
  Resolver resolver{checkNames, {}};
  FieldIdNode root;
  root.kind = FieldIdNode::Kind::kStruct;
  root.children = nodes;
  auto resolved = resolver.resolve(root, rowType, "");
  return std::move(resolved.children);
}

} // namespace gluten
