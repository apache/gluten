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

#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"

#include <google/protobuf/wrappers.pb.h>

#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/type/Type.h"

namespace gluten {

typedef ::bytedance::bolt::connector::hive::HiveColumnHandle::ColumnType ColumnType;

/// This class contains some common functions used to parse Substrait
/// components, and convert them into recognizable representations.
class SubstraitParser {
 public:
  /// Used to parse Substrait NamedStruct.
  static std::vector<bytedance::bolt::TypePtr> parseNamedStruct(
      const ::substrait::NamedStruct& namedStruct,
      bool asLowerCase = false);

  /// Used to parse column types from Substrait NamedStruct.
  static void parseColumnTypes(const ::substrait::NamedStruct& namedStruct, std::vector<ColumnType>& columnTypes);

  /// Parse Substrait Type to Bolt type.
  static bytedance::bolt::TypePtr parseType(const ::substrait::Type& substraitType, bool asLowerCase = false);

  /// Parse Substrait ReferenceSegment and extract the field index. Return false if the segment is not a valid unnested
  /// field.
  static bool parseReferenceSegment(const ::substrait::Expression::ReferenceSegment& refSegment, uint32_t& fieldIndex);

  /// Make names in the format of {prefix}_{index}.
  static std::vector<std::string> makeNames(const std::string& prefix, int size);

  /// Make node name in the format of n{nodeId}_{colIdx}.
  static std::string makeNodeName(int nodeId, int colIdx);

  /// Get the column index from a node name in the format of
  /// n{nodeId}_{colIdx}.
  static int getIdxFromNodeName(const std::string& nodeName);

  /// Find the Substrait function name according to the function id
  /// from a pre-constructed function map. The function specification can be
  /// a simple name or a compound name. The compound name format is:
  /// <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>.
  /// Currently, the input types in the function specification are not used. But
  /// in the future, they should be used for the validation according the
  /// specifications in Substrait yaml files.
  static std::string findFunctionSpec(const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id);

  /// Extracts the name of a function by splitting signature with delimiter.
  static std::string getNameBeforeDelimiter(const std::string& signature, const std::string& delimiter = ":");

  /// This function is used get the types from the compound name.
  static std::vector<std::string> getSubFunctionTypes(const std::string& subFuncSpec);

  /// Used to find the Bolt function name according to the function id
  /// from a pre-constructed function map.
  static std::string findBoltFunction(const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id, bool useIcuRegex);

  /// Map the Substrait function keyword into Bolt function keyword.
  static std::string mapToBoltFunction(const std::string& substraitFunction, bool isDecimal, bool useIcuRegex);

  /// @brief Return whether a config is set as true in AdvancedExtension
  /// optimization.
  /// @param extension Substrait advanced extension.
  /// @param config the key string of a config.
  /// @return Whether the config is set as true.
  static bool configSetInOptimization(const ::substrait::extensions::AdvancedExtension&, const std::string& config);

  static int64_t configLongValueInOptimization(
      const ::substrait::extensions::AdvancedExtension&,
      const std::string& config);

  /// Extract input types from Substrait function signature.
  static std::vector<bytedance::bolt::TypePtr> sigToTypes(const std::string& functionSig);

  // Get values for the different supported types.
  template <typename T>
  static T getLiteralValue(const ::substrait::Expression::Literal& /* literal */);

 private:
  /// A map used for mapping Substrait function keywords into Bolt functions'
  /// keywords. Key: the Substrait function keyword, Value: the Bolt function
  /// keyword. For those functions with different names in Substrait and Bolt,
  /// a mapping relation should be added here.
  static std::unordered_map<std::string, std::string> substraitBoltFunctionMap_;
  static std::unordered_map<std::string, std::string> substraitBoltFunctionMapWithICU_;

  // The map is uesd for mapping substrait type.
  // Key: type in function name.
  // Value: substrait type name.
  static const std::unordered_map<std::string, std::string> typeMap_;

  static const std::unordered_map<std::string, std::string>& substraitBoltFunctionMap(bool useIcuRegex);
};

} // namespace gluten
