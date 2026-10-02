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

#include <boost/container_hash/hash.hpp>
#include <google/protobuf/arena.h>
#include <algorithm>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "substrait/VeloxToSubstraitType.h"
#include "velox/type/Type.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace gluten {

class UdfLoader {
 public:
  struct UdfSignature {
    std::string name;
    std::string returnType;
    std::string argTypes;

    std::string intermediateType{};

    bool variableArity;
    bool allowTypeConversion;

    UdfSignature(
        std::string name,
        std::string returnType,
        std::string argTypes,
        bool variableArity,
        bool allowTypeConversion)
        : name(name),
          returnType(returnType),
          argTypes(argTypes),
          variableArity(variableArity),
          allowTypeConversion(allowTypeConversion) {}

    UdfSignature(
        std::string name,
        std::string returnType,
        std::string argTypes,
        std::string intermediateType,
        bool variableArity,
        bool allowTypeConversion)
        : name(name),
          returnType(returnType),
          argTypes(argTypes),
          intermediateType(intermediateType),
          variableArity(variableArity),
          allowTypeConversion(allowTypeConversion) {}

    ~UdfSignature() = default;
  };

  static std::shared_ptr<UdfLoader> getInstance();

  void loadUdfLibraries(const std::string& libPaths);

  std::unordered_set<std::shared_ptr<UdfSignature>> getRegisteredUdfSignatures();

  /// Names declared through RegistryUdfEntry. Their signatures live in the Velox
  /// function registry and are resolved per call site by resolveUdfType.
  std::unordered_set<std::string> getRegistryUdfNames();

  /// Names declared through RegistryUdafEntry, resolved by resolveUdafTypes.
  std::unordered_set<std::string> getRegistryUdafNames();

  std::unordered_set<std::string> getRegisteredUdafNames();

  /// What a call resolved to. 'intermediateType' is null for a scalar function.
  /// 'coercions' holds one entry per argument, null where the argument binds as
  /// it is, and the type to cast it to where it does not.
  struct Resolution {
    facebook::velox::TypePtr returnType;
    facebook::velox::TypePtr intermediateType;
    std::vector<facebook::velox::TypePtr> coercions;

    bool needsCoercion() const {
      return std::any_of(coercions.begin(), coercions.end(), [](const auto& type) { return type != nullptr; });
    }
  };

  /// Resolves scalar function 'name' called with 'argTypes'. Returns
  /// std::nullopt if no registered signature binds.
  ///
  /// A signature also binds when an argument can be widened to it, and the
  /// widening is reported in Resolution::coercions for the caller to apply or
  /// to refuse. This is what lets a partly generic signature such as
  /// array(T), bigint -> T accept an integer in its second position: the
  /// generic positions bind as they are and only the concrete one is widened.
  /// An exact match reports no coercions.
  static std::optional<Resolution> resolveUdfType(
      const std::string& name,
      const std::vector<facebook::velox::TypePtr>& argTypes);

  /// Like resolveUdfType, and additionally resolves the intermediate type.
  /// Both types come from the same bound signature, so they cannot disagree.
  static std::optional<Resolution> resolveUdafTypes(
      const std::string& name,
      const std::vector<facebook::velox::TypePtr>& argTypes);

  void registerUdf();

 private:
  void loadUdfLibrariesInternal(const std::vector<std::string>& libPaths);

  void loadRegistryNames();

  template <typename Entry>
  void loadRegistryEntries(
      void* handle,
      const std::string& libPath,
      const std::string& numSym,
      const std::string& entriesSym,
      std::unordered_set<std::string>& names);

  std::string toSubstraitTypeStr(const std::string& type);

  std::string toSubstraitTypeStr(int32_t numArgs, const char** args);

  std::unordered_map<std::string, void*> handles_;

  facebook::velox::type::fbhive::HiveTypeParser parser_{};
  google::protobuf::Arena arena_{};
  VeloxToSubstraitTypeConvertor convertor_{};

  std::unordered_set<std::shared_ptr<UdfSignature>> signatures_;
  std::unordered_set<std::string> names_;

  bool registryNamesLoaded_{false};
  std::unordered_set<std::string> registryUdfNames_;
  std::unordered_set<std::string> registryUdafNames_;
};

} // namespace gluten
