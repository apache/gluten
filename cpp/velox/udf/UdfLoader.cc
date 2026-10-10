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

#include <dlfcn.h>
#include <google/protobuf/arena.h>
#include <vector>
#include "velox/exec/Aggregate.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/type/TypeCoercer.h"
#include "velox/type/fbhive/HiveTypeParser.h"

#include "Udaf.h"
#include "Udf.h"
#include "UdfLoader.h"
#include "utils/Exception.h"
#include "utils/Macros.h"
#include "utils/StringUtil.h"

namespace {

void* loadSymFromLibrary(
    void* handle,
    const std::string& libPath,
    const std::string& func,
    bool throwIfNotFound = true) {
  // Clear any existing dlerror() state before calling dlsym.
  dlerror();
  void* sym = dlsym(handle, func.c_str());
  if (!sym && throwIfNotFound) {
    const char* error = dlerror();
    throw gluten::GlutenException(
        fmt::format("Failed to load {} in {}: {}", func, libPath, error != nullptr ? error : "unknown error"));
  }
  return sym;
}

} // namespace

namespace gluten {

void UdfLoader::loadUdfLibraries(const std::string& libPaths) {
  const auto& paths = splitPaths(libPaths, /*checkExists=*/true);
  loadUdfLibrariesInternal(paths);
}

void UdfLoader::loadUdfLibrariesInternal(const std::vector<std::string>& libPaths) {
  for (const auto& libPath : libPaths) {
    if (handles_.find(libPath) == handles_.end()) {
      void* handle = dlopen(libPath.c_str(), RTLD_LAZY);
      handles_[libPath] = handle;
    }
    LOG(INFO) << "Successfully loaded udf library: " << libPath;
  }
}

std::unordered_set<std::shared_ptr<UdfLoader::UdfSignature>> UdfLoader::getRegisteredUdfSignatures() {
  if (!signatures_.empty()) {
    return signatures_;
  }
  for (const auto& item : handles_) {
    const auto& libPath = item.first;
    const auto& handle = item.second;

    // Handle UDFs.
    void* getNumUdfSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_NUM_UDF), false);
    if (getNumUdfSym) {
      auto getNumUdf = reinterpret_cast<int (*)()>(getNumUdfSym);
      int numUdf = getNumUdf();
      // allocate
      UdfEntry* udfEntries = static_cast<UdfEntry*>(malloc(sizeof(UdfEntry) * numUdf));
      if (udfEntries == nullptr) {
        throw gluten::GlutenException("malloc failed");
      }

      void* getUdfEntriesSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_UDF_ENTRIES));
      auto getUdfEntries = reinterpret_cast<void (*)(UdfEntry*)>(getUdfEntriesSym);
      getUdfEntries(udfEntries);

      for (auto i = 0; i < numUdf; ++i) {
        const auto& entry = udfEntries[i];
        auto dataType = toSubstraitTypeStr(entry.dataType);
        auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
        signatures_.insert(std::make_shared<UdfSignature>(
            entry.name, dataType, argTypes, entry.variableArity, entry.allowTypeConversion));
      }
      free(udfEntries);
    } else {
      LOG(INFO) << "No UDF found in " << libPath;
    }

    // Handle UDAFs.
    void* getNumUdafSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_NUM_UDAF), false);
    if (getNumUdafSym) {
      auto getNumUdaf = reinterpret_cast<int (*)()>(getNumUdafSym);
      int numUdaf = getNumUdaf();
      // allocate
      UdafEntry* udafEntries = static_cast<UdafEntry*>(malloc(sizeof(UdafEntry) * numUdaf));
      if (udafEntries == nullptr) {
        throw gluten::GlutenException("malloc failed");
      }

      void* getUdafEntriesSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_UDAF_ENTRIES));
      auto getUdafEntries = reinterpret_cast<void (*)(UdafEntry*)>(getUdafEntriesSym);
      getUdafEntries(udafEntries);

      for (auto i = 0; i < numUdaf; ++i) {
        const auto& entry = udafEntries[i];
        auto dataType = toSubstraitTypeStr(entry.dataType);
        auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
        auto intermediateType = toSubstraitTypeStr(entry.intermediateType);
        signatures_.insert(std::make_shared<UdfSignature>(
            entry.name, dataType, argTypes, intermediateType, entry.variableArity, entry.allowTypeConversion));
      }
      free(udafEntries);
    } else {
      LOG(INFO) << "No UDAF found in " << libPath;
    }
  }

  return signatures_;
}

void UdfLoader::loadRegistryNames() {
  if (registryNamesLoaded_) {
    return;
  }
  registryNamesLoaded_ = true;

  for (const auto& item : handles_) {
    const auto& libPath = item.first;
    const auto& handle = item.second;

    loadRegistryEntries<RegistryUdfEntry>(
        handle,
        libPath,
        GLUTEN_TOSTRING(GLUTEN_GET_NUM_REGISTRY_UDF),
        GLUTEN_TOSTRING(GLUTEN_GET_REGISTRY_UDF_ENTRIES),
        registryUdfNames_);

    loadRegistryEntries<RegistryUdafEntry>(
        handle,
        libPath,
        GLUTEN_TOSTRING(GLUTEN_GET_NUM_REGISTRY_UDAF),
        GLUTEN_TOSTRING(GLUTEN_GET_REGISTRY_UDAF_ENTRIES),
        registryUdafNames_);
  }
}

template <typename Entry>
void UdfLoader::loadRegistryEntries(
    void* handle,
    const std::string& libPath,
    const std::string& numSym,
    const std::string& entriesSym,
    std::unordered_set<std::string>& names) {
  void* getNumSym = loadSymFromLibrary(handle, libPath, numSym, false);
  if (!getNumSym) {
    return;
  }
  auto getNum = reinterpret_cast<int (*)()>(getNumSym);
  const int num = getNum();
  if (num <= 0) {
    return;
  }

  std::vector<Entry> entries(num);
  void* getEntriesSym = loadSymFromLibrary(handle, libPath, entriesSym);
  auto getEntries = reinterpret_cast<void (*)(Entry*)>(getEntriesSym);
  getEntries(entries.data());

  for (const auto& entry : entries) {
    names.insert(entry.name);
  }
}

std::unordered_set<std::string> UdfLoader::getRegistryUdfNames() {
  loadRegistryNames();
  return registryUdfNames_;
}

std::unordered_set<std::string> UdfLoader::getRegistryUdafNames() {
  loadRegistryNames();
  return registryUdafNames_;
}

std::unordered_set<std::string> UdfLoader::getRegisteredUdafNames() {
  if (handles_.empty()) {
    return {};
  }
  if (!names_.empty()) {
    return names_;
  }
  if (signatures_.empty()) {
    getRegisteredUdfSignatures();
  }
  for (const auto& sig : signatures_) {
    if (!sig->intermediateType.empty()) {
      names_.insert(sig->name);
    }
  }
  // A RegistryUdafEntry advertises no intermediate type, so the loop above
  // cannot see it, but the plan validator gates AggregateRel on this set.
  loadRegistryNames();
  names_.insert(registryUdafNames_.begin(), registryUdafNames_.end());
  return names_;
}

namespace {
// Types VeloxToSubstraitType cannot encode. UNKNOWN emits a user-defined type the JVM rejects; a
// decimal has no case at all, so a short one silently degrades to its TypeKind, BIGINT, and would
// be read as a long. A stated UdfEntry is unaffected -- it names its types as substrait strings.
bool containsUnencodableType(const facebook::velox::TypePtr& type) {
  if (type == nullptr || type->kind() == facebook::velox::TypeKind::UNKNOWN || type->isDecimal()) {
    return true;
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (containsUnencodableType(type->childAt(i))) {
      return true;
    }
  }
  return false;
}

// Coercion targets go back over the same reply, so they are subject to the same limit.
bool coercionsAreEncodable(const std::vector<facebook::velox::TypePtr>& coercions) {
  for (const auto& coercion : coercions) {
    if (coercion != nullptr && containsUnencodableType(coercion)) {
      return false;
    }
  }
  return true;
}

// Hive only converts when both types are primitive: implicitConvertible(TypeInfo, TypeInfo)
// returns false unless both categories are PRIMITIVE. Velox's coercer instead recurses into
// arrays, maps and rows, so it would widen array(int) to array(bigint), which Hive would never
// treat as an implicit conversion. Decline a binding that leans on one.
bool coercesOnlyPrimitives(
    const std::vector<facebook::velox::TypePtr>& argTypes,
    const std::vector<facebook::velox::TypePtr>& coercions) {
  for (size_t i = 0; i < coercions.size() && i < argTypes.size(); ++i) {
    if (coercions[i] != nullptr && (!argTypes[i]->isPrimitiveType() || !coercions[i]->isPrimitiveType())) {
      return false;
    }
  }
  return true;
}

// Hive's implicit conversion rules, transcribed from
// TypeInfoUtils.implicitConvertible and the numericTypes ordering it ranks by:
// byte < short < int < long < decimal < float < double < string. Plus the cases
// that sit outside that ordering -- string -> double, date/timestamp -> string,
// and void -> anything.
//
// A UDF reached through a hive UDF class is standing in for Hive's own argument
// handling, so Hive decides what it should accept. Velox's defaults are numeric
// widening only and would refuse the numeric -> string conversions a hive UDF
// relies on; Cast.canCast would accept conversions Hive never would.
//
// Costs are Velox's graded distances rather than Hive's flat one-per-conversion.
// Hive's flat costs exist so it can spot an ambiguous overload and throw; here an
// unresolved tie declines to bind and the query falls back to the JVM, so
// reproducing the ties would only lose offloads.
const facebook::velox::TypeCoercer& hiveTypeCoercer() {
  using namespace facebook::velox;

  static const TypeCoercer instance{[]() {
    std::vector<CoercionEntry> rules;
    auto add = [&](const TypePtr& from, const std::vector<TypePtr>& to) {
      int32_t cost = 0;
      for (const auto& toType : to) {
        rules.push_back({from, toType, ++cost});
      }
    };

    // Hive's ordering runs byte < short < int < long < decimal < float < double < string. Decimal
    // is left out of it here -- see containsUnencodableType -- which drops a step from the ladder
    // without reordering what remains.
    add(TINYINT(), {SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE(), VARCHAR()});
    add(SMALLINT(), {INTEGER(), BIGINT(), REAL(), DOUBLE(), VARCHAR()});
    add(INTEGER(), {BIGINT(), REAL(), DOUBLE(), VARCHAR()});
    add(BIGINT(), {REAL(), DOUBLE(), VARCHAR()});
    add(REAL(), {DOUBLE(), VARCHAR()});
    add(DOUBLE(), {VARCHAR()});

    // Outside the numeric ordering.
    add(VARCHAR(), {DOUBLE()});
    add(DATE(), {VARCHAR()});
    add(TIMESTAMP(), {VARCHAR()});
    add(UNKNOWN(),
        {BOOLEAN(),
         TINYINT(),
         SMALLINT(),
         INTEGER(),
         BIGINT(),
         REAL(),
         DOUBLE(),
         VARCHAR(),
         VARBINARY(),
         DATE(),
         TIMESTAMP()});

    return rules;
  }()};
  return instance;
}
} // namespace

std::optional<UdfLoader::Resolution> UdfLoader::resolveUdfType(
    const std::string& name,
    const std::vector<facebook::velox::TypePtr>& argTypes) {
  using namespace facebook::velox;

  // Exact first. The simple-function registry does not prefer an exact match once coercions are
  // allowed: it ranks by signature priority before cost, and keeps only the best priority tier.
  // Priority is structural -- rank 1 concrete, rank 3 generic (SimpleFunctionMetadata.h) -- so a
  // concrete signature reachable only by widening discards a generic one that matched as it
  // stands, and the exact match never reaches the cost comparison that would have won it.
  if (auto returnType = resolveFunction(name, argTypes)) {
    if (containsUnencodableType(returnType)) {
      return std::nullopt;
    }
    return Resolution{returnType, nullptr, std::vector<TypePtr>(argTypes.size())};
  }

  std::vector<TypePtr> coercions;
  auto returnType = resolveFunctionWithCoercions(name, argTypes, coercions, hiveTypeCoercer());
  if (returnType == nullptr || containsUnencodableType(returnType)) {
    return std::nullopt;
  }
  coercions.resize(argTypes.size());
  if (!coercesOnlyPrimitives(argTypes, coercions) || !coercionsAreEncodable(coercions)) {
    return std::nullopt;
  }
  return Resolution{returnType, nullptr, std::move(coercions)};
}

std::optional<UdfLoader::Resolution> UdfLoader::resolveUdafTypes(
    const std::string& name,
    const std::vector<facebook::velox::TypePtr>& argTypes) {
  using namespace facebook::velox;

  auto signatures = exec::getAggregateFunctionSignatures(name);
  if (!signatures.has_value()) {
    return std::nullopt;
  }

  // Two phases, the same shape Axiom uses: resolve the return type with coercions, then bind the
  // intermediate type from the coerced arguments. exec::resolveIntermediateType does the second
  // step but throws on a miss and takes no coercer, so the bind is written out -- with the added
  // check that the signature it lands on returns what phase one resolved, since it would
  // otherwise take the first that binds and could answer from a different overload.
  std::vector<exec::FunctionSignaturePtr> baseSignatures(signatures.value().begin(), signatures.value().end());

  std::vector<TypePtr> coercions;
  auto returnType = exec::tryResolveReturnTypeWithCoercions(baseSignatures, argTypes, coercions, hiveTypeCoercer());
  if (returnType == nullptr) {
    return std::nullopt;
  }
  coercions.resize(argTypes.size());
  if (!coercesOnlyPrimitives(argTypes, coercions) || !coercionsAreEncodable(coercions)) {
    return std::nullopt;
  }

  auto coercedArgTypes = argTypes;
  for (size_t i = 0; i < coercions.size(); ++i) {
    if (coercions[i] != nullptr) {
      coercedArgTypes[i] = coercions[i];
    }
  }

  for (const auto& signature : signatures.value()) {
    exec::SignatureBinder binder(*signature, coercedArgTypes, hiveTypeCoercer());
    if (!binder.tryBind()) {
      continue;
    }
    auto boundReturnType = binder.tryResolveReturnType();
    if (boundReturnType == nullptr || !boundReturnType->equivalent(*returnType)) {
      continue;
    }
    auto intermediateType = binder.tryResolveType(signature->intermediateType());
    if (intermediateType == nullptr) {
      continue;
    }
    if (containsUnencodableType(returnType) || containsUnencodableType(intermediateType)) {
      return std::nullopt;
    }
    return Resolution{returnType, intermediateType, std::move(coercions)};
  }
  return std::nullopt;
}

void UdfLoader::registerUdf() {
  for (const auto& item : handles_) {
    void* sym = loadSymFromLibrary(item.second, item.first, GLUTEN_TOSTRING(GLUTEN_REGISTER_UDF));
    auto registerUdf = reinterpret_cast<void (*)()>(sym);
    registerUdf();
  }
}

std::shared_ptr<UdfLoader> UdfLoader::getInstance() {
  static auto instance = std::make_shared<UdfLoader>();
  return instance;
}

std::string UdfLoader::toSubstraitTypeStr(const std::string& type) {
  auto returnType = parser_.parse(type);
  auto substraitType = convertor_.toSubstraitType(arena_, returnType);

  std::string output;
  substraitType.SerializeToString(&output);
  return output;
}

std::string UdfLoader::toSubstraitTypeStr(int32_t numArgs, const char** args) {
  std::vector<facebook::velox::TypePtr> argTypes;
  argTypes.resize(numArgs);
  for (auto i = 0; i < numArgs; ++i) {
    argTypes[i] = parser_.parse(args[i]);
  }
  auto substraitType = convertor_.toSubstraitType(arena_, facebook::velox::ROW(std::move(argTypes)));

  std::string output;
  substraitType.SerializeToString(&output);
  return output;
}

} // namespace gluten
