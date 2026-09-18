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

facebook::velox::TypePtr UdfLoader::resolveUdfType(
    const std::string& name,
    const std::vector<facebook::velox::TypePtr>& argTypes) {
  // Covers both simple and vector functions, and returns nullptr when nothing
  // binds.
  return facebook::velox::resolveFunction(name, argTypes);
}

std::optional<std::pair<facebook::velox::TypePtr, facebook::velox::TypePtr>> UdfLoader::resolveUdafTypes(
    const std::string& name,
    const std::vector<facebook::velox::TypePtr>& argTypes) {
  using namespace facebook::velox;

  auto signatures = exec::getAggregateFunctionSignatures(name);
  if (!signatures.has_value()) {
    return std::nullopt;
  }

  // exec::resolveResultType and exec::resolveIntermediateType do this, but each
  // binds separately and both throw rather than report a miss. Binding once
  // also guarantees the two types come from the same signature.
  for (const auto& signature : signatures.value()) {
    exec::SignatureBinder binder(*signature, argTypes, TypeCoercer::defaults());
    if (!binder.tryBind()) {
      continue;
    }
    auto returnType = binder.tryResolveReturnType();
    auto intermediateType = binder.tryResolveType(signature->intermediateType());
    if (returnType != nullptr && intermediateType != nullptr) {
      return std::make_pair(returnType, intermediateType);
    }
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
