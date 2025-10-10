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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "BoltSubstraitSignature.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "bolt/core/Expressions.h"
#include "bolt/core/PlanNode.h"
#include "bolt/type/Type.h"

namespace gluten {

struct ExtensionFunctionId {
  std::string uri;
  std::string signature;

  bool operator==(const ExtensionFunctionId& other) const {
    return uri == other.uri && signature == other.signature;
  }
};

/// Assigns unique IDs to function signatures using ExtensionFunctionId.
class SubstraitExtensionCollector {
 public:
  SubstraitExtensionCollector();

  /// Given a scalar function name and argument types, return the functionId
  /// using ExtensionFunctionId.
  int getReferenceNumber(const std::string& functionName, const std::vector<TypePtr>& arguments);

  /// Add extension functions to Substrait plan.
  void addExtensionsToPlan(::substrait::Plan* plan) const;

 private:
  template <class T>
  class BiDirectionHashMap {
   public:
    bool putIfAbsent(const int& key, const T& value);

    const std::unordered_map<int, ExtensionFunctionId> forwardMap() const {
      return forwardMap_;
    }

    const std::unordered_map<T, int>& reverseMap() const {
      return reverseMap_;
    }

   private:
    std::unordered_map<int, T> forwardMap_;
    std::unordered_map<T, int> reverseMap_;
  };

  int getReferenceNumber(const ExtensionFunctionId& extensionFunctionId);

  int functionReferenceNumber = -1;
  std::shared_ptr<BiDirectionHashMap<ExtensionFunctionId>> extensionFunctions_;
};

using SubstraitExtensionCollectorPtr = std::shared_ptr<SubstraitExtensionCollector>;

} // namespace gluten

namespace std {

template <>
struct hash<gluten::ExtensionFunctionId> {
  size_t operator()(const gluten::ExtensionFunctionId& k) const {
    size_t val = hash<std::string>()(k.uri);
    val = val * 31 + hash<std::string>()(k.signature);
    return val;
  }
};

} // namespace std
