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

#include "velox/functions/lib/SubscriptUtil.h"

namespace gluten {

/// Spark GetMapValue (m[key]): returns the value stored under 'key', or NULL
/// when the key is absent. Only the map signature is registered.
///
/// Unlike Velox's element_at, which serves arrays as well and therefore keeps
/// Subscript::canPushdown() == false (a Subfield path cannot express a
/// negative array index), a map lookup with a constant key is exactly the
/// path m["k"]: the reader keeps only the entries whose key is "k", and the
/// lookup yields the same value, or NULL, as on the full map. Reporting the
/// subscript lets Expr::extractSubfields() produce m["k"] for a remaining
/// filter such as get_map_value(m, 'k').x = 1 instead of the bare column path
/// m, which would otherwise force the reader to keep every map entry and
/// defeat map-key pruning declared through HiveColumnHandle::requiredSubfields.
class SparkGetMapValueFunction : public facebook::velox::functions::SubscriptImpl<
                                     /* allowNegativeIndices */ false,
                                     /* nullOnNegativeIndices */ false,
                                     /* allowOutOfBound */ true,
                                     /* indexStartsAtOne */ true> {
 public:
  explicit SparkGetMapValueFunction(bool allowCaching) : SubscriptImpl(allowCaching) {}

  bool canPushdown() const override {
    return true;
  }

  static std::vector<std::shared_ptr<facebook::velox::exec::FunctionSignature>> signatures() {
    // map(K,V), K -> V
    return {facebook::velox::exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("V")
                .argumentType("map(K,V)")
                .argumentType("K")
                .build()};
  }
};

/// Registers Spark GetMapValue under 'name'.
void registerSparkGetMapValueFunction(const std::string& name);

} // namespace gluten
