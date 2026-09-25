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
#include <string>
#include <vector>

#include "velox/expression/VectorFunction.h"

namespace gluten {

/// Signatures of Spark's element_at: element_at(array(T), integer|bigint) -> T
/// and element_at(map(K, V), K) -> V.
std::vector<facebook::velox::exec::FunctionSignaturePtr> elementAtSignatures();

/// Creates Spark's element_at.
///
/// Overrides Velox's element_at, which always returns NULL for an index past
/// the end of an array. Spark only does that with ANSI mode off; with ANSI mode
/// on it raises an error. An index of 0 is an error either way, and a key that
/// a map does not contain gives NULL either way, so those are unchanged.
std::shared_ptr<facebook::velox::exec::VectorFunction> makeElementAt(
    const std::string& name,
    const std::vector<facebook::velox::exec::VectorFunctionArg>& inputArgs,
    const facebook::velox::core::QueryConfig& config);

} // namespace gluten
