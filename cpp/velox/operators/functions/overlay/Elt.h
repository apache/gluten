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

/// Signatures of Spark's elt: elt(n, input1, input2, ...) where all the inputs
/// are VARCHAR, or all of them are VARBINARY.
std::vector<facebook::velox::exec::FunctionSignaturePtr> eltSignatures();

/// Creates Spark's elt function, which returns the n-th input, 1-based.
///
/// Returns NULL when 'n' is NULL or when the selected input is NULL. When 'n'
/// is out of the range [1, number of inputs], the result follows Spark's ANSI
/// rule: NULL with ANSI mode off, a user error with ANSI mode on. ANSI mode is
/// read from the query config, so it is fixed for the lifetime of the returned
/// function instance.
///
/// Unlike Spark, which only evaluates the selected input, all the inputs are
/// evaluated, so an error raised by an input that 'n' does not select still
/// surfaces. This is the usual eager-evaluation difference of a Velox function
/// and not specific to elt.
std::shared_ptr<facebook::velox::exec::VectorFunction> makeElt(
    const std::string& name,
    const std::vector<facebook::velox::exec::VectorFunctionArg>& inputArgs,
    const facebook::velox::core::QueryConfig& config);

} // namespace gluten
