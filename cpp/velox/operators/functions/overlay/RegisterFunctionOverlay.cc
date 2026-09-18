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
#include "operators/functions/overlay/RegisterFunctionOverlay.h"

#include "operators/functions/overlay/Conv.h"
#include "operators/functions/overlay/ElementAt.h"
#include "operators/functions/overlay/Elt.h"
#include "operators/functions/overlay/Round.h"
#include "velox/functions/lib/RegistrationHelpers.h"

using namespace facebook;

namespace gluten {
namespace {

// Spark's round differs from Velox's in the handling of negative decimals and
// floating point rounding semantics.
void registerRoundFunction() {
  velox::functions::registerUnaryNumeric<RoundFunction>({"round"});
  velox::registerFunction<RoundFunction, int8_t, int8_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, int16_t, int16_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, int32_t, int32_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, int64_t, int64_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, double, double, int32_t>({"round"});
  velox::registerFunction<RoundFunction, float, float, int32_t>({"round"});
}

// Velox has no elt yet. It is registered here so Gluten can offload it, and it
// honors Spark's ANSI rule for an out-of-range index.
void registerEltFunction() {
  velox::exec::registerStatefulVectorFunction(
      "elt", eltSignatures(), makeElt, velox::exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());
}

// Velox's conv always lets the conversion overflow, which only matches Spark
// with ANSI mode off.
void registerConvFunction() {
  velox::registerFunction<ConvFunction, velox::Varchar, velox::Varchar, int32_t, int32_t>({"conv"});
}

// Velox's element_at always returns NULL for an index past the end of an array,
// which only matches Spark with ANSI mode off.
void registerElementAtFunction() {
  velox::exec::registerStatefulVectorFunction("element_at", elementAtSignatures(), makeElementAt);
}

} // namespace

void registerFunctionOverlay() {
  registerRoundFunction();
  registerEltFunction();
  registerConvFunction();
  registerElementAtFunction();
}

} // namespace gluten
