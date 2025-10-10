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
#include "operators/functions/RegistrationAllFunctions.h"

#include "operators/functions/Arithmetic.h"
#include "operators/functions/RowConstructorWithNull.h"
#include "operators/functions/RowFunctionWithNull.h"
#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/expression/VectorFunction.h"
// #include "bolt/functions/iceberg/Register.h"
#include "bolt/functions/lib/CheckedArithmetic.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/functions/sparksql/DecimalArithmetic.h"
#include "bolt/functions/sparksql/Hash.h"
#include "bolt/functions/sparksql/Rand.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/functions/sparksql/window/WindowFunctionsRegistration.h"

using namespace bytedance;

namespace bytedance::bolt::functions {
void registerPrestoVectorFunctions() {
  // Presto function. To be removed.
  BOLT_REGISTER_VECTOR_FUNCTION(udf_arrays_overlap, "arrays_overlap");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_transform_keys, "transform_keys");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_transform_values, "transform_values");
}
} // namespace bytedance::bolt::functions

namespace gluten {
namespace {

void registerFunctionOverwrite() {
  bolt::functions::registerUnaryNumeric<RoundFunction>({"round"});
  bolt::registerFunction<RoundFunction, int8_t, int8_t, int32_t>({"round"});
  bolt::registerFunction<RoundFunction, int16_t, int16_t, int32_t>({"round"});
  bolt::registerFunction<RoundFunction, int32_t, int32_t, int32_t>({"round"});
  bolt::registerFunction<RoundFunction, int64_t, int64_t, int32_t>({"round"});
  bolt::registerFunction<RoundFunction, double, double, int32_t>({"round"});
  bolt::registerFunction<RoundFunction, float, float, int32_t>({"round"});

  auto kRowConstructorWithNull = RowConstructorWithNullCallToSpecialForm::kRowConstructorWithNull;
  bolt::exec::registerVectorFunction(
      kRowConstructorWithNull,
      std::vector<std::shared_ptr<bolt::exec::FunctionSignature>>{},
      std::make_unique<RowFunctionWithNull</*allNull=*/false>>(),
      RowFunctionWithNull</*allNull=*/false>::metadata());
  bolt::exec::registerFunctionCallToSpecialForm(
      kRowConstructorWithNull, std::make_unique<RowConstructorWithNullCallToSpecialForm>(kRowConstructorWithNull));

  auto kRowConstructorWithAllNull = RowConstructorWithNullCallToSpecialForm::kRowConstructorWithAllNull;
  bolt::exec::registerVectorFunction(
      kRowConstructorWithAllNull,
      std::vector<std::shared_ptr<bolt::exec::FunctionSignature>>{},
      std::make_unique<RowFunctionWithNull</*allNull=*/true>>(),
      RowFunctionWithNull</*allNull=*/true>::metadata());
  bolt::exec::registerFunctionCallToSpecialForm(
      kRowConstructorWithAllNull,
      std::make_unique<RowConstructorWithNullCallToSpecialForm>(kRowConstructorWithAllNull));

  bolt::functions::registerPrestoVectorFunctions();
}

} // namespace

void registerAllFunctions() {
  // The registration order matters. Spark sql functions are registered after
  // presto sql functions to overwrite the registration for same named functions.
  bolt::functions::prestosql::registerAllScalarFunctions();
  bolt::functions::sparksql::registerFunctions("");
  bolt::aggregate::prestosql::registerAllAggregateFunctions(
      "", true /*registerCompanionFunctions*/, true /*overwrite*/);
  bolt::functions::aggregate::sparksql::registerAggregateFunctions(
      "", true /*registerCompanionFunctions*/, true /*overwrite*/);
  bolt::window::prestosql::registerAllWindowFunctions();
  bolt::functions::window::sparksql::registerWindowFunctions("");
  // Using function overwrite to handle function names mismatch between Spark
  // and Bolt.
  registerFunctionOverwrite();

  // Note: iceberg disabled for now. 
  // TODO: sync bolt and uncomment it
  // bolt::functions::iceberg::registerFunctions();
}

} // namespace gluten
