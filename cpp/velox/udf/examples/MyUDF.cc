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

#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/prestosql/Cardinality.h>
#include <iostream>
#include "udf/Udf.h"
#include "udf/examples/UdfCommon.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kDate = "date";
static const char* kVarChar = "varchar";

namespace hivestringstring {

template <typename T>
struct HiveStringStringFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result, const arg_type<Varchar>& a, const arg_type<Varchar>& b) {
    result.append(a.data());
    result.append(" ");
    result.append(b.data());
  }
};

// name: org.apache.spark.sql.hive.execution.UDFStringString
// signatures:
//    varchar, varchar -> varchar
// type: SimpleFunction
class HiveStringStringRegisterer final : public gluten::UdfRegisterer {
 public:
  int getNumUdf() override {
    return 1;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    // Set `allowTypeConversion` for hive udf.
    udfEntries[index++] = {name_.c_str(), kVarChar, 2, arg_, false, true};
  }

  void registerSignatures() override {
    facebook::velox::registerFunction<HiveStringStringFunction, Varchar, Varchar, Varchar>({name_});
  }

 private:
  const std::string name_ = "org.apache.spark.sql.hive.execution.UDFStringString";
  const char* arg_[2] = {kVarChar, kVarChar};
};

} // namespace hivestringstring

namespace myudfplusone {

template <typename T>
struct MyUdfPlusOneFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const int64_t& a) {
    result = a + 1;
  }
};

// name: myudf_plus_one
// signatures:
//    bigint -> bigint
// type: SimpleFunction
// A name with no dot, so it is callable directly without a Hive UDF class.
class MyUdfPlusOneRegisterer final : public gluten::UdfRegisterer {
 public:
  int getNumUdf() override {
    return 1;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    udfEntries[index++] = {name_.c_str(), kBigInt, 1, arg_, false, false};
  }

  void registerSignatures() override {
    facebook::velox::registerFunction<MyUdfPlusOneFunction, int64_t, int64_t>({name_});
  }

 private:
  const std::string name_ = "myudf_plus_one";
  const char* arg_[1] = {kBigInt};
};

} // namespace myudfplusone

namespace myregistryplusone {

// name: myudf_registry_plus_one
// signatures:
//    bigint -> bigint
// type: RegistryUdfEntry with no type variables
//
// Declared by name like the others, and its Velox signature is concrete. A call whose argument
// is narrower binds by widening it, where the call is standing in for a hive UDF.
const std::string kMyRegistryPlusOneName = "myudf_registry_plus_one";

void registerMyRegistryPlusOne() {
  facebook::velox::registerFunction<myudfplusone::MyUdfPlusOneFunction, int64_t, int64_t>({kMyRegistryPlusOneName});
}

} // namespace myregistryplusone

namespace mymapsizeplus {

// name: myudf_map_size_plus
// signatures:
//    map(K,V), bigint -> bigint
// type: RegistryUdfEntry, partly generic
//
// The map positions are type variables and the second argument is concrete, which is the ordinary
// shape of a function that is polymorphic in its data and monomorphic in a control parameter.
// Binding it exactly means an integer in the second position does not match; coercion widens just
// that position and leaves the generic ones alone.
template <typename T>
struct MyMapSizePlusFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(int64_t& result, const arg_type<Map<Generic<T1>, Generic<T2>>>& map, const arg_type<int64_t>& extra) {
    result = map.size() + extra;
  }
};

const std::string kMyMapSizePlusName = "myudf_map_size_plus";

void registerMyMapSizePlus() {
  registerFunction<MyMapSizePlusFunction, int64_t, Map<Generic<T1>, Generic<T2>>, int64_t>({kMyMapSizePlusName});
}

} // namespace mymapsizeplus

namespace myregistrystringstring {

// name: myudf_registry_string_string
// signatures:
//    varchar, varchar -> varchar
// type: RegistryUdfEntry
//
// Its arguments are strings, which is what separates the two candidate rule sets: Hive widens a
// numeric to a string implicitly, and a hive UDF taking a Text relies on that, while Velox's own
// defaults are numeric widening only and would refuse it.
const std::string kMyRegistryStringStringName = "myudf_registry_string_string";

// The same function under a hive UDF class name, which is how a query reaches it when the
// library is standing in for an existing hive UDF rather than adding a new name.
const std::string kMyRegistryHiveName = "org.apache.gluten.udf.RegistryHiveUDF";

void registerMyRegistryStringString() {
  registerFunction<hivestringstring::HiveStringStringFunction, Varchar, Varchar, Varchar>(
      {kMyRegistryStringStringName, kMyRegistryHiveName});
}

} // namespace myregistrystringstring

namespace mygenericpair {

// name: myudf_generic_pair
// signatures:
//    T, T -> bigint
// type: RegistryUdfEntry, fully generic
//
// Both positions are the same variable, so binding a call whose arguments differ has to settle
// on a type they can both reach. Velox binds T to their least common supertype and reports the
// widening, which is worth pinning because it is behaviour Gluten gets from the binder rather
// than implements.
template <typename T>
struct MyGenericPairFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(int64_t& result, const arg_type<Generic<T1>>& /*a*/, const arg_type<Generic<T1>>& /*b*/) {
    result = 2;
  }
};

const std::string kMyGenericPairName = "myudf_generic_pair";

void registerMyGenericPair() {
  registerFunction<MyGenericPairFunction, int64_t, Generic<T1>, Generic<T1>>({kMyGenericPairName});
}

} // namespace mygenericpair

namespace mydecimalreturn {

// name: myudf_decimal_return
// signatures:
//    bigint -> decimal(10, 2)
// type: RegistryUdfEntry, unencodable return
//
// VeloxToSubstraitType has no decimal case, so a short decimal would reach the JVM as its
// TypeKind, BIGINT, and the call would be taken for a long-returning one. Resolution has to
// decline instead. Only the signature matters here -- the call site is never executed, because
// binding refuses it before a plan is built.
class MyDecimalReturnFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& /*args*/,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, outputType, result);
    result->addNulls(rows);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder().returnType("decimal(10, 2)").argumentType("bigint").build()};
  }
};

const std::string kMyDecimalReturnName = "myudf_decimal_return";

void registerMyDecimalReturn() {
  exec::registerVectorFunction(
      kMyDecimalReturnName, MyDecimalReturnFunction::signatures(), std::make_unique<MyDecimalReturnFunction>());
}

} // namespace mydecimalreturn

namespace myambiguous {

// name: myudf_ambiguous
// signatures:
//    bigint, double -> bigint
//    double, bigint -> bigint
// type: RegistryUdfEntry, deliberately ambiguous
//
// Called with two integers, widening to either signature costs the same, so no candidate is
// strictly cheapest. Velox reports no winner rather than guessing, and the call falls back.
template <typename T>
struct MyAmbiguousFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(int64_t& result, const arg_type<int64_t>& /*a*/, const arg_type<double>& /*b*/) {
    result = 1;
  }

  void call(int64_t& result, const arg_type<double>& /*a*/, const arg_type<int64_t>& /*b*/) {
    result = 2;
  }
};

const std::string kMyAmbiguousName = "myudf_ambiguous";

void registerMyAmbiguous() {
  registerFunction<MyAmbiguousFunction, int64_t, int64_t, double>({kMyAmbiguousName});
  registerFunction<MyAmbiguousFunction, int64_t, double, int64_t>({kMyAmbiguousName});
}

} // namespace myambiguous

namespace mymapcardinality {

// name: myudf_map_cardinality
// signatures:
//    map(K,V) -> bigint
// type: RegistryUdfEntry
//
// A UdfEntry would have to restate this signature, and since K and V are type
// variables that means enumerating the key and value types the function is
// allowed to be called with. A RegistryUdfEntry names the function and leaves
// the signature where it already is, in the Velox function registry, for
// Gluten to bind against per call site.
const std::string kMyMapCardinalityName = "myudf_map_cardinality";

void registerMyMapCardinality() {
  registerFunction<facebook::velox::functions::CardinalityFunction, int64_t, Map<Generic<T1>, Generic<T2>>>(
      {kMyMapCardinalityName});
}

} // namespace mymapcardinality

std::vector<std::shared_ptr<gluten::UdfRegisterer>>& globalRegisters() {
  static std::vector<std::shared_ptr<gluten::UdfRegisterer>> registerers;
  return registerers;
}

void setupRegisterers() {
  static bool inited = false;
  if (inited) {
    return;
  }
  auto& registerers = globalRegisters();
  registerers.push_back(std::make_shared<hivestringstring::HiveStringStringRegisterer>());
  registerers.push_back(std::make_shared<myudfplusone::MyUdfPlusOneRegisterer>());
  inited = true;
}
} // namespace

DEFINE_GET_NUM_UDF {
  setupRegisterers();

  int numUdf = 0;
  for (const auto& registerer : globalRegisters()) {
    numUdf += registerer->getNumUdf();
  }
  return numUdf;
}

DEFINE_GET_UDF_ENTRIES {
  setupRegisterers();

  int index = 0;
  for (const auto& registerer : globalRegisters()) {
    registerer->populateUdfEntries(index, udfEntries);
  }
}

DEFINE_REGISTER_UDF {
  setupRegisterers();

  for (const auto& registerer : globalRegisters()) {
    registerer->registerSignatures();
  }

  mymapcardinality::registerMyMapCardinality();
  myregistryplusone::registerMyRegistryPlusOne();
  mymapsizeplus::registerMyMapSizePlus();
  myregistrystringstring::registerMyRegistryStringString();
  mygenericpair::registerMyGenericPair();
  myambiguous::registerMyAmbiguous();
  mydecimalreturn::registerMyDecimalReturn();
}

DEFINE_GET_NUM_REGISTRY_UDF {
  return 8;
}

DEFINE_GET_REGISTRY_UDF_ENTRIES {
  registryUdfEntries[0] = {mymapcardinality::kMyMapCardinalityName.c_str()};
  registryUdfEntries[1] = {myregistryplusone::kMyRegistryPlusOneName.c_str()};
  registryUdfEntries[2] = {mymapsizeplus::kMyMapSizePlusName.c_str()};
  registryUdfEntries[3] = {myregistrystringstring::kMyRegistryStringStringName.c_str()};
  registryUdfEntries[4] = {myregistrystringstring::kMyRegistryHiveName.c_str()};
  registryUdfEntries[5] = {mygenericpair::kMyGenericPairName.c_str()};
  registryUdfEntries[6] = {myambiguous::kMyAmbiguousName.c_str()};
  registryUdfEntries[7] = {mydecimalreturn::kMyDecimalReturnName.c_str()};
}
