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

#include <bolt/expression/VectorFunction.h>
#include <bolt/functions/Macros.h>
#include <bolt/functions/Registerer.h>
#include <iostream>
#include "udf/Udf.h"
#include "udf/examples/UdfCommon.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kDate = "date";
static const char* kVarChar = "varchar";

namespace hivestringstring {

template <typename T>
struct HiveStringStringFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

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
    bytedance::bolt::registerFunction<HiveStringStringFunction, Varchar, Varchar, Varchar>({name_});
  }

 private:
  const std::string name_ = "org.apache.spark.sql.hive.execution.UDFStringString";
  const char* arg_[2] = {kVarChar, kVarChar};
};

} // namespace hivestringstring

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
}
