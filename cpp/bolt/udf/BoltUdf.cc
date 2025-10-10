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

#include "BoltUdf.h"
#include "compute/BoltBackend.h"

#include <google/protobuf/arena.h>
#include <bolt/expression/SignatureBinder.h>
#include <bolt/expression/VectorFunction.h>
#include <bolt/type/fbhive/HiveTypeParser.h>
#include <vector>
#include "substrait/BoltToSubstraitType.h"

namespace bolt {

void SparkCppUdfRegisterMgr::emplace(std::function<void()>&& registerCallback) {
    udfRegisterCallbacks_.emplace_back(std::forward<std::function<void()>>(registerCallback));
}

void SparkCppUdfRegisterMgr::registerUdf() {
  try {
    for (auto&& cb : udfRegisterCallbacks_) {
      cb();
    }
  }
  catch(std::exception& ex) {
    LOG(ERROR) << "Call UDF register callback: " << ex.what();
  }
}

void SparkCppUdfRegisterMgr::registerFunctionName(const std::string& fn, const char* retType) {
  bytedance::bolt::type::fbhive::HiveTypeParser parser;
  auto arena = std::make_unique<google::protobuf::Arena>();
  auto typeConverter = gluten::BoltToSubstraitTypeConvertor();
  
  auto returnType = parser.parse(retType);
  auto substraitType = typeConverter.toSubstraitType(*arena, returnType);

  std::string output;
  substraitType.SerializeToString(&output);
  // overwrite
  udfMap_[fn] = std::move(output);
}

std::unordered_map<std::string, std::string>& SparkCppUdfRegisterMgr::getUdfMap() {
  return udfMap_;
}

}
