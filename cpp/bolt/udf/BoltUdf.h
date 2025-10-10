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

#include <functional>
#include <vector>
#include <string>
#include <unordered_map>

#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/type/Type.h"

#ifndef BOLT_CONCATE_
  #define BOLT_CONCATE_(X,Y) X##Y
  #define BOLT_CONCATE(X,Y) BOLT_CONCATE_(X,Y)
#endif

#ifndef BOLT_UNIQUE_IDENTIFIER
  #ifdef __COUNTER__
    #define BOLT_UNIQUE_IDENTIFIER(prefix) BOLT_CONCATE(prefix, __COUNTER__)
  #else
    #define BOLT_UNIQUE_IDENTIFIER(prefix) BOLT_CONCATE(prefix, __LINE__)
  #endif // __COUNTER__
#endif 


#define SPARK_UDF_REGISTER [[maybe_unused]] static bolt::UdfRegisterStaticVar BOLT_UNIQUE_IDENTIFIER(block)  = []() -> void

// NOTE: Make no sense, just for making sure all the symbols are imported.
extern bool exportModuleBoltBackend;
#define IMPORT_STATIC_LINK_MODULE_BOLT_BACKEND exportModuleBoltBackend = true;

namespace bolt {

class SparkCppUdfRegisterMgr {
public:
    void emplace(std::function<void()>&& registerCallback);

    SparkCppUdfRegisterMgr() = default;

    // Make this class non-movable and non-copiable.
    SparkCppUdfRegisterMgr(SparkCppUdfRegisterMgr&) = delete;
    SparkCppUdfRegisterMgr(SparkCppUdfRegisterMgr&&) = delete;

    static SparkCppUdfRegisterMgr& getInstance() {
        static SparkCppUdfRegisterMgr mgr;
        return mgr;
    }

    void registerUdf();

    void registerFunctionName(const std::string& fn, const char* funName);

    std::unordered_map<std::string, std::string>& getUdfMap();

private:
    std::vector<std::function<void()>> udfRegisterCallbacks_{};

    // <alias, return_type_name> 
    // for registering it into Spark
    std::unordered_map<std::string, std::string> udfMap_{};
};

class UdfRegisterStaticVar
{
public:
  template<typename F> UdfRegisterStaticVar (F&& lambda) {    
    SparkCppUdfRegisterMgr::getInstance().emplace(std::forward<F>(lambda));

    IMPORT_STATIC_LINK_MODULE_BOLT_BACKEND;
  }  
};

/// bolt register wrapper
/// so that function name can be registered into Spark or Flink 
template <template <class> typename Func, typename TReturn, typename... TArgs>
void registerFunction(const std::vector<std::string>& aliases = {}) {
  const char* retTypeName = bytedance::bolt::SimpleTypeTrait<TReturn>::name;
  for (auto&& fn : aliases) {
    SparkCppUdfRegisterMgr::getInstance().registerFunctionName(fn, retTypeName);
  }

  bytedance::bolt::registerFunction<Func, TReturn, TArgs...>(aliases);
}

} // ~ns bolt
