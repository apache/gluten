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

namespace gluten {

struct UdfEntry {
  const char* name;
  const char* dataType;

  int numArgs;
  const char** argTypes;

  bool variableArity{false};
  bool allowTypeConversion{false};
};

#define GLUTEN_GET_NUM_UDF getNumUdf
#define DEFINE_GET_NUM_UDF extern "C" int GLUTEN_GET_NUM_UDF()

#define GLUTEN_GET_UDF_ENTRIES getUdfEntries
#define DEFINE_GET_UDF_ENTRIES extern "C" void GLUTEN_GET_UDF_ENTRIES(gluten::UdfEntry* udfEntries)

#define GLUTEN_REGISTER_UDF registerUdf
#define DEFINE_REGISTER_UDF extern "C" void GLUTEN_REGISTER_UDF()

// Declares a UDF by name, leaving its signature where the library already
// stated it: the Velox function registry.
//
// A UdfEntry restates a signature that the registered Velox function has
// already declared, so the two can disagree, and a name can only be called
// with the combinations the library thought to write out. A RegistryUdfEntry
// carries no signature at all. Gluten resolves the return type by binding the
// actual argument types against what Velox holds for the name, once per call
// site. The library still registers the function itself through
// GLUTEN_REGISTER_UDF.
//
// This matters most for a signature with type variables -- array(T) -> T, or
// (K, V) -> map(K,V) -- where a UdfEntry would mean one entry per type
// combination. It is not limited to those: a function with several concrete
// signatures can be declared this way too, and none of them have to be
// repeated here.
//
// A library may declare both UdfEntry and RegistryUdfEntry functions. A
// UdfEntry takes precedence for a given name.
struct RegistryUdfEntry {
  // Name the function is registered under in the Velox function registry, and
  // the name a query calls it by.
  const char* name;
};

#define GLUTEN_GET_NUM_REGISTRY_UDF getNumRegistryUdf
#define DEFINE_GET_NUM_REGISTRY_UDF extern "C" int GLUTEN_GET_NUM_REGISTRY_UDF()

#define GLUTEN_GET_REGISTRY_UDF_ENTRIES getRegistryUdfEntries
#define DEFINE_GET_REGISTRY_UDF_ENTRIES \
  extern "C" void GLUTEN_GET_REGISTRY_UDF_ENTRIES(gluten::RegistryUdfEntry* registryUdfEntries)

} // namespace gluten
