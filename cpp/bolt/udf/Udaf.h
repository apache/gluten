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

struct UdafEntry {
  const char* name;
  const char* dataType;

  int numArgs;
  const char** argTypes;

  const char* intermediateType{nullptr};
  bool variableArity{false};
  bool allowTypeConversion{false};
};

#define GLUTEN_GET_NUM_UDAF getNumUdaf
#define DEFINE_GET_NUM_UDAF extern "C" int GLUTEN_GET_NUM_UDAF()

#define GLUTEN_GET_UDAF_ENTRIES getUdfEntries
#define DEFINE_GET_UDAF_ENTRIES extern "C" void GLUTEN_GET_UDAF_ENTRIES(gluten::UdafEntry* udafEntries)

#define GLUTEN_REGISTER_UDAF registerUdf
#define DEFINE_REGISTER_UDAF extern "C" void GLUTEN_REGISTER_UDAF()

} // namespace gluten
