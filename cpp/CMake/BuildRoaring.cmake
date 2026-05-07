# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to You under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

include_guard(GLOBAL)
include(FetchContent)

if(NOT DEFINED GLUTEN_ROARING_VERSION)
  set(GLUTEN_ROARING_VERSION "4.3.11")
endif()

set(ENABLE_ROARING_TESTS
    OFF
    CACHE BOOL "" FORCE)

message(STATUS "Building roaring from source")
FetchContent_Declare(
  roaring_fetch
  GIT_REPOSITORY "https://github.com/RoaringBitmap/CRoaring.git"
  GIT_TAG "v${GLUTEN_ROARING_VERSION}"
  GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(roaring_fetch)
