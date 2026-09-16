# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
include_guard(GLOBAL)

set(GLUTEN_GFLAGS_BUILD_SHA256_CHECKSUM
    34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf)
string(CONCAT GLUTEN_GFLAGS_SOURCE_URL
              "https://github.com/gflags/gflags/archive/refs/tags/"
              "v${GLUTEN_GFLAGS_VERSION}.tar.gz")

resolve_dependency_url(GFLAGS)

message(STATUS "Building gflags from source")
FetchContent_Declare(
  gflags
  URL ${GLUTEN_GFLAGS_SOURCE_URL}
  URL_HASH SHA256=${GLUTEN_GFLAGS_BUILD_SHA256_CHECKSUM})

set(GFLAGS_BUILD_STATIC_LIBS ON)
set(GFLAGS_BUILD_SHARED_LIBS ON)
set(GFLAGS_BUILD_gflags_LIB ON)
# glog relies on the old `google` namespace
set(GFLAGS_NAMESPACE "google;gflags")

FetchContent_MakeAvailable(gflags)

# the flag has to be added to each target we build so adjust to settings choosen
# above
target_compile_options(gflags_static PRIVATE -Wno-cast-function-type)
