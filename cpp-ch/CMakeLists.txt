# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.20)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/clickhouse.version CH_VERSION)

set(CH_SOURCE_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/ClickHouse
    CACHE STRING "ClickHouse source dir")
string(REGEX REPLACE ".*CH_ORG=([^\n]+).*" "\\1" CH_ORG "${CH_VERSION}")
string(REGEX REPLACE ".*CH_BRANCH=([^\n]+).*" "\\1" CH_BRANCH "${CH_VERSION}")
string(REGEX REPLACE ".*CH_COMMIT=([^\n]+).*" "\\1" CH_COMMIT "${CH_VERSION}")

message("CH_ORG=${CH_ORG}")
message("CH_BRANCH=${CH_BRANCH}")
message("CH_COMMIT=${CH_COMMIT}")

project(libch LANGUAGES C CXX ASM)
file(GLOB clickhouse_files "${CH_SOURCE_DIR}/*")
if("${CH_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}/ClickHouse")
  if(NOT clickhouse_files)
    execute_process(
      COMMAND
        git clone -b ${CH_BRANCH} --depth 10
        https://github.com/${CH_ORG}/ClickHouse.git ${CH_SOURCE_DIR}
        COMMAND_ERROR_IS_FATAL ANY)
    execute_process(
      COMMAND git reset --hard ${CH_COMMIT}
      WORKING_DIRECTORY ${CH_SOURCE_DIR} COMMAND_ERROR_IS_FATAL ANY)
    execute_process(
      COMMAND git submodule update --init --recursive --force --depth 1 --jobs
              32 --single-branch WORKING_DIRECTORY ${CH_SOURCE_DIR}
                                                   COMMAND_ERROR_IS_FATAL ANY)
  else()
    execute_process(
      COMMAND git fetch origin ${CH_BRANCH} --depth 10
      WORKING_DIRECTORY ${CH_SOURCE_DIR} COMMAND_ERROR_IS_FATAL ANY)
    execute_process(
      COMMAND git checkout ${CH_BRANCH}
      WORKING_DIRECTORY ${CH_SOURCE_DIR} COMMAND_ERROR_IS_FATAL ANY)
    execute_process(
      COMMAND git reset --hard ${CH_COMMIT}
      WORKING_DIRECTORY ${CH_SOURCE_DIR} COMMAND_ERROR_IS_FATAL ANY)
    execute_process(
      COMMAND git submodule update --init --recursive --force --depth 1 --jobs
              32 --single-branch WORKING_DIRECTORY ${CH_SOURCE_DIR}
                                                   COMMAND_ERROR_IS_FATAL ANY)
  endif()
else()
  if(NOT clickhouse_files)
    # Checking out *all* submodules takes > 5 min. Therefore, the smoke build
    # ("FastTest") in CI initializes only the set of submodules minimally needed
    # for a build and we cannot assume here that all submodules are populated.
    message(ERROR
            "clickhouse ${CH_SOURCE_DIR} is missing or empty. to fix try run:")
    message(
      STATUS
        "    git clone --recursive --depth 1 https://github.com/Kyligence/ClickHouse.git ${CMAKE_SOURCE_DIR}"
    )
  endif()
endif()

if(EXISTS "${CH_SOURCE_DIR}/utils/extern-local-engine")
  execute_process(COMMAND rm -rf ${CH_SOURCE_DIR}/utils/extern-local-engine)
endif()
execute_process(
  COMMAND ln -s ${CMAKE_CURRENT_SOURCE_DIR}/local-engine
          ${CH_SOURCE_DIR}/utils/extern-local-engine COMMAND_ERROR_IS_FATAL ANY)

# execute_process(COMMAND find ${CMAKE_CURRENT_SOURCE_DIR}/local-engine -regex
# '.*\.\(c\|cpp\|h\)' -exec clang-format-15 --verbose -i --style=file -i {} \;)

set(CH_BINARY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build")
option(ENABLE_CPP_TEST "Build CPP Unit test" OFF)
option(GLUTEN_ENABLE_NATIVE_PAIMON
       "Build libch with native Paimon read support" ON)
option(
  GLUTEN_BUNDLE_PAIMON
  "Download and build paimon-cpp into build/deps/paimon when Paimon_DIR is unset"
  ON)

set(GLUTEN_USE_BUNDLED_PAIMON OFF)
set(_GLUTEN_PAIMON_BUILD_DEP "")
set(_GLUTEN_CH_CMAKE_PAIMON_ARG
    "-DGLUTEN_ENABLE_NATIVE_PAIMON=${GLUTEN_ENABLE_NATIVE_PAIMON}")

if(GLUTEN_ENABLE_NATIVE_PAIMON)
  if(Paimon_DIR)
    set(GLUTEN_PAIMON_CMAKE_DIR "${Paimon_DIR}")
    message(STATUS "Using external paimon-cpp from Paimon_DIR=${Paimon_DIR}")
  elseif(GLUTEN_BUNDLE_PAIMON)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildPaimon.cmake)
    set(GLUTEN_USE_BUNDLED_PAIMON ON)
    message(
      STATUS "Bundled paimon-cpp will be installed to ${PAIMON_INSTALL_PREFIX}")
  else()
    message(
      FATAL_ERROR
        "Native Paimon requires paimon-cpp. Set -DPaimon_DIR=... or -DGLUTEN_BUNDLE_PAIMON=ON."
    )
  endif()
  if(GLUTEN_USE_BUNDLED_PAIMON)
    # The bridge sources are compiled into libch.  Keep Paimon itself in shared
    # libraries so its implementation is not copied into libch.
    set(_GLUTEN_PAIMON_BUILD_DEP paimon_cpp)
    list(APPEND _GLUTEN_CH_CMAKE_PAIMON_ARG
         "-DPaimon_DIR=${GLUTEN_PAIMON_CMAKE_DIR}"
         "-DPAIMON_BUNDLED_DEPS_DIR=${PAIMON_EP_BINARY_DIR}")
  else()
    message(
      FATAL_ERROR
        "Native Paimon support requires bundled paimon-cpp (-DGLUTEN_BUNDLE_PAIMON=ON)."
    )
  endif()
else()
  message(
    STATUS
      "GLUTEN_ENABLE_NATIVE_PAIMON=OFF: libch builds without paimon-cpp (raw Parquet Paimon scans only)."
  )
endif()

set(LIBCH_OUTPUT_DIR "${CH_BINARY_DIR}/utils/extern-local-engine")

if(ENABLE_CPP_TEST)
  add_custom_command(
    USES_TERMINAL
    COMMAND
      bash -c \"cmake -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DNO_ARMV81_OR_HIGHER=1
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DENABLE_PROTOBUF=ON
      -DENABLE_TESTS=ON -DENABLE_JEMALLOC=ON -DENABLE_MULTITARGET_CODE=ON
      -DENABLE_EXTERN_LOCAL_ENGINE=ON ${_GLUTEN_CH_CMAKE_PAIMON_ARG}
      -DCOMPILER_FLAGS='-fvisibility=hidden -fvisibility-inlines-hidden' -S
      ${CH_SOURCE_DIR} -G Ninja -B ${CH_BINARY_DIR} && cmake --build
      ${CH_BINARY_DIR} --target ch unit_tests_local_engine\"
    OUTPUT _build_ch
    DEPENDS ${_GLUTEN_PAIMON_BUILD_DEP})
else()
  add_custom_command(
    USES_TERMINAL
    COMMAND
      bash -c \"cmake -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DNO_ARMV81_OR_HIGHER=1
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DENABLE_PROTOBUF=ON
      -DENABLE_TESTS=OFF -DENABLE_JEMALLOC=ON -DENABLE_MULTITARGET_CODE=ON
      -DENABLE_EXTERN_LOCAL_ENGINE=ON -DENABLE_CAPNP=OFF -DENABLE_GRPC=OFF
      -DENABLE_RUST=OFF -DENABLE_H3=OFF -DENABLE_AMQPCPP=OFF
      -DENABLE_CASSANDRA=OFF -DENABLE_KAFKA=ON -DENABLE_NATS=OFF
      -DENABLE_LIBPQXX=OFF -DENABLE_NURAFT=OFF -DENABLE_DATASKETCHES=OFF
      -DENABLE_SQLITE=OFF -DENABLE_S2_GEOMETRY=OFF -DENABLE_ULID=OFF
      -DENABLE_MYSQL=OFF -DENABLE_BCRYPT=OFF -DENABLE_LDAP=OFF
      -DENABLE_MSGPACK=OFF -DUSE_REPLXX=OFF -DENABLE_CLICKHOUSE_ALL=OFF
      -DENABLE_GWP_ASAN=OFF -DENABLE_NUMACTL=OFF -DENABLE_GOOGLE_CLOUD_CPP=OFF
      -DENABLE_ARROW_FLIGHT=OFF -DENABLE_XRAY=0 ${_GLUTEN_CH_CMAKE_PAIMON_ARG}
      -DCOMPILER_FLAGS='-fvisibility=hidden -fvisibility-inlines-hidden' -S
      ${CH_SOURCE_DIR} -G Ninja -B ${CH_BINARY_DIR} && cmake --build
      ${CH_BINARY_DIR} --target libch\"
    OUTPUT _build_ch
    DEPENDS ${_GLUTEN_PAIMON_BUILD_DEP})
endif()

if(GLUTEN_USE_BUNDLED_PAIMON)
  gluten_bundle_paimon_libs("${LIBCH_OUTPUT_DIR}"
                            "${CMAKE_CURRENT_SOURCE_DIR}/..")
  add_custom_target(build_ch ALL DEPENDS bundle_paimon_libs)
else()
  add_custom_target(build_ch ALL DEPENDS _build_ch)
endif()
