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

# Download, build, and install paimon-cpp before libch's native Paimon reader.
# Use ClickHouse's libc++ runtime while keeping Paimon's own Arrow dependency.
include(ExternalProject)

set(PAIMON_INSTALL_PREFIX
    "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon"
    CACHE PATH "Install prefix for bundled paimon-cpp")
set(PAIMON_GIT_REPOSITORY
    "https://github.com/apache/paimon-cpp.git"
    CACHE STRING "paimon-cpp git repository")
set(PAIMON_GIT_TAG
    "v0.3.0"
    CACHE STRING "paimon-cpp git tag (must match find_package version 0.3.0)")

string(REGEX REPLACE "[^A-Za-z0-9_.-]" "_" PAIMON_BUILD_TAG "${PAIMON_GIT_TAG}")
set(PAIMON_EP_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon-${PAIMON_BUILD_TAG}-src-upstream-arrow"
)
set(PAIMON_EP_BINARY_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon-${PAIMON_BUILD_TAG}-build-upstream-arrow"
)
set(GLUTEN_PAIMON_LIBCXX_INCLUDE
    "${CMAKE_CURRENT_SOURCE_DIR}/build/contrib/llvm-project/libcxx/include/c++/v1"
)
set(GLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE
    "${CMAKE_CURRENT_SOURCE_DIR}/ClickHouse/contrib/llvm-project/libcxx/include"
)
set(GLUTEN_PAIMON_LIBCXXABI_INCLUDE
    "${CMAKE_CURRENT_SOURCE_DIR}/ClickHouse/contrib/llvm-project/libcxxabi/include"
)
set(GLUTEN_PAIMON_LIBCXX_ARCHIVE
    "${CMAKE_CURRENT_SOURCE_DIR}/build/contrib/libcxx-cmake/libcxx.a")
set(GLUTEN_PAIMON_LIBCXXABI_ARCHIVE
    "${CMAKE_CURRENT_SOURCE_DIR}/build/contrib/libcxxabi-cmake/libcxxabi.a")
set(GLUTEN_PAIMON_LIBUNWIND_ARCHIVE
    "${CMAKE_CURRENT_SOURCE_DIR}/build/contrib/libunwind-cmake/libunwind.a")
# Prefer Paimon's bundled Arrow and fmt headers over system installations;
# ClickHouse supplies only the libc++ runtime and uses its own Arrow build.
set(GLUTEN_PAIMON_CXX_FLAGS
    "-stdlib=libc++ -fuse-ld=lld -nostdlib++ -nostdinc++ -I${PAIMON_EP_BINARY_DIR}/arrow_ep-install/include -I${PAIMON_EP_BINARY_DIR}/fmt_ep-install/include -isystem ${GLUTEN_PAIMON_LIBCXX_INCLUDE} -isystem ${GLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE} -isystem ${GLUTEN_PAIMON_LIBCXXABI_INCLUDE} -include cstdlib"
)
set(GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES
    "${GLUTEN_PAIMON_LIBCXX_ARCHIVE} ${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE} ${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE} -ldl -pthread -lm"
)
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR "Native Paimon requires Clang to build with libc++")
endif()
# Paimon links ClickHouse's C++ runtime.  A clean build does not have these
# generated headers or archives yet, so prepare them before paimon_cpp starts.
add_custom_target(
  prepare_paimon_libcxx
  COMMAND
    ${CMAKE_COMMAND} -G Ninja -S ${CH_SOURCE_DIR} -B ${CH_BINARY_DIR}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DNO_ARMV81_OR_HIGHER=1
    -DENABLE_PROTOBUF=ON -DENABLE_TESTS=OFF -DENABLE_JEMALLOC=ON
    -DENABLE_MULTITARGET_CODE=ON -DENABLE_EXTERN_LOCAL_ENGINE=ON
    -DGLUTEN_ENABLE_NATIVE_PAIMON=OFF -DENABLE_CAPNP=OFF -DENABLE_GRPC=OFF
    -DENABLE_RUST=OFF -DENABLE_H3=OFF -DENABLE_AMQPCPP=OFF
    -DENABLE_CASSANDRA=OFF -DENABLE_KAFKA=ON -DENABLE_NATS=OFF
    -DENABLE_LIBPQXX=OFF -DENABLE_NURAFT=OFF -DENABLE_DATASKETCHES=OFF
    -DENABLE_SQLITE=OFF -DENABLE_S2_GEOMETRY=OFF -DENABLE_ULID=OFF
    -DENABLE_MYSQL=OFF -DENABLE_BCRYPT=OFF -DENABLE_LDAP=OFF
    -DENABLE_MSGPACK=OFF -DUSE_REPLXX=OFF -DENABLE_CLICKHOUSE_ALL=OFF
    -DENABLE_GWP_ASAN=OFF -DENABLE_NUMACTL=OFF -DENABLE_GOOGLE_CLOUD_CPP=OFF
    -DENABLE_ARROW_FLIGHT=OFF -DENABLE_XRAY=0
    "-DCOMPILER_FLAGS=-fvisibility=hidden -fvisibility-inlines-hidden"
  COMMAND ${CMAKE_COMMAND} --build ${CH_BINARY_DIR} --target cxx
  COMMAND
    ${CMAKE_COMMAND}
    -DGLUTEN_PAIMON_LIBCXX_INCLUDE=${GLUTEN_PAIMON_LIBCXX_INCLUDE}
    -DGLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE=${GLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE}
    -DGLUTEN_PAIMON_LIBCXXABI_INCLUDE=${GLUTEN_PAIMON_LIBCXXABI_INCLUDE}
    -DGLUTEN_PAIMON_LIBCXX_ARCHIVE=${GLUTEN_PAIMON_LIBCXX_ARCHIVE}
    -DGLUTEN_PAIMON_LIBCXXABI_ARCHIVE=${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}
    -DGLUTEN_PAIMON_LIBUNWIND_ARCHIVE=${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}
    -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckPaimonLibcxx.cmake
  USES_TERMINAL
  VERBATIM)

file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/deps")

ExternalProject_Add(
  paimon_cpp
  DEPENDS prepare_paimon_libcxx
  GIT_REPOSITORY ${PAIMON_GIT_REPOSITORY}
  GIT_TAG ${PAIMON_GIT_TAG}
  GIT_SHALLOW TRUE
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PAIMON_EP_SOURCE_DIR}
  BINARY_DIR ${PAIMON_EP_BINARY_DIR}
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${PAIMON_INSTALL_PREFIX}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${GLUTEN_PAIMON_CXX_FLAGS}
    -DCMAKE_CXX_STANDARD_LIBRARIES=${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}
    -DGLUTEN_PAIMON_LIBCXX_ARCHIVE=${GLUTEN_PAIMON_LIBCXX_ARCHIVE}
    -DGLUTEN_PAIMON_LIBCXXABI_ARCHIVE=${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}
    -DGLUTEN_PAIMON_LIBUNWIND_ARCHIVE=${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}
    -DGLUTEN_PAIMON_CXX_STANDARD_LIBRARIES=${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}
    -DGLUTEN_PAIMON_COMPAT_SOURCE=${CMAKE_CURRENT_SOURCE_DIR}/cmake/PaimonGlibcCompat.c
    -DLIBUNWIND_LIBRARY=${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}
    -DCMAKE_PREFIX_PATH=
    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF
    -DPAIMON_BUILD_TESTS=OFF
    -DPAIMON_BUILD_SHARED=ON
    -DPAIMON_DEPENDENCY_SOURCE=BUNDLED
    -Dfmt_SOURCE=BUNDLED
    -Dglog_SOURCE=BUNDLED
    -DRapidJSON_SOURCE=BUNDLED
    -DCMAKE_IGNORE_PATH=${CMAKE_CURRENT_SOURCE_DIR}/../ep
  PATCH_COMMAND
    ${CMAKE_COMMAND} -DPAIMON_SOURCE_DIR=<SOURCE_DIR>
    -DGLUTEN_PAIMON_LIBCXX_ARCHIVE=${GLUTEN_PAIMON_LIBCXX_ARCHIVE}
    -DGLUTEN_PAIMON_LIBCXXABI_ARCHIVE=${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}
    -DGLUTEN_PAIMON_LIBUNWIND_ARCHIVE=${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}
    -DGLUTEN_PAIMON_CXX_STANDARD_LIBRARIES=${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}
    -DGLUTEN_PAIMON_COMPAT_SOURCE=${CMAKE_CURRENT_SOURCE_DIR}/cmake/PaimonGlibcCompat.c
    -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/PatchPaimonArrowThrift.cmake
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --parallel
  BUILD_ALWAYS TRUE
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install
  USES_TERMINAL_DOWNLOAD ON
  USES_TERMINAL_BUILD ON
  UPDATE_DISCONNECTED TRUE)

set(GLUTEN_PAIMON_CMAKE_DIR "${PAIMON_INSTALL_PREFIX}/lib/cmake/Paimon")
