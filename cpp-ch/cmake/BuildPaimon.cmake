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

# Paimon's bundled Arrow patch is applied with GNU patch. Some CI builder images
# do not include it, while the build container has apt available.
find_program(GLUTEN_PATCH_EXECUTABLE patch)
if(NOT GLUTEN_PATCH_EXECUTABLE)
  execute_process(COMMAND apt-get update
                  RESULT_VARIABLE _patch_apt_update_result)
  if(NOT _patch_apt_update_result EQUAL 0)
    message(
      FATAL_ERROR
        "GNU patch is required to build Paimon, and apt-get update failed")
  endif()
  execute_process(COMMAND apt-get install -y patch
                  RESULT_VARIABLE _patch_apt_install_result)
  if(NOT _patch_apt_install_result EQUAL 0)
    message(
      FATAL_ERROR
        "GNU patch is required to build Paimon, and apt-get install failed")
  endif()
  unset(GLUTEN_PATCH_EXECUTABLE CACHE)
  find_program(GLUTEN_PATCH_EXECUTABLE patch)
endif()
if(NOT GLUTEN_PATCH_EXECUTABLE)
  message(FATAL_ERROR "GNU patch is required to build Paimon")
endif()

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
    "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon-${PAIMON_BUILD_TAG}-build-upstream-arrow-sysroot"
)
# Read ClickHouse's toolchain in a function so its settings do not change the
# outer Gluten build. Keep the sysroot in the compiler flags as well: Paimon and
# Arrow forward C/CXX flags to their nested ExternalProject dependencies, but do
# not forward CMAKE_SYSROOT or CMAKE_TOOLCHAIN_FILE.
function(gluten_paimon_sysroot_flags)
  string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" _processor)
  if(_processor MATCHES "^(amd64|x86_64)$")
    set(_processor x86_64)
  elseif(_processor MATCHES "^(arm64|aarch64)$")
    set(_processor aarch64)
  else()
    message(
      FATAL_ERROR "Unsupported Paimon sysroot architecture: ${_processor}")
  endif()
  set(_toolchain "${CH_SOURCE_DIR}/cmake/linux/toolchain-${_processor}.cmake")
  include("${_toolchain}")
  if(NOT EXISTS "${CMAKE_SYSROOT}/usr/include/stdlib.h")
    message(
      FATAL_ERROR "ClickHouse sysroot is not initialized: ${CMAKE_SYSROOT}")
  endif()
  set(GLUTEN_PAIMON_SYSROOT
      "${CMAKE_SYSROOT}"
      PARENT_SCOPE)
  set(GLUTEN_PAIMON_BUILTINS_ARCHIVE
      "${CH_BINARY_DIR}/clang-builtins/lib/linux/libclang_rt.builtins-${_processor}.a"
      PARENT_SCOPE)
  # Do not let Clang locate runtime libraries in its system resource directory:
  # CI may have Clang installed without compiler-rt. ClickHouse builds these
  # builtins before Paimon is configured. Forward them in the compiler flags so
  # nested dependency builds (including C-only executables) use them too.
  set(GLUTEN_PAIMON_SYSROOT_FLAGS
      "--target=${CMAKE_CXX_COMPILER_TARGET} --sysroot=${CMAKE_SYSROOT} --gcc-toolchain=${TOOLCHAIN_PATH} -fuse-ld=lld -nodefaultlibs -Wl,--start-group,${CH_BINARY_DIR}/clang-builtins/lib/linux/libclang_rt.builtins-${_processor}.a,${CH_BINARY_DIR}/contrib/libunwind-cmake/libunwind.a,--end-group -lc -lm -lpthread -ldl"
      PARENT_SCOPE)
endfunction()
gluten_paimon_sysroot_flags()
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
# Match ClickHouse's modified std::exception layout. This flag must also reach
# every bundled C++ dependency through Paimon's EP_CXX_FLAGS; otherwise runtime
# constructors write past the smaller exception objects allocated by callers.
set(GLUTEN_PAIMON_CXX_FLAGS
    "${GLUTEN_PAIMON_SYSROOT_FLAGS} -DSTD_EXCEPTION_HAS_STACK_TRACE=1 -stdlib=libc++ -nostdlib++ -nostdinc++ -I${PAIMON_EP_BINARY_DIR}/arrow_ep-install/include -I${PAIMON_EP_BINARY_DIR}/fmt_ep-install/include -isystem ${GLUTEN_PAIMON_LIBCXX_INCLUDE} -isystem ${GLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE} -isystem ${GLUTEN_PAIMON_LIBCXXABI_INCLUDE} -include cstdlib"
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
    -DENABLE_ARROW_FLIGHT=OFF -DENABLE_XRAY=0 -DUSE_SYSTEM_COMPILER_RT=OFF
    "-DCOMPILER_FLAGS=-fvisibility=hidden -fvisibility-inlines-hidden"
  COMMAND ${CMAKE_COMMAND} --build ${CH_BINARY_DIR} --target cxx
  COMMAND
    ${CMAKE_COMMAND}
    -DGLUTEN_PAIMON_LIBCXX_INCLUDE=${GLUTEN_PAIMON_LIBCXX_INCLUDE}
    -DGLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE=${GLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE}
    -DGLUTEN_PAIMON_LIBCXXABI_INCLUDE=${GLUTEN_PAIMON_LIBCXXABI_INCLUDE}
    -DGLUTEN_PAIMON_LIBCXX_ARCHIVE=${GLUTEN_PAIMON_LIBCXX_ARCHIVE}
    -DGLUTEN_PAIMON_LIBCXXABI_ARCHIVE=${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}
    -DGLUTEN_PAIMON_BUILTINS_ARCHIVE=${GLUTEN_PAIMON_BUILTINS_ARCHIVE}
    -DGLUTEN_PAIMON_LIBUNWIND_ARCHIVE=${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE} -P
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckPaimonLibcxx.cmake
  USES_TERMINAL VERBATIM)

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
    -DCMAKE_SYSROOT=${GLUTEN_PAIMON_SYSROOT}
    -DCMAKE_C_FLAGS=${GLUTEN_PAIMON_SYSROOT_FLAGS}
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

# Paimon's object libraries include headers installed by these ExternalProject
# targets, but its build graph does not wait for their install steps. Build the
# bundled dependencies before compiling Paimon in a parallel build.
ExternalProject_Add_Step(
  paimon_cpp prepare_dependencies
  COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target arrow_ep avro_ep fmt_ep
          glog_ep orc_ep protobuf_ep rapidjson_ep tbb_ep --parallel
  DEPENDEES configure
  DEPENDERS build
  USES_TERMINAL TRUE)

set(GLUTEN_PAIMON_CMAKE_DIR "${PAIMON_INSTALL_PREFIX}/lib/cmake/Paimon")

# Register after the caller defines the libch build command and output
# directory.
function(gluten_bundle_paimon_libs libch_output_dir deployment_dir)
  # Ship the combined Paimon shared library next to libch.so for deployment.
  add_custom_target(
    bundle_paimon_libs
    COMMAND ${CMAKE_COMMAND} -E make_directory ${libch_output_dir}
    COMMAND
      ${CMAKE_COMMAND} -E copy_if_different
      ${PAIMON_INSTALL_PREFIX}/lib/libpaimon.so ${libch_output_dir}/libpaimon.so
    # Jenkins removes cpp-ch after copying libch.so to the repository root.
    # Preserve both runtime libraries there before the build tree is removed.
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${libch_output_dir}/libch.so
            ${deployment_dir}/libch.so
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${libch_output_dir}/libpaimon.so ${deployment_dir}/libpaimon.so
    DEPENDS _build_ch
    COMMENT "Copy libch.so and libpaimon.so for deployment"
    VERBATIM)
endfunction()
