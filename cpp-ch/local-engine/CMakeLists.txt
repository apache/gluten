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

if(ENABLE_MULTITARGET_CODE)
  add_definitions(-DENABLE_MULTITARGET_CODE=1)
else()
  add_definitions(-DENABLE_MULTITARGET_CODE=0)
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w -ffunction-sections -fdata-sections")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w -ffunction-sections -fdata-sections")
if(APPLE)
  add_definitions(-D_GNU_SOURCE)
else()
  set(CMAKE_SHARED_LINKER_FLAGS
      "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-export-dynamic -Wl,--gc-sections")
endif()
if(COMPILER_CLANG AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 16)
  set(CMAKE_SHARED_LINKER_FLAGS
      "${CMAKE_SHARED_LINKER_FLAGS} --ld-path=${LLD_WRAPPER}")
else()
  set(CMAKE_SHARED_LINKER_FLAGS
      "${CMAKE_SHARED_LINKER_FLAGS} --ld-path=${LLD_PATH}")
endif()

set(THRIFT_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src")

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

# set(JNI_NATIVE_SOURCES local_engine_jni.cpp)
set(LOCALENGINE_SHARED_LIB _gluten_ch)

add_subdirectory(proto)

add_headers_and_sources(builder Builder)
add_headers_and_sources(join Join)
add_headers_and_sources(parser Parser)
add_headers_and_sources(parser Parser/RelParsers)
add_headers_and_sources(rewriter Rewriter)
add_headers_and_sources(storages Storages)
add_headers_and_sources(storages Storages/Kafka)
add_headers_and_sources(storages Storages/Output)
add_headers_and_sources(storages Storages/Serializations)
add_headers_and_sources(storages Storages/IO)
add_headers_and_sources(storages Storages/MergeTree)
add_headers_and_sources(storages Storages/Kafka)
add_headers_and_sources(storages Storages/Cache)
add_headers_and_sources(common Common)
add_headers_and_sources(external External)
add_headers_and_sources(shuffle Shuffle)
add_headers_and_sources(operator Operator)
add_headers_and_sources(jni jni)
add_headers_and_sources(aggregate_functions AggregateFunctions)
add_headers_and_sources(disks Disks)
add_headers_and_sources(disks Disks/ObjectStorages)
add_headers_and_sources(io IO)

include_directories(
  ${JNI_INCLUDE_DIRS}
  ${CMAKE_CURRENT_BINARY_DIR}/proto
  ${THRIFT_INCLUDE_DIR}
  ${CMAKE_BINARY_DIR}/contrib/thrift-cmake
  ${CMAKE_BINARY_DIR}/contrib/llvm-project/llvm/include
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${ClickHouse_SOURCE_DIR}/src
  ${ClickHouse_SOURCE_DIR}/base
  ${ClickHouse_SOURCE_DIR}/contrib/orc/c++/include
  ${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src/orc/c++/include
  ${CMAKE_BINARY_DIR}/contrib/orc/c++/include
  ${ClickHouse_SOURCE_DIR}/contrib/azure/sdk/storage/azure-storage-blobs/inc
  ${ClickHouse_SOURCE_DIR}/contrib/azure/sdk/core/azure-core/inc
  ${ClickHouse_SOURCE_DIR}/contrib/azure/sdk/storage/azure-storage-common/inc
  ${ClickHouse_SOURCE_DIR}/contrib/llvm-project/llvm/include
  ${ClickHouse_SOURCE_DIR}/contrib/llvm-project/utils/bazel/llvm-project-overlay/llvm/include
  ${ClickHouse_SOURCE_DIR}/contrib/libdivide
  ${ClickHouse_SOURCE_DIR}/contrib/libdivide-cmake)

add_subdirectory(Storages/Parquet)
add_subdirectory(Storages/SubstraitSource)
add_subdirectory(Functions)

add_library(
  gluten_clickhouse_backend_libs
  ${builder_sources}
  ${join_sources}
  ${parser_sources}
  ${rewriter_sources}
  ${storages_sources}
  ${common_sources}
  ${external_sources}
  ${shuffle_sources}
  ${operator_sources}
  ${aggregate_functions_sources}
  ${jni_sources}
  ${disks_sources}
  ${io_sources})

target_link_libraries(
  gluten_clickhouse_backend_libs
  PUBLIC substrait_source clickhouse_aggregate_functions clickhouse_functions
         gluten_spark_functions ch_contrib::xxHash)

option(USE_SYMLINK "Use symlink instead of rename for library" OFF)

# Add udf sources files in sub directories to functions_sources
option(ENABLE_LOCAL_UDFS
       "Build UDFs in 'local-engine/Parser/*_udf' subdirectories" ON)
if(ENABLE_LOCAL_UDFS)
  file(
    GLOB children CONFIGURE_DEPENDS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    Parser/*_udf)
  foreach(child ${children})
    add_headers_and_sources(local_udfs ${child})
  endforeach()
endif()

file(
  GLOB children CONFIGURE_DEPENDS
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  Parser/*_function_parser)
foreach(child ${children})
  add_headers_and_sources(function_parsers ${child})
endforeach()

# Notice: sources files under Parser/*_udf subdirectories must be built into
# target ${LOCALENGINE_SHARED_LIB} directly to make sure all function parsers
# are registered successfully.
add_library(
  ${LOCALENGINE_SHARED_LIB} SHARED
  local_engine_jni.cpp ${local_udfs_sources} ${function_parsers_sources}
  $<TARGET_OBJECTS:clickhouse_malloc>) # why add clickhouse_malloc? check
                                       # clickhouse PR-8046

target_compile_options(${LOCALENGINE_SHARED_LIB} PUBLIC -fPIC
                                                        -Wno-shorten-64-to-32)

target_link_libraries(
  ${LOCALENGINE_SHARED_LIB}
  PUBLIC clickhouse_new_delete
         clickhouse_common_config
         clickhouse_common_io
         clickhouse_parsers
         clickhouse_storages_system
         loggers
         gluten_clickhouse_backend_libs
         ch_contrib::protobuf
  PRIVATE substrait)

target_link_libraries(${LOCALENGINE_SHARED_LIB} PUBLIC ch_parquet)

# Wrap the malloc/free and other C-style functions with our own ones to inject
# memory tracking mechanism into them. Sanitizers have their own way of
# intercepting the allocations and deallocations, so we skip this step for them.
# Define a macro to wrap memory allocation/deallocation functions for memory
# tracking Param: target_lib - The target library to apply these wrappers to
macro(add_memory_tracking_wrappers target_lib)
  # Only apply these wrappers when not using sanitizers and not on macOS or
  # FreeBSD
  if(NOT
     (SANITIZE
      OR SANITIZE_COVERAGE
      OR OS_DARWIN
      OR OS_FREEBSD))
    # Add linker options to wrap standard C memory allocation functions
    target_link_options(
      ${target_lib}
      PRIVATE
      "LINKER:--wrap=malloc"
      "LINKER:--wrap=free"
      "LINKER:--wrap=calloc"
      "LINKER:--wrap=realloc"
      "LINKER:--wrap=aligned_alloc"
      "LINKER:--wrap=posix_memalign"
      "LINKER:--wrap=valloc"
      "LINKER:--wrap=memalign"
      "LINKER:--wrap=reallocarray")

    # Wrap pvalloc only when not using MUSL C library
    if(NOT USE_MUSL)
      target_link_options(${target_lib} PRIVATE "LINKER:--wrap=pvalloc")
    endif()
  endif()
endmacro()

add_memory_tracking_wrappers(${LOCALENGINE_SHARED_LIB})

if(NOT APPLE)
  if(ENABLE_JEMALLOC)
    target_link_options(
      ${LOCALENGINE_SHARED_LIB} PRIVATE
      -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/libch.map
      -Wl,-Bsymbolic-functions)
  else()
    target_link_options(
      ${LOCALENGINE_SHARED_LIB} PRIVATE
      -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/libch-hide-jemalloc.map)
  endif()
endif()
if("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
  set(LOCALENGINE_SHARED_LIB_NAME "libchd.so")
else()
  set(LOCALENGINE_SHARED_LIB_NAME "libch.so")
endif()

option(ENABLE_SEPARATE_SYMBOLS "support separate debug symbols from so" OFF)
if(ENABLE_SEPARATE_SYMBOLS)
  set(SYMBOL_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/debug_symbols)
  file(MAKE_DIRECTORY ${SYMBOL_OUTPUT_DIRECTORY})
  function(separate_symbols target)
    add_custom_command(
      TARGET ${target}
      POST_BUILD
      COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:${target}>
              ${SYMBOL_OUTPUT_DIRECTORY}/$<TARGET_FILE_NAME:${target}>.debug
      COMMAND ${CMAKE_OBJCOPY} --strip-debug $<TARGET_FILE:${target}>
      COMMAND
        ${CMAKE_OBJCOPY}
        --add-gnu-debuglink=${SYMBOL_OUTPUT_DIRECTORY}/$<TARGET_FILE_NAME:${target}>.debug
        $<TARGET_FILE:${target}>
      COMMENT "Separating debug symbols for target: ${target}")
  endfunction()
  separate_symbols(${LOCALENGINE_SHARED_LIB})
endif()

if(USE_SYMLINK)
  add_custom_command(
    OUTPUT ${LOCALENGINE_SHARED_LIB_NAME}
    COMMAND
      ${CMAKE_COMMAND} -E create_symlink
      $<TARGET_FILE:${LOCALENGINE_SHARED_LIB}> ${LOCALENGINE_SHARED_LIB_NAME}
    COMMENT
      "Creating symlink from $<TARGET_FILE:${LOCALENGINE_SHARED_LIB}> to ${LOCALENGINE_SHARED_LIB_NAME}"
    DEPENDS ${LOCALENGINE_SHARED_LIB})
else()
  add_custom_command(
    OUTPUT ${LOCALENGINE_SHARED_LIB_NAME}
    COMMAND ${CMAKE_COMMAND} -E rename $<TARGET_FILE:${LOCALENGINE_SHARED_LIB}>
            ${LOCALENGINE_SHARED_LIB_NAME}
    COMMENT
      "Renaming $<TARGET_FILE:${LOCALENGINE_SHARED_LIB}> to ${LOCALENGINE_SHARED_LIB_NAME}"
    DEPENDS ${LOCALENGINE_SHARED_LIB})
endif()

add_custom_target(libch ALL DEPENDS ${LOCALENGINE_SHARED_LIB_NAME})

add_subdirectory(tests)

if(ENABLE_EXAMPLES)
  add_subdirectory(examples)
endif()
