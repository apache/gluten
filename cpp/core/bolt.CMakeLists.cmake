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



cmake_minimum_required(VERSION 3.25)

project(gluten)

include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
message("Core module final CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")


set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})

set(GLUTEN_PROTO_SRC_DIR
    ${GLUTEN_HOME}/gluten-core/src/main/resources/org/apache/gluten/proto)
message(STATUS "Set Gluten Proto Directory in ${GLUTEN_PROTO_SRC_DIR}")

set(SUBSTRAIT_PROTO_SRC_DIR
    ${GLUTEN_HOME}/gluten-substrait/src/main/resources/substrait/proto)
message(STATUS "Set Substrait Proto Directory in ${SUBSTRAIT_PROTO_SRC_DIR}")

if(NOT CMAKE_C_COMPILER_LAUNCHER AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  find_program(CCACHE_FOUND ccache)
  if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_FOUND})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_FOUND})
    # keep comments as they might matter to the compiler
    set(ENV{CCACHE_COMMENTS} "1")
  endif(CCACHE_FOUND)
endif()

# Set up Proto
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Found Protobuf: ${PROTOBUF_LIBRARY}")

set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)

# List Substrait Proto compiled files
set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF CACHE BOOL "disable APPEND_PATH")
file(GLOB SUBSTRAIT_PROTO_FILES ${SUBSTRAIT_PROTO_SRC_DIR}/substrait/*.proto
     ${SUBSTRAIT_PROTO_SRC_DIR}/substrait/extensions/*.proto)
set(SUBSTRAIT_PROTO_SRCS)
set(SUBSTRAIT_PROTO_HDRS)
set(Protobuf_IMPORT_DIRS ${SUBSTRAIT_PROTO_SRC_DIR})

protobuf_generate_cpp(SUBSTRAIT_PROTO_SRCS SUBSTRAIT_PROTO_HDRS ${SUBSTRAIT_PROTO_FILES})
set(SUBSTRAIT_PROTO_OUTPUT_FILES ${SUBSTRAIT_PROTO_HDRS}
                                 ${SUBSTRAIT_PROTO_SRCS})

# List Gluten Proto compiled files
file(GLOB GLUTEN_PROTO_FILES ${GLUTEN_PROTO_SRC_DIR}/*.proto)

set(GLUTEN_PROTO_SRCS)
set(GLUTEN_PROTO_HDRS)
set(Protobuf_IMPORT_DIRS ${GLUTEN_PROTO_SRC_DIR})
protobuf_generate_cpp(GLUTEN_PROTO_SRCS GLUTEN_PROTO_HDRS ${GLUTEN_PROTO_FILES})

set(GLUTEN_PROTO_OUTPUT_FILES ${GLUTEN_PROTO_HDRS} ${GLUTEN_PROTO_SRCS})

set(SPARK_COLUMNAR_PLUGIN_SRCS
    ${SUBSTRAIT_PROTO_SRCS}
    ${GLUTEN_PROTO_SRCS}
    compute/Runtime.cc
    compute/ProtobufUtils.cc
    compute/ResultIterator.cc
    config/GlutenConfig.cc
    jni/JniWrapper.cc
    memory/AllocationListener.cc
    memory/MemoryAllocator.cc
    memory/MemoryManager.cc
    memory/ArrowMemoryPool.cc
    memory/ColumnarBatch.cc
    memory/BoltGlutenMemoryManager.cc
    threads/ThreadInitializer.cc
    threads/ThreadManager.cc
    shuffle/Dictionary.cc
    shuffle/FallbackRangePartitioner.cc
    shuffle/HashPartitioner.cc
    shuffle/LocalPartitionWriter.cc
    shuffle/Partitioner.cc
    shuffle/Partitioning.cc
    shuffle/Payload.cc
    shuffle/rss/RssPartitionWriter.cc
    shuffle/RandomPartitioner.cc
    shuffle/RoundRobinPartitioner.cc
    shuffle/ShuffleWriter.cc
    shuffle/SinglePartitioner.cc
    shuffle/Spill.cc
    shuffle/Utils.cc
    utils/Compression.cc
    utils/tac/FForCodec.cc
    utils/tac/TypeAwareCompressCodec.cc
    utils/StringUtil.cc
    utils/ObjectStore.cc
    utils/ConfigResolver.cc

    jni/JniError.cc
    jni/JniCommon.cc
    jni/TaskContextJniWrapper.cc)

file(MAKE_DIRECTORY ${root_directory}/releases)
add_library(gluten STATIC ${SPARK_COLUMNAR_PLUGIN_SRCS})
add_dependencies(gluten jni_proto)

# Hide symbols of some static dependencies. Otherwise, if such dependencies are
# already statically linked to libbolt.so, a runtime error will be reported:
# xxx is being linked both statically and dynamically.
if(NOT CMAKE_SYSTEM_NAME MATCHES "Darwin")
  target_link_options(
    gluten PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()

if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
  execute_process(
    COMMAND ${CMAKE_C_COMPILER} -print-file-name=libstdc++fs.a
    RESULT_VARIABLE LIBSTDCXXFS_STATIC_RESULT
    OUTPUT_VARIABLE LIBSTDCXXFS_STATIC_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(LIBSTDCXXFS_STATIC_RESULT EQUAL 0 AND EXISTS "${LIBSTDCXXFS_STATIC_PATH}")
    message(STATUS "libstdc++fs.a found at: ${LIBSTDCXXFS_STATIC_PATH}")
    target_link_libraries(gluten PRIVATE ${LIBSTDCXXFS_STATIC_PATH})
  else()
    find_library(LIBSTDCXXFS stdc++fs REQUIRED)
    target_link_libraries(gluten PUBLIC ${LIBSTDCXXFS})
  endif()
endif()

find_package(bolt CONFIG)
# to include generated *.pb.h, and expose these as PUBLIC
target_include_directories(gluten PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${bolt_INCLUDE_DIRS} ${bolt_INCLUDE_DIRS}/bolt)
target_link_libraries(gluten PUBLIC protobuf::libprotobuf)

if(ENABLE_QAT)
  include(BuildQATzip)
  include(BuildQATZstd)
  target_sources(gluten PRIVATE utils/qat/QatCodec.cc)
  target_include_directories(gluten PUBLIC ${QATZIP_INCLUDE_DIR}
                                           ${QATZSTD_INCLUDE_DIR})
  target_link_libraries(gluten PUBLIC qatzip::qatzip qatzstd::qatzstd)
endif()


add_custom_target(jni_proto ALL DEPENDS ${SUBSTRAIT_PROTO_OUTPUT_FILES}
                                        ${GLUTEN_PROTO_OUTPUT_FILES})
add_dependencies(jni_proto protobuf::libprotobuf)

target_include_directories(
  gluten
  PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS}
         ${CMAKE_CURRENT_SOURCE_DIR} ${PROTO_OUTPUT_DIR} ${PROTOBUF_INCLUDE})
set_target_properties(gluten PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                        ${root_directory}/releases)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()


if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

add_subdirectory(nativeLoader)


find_package(Arrow CONFIG REQUIRED)

# operators is using folly..
find_package(folly CONFIG REQUIRED)

target_link_libraries(gluten
    PRIVATE 
    Folly::folly
    arrow::arrow
    arrow::libparquet
    glog::glog)
