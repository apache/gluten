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

cmake_minimum_required(VERSION 3.16)

project(gluten)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

message("Core module final CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

set(BOOST_MIN_VERSION "1.42.0")
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
set(source_root_directory ${CMAKE_CURRENT_SOURCE_DIR})

if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})

set(GLUTEN_PROTO_SRC_DIR
    ${GLUTEN_HOME}/gluten-core/src/main/resources/org/apache/gluten/proto)
message(STATUS "Set Gluten Proto Directory in ${GLUTEN_PROTO_SRC_DIR}")

set(SUBSTRAIT_PROTO_SRC_DIR
    ${GLUTEN_HOME}/gluten-substrait/src/main/resources/substrait/proto)
message(STATUS "Set Substrait Proto Directory in ${SUBSTRAIT_PROTO_SRC_DIR}")

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

macro(find_protobuf)
  set(CMAKE_FIND_LIBRARY_SUFFIXES_BCK ${CMAKE_FIND_LIBRARY_SUFFIXES})
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")

  if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    # brew --prefix protobuf will not return the correct path. Update `brew
    # --prefix protobuf@21` if protobuf version is changed.
    execute_process(
      COMMAND brew --prefix protobuf@21
      RESULT_VARIABLE BREW_PROTOBUF
      OUTPUT_VARIABLE BREW_PROTOBUF_PREFIX
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(BREW_PROTOBUF EQUAL 0 AND EXISTS "${BREW_PROTOBUF_PREFIX}")
      message(
        STATUS "Found protobuf installed by Homebrew at ${BREW_PROTOBUF_PREFIX}"
      )
      list(APPEND CMAKE_PREFIX_PATH "${BREW_PROTOBUF_PREFIX}")
    else()
      message(WARNING "Homebrew protobuf not found.")
    endif()
  endif()

  find_package(Protobuf)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES_BCK})
  if("${Protobuf_LIBRARY}" STREQUAL "Protobuf_LIBRARY-NOTFOUND")
    message(FATAL_ERROR "Protobuf Library Not Found")
  endif()
  set(PROTOC_BIN ${Protobuf_PROTOC_EXECUTABLE})
  set(PROTOBUF_INCLUDE
      "${Protobuf_INCLUDE_DIRS}"
      CACHE PATH "Protobuf include path")
endmacro()

# Set up Proto
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)

# List Substrait Proto compiled files
file(GLOB SUBSTRAIT_PROTO_FILES ${SUBSTRAIT_PROTO_SRC_DIR}/substrait/*.proto
     ${SUBSTRAIT_PROTO_SRC_DIR}/substrait/extensions/*.proto)
foreach(PROTO ${SUBSTRAIT_PROTO_FILES})
  file(RELATIVE_PATH REL_PROTO ${SUBSTRAIT_PROTO_SRC_DIR} ${PROTO})
  string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
  list(APPEND SUBSTRAIT_PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
  list(APPEND SUBSTRAIT_PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
endforeach()
set(SUBSTRAIT_PROTO_OUTPUT_FILES ${SUBSTRAIT_PROTO_HDRS}
                                 ${SUBSTRAIT_PROTO_SRCS})
set_source_files_properties(${SUBSTRAIT_PROTO_OUTPUT_FILES} PROPERTIES GENERATED
                                                                       TRUE)
get_filename_component(SUBSTRAIT_PROTO_DIR ${SUBSTRAIT_PROTO_SRC_DIR}/
                       DIRECTORY)

# List Gluten Proto compiled files
file(GLOB GLUTEN_PROTO_FILES ${GLUTEN_PROTO_SRC_DIR}/*.proto)
foreach(PROTO ${GLUTEN_PROTO_FILES})
  file(RELATIVE_PATH REL_PROTO ${GLUTEN_PROTO_SRC_DIR} ${PROTO})
  string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
  list(APPEND GLUTEN_PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
  list(APPEND GLUTEN_PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
endforeach()
set(GLUTEN_PROTO_OUTPUT_FILES ${GLUTEN_PROTO_HDRS} ${GLUTEN_PROTO_SRCS})
set_source_files_properties(${GLUTEN_PROTO_OUTPUT_FILES} PROPERTIES GENERATED
                                                                    TRUE)
get_filename_component(GLUTEN_PROTO_DIR ${GLUTEN_PROTO_SRC_DIR}/ DIRECTORY)

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
    utils/StringUtil.cc
    utils/ObjectStore.cc
    jni/JniError.cc
    jni/JniCommon.cc)

file(MAKE_DIRECTORY ${root_directory}/releases)
add_library(gluten SHARED ${SPARK_COLUMNAR_PLUGIN_SRCS})
add_dependencies(gluten jni_proto)

# Hide symbols of some static dependencies. Otherwise, if such dependencies are
# already statically linked to libvelox.so, a runtime error will be reported:
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

find_arrow_lib(${ARROW_LIB_NAME})
find_arrow_lib(${ARROW_BUNDLED_DEPS})

if(ENABLE_QAT)
  include(BuildQATzip)
  include(BuildQATZstd)
  target_sources(gluten PRIVATE utils/qat/QatCodec.cc)
  target_include_directories(gluten PUBLIC ${QATZIP_INCLUDE_DIR}
                                           ${QATZSTD_INCLUDE_DIR})
  target_link_libraries(gluten PUBLIC qatzip::qatzip qatzstd::qatzstd)
endif()

find_protobuf()
message(STATUS "Found Protobuf: ${PROTOBUF_LIBRARY}")
target_link_libraries(gluten LINK_PUBLIC ${PROTOBUF_LIBRARY})

add_custom_command(
  OUTPUT ${SUBSTRAIT_PROTO_OUTPUT_FILES}
  COMMAND ${PROTOC_BIN} --proto_path ${SUBSTRAIT_PROTO_SRC_DIR}/ --cpp_out
          ${PROTO_OUTPUT_DIR} ${SUBSTRAIT_PROTO_FILES}
  DEPENDS ${SUBSTRAIT_PROTO_DIR}
  COMMENT "Running Substrait PROTO compiler"
  VERBATIM)

add_custom_command(
  OUTPUT ${GLUTEN_PROTO_OUTPUT_FILES}
  COMMAND ${PROTOC_BIN} --proto_path ${GLUTEN_PROTO_SRC_DIR}/ --cpp_out
          ${PROTO_OUTPUT_DIR} ${GLUTEN_PROTO_FILES}
  DEPENDS ${GLUTEN_PROTO_DIR}
  COMMENT "Running Gluten PROTO compiler"
  VERBATIM)

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

if(DEFINED ENV{HADOOP_HOME})
  set(LIBHDFS3_DESTINATION $ENV{HADOOP_HOME}/lib/native)
else()
  set(LIBHDFS3_DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

target_link_libraries(gluten PUBLIC Arrow::arrow
                                    Arrow::arrow_bundled_dependencies)
target_link_libraries(gluten PRIVATE google::glog)

install(TARGETS gluten DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/resources/libhdfs.so
        DESTINATION ${LIBHDFS3_DESTINATION})

add_custom_command(
  TARGET gluten
  POST_BUILD
  COMMAND ld $<TARGET_FILE:gluten> || true
  COMMENT "Checking ld result of libgluten.so")
add_custom_command(
  TARGET gluten
  POST_BUILD
  COMMAND ldd $<TARGET_FILE:gluten> || true
  COMMENT "Checking ldd result of libgluten.so")
