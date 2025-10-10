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

set(CMAKE_POLICY_WARNING_CMP0177 OFF)
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS ON CACHE BOOL "Suppress developer warnings")

project(bolt_backend)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)
include(FindPackageHandleStandardArgs)

set(SYSTEM_LIB_PATH
    "/usr/lib"
    CACHE PATH "System Lib dir")
set(SYSTEM_LIB64_PATH
    "/usr/lib64"
    CACHE PATH "System Lib64 dir")
set(SYSTEM_LOCAL_LIB_PATH
    "/usr/local/lib"
    CACHE PATH "System Local Lib dir")
set(SYSTEM_LOCAL_LIB64_PATH
    "/usr/local/lib64"
    CACHE PATH "System Local Lib64 dir")
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(SYSTEM_LIB_MULTIARCH_PATH
      "/usr/lib/x86_64-linux-gnu"
      CACHE PATH "System Lib MultiArch dir")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL aarch64)
  set(SYSTEM_LIB_MULTIARCH_PATH
      "/usr/lib/aarch64-linux-gnu"
      CACHE PATH "System Lib MultiArch dir")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL arm64)
  set(SYSTEM_LIB_MULTIARCH_PATH
      "/usr/lib"
      CACHE PATH "System Lib MultiArch dir")
else()
  message(FATAL_ERROR "Unsupported processor type: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

# if(NOT DEFINED BOLT_HOME)
#   set(BOLT_HOME ${GLUTEN_HOME}/ep/build-bolt/build/bolt_ep)
#   message(STATUS "Set BOLT_HOME to ${BOLT_HOME}")
# endif()


if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx -mavx2")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-attributes")
if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess")
endif()

message("Bolt module final CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

macro(find_awssdk)
  find_package(AWSSDK REQUIRED COMPONENTS s3;identity-management)
endmacro()

macro(find_gcssdk)
  find_package(google_cloud_cpp_storage CONFIG 2.22.0 REQUIRED)
endmacro()

macro(find_azure)
  find_package(CURL REQUIRED)
  find_package(LibXml2 REQUIRED)
  find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
  find_package(azure-storage-files-datalake-cpp CONFIG REQUIRED)
  find_package(azure-identity-cpp CONFIG REQUIRED)
endmacro()

# Note: we don't use iceberg, disable it for now
# TODO sync bolt and enable iceberg
# Set up Proto for iceberge
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Found Protobuf: ${PROTOBUF_LIBRARY}")
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)

set(BOLT_PROTO_SRC_DIR
    ${GLUTEN_HOME}/backends-bolt/src/main/resources/org/apache/gluten/proto)
message(STATUS "Set Gluten Directory in ${BOLT_PROTO_SRC_DIR}")

set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF CACHE BOOL "disable APPEND_PATH")
file(GLOB BOLT_PROTO_FILES ${BOLT_PROTO_SRC_DIR}/*.proto)
message(STATUS "Found Bolt Proto files: ${BOLT_PROTO_FILES}")
set(BOLT_PROTO_SRCS "")
set(BOLT_PROTO_HDRS "")
set(Protobuf_IMPORT_DIRS ${BOLT_PROTO_SRC_DIR})
protobuf_generate_cpp(BOLT_PROTO_SRCS BOLT_PROTO_HDRS ${BOLT_PROTO_FILES})

set(BOLT_PROTO_OUTPUT_FILES ${BOLT_PROTO_SRCS} ${BOLT_PROTO_HDRS})

message(STATUS "BOLT_PROTO_OUTPUT_FILES is ${BOLT_PROTO_OUTPUT_FILES}")

# Build Bolt backend.
add_subdirectory(version)

set(BOLT_SRCS
    ${BOLT_PROTO_SRCS}
    compute/BoltBackend.cc
    compute/BoltRuntime.cc
    compute/BoltPlanConverter.cc
    compute/TaskStatusListener.cc
    compute/WholeStageResultIterator.cc
    # compute/iceberg/IcebergPlanConverter.cc
    compute/paimon/PaimonPlanUtils.cc
    jni/JniFileSystem.cc
    jni/JniUdf.cc
    jni/BoltJniWrapper.cc
    memory/BufferOutputStream.cc
    memory/BoltColumnarBatch.cc
    memory/BoltMemoryManager.cc
    operators/functions/RegistrationAllFunctions.cc
    operators/functions/RowConstructorWithNull.cc
    operators/functions/SparkExprToSubfieldFilterParser.cc
    operators/reader/FileReaderIterator.cc
    operators/reader/ParquetReaderIterator.cc
    operators/serializer/BoltColumnarBatchSerializer.cc
    operators/serializer/BoltColumnarToRowConverter.cc
    operators/serializer/BoltRowToColumnarConverter.cc
    operators/writer/BoltColumnarBatchWriter.cc
    operators/writer/BoltParquetDataSource.cc
    substrait/SubstraitExtensionCollector.cc
    substrait/SubstraitParser.cc
    substrait/SubstraitToBoltExpr.cc
    substrait/SubstraitToBoltPlan.cc
    substrait/SubstraitToBoltPlanValidator.cc
    substrait/VariantToVectorConverter.cc
    substrait/BoltSubstraitSignature.cc
    substrait/BoltToSubstraitExpr.cc
    substrait/BoltToSubstraitPlan.cc
    substrait/BoltToSubstraitType.cc
    udf/UdfLoader.cc
    udf/BoltUdf.cc
    utils/Common.cc
    utils/ConfigExtractor.cc
    utils/BoltArrowUtils.cc
    utils/BoltBatchResizer.cc
    utils/BoltWholeStageDumper.cc
    utils/BoltWriterUtils.cc)

if(ENABLE_S3)
  find_package(ZLIB)
endif()

if(ENABLE_GPU)
  list(APPEND BOLT_SRCS cudf/CudfPlanValidator.cc)
endif()

# TODO sync bolt and enable iceberg
# if(ENABLE_ENHANCED_FEATURES)
#   list(APPEND BOLT_SRCS compute/iceberg/IcebergFormat.cc
#        compute/iceberg/IcebergWriter.cc)
# endif()

if(BUILD_TESTS OR BUILD_BENCHMARKS)
  set(BUILD_TEST_UTILS ON)
endif()

if(BUILD_TEST_UTILS)
  list(APPEND BOLT_SRCS tests/utils/TestAllocationListener.cc)
endif()

add_library(${PROJECT_NAME} ${BOLT_SRCS})
target_compile_options(${PROJECT_NAME} PUBLIC -fPIC)

if (BUILD_STATIC)
  add_library(${PROJECT_NAME}_static STATIC ${BOLT_SRCS})
endif()

get_target_property(target_type ${PROJECT_NAME} TYPE)

if (${target_type} STREQUAL "SHARED_LIBRARY")
  target_link_options(${PROJECT_NAME} PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()

target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(${PROJECT_NAME} PUBLIC ${JNI_INCLUDE_DIRS})

if (BUILD_STATIC)
  target_include_directories(${PROJECT_NAME}_static PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_include_directories(${PROJECT_NAME}_static PUBLIC ${JNI_INCLUDE_DIRS})
endif()


if(ENABLE_GLUTEN_VCPKG AND NOT CMAKE_SYSTEM_NAME MATCHES "Darwin")
  # Hide some symbols to avoid conflict.
  target_link_options(
    ${PROJECT_NAME} PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()



add_custom_target(bolt_jni_proto ALL DEPENDS ${SUBSTRAIT_PROTO_OUTPUT_FILES}
                                              ${BOLT_PROTO_OUTPUT_FILES})
add_dependencies(bolt_jni_proto protobuf::libprotobuf)
add_dependencies(${PROJECT_NAME} bolt_jni_proto)
target_include_directories(
  ${PROJECT_NAME}
  PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH}
         ${BOLT_HOME}
         ${PROTO_OUTPUT_DIR}
         ${CMAKE_CURRENT_BINARY_DIR}
         ${PROTOBUF_INCLUDE})

if (BUILD_STATIC)
  add_dependencies(${PROJECT_NAME}_static bolt_jni_proto)
  target_include_directories(
    ${PROJECT_NAME}_static
    PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH}
           ${BOLT_HOME}
           ${PROTO_OUTPUT_DIR}
           ${CMAKE_CURRENT_BINARY_DIR}
           ${PROTOBUF_INCLUDE})
endif()

# set_target_properties(${PROJECT_NAME} PROPERTIES LIBRARY_OUTPUT_DIRECTORY
#                                        ${root_directory}/releases)

# If folly is not installed in system lib paths, please add
# `-DCMAKE_PREFIX_PATH="${folly lib path}" to cmake arguments. It is also
# applicable to other dependencies.
find_package(Folly REQUIRED CONFIG)
find_package(gfx-timsort REQUIRED CONFIG)

if(ENABLE_GLUTEN_VCPKG)
  find_package(gflags REQUIRED COMPONENTS static CONFIG)
else()
  find_package(gflags REQUIRED COMPONENTS shared CONFIG)
endif()


find_package(bolt CONFIG REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(libbacktrace REQUIRED CONFIG)

# Note: jemalloc is used in runtime instead of compile time.
# if(ENABLE_JEMALLOC_STATS)
#   find_package(jemalloc REQUIRED CONFIG)
#   # TODO confirm if jemalloc is required in conan
#   #include(Findjemalloc)
#   #find_jemalloc()
#   #if(JEMALLOC_NOT_FOUND)
#   #  include(Buildjemalloc)
#   #  build_jemalloc()
#   #endif()
#   add_definitions(-DENABLE_JEMALLOC_STATS)
#   target_link_libraries(${PROJECT_NAME} PUBLIC jemalloc::jemalloc)
# endif()

target_link_libraries(${PROJECT_NAME} PUBLIC gluten bolt::bolt xxHash::xxhash gfx::timsort libbacktrace::libbacktrace folly::folly gflags::gflags)
target_include_directories(${PROJECT_NAME} PUBLIC gfx::timsort)

execute_process(
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/bolt-build-info.sh ${bolt_INCLUDE_DIRS}
    OUTPUT_VARIABLE bolt_build_info
)
message(STATUS "Bolt Build Info:\n${bolt_build_info}")

if (BUILD_STATIC)
  target_link_libraries(${PROJECT_NAME}_static PUBLIC gluten bolt::bolt xxHash::xxhash gfx::timsort libbacktrace::libbacktrace folly::folly gflags::gflags)

  #if(ENABLE_JEMALLOC_STATS)
  #  target_link_libraries(${PROJECT_NAME}_static PUBLIC jemalloc::jemalloc)
  #endif()
endif()



find_package(re2 CONFIG REQUIRED)
target_link_libraries(${PROJECT_NAME} PUBLIC ${RE2_LIBRARY})
find_package(Arrow CONFIG REQUIRED)

string(TOUPPER "${CMAKE_BUILD_TYPE}" BUILD_TYPE_UPPER)
set(ARROW_PATH ${arrow_arrow_dataset_LIB_DIRS_${BUILD_TYPE_UPPER}})
message(STATUS "ARROW_PATH ${ARROW_PATH}")

target_link_libraries(${PROJECT_NAME} PUBLIC
  "-Wl,--whole-archive" "${ARROW_PATH}/libarrow_dataset.a" "-Wl,--no-whole-archive"
  "-Wl,--whole-archive" "${ARROW_PATH}/libarrow_acero.a" "-Wl,--no-whole-archive"
  "-Wl,--whole-archive" "${ARROW_PATH}/libarrow_substrait.a" "-Wl,--no-whole-archive"
  "${ARROW_PATH}/libarrow_bundled_dependencies.a"
)

if (BUILD_STATIC)
  target_link_libraries(${PROJECT_NAME}_static PUBLIC ${RE2_LIBRARY})
endif()

find_package(simdjson CONFIG)
if(simdjson_FOUND AND TARGET simdjson::simdjson)
  target_link_libraries(${PROJECT_NAME} PUBLIC simdjson::simdjson)
  if (BUILD_STATIC)
    target_link_libraries(${PROJECT_NAME}_static PUBLIC simdjson::simdjson)
  endif()
endif()


find_package(
  ICU
  COMPONENTS i18n uc data
  REQUIRED)
target_link_libraries(${PROJECT_NAME} PUBLIC ICU::i18n ICU::uc ICU::data)
if (BUILD_STATIC)
  target_link_libraries(${PROJECT_NAME}_static PUBLIC ICU::i18n ICU::uc ICU::data)
endif()


if(ENABLE_S3)
  add_definitions(-DENABLE_S3)
  find_awssdk()
  target_link_libraries(${PROJECT_NAME} PUBLIC ${AWSSDK_LIBRARIES})
  if (BUILD_STATIC)
    target_link_libraries(${PROJECT_NAME}_static PUBLIC ${AWSSDK_LIBRARIES})
  endif()
endif()

if(ENABLE_GCS)
  add_definitions(-DENABLE_GCS)
  find_gcssdk()
  target_link_libraries(${PROJECT_NAME} PUBLIC google-cloud-cpp::storage)
  if (BUILD_STATIC)
    target_link_libraries(${PROJECT_NAME}_static PUBLIC google-cloud-cpp::storage)
  endif()
endif()

if(ENABLE_ABFS)
  add_definitions(-DENABLE_ABFS)
  find_azure()
  target_link_libraries(${PROJECT_NAME} PUBLIC Azure::azure-storage-blobs)
  target_link_libraries(${PROJECT_NAME} PUBLIC Azure::azure-storage-files-datalake)
  target_link_libraries(${PROJECT_NAME} PUBLIC Azure::azure-identity)
  if (BUILD_STATIC)
    target_link_libraries(${PROJECT_NAME}_static PUBLIC Azure::azure-storage-blobs)
    target_link_libraries(${PROJECT_NAME}_static PUBLIC Azure::azure-storage-files-datalake)
    target_link_libraries(${PROJECT_NAME}_static PUBLIC Azure::azure-identity)
  endif()
endif()

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()


if(BUILD_EXAMPLES)
  add_subdirectory(udf/examples)
endif()

# add arch suffix (.amd64 or .aarch64)
# Aligned with java ``` System.getProperty("os.arch") ```
IF((${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64") OR (${CMAKE_SYSTEM_PROCESSOR} MATCHES "AMD64"))
  set(ARCH "amd64")
ELSEIF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  set(ARCH "aarch64")
ELSE()
  message(FATAL_ERROR "Unsupported System Architecture: ${CMAKE_SYSTEM_PROCESSOR}")
ENDIF()

get_target_property(target_type ${PROJECT_NAME} TYPE)

if (${target_type} STREQUAL "SHARED_LIBRARY")
  set_target_properties(${PROJECT_NAME} PROPERTIES SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}")
  install(TARGETS ${PROJECT_NAME}
        DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../build/releases)
endif()

# For conan exporting
# string(FIND "${CMAKE_INSTALL_PREFIX}" "conan" IS_CONAN_EXPORT)
# ${IS_CONAN_EXPORT} GREATER_EQUAL 0
if ( ${target_type} STREQUAL "STATIC_LIBRARY" )

  # the user can work only with bolt's library/header files
  # Anyway, export gluten's Udf.h here.
  install(
    DIRECTORY "${CMAKE_SOURCE_DIR}/bolt/udf/" # source directory
    DESTINATION "include/gluten/udf" # target directory
    FILES_MATCHING # install only matched files
    # PATTERN "test" EXCLUDE
    PATTERN "BoltUdf.h" # select header files
    PATTERN "boltExamples" EXCLUDE
    PATTERN "examples" EXCLUDE
  )

  install(TARGETS ${PROJECT_NAME}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

  install(TARGETS gluten
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

