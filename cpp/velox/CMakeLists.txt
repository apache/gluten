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

project(velox)

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
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL ppc64le)
  set(SYSTEM_LIB_MULTIARCH_PATH
      "/usr/lib"
      CACHE PATH "System Lib MultiArch dir")
else()
  message(FATAL_ERROR "Unsupported processor type: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

if(NOT DEFINED VELOX_HOME)
  set(VELOX_HOME ${GLUTEN_HOME}/ep/build-velox/build/velox_ep)
  message(STATUS "Set VELOX_HOME to ${VELOX_HOME}")
endif()

message("Velox module final CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

# User can specify VELOX_BUILD_PATH, if Velox are built elsewhere.
if(NOT DEFINED VELOX_BUILD_PATH)
  if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(VELOX_BUILD_PATH
        "${VELOX_HOME}/_build/debug"
        CACHE PATH "Velox build directory.")
  else()
    set(VELOX_BUILD_PATH
        "${VELOX_HOME}/_build/release"
        CACHE PATH "Velox build directory.")
  endif()
endif()

find_package(Roaring REQUIRED)
set(GLUTEN_ROARING_LINK_LIBRARY roaring)

set(VELOX_PROTO_SRC_DIR
    ${GLUTEN_HOME}/backends-velox/src/main/resources/org/apache/gluten/proto)
message(STATUS "Set Gluten Proto Directory in ${VELOX_PROTO_SRC_DIR}")

function(import_library TARGET_NAME LIB_PATH)
  if(NOT EXISTS ${LIB_PATH})
    message(FATAL_ERROR "Library does not exist: ${LIB_PATH}")
  endif()
  add_library(${TARGET_NAME} STATIC IMPORTED)
  set_target_properties(${TARGET_NAME} PROPERTIES IMPORTED_LOCATION ${LIB_PATH})
endfunction()

macro(add_duckdb)
  find_package(DuckDB)
  if(NOT DuckDB_FOUND)
    message(FATAL_ERROR "Cannot find DuckDB.")
  else()
    message(STATUS "Found DuckDB library from ${DuckDB_DIR}")
    target_link_libraries(facebook::velox INTERFACE duckdb_static)
  endif()
endmacro()

macro(find_re2)
  find_package(re2 CONFIG)
  if(re2_FOUND AND TARGET re2::re2)
    set(RE2_LIBRARY re2::re2)
  else()
    find_library(
      RE2_LIBRARY
      NAMES re2
      PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH}
            ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH}
      NO_DEFAULT_PATH)
  endif()

  if(NOT RE2_LIBRARY)
    message(FATAL_ERROR "RE2 Library Not Found")
  else()
    message(STATUS "RE2 Library Can Be Found in ${RE2_LIBRARY}")
  endif()
endmacro()

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

# Set up Proto
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)

# List Gluten Proto compiled files
file(GLOB VELOX_PROTO_FILES ${VELOX_PROTO_SRC_DIR}/*.proto)
foreach(PROTO ${VELOX_PROTO_FILES})
  file(RELATIVE_PATH REL_PROTO ${VELOX_PROTO_SRC_DIR} ${PROTO})
  string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
  list(APPEND VELOX_PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
  list(APPEND VELOX_PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
endforeach()
set(VELOX_PROTO_OUTPUT_FILES ${VELOX_PROTO_HDRS} ${VELOX_PROTO_SRCS})
set_source_files_properties(${VELOX_PROTO_OUTPUT_FILES} PROPERTIES GENERATED
                                                                   TRUE)
get_filename_component(VELOX_PROTO_DIR ${VELOX_PROTO_SRC_DIR}/ DIRECTORY)

# Build Velox backend.
set(VELOX_SRCS
    ${VELOX_PROTO_SRCS}
    compute/VeloxBackend.cc
    compute/VeloxRuntime.cc
    compute/VeloxPlanConverter.cc
    compute/WholeStageResultIterator.cc
    compute/delta/DeltaConnector.cpp
    compute/delta/DeltaDataSource.cpp
    compute/delta/DeltaDeletionVectorReader.cpp
    compute/delta/DeltaSplit.cpp
    compute/delta/DeltaSplitReader.cpp
    compute/delta/RoaringBitmapArray.cpp
    compute/iceberg/IcebergPlanConverter.cc
    jni/JniFileSystem.cc
    jni/JniUdf.cc
    jni/VeloxJniWrapper.cc
    jni/JniHashTable.cc
    memory/BufferOutputStream.cc
    memory/VeloxColumnarBatch.cc
    memory/VeloxMemoryManager.cc
    operators/functions/RegistrationAllFunctions.cc
    operators/functions/delta/DeltaBitmapAggregator.cc
    operators/functions/RowConstructorWithNull.cc
    operators/functions/SparkExprToSubfieldFilterParser.cc
    operators/plannodes/RowVectorStream.cc
    operators/hashjoin/HashTableBuilder.cc
    operators/hashjoin/HashTableSerializer.cc
    operators/reader/FileReaderIterator.cc
    operators/reader/ParquetReaderIterator.cc
    operators/serializer/VeloxColumnarBatchSerializer.cc
    operators/serializer/VeloxColumnarToRowConverter.cc
    operators/serializer/VeloxRowToColumnarConverter.cc
    operators/writer/VeloxColumnarBatchWriter.cc
    operators/writer/VeloxParquetDataSource.cc
    shuffle/ArrowShuffleDictionaryWriter.cc
    shuffle/ReaderThreadPool.cc
    shuffle/VeloxHashShuffleWriter.cc
    shuffle/VeloxRssSortShuffleWriter.cc
    shuffle/VeloxShuffleReader.cc
    shuffle/VeloxShuffleWriter.cc
    shuffle/VeloxSortShuffleWriter.cc
    substrait/SubstraitExtensionCollector.cc
    substrait/SubstraitParser.cc
    substrait/SubstraitToVeloxExpr.cc
    substrait/SubstraitToVeloxPlan.cc
    substrait/SubstraitToVeloxPlanValidator.cc
    substrait/VariantToVectorConverter.cc
    substrait/VeloxSubstraitSignature.cc
    substrait/VeloxToSubstraitExpr.cc
    substrait/VeloxToSubstraitPlan.cc
    substrait/VeloxToSubstraitType.cc
    udf/UdfLoader.cc
    utils/Common.cc
    utils/ConfigExtractor.cc
    utils/VeloxArrowUtils.cc
    utils/VeloxBatchResizer.cc
    utils/VeloxWholeStageDumper.cc
    utils/VeloxWriterUtils.cc)

if(ENABLE_S3)
  list(APPEND VELOX_SRCS filesystem/GlutenS3FileSystem.cc)
  find_package(ZLIB)
endif()

if(ENABLE_GPU)
  list(
    APPEND
    VELOX_SRCS
    cudf/CudfPlanValidator.cc
    cudf/GpuLock.cc
    operators/plannodes/CudfVectorStream.cc
    shuffle/VeloxGpuAsyncShuffleReader.cc
    shuffle/VeloxGpuShuffleReader.cc
    shuffle/VeloxGpuShuffleWriter.cc
    operators/serializer/VeloxGpuColumnarBatchSerializer.cc
    utils/GpuBufferBatchResizer.cc
    memory/GpuBufferColumnarBatch.cc)
endif()

list(APPEND VELOX_SRCS compute/iceberg/IcebergFormat.cc
     compute/iceberg/IcebergWriter.cc)

if(BUILD_TESTS OR BUILD_BENCHMARKS)
  set(BUILD_TEST_UTILS ON)
endif()

if(BUILD_TEST_UTILS)
  list(APPEND VELOX_SRCS tests/utils/LocalRssClient.cc
       tests/utils/TestAllocationListener.cc)
endif()

add_library(velox SHARED ${VELOX_SRCS})

# Keep ABI sensitive Velox feature macros in sync
set(GLUTEN_VELOX_CMAKE_CACHE "${VELOX_BUILD_PATH}/CMakeCache.txt")
set(GLUTEN_VELOX_ENABLE_PARQUET ${VELOX_ENABLE_PARQUET})
if(NOT GLUTEN_VELOX_ENABLE_PARQUET AND EXISTS "${GLUTEN_VELOX_CMAKE_CACHE}")
  file(STRINGS "${GLUTEN_VELOX_CMAKE_CACHE}" GLUTEN_VELOX_ENABLE_PARQUET
       REGEX "^VELOX_ENABLE_PARQUET:BOOL=(ON|TRUE|1)$")
endif()
if(GLUTEN_VELOX_ENABLE_PARQUET)
  target_compile_definitions(velox PRIVATE VELOX_ENABLE_PARQUET)
  message(STATUS "Propagating VELOX_ENABLE_PARQUET to Gluten Velox backend")
endif()

if(ENABLE_GLUTEN_VCPKG AND NOT CMAKE_SYSTEM_NAME MATCHES "Darwin")
  # Hide some symbols to avoid conflict.
  target_link_options(
    velox PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  # Make libvelox.dylib own the Velox mono archive. Test utilities can then
  # resolve Velox symbols from the dylib instead of linking a second copy.
  target_link_options(velox PRIVATE
                      "LINKER:-force_load,${VELOX_BUILD_PATH}/lib/libvelox.a")
endif()

find_protobuf()
message(STATUS "Found Protobuf: ${PROTOBUF_LIBRARY}")

add_custom_command(
  OUTPUT ${VELOX_PROTO_OUTPUT_FILES}
  COMMAND ${PROTOC_BIN} --proto_path ${VELOX_PROTO_SRC_DIR}/ --cpp_out
          ${PROTO_OUTPUT_DIR} ${VELOX_PROTO_FILES}
  DEPENDS ${VELOX_PROTO_DIR}
  COMMENT "Running Gluten Velox PROTO compiler"
  VERBATIM)

add_custom_target(velox_jni_proto ALL DEPENDS ${SUBSTRAIT_PROTO_OUTPUT_FILES}
                                              ${VELOX_PROTO_OUTPUT_FILES})
add_dependencies(velox_jni_proto protobuf::libprotobuf)

add_dependencies(velox velox_jni_proto)

target_include_directories(
  velox
  PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH}
         ${JNI_INCLUDE_DIRS}
         ${VELOX_BUILD_PATH}
         ${CMAKE_CURRENT_SOURCE_DIR}
         ${VELOX_HOME}
         ${PROTO_OUTPUT_DIR}
         ${PROTOBUF_INCLUDE})
if(GLUTEN_PREFIX_INCLUDE_DIRS)
  target_include_directories(velox BEFORE PUBLIC ${GLUTEN_PREFIX_INCLUDE_DIRS})
endif()

set_target_properties(velox PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                       ${root_directory}/releases)

if(ENABLE_JEMALLOC_STATS)
  include(Findjemalloc)
  find_jemalloc()
  if(JEMALLOC_NOT_FOUND)
    include(Buildjemalloc)
    build_jemalloc()
  endif()
  add_definitions(-DENABLE_JEMALLOC_STATS)
  target_link_libraries(velox PUBLIC jemalloc::jemalloc)
endif()

target_link_libraries(velox PUBLIC gluten)

# Requires VELOX_MONO_LIBRARY=ON when building Velox.
import_library(facebook::velox ${VELOX_BUILD_PATH}/lib/libvelox.a)
set(GLUTEN_VELOX_LINK_SCOPE PUBLIC)
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set(GLUTEN_VELOX_LINK_SCOPE PRIVATE)
endif()

if(BUILD_TESTS)
  add_duckdb()

  import_library(facebook::velox::dbgen
                 ${VELOX_BUILD_PATH}/velox/tpch/gen/dbgen/libdbgen.a)
  target_link_libraries(facebook::velox INTERFACE facebook::velox::dbgen)

  import_library(
    facebook::velox::vector_test_lib
    ${VELOX_BUILD_PATH}/velox/vector/tests/utils/libvelox_vector_test_lib.a)
  import_library(
    facebook::velox::dwio_common_test
    ${VELOX_BUILD_PATH}/velox/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a
  )
  import_library(
    facebook::velox::file_test_utils
    ${VELOX_BUILD_PATH}/velox/common/file/tests/libvelox_file_test_utils.a)
  import_library(
    facebook::velox::exec_test_lib
    ${VELOX_BUILD_PATH}/velox/exec/tests/utils/libvelox_exec_test_lib.a)
  target_link_libraries(
    facebook::velox::exec_test_lib
    INTERFACE facebook::velox::vector_test_lib
              facebook::velox::dwio_common_test
              facebook::velox::file_test_utils)
  target_link_libraries(velox PUBLIC facebook::velox::exec_test_lib)
endif()

target_link_libraries(velox ${GLUTEN_VELOX_LINK_SCOPE} facebook::velox)

target_link_libraries(velox PUBLIC ${GLUTEN_ROARING_LINK_LIBRARY})

find_re2()
target_link_libraries(velox PUBLIC ${RE2_LIBRARY})

set(CMAKE_FIND_LIBRARY_SUFFIXES_BCK ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")

find_library(STEMMER_LIB NAMES libstemmer.a)
if(STEMMER_LIB)
  message(STATUS "Found stemmer: ${STEMMER_LIB}")
  add_library(external::stemmer STATIC IMPORTED)
  set_target_properties(external::stemmer PROPERTIES IMPORTED_LOCATION
                                                     ${STEMMER_LIB})
else()
  import_library(
    external::stemmer
    ${VELOX_BUILD_PATH}/_deps/libstemmer/src/libstemmer/libstemmer.a)
endif()
target_link_libraries(velox PUBLIC external::stemmer)

find_package(geos)
if(geos_FOUND AND TARGET GEOS::geos)
  add_library(external::geos ALIAS GEOS::geos)
else()
  message(STATUS "import Velox bundled geos")
  import_library(external::geos ${VELOX_BUILD_PATH}/lib/libgeos.a)
endif()
target_link_libraries(velox PUBLIC external::geos)

find_package(veloxthrift CONFIG)
if(veloxthrift_FOUND AND TARGET veloxthrift::veloxthrift)
  add_library(external::veloxthrift ALIAS veloxthrift::veloxthrift)
else()
  message(STATUS "import Velox bundled veloxthrift")
  import_library(
    external::veloxthrift
    ${VELOX_BUILD_PATH}/velox/dwio/parquet/thrift/libvelox_dwio_parquet_thrift_raw.a
  )
endif()
target_link_libraries(velox PUBLIC external::veloxthrift)

# If folly is not installed in system lib paths, please add
# `-DCMAKE_PREFIX_PATH="${folly lib path}" to cmake arguments. It is also
# applicable to other dependencies.
find_package(Folly REQUIRED CONFIG)
if(CMAKE_SYSTEM_NAME MATCHES "Darwin" AND TARGET gflags_shared)
  # Folly's exported interface references the static gflags target while glog
  # links the shared gflags library. Loading both copies into one process makes
  # gflags register its built-in flags twice and abort test binaries. Point
  # folly's interface at the shared gflags target instead.
  foreach(FOLLY_TARGET Folly::folly Folly::folly_deps)
    if(TARGET ${FOLLY_TARGET})
      get_target_property(FOLLY_LINK_LIBS ${FOLLY_TARGET}
                          INTERFACE_LINK_LIBRARIES)
      if(FOLLY_LINK_LIBS MATCHES "gflags_static")
        list(TRANSFORM FOLLY_LINK_LIBS REPLACE "gflags_static" "gflags_shared")
        set_target_properties(
          ${FOLLY_TARGET} PROPERTIES INTERFACE_LINK_LIBRARIES
                                     "${FOLLY_LINK_LIBS}")
      endif()
    endif()
  endforeach()
endif()
target_link_libraries(velox PUBLIC Folly::folly)

# Link thrift libraries - check vcpkg first, then fall back to system libraries
# Determine vcpkg triplet directory based on architecture
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(VCPKG_TRIPLET_DIR "x64-linux-avx")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(aarch64)|(arm64)")
  set(VCPKG_TRIPLET_DIR "arm64-linux-neon")
else()
  message(
    WARNING
      "Unknown processor type for vcpkg: ${CMAKE_SYSTEM_PROCESSOR}, defaulting to x64-linux-avx"
  )
  set(VCPKG_TRIPLET_DIR "x64-linux-avx")
endif()

if(DEFINED VCPKG_INSTALLED_DIR
   AND EXISTS "${VCPKG_INSTALLED_DIR}/${VCPKG_TRIPLET_DIR}/lib/libthriftcpp2.a")
  message(
    STATUS
      "Using vcpkg thrift libraries from ${VCPKG_INSTALLED_DIR}/${VCPKG_TRIPLET_DIR}"
  )
  target_link_libraries(
    velox
    PRIVATE ${VCPKG_INSTALLED_DIR}/${VCPKG_TRIPLET_DIR}/lib/libthriftcpp2.a
            ${VCPKG_INSTALLED_DIR}/${VCPKG_TRIPLET_DIR}/lib/libthriftprotocol.a)
else()
  set(THRIFT_LIBRARY_DIRS)
  if(CMAKE_INSTALL_PREFIX)
    list(APPEND THRIFT_LIBRARY_DIRS "${CMAKE_INSTALL_PREFIX}/lib")
  endif()
  foreach(PREFIX_PATH IN LISTS CMAKE_PREFIX_PATH)
    list(APPEND THRIFT_LIBRARY_DIRS "${PREFIX_PATH}/lib" "${PREFIX_PATH}")
  endforeach()
  list(APPEND THRIFT_LIBRARY_DIRS /usr/local/lib)
  list(REMOVE_DUPLICATES THRIFT_LIBRARY_DIRS)

  message(STATUS "Searching thrift libraries in ${THRIFT_LIBRARY_DIRS}")
  find_library(
    THRIFTCPP2_LIB
    NAMES thriftcpp2
    PATHS ${THRIFT_LIBRARY_DIRS}
    NO_DEFAULT_PATH)
  find_library(
    THRIFTPROTOCOL_LIB
    NAMES thriftprotocol
    PATHS ${THRIFT_LIBRARY_DIRS}
    NO_DEFAULT_PATH)

  if(THRIFTCPP2_LIB AND THRIFTPROTOCOL_LIB)
    target_link_libraries(velox PRIVATE ${THRIFTCPP2_LIB} ${THRIFTPROTOCOL_LIB})
  else()
    message(
      FATAL_ERROR
        "Could not find thrift libraries. Please install them or use vcpkg.")
  endif()
endif()

find_package(simdjson CONFIG)
if(simdjson_FOUND AND TARGET simdjson::simdjson)
  target_link_libraries(velox PUBLIC simdjson::simdjson)
else()
  import_library(external::simdjson
                 ${VELOX_BUILD_PATH}/_deps/simdjson-build/libsimdjson.a)
  target_link_libraries(velox PUBLIC external::simdjson)
endif()

set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES_BCK})

# Adopted from Velox's CMakeLists.txt.
# https://github.com/facebookincubator/velox/pull/11410
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  execute_process(
    COMMAND brew --prefix icu4c
    RESULT_VARIABLE BREW_ICU4C
    OUTPUT_VARIABLE BREW_ICU4C_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_ICU4C EQUAL 0 AND EXISTS "${BREW_ICU4C_PREFIX}")
    message(STATUS "Found icu4c installed by Homebrew at ${BREW_ICU4C_PREFIX}")
    list(APPEND CMAKE_PREFIX_PATH "${BREW_ICU4C_PREFIX}")
  else()
    list(APPEND CMAKE_PREFIX_PATH "/usr/local/opt/icu4c")
  endif()
endif()

find_package(
  ICU
  COMPONENTS i18n uc data
  REQUIRED)
target_link_libraries(velox PUBLIC ICU::i18n ICU::uc ICU::data)

if(BUILD_TESTS)
  add_subdirectory(compute/delta/tests)
  add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

if(ENABLE_HDFS)
  add_definitions(-DENABLE_HDFS)
endif()

if(ENABLE_S3)
  add_definitions(-DENABLE_S3)
  find_awssdk()
  target_link_libraries(velox PUBLIC ${AWSSDK_LIBRARIES})
endif()

if(ENABLE_GCS)
  add_definitions(-DENABLE_GCS)
  find_gcssdk()
  target_link_libraries(velox PUBLIC google-cloud-cpp::storage)
endif()

if(ENABLE_ABFS)
  add_definitions(-DENABLE_ABFS)
  find_azure()
  target_link_libraries(velox PUBLIC Azure::azure-storage-blobs)
  target_link_libraries(velox PUBLIC Azure::azure-storage-files-datalake)
  target_link_libraries(velox PUBLIC Azure::azure-identity)
endif()

if(BUILD_EXAMPLES)
  add_subdirectory(udf/examples)
endif()

if(ENABLE_GPU)
  set(cudf_DIR "${VELOX_BUILD_PATH}/_deps/cudf-build")
  find_package(cudf CONFIG REQUIRED)

  import_library(
    facebook::velox::velox_cudf_expression
    ${VELOX_BUILD_PATH}/velox/experimental/cudf/expression/libvelox_cudf_expression.a
  )
  import_library(
    facebook::velox::velox_cudf_exec
    ${VELOX_BUILD_PATH}/velox/experimental/cudf/exec/libvelox_cudf_exec.a)
  import_library(
    facebook::velox::velox_cudf_vector
    ${VELOX_BUILD_PATH}/velox/experimental/cudf/vector/libvelox_cudf_vector.a)
  import_library(
    facebook::velox::velox_cudf_hive_connector
    ${VELOX_BUILD_PATH}/velox/experimental/cudf/connectors/hive/libvelox_cudf_hive_connector.a
  )
  import_library(
    facebook::velox::velox_cudf_iceberg_connector
    ${VELOX_BUILD_PATH}/velox/experimental/cudf/connectors/hive/iceberg/libvelox_cudf_iceberg_connector.a
  )

  target_compile_definitions(
    velox PRIVATE LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE)

  add_library(velox_curl SHARED IMPORTED GLOBAL)
  set_target_properties(
    velox_curl PROPERTIES IMPORTED_LOCATION
                          ${VELOX_BUILD_PATH}/_deps/curl-build/lib/libcurl.so)

  target_link_libraries(
    velox
    PUBLIC facebook::velox::velox_cudf_exec
           facebook::velox::velox_cudf_vector
           facebook::velox::velox_cudf_iceberg_connector
           facebook::velox::velox_cudf_hive_connector
           facebook::velox::velox_cudf_expression
           cudf::cudf
           velox_curl)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_custom_command(
    TARGET velox
    POST_BUILD
    COMMAND ld $<TARGET_FILE:velox> || true
    COMMENT "Checking ld result of libvelox.so")
  add_custom_command(
    TARGET velox
    POST_BUILD
    COMMAND ldd $<TARGET_FILE:velox> || true
    COMMENT "Checking ldd result of libvelox.so")
endif()
