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

if(CCACHE_FOUND)
  set_property(DIRECTORY PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(DIRECTORY PROPERTY RULE_LAUNCH_LINK ccache)
endif()

find_package(Roaring MODULE REQUIRED)

set(VELOX_PROTO_SRC_DIR
    ${GLUTEN_HOME}/backends-velox/src/main/resources/org/apache/gluten/proto)
message(STATUS "Set Gluten Proto Directory in ${VELOX_PROTO_SRC_DIR}")

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
    jni/JniHashTable.cc
    memory/BufferOutputStream.cc
    memory/VeloxColumnarBatch.cc
    memory/VeloxMemoryManager.cc
    operators/functions/RegistrationAllFunctions.cc
    operators/functions/delta/DeltaBitmapAggregator.cc
    operators/functions/overlay/GetMapValue.cc
    operators/functions/overlay/RegisterFunctionOverlay.cc
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

add_library(gluten_velox_objects OBJECT ${VELOX_SRCS})
set_target_properties(gluten_velox_objects PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_library(gluten_velox_backend SHARED jni/VeloxJniWrapper.cc)
add_library(gluten::velox_backend ALIAS gluten_velox_backend)
target_link_libraries(gluten_velox_backend PUBLIC gluten_velox_objects gluten)

target_compile_definitions(gluten_velox_objects PRIVATE VELOX_ENABLE_PARQUET)

if(ENABLE_GLUTEN_VCPKG AND NOT CMAKE_SYSTEM_NAME MATCHES "Darwin")
  # Hide some symbols to avoid conflict.
  target_link_options(
    gluten_velox_backend PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  # Make libvelox.dylib own the Velox mono archive. Test utilities can then
  # resolve Velox symbols from the dylib instead of linking a second copy.
  target_link_options(gluten_velox_backend PRIVATE
                      "LINKER:-force_load,$<TARGET_FILE:facebook::velox>")
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

add_dependencies(gluten_velox_objects jni_proto velox_jni_proto)

target_include_directories(
  gluten_velox_objects
  PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH}
         ${JNI_INCLUDE_DIRS}
         ${VELOX_BUILD_PATH}
         ${CMAKE_CURRENT_SOURCE_DIR}
         ${VELOX_HOME}
         ${PROTO_OUTPUT_DIR}
         ${PROTOBUF_INCLUDE})
if(GLUTEN_PREFIX_INCLUDE_DIRS)
  target_include_directories(gluten_velox_objects BEFORE
                             PUBLIC ${GLUTEN_PREFIX_INCLUDE_DIRS})
endif()

set_target_properties(gluten_velox_backend PROPERTIES OUTPUT_NAME velox
                                                     LIBRARY_OUTPUT_DIRECTORY
                                                     ${root_directory}/releases)

if(ENABLE_JEMALLOC_STATS)
  include(Findjemalloc)
  find_jemalloc()
  if(JEMALLOC_NOT_FOUND)
    include(Buildjemalloc)
    build_jemalloc()
  endif()
  add_definitions(-DENABLE_JEMALLOC_STATS)
  target_link_libraries(gluten_velox_objects PUBLIC jemalloc::jemalloc)
endif()

target_link_libraries(gluten_velox_objects PUBLIC gluten_core_objects facebook::velox roaring)

# Find and link librdkafka for Kafka streaming support
if(ENABLE_KAFKA)
  target_compile_definitions(gluten_velox_objects PUBLIC ENABLE_KAFKA)
  find_package(RdKafka CONFIG QUIET)
  if(RdKafka_FOUND AND TARGET RdKafka::rdkafka++)
    message(STATUS "Found RdKafka: ${RdKafka_DIR}")
    target_link_libraries(gluten_velox_objects PUBLIC RdKafka::rdkafka++)
  else()
    # Link the C++ client and C mock API.
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
      pkg_check_modules(RDKAFKA QUIET IMPORTED_TARGET rdkafka++ rdkafka)
      if(RDKAFKA_FOUND)
        message(STATUS "Found librdkafka via pkg-config: ${RDKAFKA_LIBRARIES}")
        target_link_libraries(gluten_velox_objects PUBLIC PkgConfig::RDKAFKA)
      else()
        message(
          FATAL_ERROR
            "Kafka is enabled, but librdkafka (C/C++) was not found.\n"
            "Install it: ./dev/builddeps-veloxbe.sh install_librdkafka\n"
            "For an existing install, set RdKafka_DIR, CMAKE_PREFIX_PATH or PKG_CONFIG_PATH.\n"
            "Disable Kafka with --enable_kafka=OFF (script) or -DENABLE_KAFKA=OFF (CMake)."
        )
      endif()
    else()
      message(
        FATAL_ERROR
          "librdkafka was not found through CMake, and pkg-config is unavailable.\n"
          "Install it: ./dev/builddeps-veloxbe.sh install_librdkafka\n"
          "For an existing install, set RdKafka_DIR or CMAKE_PREFIX_PATH.\n"
          "Disable Kafka with --enable_kafka=OFF (script) or -DENABLE_KAFKA=OFF (CMake)."
      )
    endif()
  endif()
endif()

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
endif()

if(ENABLE_GCS)
  add_definitions(-DENABLE_GCS)
endif()

if(ENABLE_ABFS)
  add_definitions(-DENABLE_ABFS)
endif()

if(BUILD_EXAMPLES)
  add_subdirectory(udf/examples)
endif()

if(ENABLE_GPU)
  target_compile_definitions(
    gluten_velox_objects PRIVATE LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE)
  target_link_libraries(
    gluten_velox_objects
    PUBLIC facebook::velox::velox_cudf_exec
           facebook::velox::velox_cudf_vector
           facebook::velox::velox_cudf_iceberg_connector
           facebook::velox::velox_cudf_hive_connector
           facebook::velox::velox_cudf_expression
           facebook::velox::velox_cudf_expression_registry
           cudf::cudf
           velox_curl)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_custom_command(
    TARGET gluten_velox_backend
    POST_BUILD
    COMMAND ld $<TARGET_FILE:gluten_velox_backend> || true
    COMMENT "Checking ld result of libvelox.so")
  add_custom_command(
    TARGET gluten_velox_backend
    POST_BUILD
    COMMAND ldd $<TARGET_FILE:gluten_velox_backend> || true
    COMMENT "Checking ldd result of libvelox.so")
endif()
