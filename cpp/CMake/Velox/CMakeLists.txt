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

option(VELOX_BUILD_TESTING "Build native Velox tests" OFF)
option(VELOX_ENABLE_BENCHMARKS "Build native Velox benchmarks" OFF)

set(CMAKE_FIND_PACKAGE_PREFER_CONFIG OFF)
set(VELOX_MONO_LIBRARY ON)
set(VELOX_BUILD_SHARED OFF)
set(VELOX_BUILD_RUNNER OFF)
set(VELOX_ENABLE_PARQUET ON)
set(VELOX_SIMDJSON_SKIPUTF8VALIDATION ON)
set(VELOX_ENABLE_S3 "${ENABLE_S3}")
set(VELOX_ENABLE_GCS "${ENABLE_GCS}")
set(VELOX_ENABLE_HDFS "${ENABLE_HDFS}")
set(VELOX_ENABLE_ABFS "${ENABLE_ABFS}")
set(VELOX_ENABLE_CUDF "${ENABLE_GPU}")
set(VELOX_ENABLE_LTO "${ENABLE_LTO}")
if(BUILD_TESTS OR BUILD_BENCHMARKS OR BUILD_TEST_UTILS
   OR VELOX_BUILD_TESTING)
  set(VELOX_BUILD_TEST_UTILS ON)
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=stringop-overflow -Wno-error=cpp")
add_subdirectory("${VELOX_HOME}" "${VELOX_BUILD_PATH}")
add_library(facebook::velox ALIAS velox)
if(TARGET CURL::libcurl)
  get_target_property(_curl_target CURL::libcurl ALIASED_TARGET)
  if(NOT _curl_target)
    set(_curl_target CURL::libcurl)
  endif()
  add_library(velox_curl ALIAS ${_curl_target})
endif()
if(TARGET velox_exec_test_lib)
  add_library(facebook::velox::exec_test_lib ALIAS velox_exec_test_lib)
endif()
if(ENABLE_GPU)
  add_library(facebook::velox::velox_cudf_exec ALIAS velox_cudf_exec)
  add_library(facebook::velox::velox_cudf_vector ALIAS velox_cudf_vector)
  add_library(facebook::velox::velox_cudf_iceberg_connector ALIAS velox_cudf_iceberg_connector)
  add_library(facebook::velox::velox_cudf_hive_connector ALIAS velox_cudf_hive_connector)
  add_library(facebook::velox::velox_cudf_expression ALIAS velox_cudf_expression)
  add_library(facebook::velox::velox_cudf_expression_registry ALIAS velox_cudf_expression_registry)
endif()

# GCC 13.3.0 reports free-nonheap-object when optimizing this Velox test.
# Keep the warning non-fatal in Release builds.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
   AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "13"
   AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "14"
   AND TARGET velox_dwio_parquet_delta_bp_decoder_test)
  target_compile_options(
    velox_dwio_parquet_delta_bp_decoder_test PRIVATE
    "$<$<CONFIG:Release>:-Wno-error=free-nonheap-object>")
endif()
