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

function(add_velox_test TEST_EXEC)
  set(options)
  set(one_value_args)
  set(multi_value_args SOURCES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}"
                        "${multi_value_args}" ${ARGN})

  if(ARG_SOURCES)
    set(SOURCES ${ARG_SOURCES})
  else()
    message(FATAL_ERROR "No sources specified for test ${TEST_NAME}")
  endif()
  add_executable(${TEST_EXEC} ${SOURCES} ${VELOX_TEST_COMMON_SRCS})
  target_include_directories(${TEST_EXEC} PRIVATE ${CMAKE_SOURCE_DIR}/velox
                                                  ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${TEST_EXEC} velox GTest::gtest GTest::gtest_main)
  if(ENABLE_GPU)
    target_link_libraries(${TEST_EXEC} velox_curl)
  endif()
  gtest_discover_tests(${TEST_EXEC} DISCOVERY_MODE PRE_TEST)
endfunction()

if(ENABLE_GPU)
  function(import_library TARGET_NAME LIB_PATH)
    if(NOT EXISTS ${LIB_PATH})
      message(FATAL_ERROR "Library does not exist: ${LIB_PATH}")
    endif()
    add_library(${TARGET_NAME} STATIC IMPORTED)
    set_target_properties(${TARGET_NAME} PROPERTIES IMPORTED_LOCATION
                                                    ${LIB_PATH})
  endfunction()

  function(add_velox_gpu_test TEST_EXEC)
    set(options)
    set(one_value_args)
    set(multi_value_args SOURCES)
    cmake_parse_arguments(ARG "${options}" "${one_value_args}"
                          "${multi_value_args}" ${ARGN})

    if(ARG_SOURCES)
      set(SOURCES ${ARG_SOURCES})
    else()
      message(FATAL_ERROR "No sources specified for test ${TEST_NAME}")
    endif()
    add_executable(${TEST_EXEC} ${SOURCES} ${VELOX_TEST_COMMON_SRCS})
    target_include_directories(${TEST_EXEC} PRIVATE ${CMAKE_SOURCE_DIR}/velox
                                                    ${CMAKE_SOURCE_DIR}/src)
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
    target_compile_definitions(
      ${TEST_EXEC} PUBLIC LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE)
    target_link_libraries(
      ${TEST_EXEC}
      PUBLIC facebook::velox::velox_cudf_expression
             facebook::velox::velox_cudf_exec
             facebook::velox::velox_cudf_vector
             facebook::velox::velox_cudf_hive_connector)
    target_link_libraries(${TEST_EXEC} PRIVATE velox cudf::cudf velox_curl
                                               GTest::gtest GTest::gtest_main)
    gtest_discover_tests(
      ${TEST_EXEC} DISCOVERY_MODE PRE_TEST
      PROPERTIES LABELS "cuda_driver"
      TIMEOUT 3000)
  endfunction()

  add_velox_gpu_test(velox_gpu_shuffle_writer_test SOURCES
                     VeloxGpuShuffleWriterTest.cc)
endif()

set(VELOX_TEST_COMMON_SRCS JsonToProtoConverter.cc FilePathGenerator.cc)

add_velox_test(velox_shuffle_writer_test SOURCES VeloxShuffleWriterTest.cc)

add_velox_test(velox_shuffle_writer_spill_test SOURCES
               VeloxShuffleWriterSpillTest.cc)

add_velox_test(velox_rss_sort_shuffle_writer_test SOURCES
               VeloxRssSortShuffleWriterTest.cc)

add_velox_test(velox_sort_shuffle_writer_test SOURCES
               VeloxSortShuffleWriterTest.cc)

# TODO: ORC is not well supported. add_velox_test(orc_test SOURCES OrcTest.cc)
add_velox_test(
  velox_operators_test
  SOURCES
  VeloxColumnarToRowTest.cc
  VeloxRowToColumnarTest.cc
  VeloxColumnarBatchSerializerTest.cc
  VeloxColumnarBatchTest.cc
  VeloxBatchResizerTest.cc)
add_velox_test(
  velox_plan_conversion_test
  SOURCES
  Substrait2VeloxPlanConversionTest.cc
  Substrait2VeloxPlanValidatorTest.cc
  Substrait2VeloxValuesNodeConversionTest.cc
  SubstraitExtensionCollectorTest.cc
  VeloxSubstraitRoundTripTest.cc
  VeloxSubstraitSignatureTest.cc
  VeloxToSubstraitTypeTest.cc)
add_velox_test(spark_functions_test SOURCES SparkFunctionTest.cc
               FunctionTest.cc)
add_velox_test(runtime_test SOURCES RuntimeTest.cc)
add_velox_test(velox_memory_test SOURCES MemoryManagerTest.cc)
add_velox_test(buffer_outputstream_test SOURCES BufferOutputStreamTest.cc)
if(BUILD_EXAMPLES)
  add_velox_test(my_udf_test SOURCES MyUdfTest.cc)
endif()

if(ENABLE_ENHANCED_FEATURES)
  add_velox_test(velox_iceberg_test SOURCES iceberg/IcebergWriteTest.cc)
endif()
