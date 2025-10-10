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

function(add_bolt_test TEST_EXEC)
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
  add_executable(${TEST_EXEC} ${SOURCES} ${BOLT_TEST_COMMON_SRCS})
  target_include_directories(${TEST_EXEC} PRIVATE ${CMAKE_SOURCE_DIR}/bolt
                                                  ${CMAKE_SOURCE_DIR}/src
                                                  ${CMAKE_SOURCE_DIR}/core
                                                  ${bolt_INCLUDE_DIRS}
                                                  ${bolt_INCLUDE_DIRS}/bolt/external/duckdb)
  target_link_libraries(${TEST_EXEC} ${PROJECT_NAME}_static gluten bolt::bolt GTest::gtest GTest::gtest_main glog::glog benchmark::benchmark gfx::timsort)

  gtest_discover_tests(${TEST_EXEC} DISCOVERY_MODE PRE_TEST)
endfunction()

# codegen linker flags, -export-dynamic for rtti
add_link_options("-Wl,-export-dynamic")

set(BOLT_TEST_COMMON_SRCS JsonToProtoConverter.cc FilePathGenerator.cc)

# TODO: ORC is not well supported. add_bolt_test(orc_test SOURCES OrcTest.cc)
add_bolt_test(
  bolt_operators_test
  SOURCES
  BoltColumnarToRowTest.cc
  BoltRowToColumnarTest.cc
  BoltColumnarBatchTest.cc
  BoltBatchResizerTest.cc)
add_bolt_test(
  bolt_plan_conversion_test
  SOURCES
  Substrait2BoltPlanConversionTest.cc
  Substrait2BoltPlanValidatorTest.cc
  Substrait2BoltValuesNodeConversionTest.cc
  SubstraitExtensionCollectorTest.cc
  BoltSubstraitRoundTripTest.cc
  BoltSubstraitSignatureTest.cc
  BoltToSubstraitTypeTest.cc)
add_bolt_test(spark_functions_test SOURCES SparkFunctionTest.cc
               FunctionTest.cc)
add_bolt_test(runtime_test SOURCES RuntimeTest.cc)
add_bolt_test(bolt_memory_test SOURCES MemoryManagerTest.cc)
add_bolt_test(buffer_outputstream_test SOURCES BufferOutputStreamTest.cc)

if(BUILD_EXAMPLES)
  add_bolt_test(my_udf_test SOURCES MyUdfTest.cc)
endif()

if(ENABLE_ENHANCED_FEATURES)
  add_bolt_test(bolt_iceberg_test SOURCES iceberg/IcebergWriteTest.cc)
endif()

# copy test data directory
set(DATA_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data)
set(DATA_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/data)
add_custom_target(copy_data_directory ALL
    COMMENT "copy test data: ${DATA_SOURCE_DIR} -> ${DATA_TARGET_DIR}"
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${DATA_SOURCE_DIR} ${DATA_TARGET_DIR}
    VERBATIM
)