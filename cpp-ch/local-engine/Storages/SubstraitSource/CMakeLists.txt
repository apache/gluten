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

set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")

option(GLUTEN_ENABLE_NATIVE_PAIMON "Enable Paimon native read in libch" ON)

add_headers_and_sources(substrait_source .)
add_headers_and_sources(substrait_source Delta)
add_headers_and_sources(substrait_source Delta/Bitmap)
add_headers_and_sources(substrait_source Iceberg)
if(GLUTEN_ENABLE_NATIVE_PAIMON)
  add_headers_and_sources(substrait_source Paimon)
endif()

add_library(substrait_source ${substrait_source_sources})

if(GLUTEN_ENABLE_NATIVE_PAIMON)
  find_package(Paimon 0.3.0 CONFIG REQUIRED)
  if(NOT PAIMON_BUNDLED_DEPS_DIR)
    message(FATAL_ERROR "PAIMON_BUNDLED_DEPS_DIR is required for native Paimon")
  endif()
  target_compile_definitions(substrait_source
                             PUBLIC GLUTEN_ENABLE_NATIVE_PAIMON=1)
  target_link_libraries(substrait_source PUBLIC Paimon::paimon_shared)
  target_link_options(substrait_source PUBLIC "-Wl,-rpath,\$ORIGIN")
else()
  target_compile_definitions(substrait_source
                             PUBLIC GLUTEN_ENABLE_NATIVE_PAIMON=0)
endif()

target_compile_options(
  substrait_source PRIVATE -Wno-suggest-destructor-override
                           -Wno-inconsistent-missing-destructor-override)
if(ENABLE_HDFS)
  target_link_libraries(
    substrait_source
    PUBLIC boost::headers_only
           ch_contrib::protobuf
           clickhouse_common_io
           ch_contrib::hdfs
           substrait
           ch_contrib::roaring
           ch_contrib::rapidjson)
else()
  target_link_libraries(
    substrait_source
    PUBLIC boost::headers_only ch_contrib::protobuf clickhouse_common_io
           substrait ch_contrib::roaring ch_contrib::rapidjson)
endif()
target_include_directories(
  substrait_source SYSTEM BEFORE
  PUBLIC ${ARROW_INCLUDE_DIR} ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/cpp/src
         ${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src)
