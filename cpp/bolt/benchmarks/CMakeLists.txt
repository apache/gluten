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

find_package(Arrow CONFIG REQUIRED)
find_package(gfx-timsort REQUIRED CONFIG)

set(BOLT_BENCHMARK_COMMON_SRCS common/BenchmarkUtils.cc)
add_library(bolt_benchmark_common STATIC ${BOLT_BENCHMARK_COMMON_SRCS})
target_include_directories(
  bolt_benchmark_common PUBLIC ${CMAKE_SOURCE_DIR}/bolt
                                ${CMAKE_SOURCE_DIR}/core)
target_link_libraries(bolt_benchmark_common PUBLIC gluten ${PROJECT_NAME}_static arrow::arrow bolt::bolt benchmark::benchmark glog::glog gfx::timsort)

function(add_bolt_benchmark BM_EXEC BM_FILE)
  add_executable(${BM_EXEC} ${BM_FILE})
  target_link_libraries(${BM_EXEC} PRIVATE bolt_benchmark_common)
  install(TARGETS ${BM_EXEC}
        DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../../build/benchmark)
endfunction()

# Generic benchmark
add_bolt_benchmark(generic_benchmark GenericBenchmark.cc)

add_bolt_benchmark(parquet_write_benchmark ParquetWriteBenchmark.cc)

add_bolt_benchmark(plan_validator_util PlanValidatorUtil.cc)

add_bolt_benchmark(extract_null_bits_benchmark ExtractNullBitsBenchmark.cc)

