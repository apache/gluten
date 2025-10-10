/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <benchmark/benchmark.h>
#include <execinfo.h>
#include <sched.h>

#include <chrono>

#include "benchmarks/common/BenchmarkUtils.h"
#include "bolt/shuffle/sparksql/simd.h"

using namespace bytedance::bolt::shuffle::sparksql;

namespace gluten {

class BenchmarkExtractNullBits {
 public:
  BenchmarkExtractNullBits(bool useSimd) : useSimd_(useSimd) {
    uint16_t size = 16 * 1024;
    for (int i = 0; i < size; i++) {
      offset_.push_back(rand() % size);
      if (i % 8 == 0) {
        nullBuffer_.push_back(rand() % UINT8_MAX);
      }
    }
  }

  void operator()(benchmark::State& state) {
    if (useSimd_) {
      for (auto _ : state) {
        for (int i = 0; i < offset_.size() - 8; i += 8) {
          uint8_t result = extractBitsToByteSimd(nullBuffer_.data(), &offset_[i]);
          benchmark::DoNotOptimize(result);
        }
      }
    } else {
      for (auto _ : state) {
        for (int i = 0; i < offset_.size() - 8; i += 8) {
          uint8_t result = extractBitsToByte(nullBuffer_.data(), &offset_[i]);
          benchmark::DoNotOptimize(result);
        }
      }
    }
  }

 protected:
  bool useSimd_;
  std::vector<uint32_t> offset_;
  std::vector<uint8_t> nullBuffer_;
};

} // namespace gluten

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  gluten::BenchmarkExtractNullBits extractNullBitBenchmark(false);
  gluten::BenchmarkExtractNullBits extractNullBitBenchmarkSimd(true);

  auto bm = benchmark::RegisterBenchmark("ExtractNullBitBenchmark", extractNullBitBenchmark);
  auto bmSimd = benchmark::RegisterBenchmark("ExtractNullBitBenchmark::Simd", extractNullBitBenchmarkSimd);

  if (FLAGS_iterations > 0) {
    bm->Iterations(FLAGS_iterations);
    bmSimd->Iterations(FLAGS_iterations);
  }

  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
