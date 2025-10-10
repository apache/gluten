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

#include "benchmarks/common/BenchmarkUtils.h"
#include "compute/Runtime.h"
#include "compute/BoltBackend.h"
#include "memory/BoltMemoryManager.h"
#include "operators/reader/ParquetReaderIterator.h"
#include "operators/writer/BoltParquetDataSource.h"
#include "utils/BoltArrowUtils.h"

namespace gluten {

const int kBatchBufferSize = 32768;

class GoogleBenchmarkBoltParquetWriteCacheScanBenchmark {
 public:
  GoogleBenchmarkBoltParquetWriteCacheScanBenchmark(const std::string& fileName, const std::string& outputPath)
      : fileName_(fileName), outputPath_(outputPath) {}

  void operator()(benchmark::State& state) {
    if (state.range(0) == 0xffffffff) {
      setCpu(state.thread_index());
    } else {
      setCpu(state.range(0));
    }

    std::shared_ptr<arrow::RecordBatch> recordBatch;
    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t numColumns = 0;
    int64_t initTime = 0;
    int64_t writeTime = 0;

    // reuse the ParquetWriteConverter for batches caused system % increase a lot

    auto memoryManager = getDefaultMemoryManager();
    auto runtime = Runtime::create(kBoltBackendKind, memoryManager, 1);
    auto boltPool = memoryManager->getAggregateMemoryPool();

    for (auto _ : state) {
      const auto output = "bolt_parquet_write.parquet";

      // Init BoltParquetDataSource
      auto reader = [&] {
        ScopedTimer timer(&elapseRead);
        return std::make_unique<ParquetBufferedReaderIterator>(fileName_, kBatchBufferSize, boltPool);
      }();

      const auto localSchema = toArrowSchema(reader->getRowType(), boltPool.get());

      auto boltParquetDataSource = std::make_unique<gluten::BoltParquetDataSource>(
          outputPath_ + "/" + output,
          boltPool->addAggregateChild("writer_benchmark"),
          boltPool->addLeafChild("sink_pool"),
          localSchema);

      boltParquetDataSource->init(runtime->getConfMap());

      while (auto batch = reader->next()) {
        ScopedTimer timer(&elapseRead);
        boltParquetDataSource->write(batch);
      }

      boltParquetDataSource->close();
    }

    state.counters["columns"] =
        benchmark::Counter(numColumns, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batches"] =
        benchmark::Counter(numBatches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_rows"] =
        benchmark::Counter(numRows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] =
        benchmark::Counter(kBatchBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapseRead, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["init_time"] =
        benchmark::Counter(initTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] =
        benchmark::Counter(writeTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    Runtime::release(runtime);
  }

 private:
  std::string fileName_;
  std::string outputPath_;
};

} // namespace gluten

// GoogleBenchmarkBoltParquetWriteCacheScanBenchmark usage
// ./parquet_write_benchmark --threads=1 --file /mnt/DP_disk1/int.parquet --output file:/tmp/parquet-write
// GoogleBenchmarkArrowParquetWriteCacheScanBenchmark usage
// ./parquet_write_benchmark --threads=1 --file /mnt/DP_disk1/int.parquet --output /tmp/parquet-write
int main(int argc, char** argv) {
  gluten::initBoltBackend();
  uint32_t iterations = 1;
  uint32_t threads = 1;
  std::string datafile;
  uint32_t cpu = 0xffffffff;
  std::string output;

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--iterations") == 0) {
      iterations = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--threads") == 0) {
      threads = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--file") == 0) {
      datafile = argv[i + 1];
    } else if (strcmp(argv[i], "--cpu") == 0) {
      cpu = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--output") == 0) {
      output = (argv[i + 1]);
    }
  }
  LOG(INFO) << "iterations = " << iterations;
  LOG(INFO) << "threads = " << threads;
  LOG(INFO) << "datafile = " << datafile;
  LOG(INFO) << "cpu = " << cpu;
  LOG(INFO) << "output = " << output;

  gluten::GoogleBenchmarkBoltParquetWriteCacheScanBenchmark bck(datafile, output);

  benchmark::RegisterBenchmark("GoogleBenchmarkParquetWrite::CacheScan", bck)
      ->Args({
          cpu,
      })
      ->Iterations(iterations)
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
