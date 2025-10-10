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

#include <chrono>
#include <thread>

#include <arrow/c/bridge.h>
#include <arrow/util/range.h>

#include <benchmark/benchmark.h>

#include <gflags/gflags.h>

#include "benchmarks/common/BenchmarkUtils.h"
#include "compute/BoltBackend.h"
#include "compute/BoltRuntime.h"
#include "config/GlutenConfig.h"
#include "config/BoltConfig.h"
#include "operators/reader/FileReaderIterator.h"
#include "operators/writer/BoltColumnarBatchWriter.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/BoltShuffleWriter.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "tests/utils/LocalRssClient.h"
#include "tests/utils/TestAllocationListener.h"
#include "tests/utils/TestStreamReader.h"
#include "utils/Exception.h"
#include "utils/StringUtil.h"
#include "utils/Timer.h"
#include "utils/BoltArrowUtils.h"
#include "bolt/exec/PlanNodeStats.h"

using namespace gluten;

namespace {

DEFINE_bool(print_result, true, "Print result for execution");
DEFINE_string(save_output, "", "Path to parquet file for saving the task output iterator");
DEFINE_bool(with_shuffle, false, "Add shuffle split at end.");
DEFINE_bool(run_shuffle, false, "Only run shuffle write.");
DEFINE_bool(run_shuffle_read, false, "Whether to run shuffle read when run_shuffle is true.");
DEFINE_string(shuffle_writer, "hash", "Shuffle writer type. Can be hash or sort");
DEFINE_string(
    partitioning,
    "rr",
    "Short partitioning name. Valid options are rr, hash, range, single, random (only for test purpose)");
DEFINE_bool(rss, false, "Mocking rss.");
DEFINE_string(compression, "lz4", "Specify the compression codec. Valid options are none, lz4, zstd");
DEFINE_int32(shuffle_partitions, 200, "Number of shuffle split (reducer) partitions");
DEFINE_bool(shuffle_dictionary, false, "Whether to enable dictionary encoding for shuffle write.");

DEFINE_string(plan, "", "Path to input json file of the substrait plan.");
DEFINE_string(
    split,
    "",
    "Path to input json file of the splits. Only valid for simulating the first stage. Use comma-separated list for multiple splits.");
DEFINE_string(data, "", "Path to input data files in parquet format. Use comma-separated list for multiple files.");
DEFINE_string(conf, "", "Path to the configuration file.");
DEFINE_string(write_path, "/tmp", "Path to save the output from write tasks.");
DEFINE_int64(memory_limit, std::numeric_limits<int64_t>::max(), "Memory limit used to trigger spill.");
DEFINE_string(
    scan_mode,
    "stream",
    "Scan mode for reading parquet data."
    "'stream' mode: Input file scan happens inside of the pipeline."
    "'buffered' mode: First read all data into memory and feed the pipeline with it.");
DEFINE_bool(debug_mode, false, "Whether to enable debug mode. Same as setting `spark.gluten.sql.debug`");
DEFINE_bool(query_trace_enabled, false, "Whether to enable query trace.");
DEFINE_string(query_trace_dir, "", "Base dir of a query to store tracing data.");
DEFINE_string(
    query_trace_node_ids,
    "",
    "A comma-separated list of plan node ids whose input data will be traced. Empty string if only want to trace the query metadata.");
DEFINE_int64(query_trace_max_bytes, 0, "The max trace bytes limit. Tracing is disabled if zero.");
DEFINE_string(
    query_trace_task_reg_exp,
    "",
    "The regexp of traced task id. We only enable trace on a task if its id matches.");

struct WriterMetrics {
  int64_t splitTime{0};
  int64_t evictTime{0};
  int64_t writeTime{0};
  int64_t compressTime{0};

  int64_t dataSize{0};
  int64_t bytesSpilled{0};
  int64_t bytesWritten{0};
};

struct ReaderMetrics {
  int64_t decompressTime{0};
  int64_t deserializeTime{0};
};

void setUpBenchmark(::benchmark::internal::Benchmark* bm) {
  if (FLAGS_threads > 0) {
    bm->Threads(FLAGS_threads);
  } else {
    bm->ThreadRange(1, std::thread::hardware_concurrency());
  }
  if (FLAGS_iterations > 0) {
    bm->Iterations(FLAGS_iterations);
  }
}

std::string generateUniqueSubdir(const std::string& parent, const std::string& prefix = "") {
  auto path = std::filesystem::path(parent) / (prefix + generateUuid());
  std::error_code ec{};
  while (!std::filesystem::create_directories(path, ec)) {
    if (ec) {
      LOG(ERROR) << fmt::format("Failed to created spill directory: {}, error code: {}", path, ec.message());
      std::exit(EXIT_FAILURE);
    }
    path = std::filesystem::path(parent) / (prefix + generateUuid());
  }
  return path;
}

std::vector<std::string> createLocalDirs() {
  static const std::string kBenchmarkDirPrefix = "generic-benchmark-";
  std::vector<std::string> localDirs;

  auto joinedDirsC = std::getenv(gluten::kGlutenSparkLocalDirs.c_str());
  // Check if local dirs are set from env.
  if (joinedDirsC != nullptr && strcmp(joinedDirsC, "") > 0) {
    auto joinedDirs = std::string(joinedDirsC);
    auto dirs = gluten::splitPaths(joinedDirs);
    for (const auto& dir : dirs) {
      localDirs.push_back(generateUniqueSubdir(dir, kBenchmarkDirPrefix));
    }
  } else {
    // Otherwise create 1 temp dir.
    localDirs.push_back(generateUniqueSubdir(std::filesystem::temp_directory_path(), kBenchmarkDirPrefix));
  }
  return localDirs;
}

void cleanupLocalDirs(const std::vector<std::string>& localDirs) {
  for (const auto& localDir : localDirs) {
    std::error_code ec;
    std::filesystem::remove_all(localDir, ec);
    if (ec) {
      LOG(WARNING) << fmt::format("Failed to remove directory: {}, error message: {}", localDir, ec.message());
    } else {
      LOG(INFO) << "Removed local dir: " << localDir;
    }
  }
}

void setCompressionTypeFromFlag(arrow::Compression::type& compressionType, CodecBackend& codecBackend) {
  codecBackend = CodecBackend::NONE;
  if (FLAGS_compression == "none") {
    compressionType = arrow::Compression::UNCOMPRESSED;
  } else if (FLAGS_compression == "lz4") {
    compressionType = arrow::Compression::LZ4_FRAME;
  } else if (FLAGS_compression == "zstd") {
    compressionType = arrow::Compression::ZSTD;
  } else {
    throw GlutenException("Unrecognized compression type: " + FLAGS_compression);
  }
}

std::unique_ptr<arrow::util::Codec> createCodec() {
  // Configure compression.
  if (FLAGS_compression == "none") {
    return nullptr;
  }

  arrow::Compression::type compressionType;
  CodecBackend codecBackend;

  setCompressionTypeFromFlag(compressionType, codecBackend);

  return createCompressionCodec(compressionType, codecBackend);
}

std::shared_ptr<PartitionWriter>
createPartitionWriter(Runtime* runtime, const std::string& dataFile, const std::vector<std::string>& localDirs) {
  std::unique_ptr<PartitionWriter> partitionWriter;
  if (FLAGS_rss) {
    auto options = std::make_shared<RssPartitionWriterOptions>();
    auto rssClient = std::make_unique<LocalRssClient>(dataFile);
    return std::make_shared<RssPartitionWriter>(
        FLAGS_shuffle_partitions, createCodec(), runtime->memoryManager(), options, std::move(rssClient));
  }

  auto options = std::make_shared<LocalPartitionWriterOptions>();
  options->enableDictionary = FLAGS_shuffle_dictionary;
  return std::make_unique<LocalPartitionWriter>(
      FLAGS_shuffle_partitions, createCodec(), runtime->memoryManager(), options, dataFile, localDirs);
}

std::shared_ptr<ShuffleWriterOptions> createShuffleWriterOptions() {
  std::shared_ptr<ShuffleWriterOptions> options;

  switch (ShuffleWriter::stringToType(FLAGS_shuffle_writer)) {
    case ShuffleWriterType::kHashShuffle:
      options = std::make_shared<HashShuffleWriterOptions>();
      break;
    case ShuffleWriterType::kSortShuffle:
      options = std::make_shared<SortShuffleWriterOptions>();
      break;
    case ShuffleWriterType::kRssSortShuffle:
      options = std::make_shared<RssSortShuffleWriterOptions>();
      break;
    default:
      throw GlutenException("Unsupported shuffle writer type: " + FLAGS_shuffle_writer);
  }

  options->partitioning = toPartitioning(FLAGS_partitioning);
  return options;
}

std::shared_ptr<BoltShuffleWriter> createShuffleWriter(
    Runtime* runtime,
    std::shared_ptr<PartitionWriter> partitionWriter) {
  auto shuffleWriterOptions = createShuffleWriterOptions();
  auto shuffleWriter =
      runtime->createShuffleWriter(FLAGS_shuffle_partitions, std::move(partitionWriter), shuffleWriterOptions);
  return std::dynamic_pointer_cast<BoltShuffleWriter>(shuffleWriter);
}

std::shared_ptr<ShuffleReader> createShuffleReader(Runtime* runtime, const std::shared_ptr<arrow::Schema>& schema) {
  auto readerOptions = ShuffleReaderOptions{};
  readerOptions.shuffleWriterType = ShuffleWriter::stringToType(FLAGS_shuffle_writer),
  setCompressionTypeFromFlag(readerOptions.compressionType, readerOptions.codecBackend);
  return runtime->createShuffleReader(schema, readerOptions);
}

void populateWriterMetrics(
    const std::shared_ptr<BoltShuffleWriter>& shuffleWriter,
    int64_t totalTime,
    WriterMetrics& metrics) {
  metrics.compressTime += shuffleWriter->totalCompressTime();
  metrics.evictTime += shuffleWriter->totalEvictTime();
  metrics.writeTime += shuffleWriter->totalWriteTime();
  auto splitTime = totalTime - metrics.compressTime - metrics.evictTime - metrics.writeTime;
  if (splitTime > 0) {
    metrics.splitTime += splitTime;
  }
  metrics.dataSize +=
      std::accumulate(shuffleWriter->rawPartitionLengths().begin(), shuffleWriter->rawPartitionLengths().end(), 0LL);
  metrics.bytesWritten += shuffleWriter->totalBytesWritten();
  metrics.bytesSpilled += shuffleWriter->totalBytesEvicted();
}

void setCpu(::benchmark::State& state) {
  // Pin each threads to different CPU# starting from 0 or --cpu.
  auto cpu = state.thread_index();
  if (FLAGS_cpu != -1) {
    cpu += FLAGS_cpu;
  }
  LOG(WARNING) << "Setting CPU for thread " << state.thread_index() << " to " << cpu;
  gluten::setCpu(cpu);
}

void runShuffle(
    Runtime* runtime,
    TestAllocationListener* listener,
    const std::shared_ptr<gluten::ResultIterator>& resultIter,
    WriterMetrics& writerMetrics,
    ReaderMetrics& readerMetrics,
    bool readAfterWrite,
    const std::vector<std::string>& localDirs,
    const std::string& dataFileDir) {
  GLUTEN_ASSIGN_OR_THROW(auto dataFile, gluten::createTempShuffleFile(dataFileDir));

  auto partitionWriter = createPartitionWriter(runtime, dataFile, localDirs);
  auto shuffleWriter = createShuffleWriter(runtime, std::move(partitionWriter));
  listener->setShuffleWriter(shuffleWriter.get());

  int64_t totalTime = 0;
  std::shared_ptr<ArrowSchema> cSchema;
  {
    gluten::ScopedTimer timer(&totalTime);
    while (resultIter->hasNext()) {
      auto cb = resultIter->next();
      if (!cSchema) {
        cSchema = cb->exportArrowSchema();
      }
      GLUTEN_THROW_NOT_OK(shuffleWriter->write(cb, ShuffleWriter::kMaxMemLimit - shuffleWriter->cachedPayloadSize()));
    }
    GLUTEN_THROW_NOT_OK(shuffleWriter->stop());
  }

  populateWriterMetrics(shuffleWriter, totalTime, writerMetrics);

  if (readAfterWrite && cSchema != nullptr) {
    const auto schema = arrowGetOrThrow(arrow::ImportSchema(cSchema.get()));
    const auto reader = createShuffleReader(runtime, schema);

    GLUTEN_ASSIGN_OR_THROW(auto in, arrow::io::ReadableFile::Open(dataFile));
    auto streamReader = std::make_shared<TestStreamReader>(std::move(in));
    // Read all partitions.
    auto iter = reader->read(streamReader);
    while (iter->hasNext()) {
      // Read and discard.
      auto cb = iter->next();
    }
    // Call the dtor to collect the metrics.
    iter.reset();

    readerMetrics.decompressTime = reader->getDecompressTime();
    readerMetrics.deserializeTime = reader->getDeserializeTime();
  }

  if (std::filesystem::remove(dataFile)) {
    LOG(INFO) << "Removed shuffle data file: " << dataFile;
  } else {
    LOG(WARNING) << "Failed to remove shuffle data file. File does not exist: " << dataFile;
  }
}

void updateBenchmarkMetrics(
    ::benchmark::State& state,
    const int64_t& elapsedTime,
    const int64_t& readInputTime,
    const WriterMetrics& writerMetrics,
    const ReaderMetrics& readerMetrics) {
  state.counters["read_input_time"] =
      benchmark::Counter(readInputTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["elapsed_time"] =
      benchmark::Counter(elapsedTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);

  if (FLAGS_run_shuffle || FLAGS_with_shuffle) {
    state.counters["shuffle_write_time"] = benchmark::Counter(
        writerMetrics.writeTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_spill_time"] = benchmark::Counter(
        writerMetrics.evictTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_compress_time"] = benchmark::Counter(
        writerMetrics.compressTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_decompress_time"] = benchmark::Counter(
        readerMetrics.decompressTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_deserialize_time"] = benchmark::Counter(
        readerMetrics.deserializeTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);

    auto splitTime = writerMetrics.splitTime;
    if (FLAGS_scan_mode == "stream") {
      splitTime -= readInputTime;
    }
    state.counters["shuffle_split_time"] =
        benchmark::Counter(splitTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);

    state.counters["shuffle_data_size"] = benchmark::Counter(
        writerMetrics.dataSize, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1024);
    state.counters["shuffle_spilled_bytes"] = benchmark::Counter(
        writerMetrics.bytesSpilled, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1024);
    state.counters["shuffle_write_bytes"] = benchmark::Counter(
        writerMetrics.bytesWritten, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1024);
  }
}

void setQueryTraceConfig(std::unordered_map<std::string, std::string>& configs) {
  if (!FLAGS_query_trace_enabled) {
    return;
  }
  configs[kQueryTraceEnabled] = "true";
  if (FLAGS_query_trace_dir != "") {
    configs[kQueryTraceDir] = FLAGS_query_trace_dir;
  }
  if (FLAGS_query_trace_max_bytes) {
    configs[kQueryTraceMaxBytes] = std::to_string(FLAGS_query_trace_max_bytes);
  }
  if (FLAGS_query_trace_task_reg_exp != "") {
    configs[kQueryTraceTaskRegExp] = FLAGS_query_trace_task_reg_exp;
  }
}
} // namespace

using RuntimeFactory = std::function<BoltRuntime*(MemoryManager* memoryManager)>;

auto BM_Generic = [](::benchmark::State& state,
                     const std::string& planFile,
                     const std::vector<std::string>& splitFiles,
                     const std::vector<std::string>& dataFiles,
                     const std::vector<std::string>& localDirs,
                     RuntimeFactory runtimeFactory,
                     FileReaderType readerType) {
  setCpu(state);

  auto listener = std::make_unique<TestAllocationListener>();
  listener->updateLimit(FLAGS_memory_limit);

  auto* listenerPtr = listener.get();
  auto* memoryManager = MemoryManager::create(kBoltBackendKind, std::move(listener), "bm-generic");
  auto runtime = runtimeFactory(memoryManager);

  auto plan = getPlanFromFile("Plan", planFile);
  std::vector<std::string> splits{};
  for (const auto& splitFile : splitFiles) {
    splits.push_back(getPlanFromFile("ReadRel.LocalFiles", splitFile));
  }

  const auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
  const auto spillDirIndex = tid % localDirs.size();
  const auto boltSpillDir = generateUniqueSubdir(std::filesystem::path(localDirs[spillDirIndex]) / "gluten-spill");

  std::vector<std::string> shuffleSpillDirs;
  std::transform(localDirs.begin(), localDirs.end(), std::back_inserter(shuffleSpillDirs), [](const auto& dir) {
    auto path = std::filesystem::path(dir) / "shuffle-write";
    return path;
  });
  // Use a different directory for data file.
  const auto dataFileDir = gluten::getShuffleSpillDir(
      shuffleSpillDirs[(spillDirIndex + 1) % localDirs.size()], state.thread_index() % gluten::kDefaultNumSubDirs);

  WriterMetrics writerMetrics{};
  ReaderMetrics readerMetrics{};
  int64_t readInputTime = 0;
  int64_t elapsedTime = 0;

  {
    ScopedTimer timer(&elapsedTime);
    for (auto _ : state) {
      std::vector<std::shared_ptr<gluten::ResultIterator>> inputIters;
      std::vector<FileReaderIterator*> inputItersRaw;
      if (!dataFiles.empty()) {
        for (const auto& input : dataFiles) {
          inputIters.push_back(FileReaderIterator::getInputIteratorFromFileReader(
              readerType, input, FLAGS_batch_size, runtime->memoryManager()->getLeafMemoryPool()));
        }
        std::transform(
            inputIters.begin(),
            inputIters.end(),
            std::back_inserter(inputItersRaw),
            [](std::shared_ptr<gluten::ResultIterator> iter) {
              return static_cast<FileReaderIterator*>(iter->getInputIter());
            });
      }
      *Runtime::localWriteFilesTempPath() = FLAGS_write_path;
      runtime->parsePlan(reinterpret_cast<uint8_t*>(plan.data()), plan.size());
      for (auto i = 0; i < splits.size(); i++) {
        auto split = splits[i];
        runtime->parseSplitInfo(reinterpret_cast<uint8_t*>(split.data()), split.size(), i);
      }

      auto resultIter = runtime->createResultIterator(boltSpillDir, std::move(inputIters), runtime->getConfMap());
      listenerPtr->setIterator(resultIter.get());

      if (FLAGS_with_shuffle) {
        runShuffle(
            runtime, listenerPtr, resultIter, writerMetrics, readerMetrics, false, shuffleSpillDirs, dataFileDir);
      } else {
        // May write the output into file.
        std::shared_ptr<BoltColumnarBatchWriter> writer{nullptr};

        while (resultIter->hasNext()) {
          auto cb = resultIter->next();

          state.PauseTiming();

          if (!FLAGS_save_output.empty()) {
            if (writer == nullptr) {
              writer = std::make_shared<BoltColumnarBatchWriter>(
                  FLAGS_save_output, FLAGS_batch_size, runtime->memoryManager()->getAggregateMemoryPool());
            }
            GLUTEN_THROW_NOT_OK(writer->write(cb));
          }

          if (FLAGS_print_result) {
            auto rowVector =
                BoltColumnarBatch::from(runtime->memoryManager()->getLeafMemoryPool().get(), cb)->getRowVector();
            LOG(WARNING) << rowVector->toString(0, 20);
          }

          state.ResumeTiming();
        }

        state.PauseTiming();
        if (!FLAGS_save_output.empty()) {
          GLUTEN_THROW_NOT_OK(writer->close());
        }
        state.ResumeTiming();
      }

      readInputTime +=
          std::accumulate(inputItersRaw.begin(), inputItersRaw.end(), 0, [](int64_t sum, FileReaderIterator* iter) {
            return sum + iter->getCollectBatchTime();
          });

      auto* rawIter = static_cast<gluten::WholeStageResultIterator*>(resultIter->getInputIter());
      const auto* task = rawIter->task();
      LOG(WARNING) << task->toString();
      const auto* planNode = rawIter->boltPlan();
      auto statsStr = bytedance::bolt::exec::printPlanWithStats(*planNode, task->taskStats(), true);
      LOG(WARNING) << statsStr;
    }
  }

  updateBenchmarkMetrics(state, elapsedTime, readInputTime, writerMetrics, readerMetrics);
  Runtime::release(runtime);
  MemoryManager::release(memoryManager);
};

auto BM_ShuffleWriteRead = [](::benchmark::State& state,
                              const std::string& inputFile,
                              const std::vector<std::string>& localDirs,
                              RuntimeFactory runtimeFactory,
                              FileReaderType readerType) {
  setCpu(state);

  auto listener = std::make_unique<TestAllocationListener>();
  listener->updateLimit(FLAGS_memory_limit);

  auto* listenerPtr = listener.get();
  auto* memoryManager = MemoryManager::create(kBoltBackendKind, std::move(listener), "bm-shuffle-write-read");
  auto runtime = runtimeFactory(memoryManager);

  const size_t dirIndex = std::hash<std::thread::id>{}(std::this_thread::get_id()) % localDirs.size();
  const auto dataFileDir =
      gluten::getShuffleSpillDir(localDirs[dirIndex], state.thread_index() % gluten::kDefaultNumSubDirs);

  WriterMetrics writerMetrics{};
  ReaderMetrics readerMetrics{};
  int64_t readInputTime = 0;
  int64_t elapsedTime = 0;
  {
    ScopedTimer timer(&elapsedTime);
    for (auto _ : state) {
      auto resultIter = FileReaderIterator::getInputIteratorFromFileReader(
          readerType, inputFile, FLAGS_batch_size, runtime->memoryManager()->getLeafMemoryPool());
      runShuffle(
          runtime,
          listenerPtr,
          resultIter,
          writerMetrics,
          readerMetrics,
          FLAGS_run_shuffle_read,
          localDirs,
          dataFileDir);

      auto reader = static_cast<FileReaderIterator*>(resultIter->getInputIter());
      readInputTime += reader->getCollectBatchTime();
    }
  }

  updateBenchmarkMetrics(state, elapsedTime, readInputTime, writerMetrics, readerMetrics);
  Runtime::release(runtime);
  MemoryManager::release(memoryManager);
};

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::ostringstream ss;
  ss << "Setting flags from command line args: " << std::endl;
  std::vector<gflags::CommandLineFlagInfo> flags;
  gflags::GetAllFlags(&flags);
  auto filename = std::filesystem::path(__FILE__).filename();
  for (const auto& flag : flags) {
    if (std::filesystem::path(flag.filename).filename() == filename) {
      ss << "    FLAGS_" << flag.name << ": default = " << flag.default_value << ", current = " << flag.current_value
         << std::endl;
    }
  }
  LOG(WARNING) << ss.str();

  ::benchmark::Initialize(&argc, argv);

  // Init Bolt backend.
  std::unordered_map<std::string, std::string> backendConf{};
  std::unordered_map<std::string, std::string> sessionConf{};
  backendConf.insert({gluten::kDebugModeEnabled, std::to_string(FLAGS_debug_mode)});
  backendConf.insert({gluten::kGlogVerboseLevel, std::to_string(FLAGS_v)});
  backendConf.insert({gluten::kGlogSeverityLevel, std::to_string(FLAGS_minloglevel)});
  if (!FLAGS_conf.empty()) {
    abortIfFileNotExists(FLAGS_conf);
    std::ifstream file(FLAGS_conf);

    if (!file.is_open()) {
      LOG(ERROR) << "Unable to open configuration file.";
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }

    // Parse the ini file.
    // Load all key-values under [Backend Conf] to backendConf, under [Session Conf] to sessionConf.
    // If no [Session Conf] section specified, all key-values are loaded for both backendConf and sessionConf.
    bool isBackendConf = true;
    std::string line;
    while (std::getline(file, line)) {
      if (line.empty() || line[0] == ';') {
        continue;
      }
      if (line[0] == '[') {
        if (line == "[Backend Conf]") {
          isBackendConf = true;
        } else if (line == "[Session Conf]") {
          isBackendConf = false;
        } else {
          LOG(ERROR) << "Invalid section: " << line;
          ::benchmark::Shutdown();
          std::exit(EXIT_FAILURE);
        }
        continue;
      }
      std::istringstream iss(line);
      std::string key, value;

      iss >> key;

      // std::ws is used to consume any leading whitespace.
      std::getline(iss >> std::ws, value);

      if (isBackendConf) {
        backendConf[key] = value;
      } else {
        sessionConf[key] = value;
      }
    }
  }
  if (sessionConf.empty()) {
    sessionConf = backendConf;
  }
  setQueryTraceConfig(sessionConf);
  setQueryTraceConfig(backendConf);

  initBoltBackend(backendConf);
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

  // Parse substrait plan, split file and data files.
  std::string substraitJsonFile = FLAGS_plan;
  std::vector<std::string> splitFiles{};
  std::vector<std::string> dataFiles{};

  if (FLAGS_run_shuffle) {
    std::string errorMsg{};
    if (FLAGS_data.empty()) {
      errorMsg = "Missing '--split' or '--data' option.";
    } else if (FLAGS_partitioning != "rr" && FLAGS_partitioning != "random") {
      errorMsg = "--run-shuffle only support round-robin partitioning and random partitioning.";
    }
    if (errorMsg.empty()) {
      try {
        dataFiles = gluten::splitPaths(FLAGS_data, true);
        if (dataFiles.size() > 1) {
          errorMsg = "Only one data file is allowed for shuffle write.";
        }
      } catch (const std::exception& e) {
        errorMsg = e.what();
      }
    }
    if (!errorMsg.empty()) {
      LOG(ERROR) << "Incorrect usage: " << errorMsg << std::endl;
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }
  } else {
    // Validate input args.
    std::string errorMsg{};
    if (substraitJsonFile.empty()) {
      errorMsg = "Missing '--plan' option.";
    } else if (!checkPathExists(substraitJsonFile)) {
      errorMsg = "File path does not exist: " + substraitJsonFile;
    } else if (FLAGS_split.empty() && FLAGS_data.empty()) {
      errorMsg = "Missing '--split' or '--data' option.";
    }

    if (errorMsg.empty()) {
      try {
        if (!FLAGS_data.empty()) {
          dataFiles = gluten::splitPaths(FLAGS_data, true);
        }
        if (!FLAGS_split.empty()) {
          splitFiles = gluten::splitPaths(FLAGS_split, true);
        }
      } catch (const std::exception& e) {
        errorMsg = e.what();
      }
    }

    if (!errorMsg.empty()) {
      LOG(ERROR) << "Incorrect usage: " << errorMsg << std::endl
                 << "*** Please check docs/developers/MicroBenchmarks.md for the full usage. ***";
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }
  }

  LOG(WARNING) << "Using substrait json file: " << std::endl << substraitJsonFile;
  if (!splitFiles.empty()) {
    LOG(WARNING) << "Using " << splitFiles.size() << " input split file(s): ";
    for (const auto& splitFile : splitFiles) {
      LOG(WARNING) << splitFile;
    }
  }
  if (!dataFiles.empty()) {
    LOG(WARNING) << "Using " << dataFiles.size() << " input data file(s): ";
    for (const auto& dataFile : dataFiles) {
      LOG(WARNING) << dataFile;
    }
  }

  RuntimeFactory runtimeFactory = [=](MemoryManager* memoryManager) {
    return dynamic_cast<BoltRuntime*>(Runtime::create(kBoltBackendKind, memoryManager, 1, sessionConf));
  };

  const auto localDirs = createLocalDirs();

#define GENERIC_BENCHMARK(READER_TYPE)         \
  do {                                         \
    auto* bm = ::benchmark::RegisterBenchmark( \
                   "GenericBenchmark",         \
                   BM_Generic,                 \
                   substraitJsonFile,          \
                   splitFiles,                 \
                   dataFiles,                  \
                   localDirs,                  \
                   runtimeFactory,             \
                   READER_TYPE)                \
                   ->MeasureProcessCPUTime()   \
                   ->UseRealTime();            \
    setUpBenchmark(bm);                        \
  } while (0)

#define SHUFFLE_WRITE_READ_BENCHMARK(READER_TYPE)                                                                 \
  do {                                                                                                            \
    auto* bm = ::benchmark::RegisterBenchmark(                                                                    \
                   "ShuffleWriteRead", BM_ShuffleWriteRead, dataFiles[0], localDirs, runtimeFactory, READER_TYPE) \
                   ->MeasureProcessCPUTime()                                                                      \
                   ->UseRealTime();                                                                               \
    setUpBenchmark(bm);                                                                                           \
  } while (0)

  if (dataFiles.empty()) {
    GENERIC_BENCHMARK(FileReaderType::kNone);
  } else {
    FileReaderType readerType;
    if (FLAGS_scan_mode == "buffered") {
      readerType = FileReaderType::kBuffered;
      LOG(WARNING) << "Using buffered mode for reading parquet data.";
    } else {
      readerType = FileReaderType::kStream;
      LOG(WARNING) << "Using stream mode for reading parquet data.";
    }
    if (FLAGS_run_shuffle) {
      SHUFFLE_WRITE_READ_BENCHMARK(readerType);
    } else {
      GENERIC_BENCHMARK(readerType);
    }
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  gluten::BoltBackend::get()->tearDown();

  cleanupLocalDirs(localDirs);

  return 0;
}
