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

#include "compute/BoltRuntime.h"
#include "config/GlutenConfig.h"

#include <gtest/gtest.h>
#include "compute/BoltBackend.h"
#include "memory.pb.h"
#include "threads/ThreadInitializer.h"

namespace gluten {

class DummyMemoryManager final : public MemoryManager {
 public:
  DummyMemoryManager(const std::string& kind) : MemoryManager(kind, "dummy-memory-manager"){};

  arrow::MemoryPool* defaultArrowMemoryPool() override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<arrow::MemoryPool> getOrCreateArrowMemoryPool(const std::string& name) override {
    throw GlutenException("Not yet implemented");
  }
  const MemoryUsageStats collectMemoryUsageStats() const override {
    throw GlutenException("Not yet implemented");
  }
  const int64_t shrink(int64_t size) override {
    throw GlutenException("Not yet implemented");
  }
  void hold() override {
    throw GlutenException("Not yet implemented");
  }
};

inline static const std::string kDummyBackendKind{"dummy"};

class DummyThreadManager final : public ThreadManager {
 public:
  explicit DummyThreadManager(const std::string& kind) : ThreadManager(kind), initializer_(ThreadInitializer::noop()) {}

  ThreadInitializer* getThreadInitializer() override {
    return initializer_.get();
  }

 private:
  std::shared_ptr<ThreadInitializer> initializer_;
};

class DummyRuntime final : public Runtime {
 public:
  DummyRuntime(
      const std::string& kind,
      DummyMemoryManager* mm,
      ThreadManager* tm,
      const std::unordered_map<std::string, std::string>& conf,
      int64_t taskId)
      : Runtime(kind, mm, tm, conf, taskId) {}

  void parsePlan(const uint8_t* data, int32_t size) override {}

  void parseSplitInfo(const uint8_t* data, int32_t size, int32_t idx) override {}

  std::shared_ptr<ResultIterator> createResultIterator(
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs) override {
    auto resIter = std::make_unique<DummyResultIterator>();
    auto iter = std::make_shared<ResultIterator>(std::move(resIter));
    return iter;
  }

  void noMoreSplits(ResultIterator* iter) override {
    // Do nothing.
  }

  MemoryManager* memoryManager() override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(int64_t column2RowMemThreshold) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(struct ArrowSchema* cSchema) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>&) override {
    throw GlutenException("Not yet implemented");
  }
  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    static Metrics m(1);
    return &m;
  }
  std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options) override {
    throw GlutenException("Not yet implemented");
  }
  std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarBatch> select(std::shared_ptr<ColumnarBatch>, const std::vector<int32_t>&) override {
    throw GlutenException("Not yet implemented");
  }
  std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) override {
    throw GlutenException("Not yet implemented");
  }

 private:
  class DummyResultIterator : public ColumnarBatchIterator {
   public:
    std::shared_ptr<ColumnarBatch> next() override {
      if (!hasNext_) {
        return nullptr;
      }
      hasNext_ = false;

      return gluten::createZeroColumnBatch(1);
    }

   private:
    bool hasNext_ = true;
  };
};

static Runtime* dummyRuntimeFactory(
    const std::string& kind,
    MemoryManager* mm,
    ThreadManager* tm,
    const std::unordered_map<std::string, std::string>& conf,
    int64_t taskId) {
  return new DummyRuntime(kind, dynamic_cast<DummyMemoryManager*>(mm), tm, conf, taskId);
}

static void dummyRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}

TEST(TestRuntime, CreateRuntime) {
  Runtime::registerFactory(kDummyBackendKind, dummyRuntimeFactory, dummyRuntimeReleaser);
  DummyMemoryManager mm(kDummyBackendKind);
  DummyThreadManager tm(kDummyBackendKind);
  auto runtime = Runtime::create(kDummyBackendKind, &mm, &tm, {}, 1);
  ASSERT_EQ(typeid(*runtime), typeid(DummyRuntime));
  Runtime::release(runtime);
}

TEST(TestRuntime, CreateBoltRuntime) {
  BoltBackend::create(AllocationListener::noop(), {{kSparkOffHeapMemory, "7516192768"}});
  auto mm = MemoryManager::create(kBoltBackendKind, AllocationListener::noop(), "test-bolt-runtime");
  auto tm = ThreadManager::create(kBoltBackendKind, ThreadInitializer::noop());
  auto runtime = Runtime::create(kBoltBackendKind, mm, tm, {{kSparkOffHeapMemory, "7516192768"}}, 1);
  ASSERT_EQ(typeid(*runtime), typeid(BoltRuntime));
  Runtime::release(runtime);
  ThreadManager::release(tm);
}

TEST(TestRuntime, GetResultIterator) {
  DummyMemoryManager mm(kDummyBackendKind);
  DummyThreadManager tm(kDummyBackendKind);
  auto runtime =
      std::make_shared<DummyRuntime>(kDummyBackendKind, &mm, &tm, std::unordered_map<std::string, std::string>(), 1);
  auto iter = runtime->createResultIterator("/tmp/test-spill", {});
  runtime->noMoreSplits(iter.get());
  ASSERT_TRUE(iter->hasNext());
  auto next = iter->next();
  ASSERT_NE(next, nullptr);
  ASSERT_FALSE(iter->hasNext());
  next = iter->next();
  ASSERT_EQ(next, nullptr);
}

} // namespace gluten
