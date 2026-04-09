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

#include <arrow/c/bridge.h>
#include <arrow/io/api.h>

#include "config/GlutenConfig.h"
#include "shuffle/VeloxGpuShuffleWriter.h"
#include "shuffle/VeloxHashShuffleWriter.h"
#include "tests/VeloxShuffleWriterTestBase.h"
#include "tests/utils/TestAllocationListener.h"
#include "tests/utils/TestStreamReader.h"
#include "tests/utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"
#include "memory/GpuBufferColumnarBatch.h"
#include "utils/GpuBufferBatchResizer.h"

#include "velox/experimental/cudf/CudfNoDefaults.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {

namespace {

class ColumnarBatchArray : public ColumnarBatchIterator {
 public:
  explicit ColumnarBatchArray(const std::vector<std::shared_ptr<GpuBufferColumnarBatch>> batches)
      : batches_(std::move(batches)) {}

  std::shared_ptr<ColumnarBatch> next() override {
    if (cursor_ >= batches_.size()) {
      return nullptr;
    }
    return batches_[cursor_++];
  }

 private:
  const std::vector<std::shared_ptr<GpuBufferColumnarBatch>> batches_;
  int32_t cursor_ = 0;
};

struct GpuShuffleTestParams {
  ShuffleWriterType shuffleWriterType;
  PartitionWriterType partitionWriterType;
  arrow::Compression::type compressionType;
  int32_t compressionThreshold{0};
  int32_t mergeBufferSize{0};
  int32_t diskWriteBufferSize{0};
  bool enableDictionary{false};
  int64_t deserializerBufferSize{0};

  std::string toString() const {
    std::ostringstream out;
    out << "shuffleWriterType = " << ShuffleWriter::typeToString(shuffleWriterType)
        << ", partitionWriterType = " << PartitionWriter::typeToString(partitionWriterType)
        << ", compressionType = " << arrow::util::Codec::GetCodecAsString(compressionType)
        << ", compressionThreshold = " << compressionThreshold << ", mergeBufferSize = " << mergeBufferSize
        << ", compressionBufferSize = " << diskWriteBufferSize
        << ", deserializerBufferSize = " << deserializerBufferSize;
    return out.str();
  }
};

std::vector<RowVectorPtr> takeRows(
    const std::vector<RowVectorPtr>& sources,
    const std::vector<std::vector<int32_t>>& indices) {
  std::vector<RowVectorPtr> result;

  for (size_t i = 0; i < sources.size(); ++i) {
    if (indices[i].empty()) {
      result.push_back(sources[i]);
      continue;
    }

    auto copy = RowVector::createEmpty(sources[0]->type(), sources[0]->pool());
    for (const auto row : indices[i]) {
      GLUTEN_CHECK(row < sources[i]->size(), fmt::format("Index out of bound: {}", row));
      copy->append(sources[i]->slice(row, 1).get());
    }
    result.push_back(std::move(copy));
  }

  return result;
}

RowVectorPtr mergeRowVectors(const std::vector<RowVectorPtr>& sources) {
  RowVectorPtr result = RowVector::createEmpty(sources[0]->type(), sources[0]->pool());

  for (const auto& source : sources) {
    result->append(source.get());
  }

  return result;
}

RowVectorPtr mergeBufferColumnarBatches(std::vector<std::shared_ptr<GpuBufferColumnarBatch>>& bufferBatches) {
  GpuBufferBatchResizer resizer(
      getDefaultMemoryManager()->defaultArrowMemoryPool(),
      getDefaultMemoryManager()->getLeafMemoryPool().get(),
      1200, // output one batch
      102400000, // prefetch up to 100MB data
      std::make_unique<ColumnarBatchArray>(bufferBatches));
  auto cb = resizer.next();
  auto batch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  VELOX_CHECK_NOT_NULL(batch);
  auto vector = std::dynamic_pointer_cast<cudf_velox::CudfVector>(batch->getRowVector());
  VELOX_CHECK_NOT_NULL(vector);

  auto tableView = vector->getTableView();

  // Convert back to Velox
  return cudf_velox::with_arrow::toVeloxColumn(
      tableView, getDefaultMemoryManager()->getLeafMemoryPool().get(), "", vector->stream(), cudf_velox::get_temp_mr());
}

std::vector<GpuShuffleTestParams> getTestParams() {
  static std::vector<GpuShuffleTestParams> params{};

  if (!params.empty()) {
    return params;
  }

  const std::vector<arrow::Compression::type> compressions = {
      arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD};
  const std::vector<int32_t> compressionThresholds = {-1, 0, 3, 4, 10, 4096};
  const std::vector<int32_t> mergeBufferSizes = {0, 3, 4, 10, 4096};

  for (const auto& compression : compressions) {
    // Hash-based shuffle.
    for (const auto compressionThreshold : compressionThresholds) {
      // Local.
      for (const auto mergeBufferSize : mergeBufferSizes) {
        params.push_back(
            GpuShuffleTestParams{
                .shuffleWriterType = ShuffleWriterType::kGpuHashShuffle,
                .partitionWriterType = PartitionWriterType::kLocal,
                .compressionType = compression,
                .compressionThreshold = compressionThreshold,
                .mergeBufferSize = mergeBufferSize});
      }

      // Rss.
      params.push_back(
          GpuShuffleTestParams{
              .shuffleWriterType = ShuffleWriterType::kGpuHashShuffle,
              .partitionWriterType = PartitionWriterType::kRss,
              .compressionType = compression,
              .compressionThreshold = compressionThreshold});
    }
  }

  return params;
}

std::shared_ptr<PartitionWriter> createPartitionWriter(
    PartitionWriterType partitionWriterType,
    uint32_t numPartitions,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs,
    arrow::Compression::type compressionType,
    int32_t mergeBufferSize,
    int32_t compressionThreshold,
    bool enableDictionary) {
  GLUTEN_ASSIGN_OR_THROW(auto codec, arrow::util::Codec::Create(compressionType));
  switch (partitionWriterType) {
    case PartitionWriterType::kLocal: {
      auto options = std::make_shared<LocalPartitionWriterOptions>();
      options->mergeBufferSize = mergeBufferSize;
      options->compressionThreshold = compressionThreshold;
      options->enableDictionary = enableDictionary;
      return std::make_shared<LocalPartitionWriter>(
          numPartitions, std::move(codec), getDefaultMemoryManager(), options, dataFile, std::move(localDirs));
    }
    case PartitionWriterType::kRss: {
      auto options = std::make_shared<RssPartitionWriterOptions>();
      auto rssClient = std::make_unique<LocalRssClient>(dataFile);
      return std::make_shared<RssPartitionWriter>(
          numPartitions, std::move(codec), getDefaultMemoryManager(), options, std::move(rssClient));
    }
    default:
      throw GlutenException("Unsupported partition writer type.");
  }
}

} // namespace

class GpuVeloxShuffleWriterTestEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    VeloxShuffleWriterTestBase::setUpVeloxBackend();
  }

  void TearDown() override {
    VeloxShuffleWriterTestBase::tearDownVeloxBackend();
  }
};

class GpuVeloxShuffleWriterTest : public ::testing::TestWithParam<GpuShuffleTestParams>,
                                  public VeloxShuffleWriterTestBase {
 protected:
  static std::shared_ptr<ShuffleWriterOptions> createShuffleWriterOptions(
      Partitioning partitioning,
      int32_t splitBufferSize) {
    std::shared_ptr<ShuffleWriterOptions> options;
    const auto& params = GetParam();
    switch (params.shuffleWriterType) {
      case ShuffleWriterType::kGpuHashShuffle: {
        auto hashOptions = std::make_shared<GpuHashShuffleWriterOptions>();
        hashOptions->splitBufferSize = splitBufferSize;
        options = hashOptions;
      } break;
      default:
        throw GlutenException("Unreachable");
    }
    options->partitioning = partitioning;

    return options;
  }

  virtual std::shared_ptr<ShuffleWriterOptions> defaultShuffleWriterOptions() = 0;

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(
      uint32_t numPartitions,
      std::shared_ptr<ShuffleWriterOptions> shuffleWriterOptions = nullptr) {
    if (shuffleWriterOptions == nullptr) {
      shuffleWriterOptions = defaultShuffleWriterOptions();
    }

    const auto& params = GetParam();
    const auto partitionWriter = createPartitionWriter(
        params.partitionWriterType,
        numPartitions,
        dataFile_,
        localDirs_,
        params.compressionType,
        params.mergeBufferSize,
        params.compressionThreshold,
        params.enableDictionary);

    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxShuffleWriter::create(
            params.shuffleWriterType, numPartitions, partitionWriter, shuffleWriterOptions, getDefaultMemoryManager()));

    return shuffleWriter;
  }

  void SetUp() override {
    std::cout << "Running test with param: " << GetParam().toString() << std::endl;
    VeloxShuffleWriterTestBase::setUpTestData();
    // Remove the UNKNOWN type because cudf does not support.
    children1_.pop_back();
    children2_.pop_back();
    inputVector1_ = makeRowVector(children1_);
    inputVector2_ = makeRowVector(children2_);
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
  }

  static void checkFileExists(const std::string& fileName) {
    ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(fileName)), true);
  }

  std::shared_ptr<arrow::Schema> getArrowSchema(const facebook::velox::RowVectorPtr& rowVector) const {
    return toArrowSchema(rowVector->type(), getDefaultMemoryManager()->getLeafMemoryPool().get());
  }

  void setReadableFile(const std::string& fileName) {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
    GLUTEN_ASSIGN_OR_THROW(file_, arrow::io::ReadableFile::Open(fileName))
  }

  void getBufferColumnarBatches(
      arrow::Compression::type compressionType,
      const RowTypePtr& rowType,
      std::vector<std::shared_ptr<GpuBufferColumnarBatch>>& bufferBatches,
      std::shared_ptr<arrow::io::InputStream> in) {
    const auto veloxCompressionType = arrowCompressionTypeToVelox(compressionType);
    const auto schema = toArrowSchema(rowType, getDefaultMemoryManager()->getLeafMemoryPool().get());
    auto codec = createCompressionCodec(compressionType, CodecBackend::NONE);

    auto deserializerFactory = std::make_unique<gluten::VeloxShuffleReaderDeserializerFactory>(
        schema,
        std::move(codec),
        veloxCompressionType,
        rowType,
        kDefaultBatchSize,
        kDefaultReadBufferSize,
        GetParam().deserializerBufferSize,
        getDefaultMemoryManager(),
        GetParam().shuffleWriterType);

    const auto reader = std::make_shared<VeloxShuffleReader>(std::move(deserializerFactory));
    const auto iter = reader->read(std::make_shared<TestStreamReader>(std::move(in)));

    while (iter->hasNext()) {
      auto cb = std::dynamic_pointer_cast<GpuBufferColumnarBatch>(iter->next());
      VELOX_CHECK_NOT_NULL(cb);
      bufferBatches.emplace_back(cb);
    }

  }

  void shuffleWriteReadMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      int32_t expectPartitionLength,
      const std::vector<std::vector<RowVectorPtr>>& expectedVectors) {
    ASSERT_NOT_OK(shuffleWriter.stop());
    checkFileExists(dataFile_);
    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), expectPartitionLength);

    // Compute total size and verify file size
    int64_t lengthSum = std::accumulate(lengths.begin(), lengths.end(), 0L);

    setReadableFile(dataFile_);
    int64_t fileSize = *file_->GetSize();
    ASSERT_EQ(fileSize, lengthSum);

    int64_t offset = 0;
    for (int32_t i = 0; i < expectPartitionLength; i++) {
      if (expectedVectors[i].empty()) {
        ASSERT_EQ(lengths[i], 0);
      } else {
        std::vector<std::shared_ptr<GpuBufferColumnarBatch>> deserializedVectors;

        // Compute byte range for this partition
        int64_t start = offset;
        int64_t size = lengths[i];
        offset += size;

        GLUTEN_ASSIGN_OR_THROW(auto in, arrow::io::RandomAccessFile::GetStream(file_, start, size));

        getBufferColumnarBatches(
            GetParam().compressionType, asRowType(expectedVectors[i][0]->type()), deserializedVectors, in);

        auto expectedVector = mergeRowVectors(expectedVectors[i]);
        auto deserializedVector = mergeBufferColumnarBatches(deserializedVectors);

        facebook::velox::test::assertEqualVectors(expectedVector, deserializedVector);
      }
    }

  }

  void testShuffleRoundTrip(
      VeloxShuffleWriter& shuffleWriter,
      const std::vector<std::shared_ptr<ColumnarBatch>>& batches,
      int32_t expectPartitionLength,
      const std::vector<std::vector<RowVectorPtr>>& expectedVectors) {
    for (const auto& batch : batches) {
      ASSERT_NOT_OK(shuffleWriter.write(batch, ShuffleWriter::kMinMemLimit));
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, expectedVectors);
  }

  void testShuffleRoundTrip(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<RowVectorPtr> inputs,
      int32_t expectPartitionLength,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) {
    std::vector<std::shared_ptr<ColumnarBatch>> batches;
    for (const auto& input : inputs) {
      batches.emplace_back(std::make_shared<VeloxColumnarBatch>(input));
    }
    testShuffleRoundTrip(shuffleWriter, batches, expectPartitionLength, expectedVectors);
  }

  inline static TestAllocationListener* listener_{nullptr};

  std::shared_ptr<arrow::io::ReadableFile> file_;
};

class GpuSinglePartitioningShuffleWriterTest : public GpuVeloxShuffleWriterTest {
 protected:
  std::shared_ptr<ShuffleWriterOptions> defaultShuffleWriterOptions() override {
    return createShuffleWriterOptions(Partitioning::kSingle, 10);
  }
};

class GpuHashPartitioningShuffleWriterTest : public GpuVeloxShuffleWriterTest {
 protected:
  void SetUp() override {
    GpuVeloxShuffleWriterTest::SetUp();

    children1_.insert((children1_.begin()), makeFlatVector<int32_t>({1, 2, 2, 2, 2, 1, 1, 1, 2, 1}));
    hashInputVector1_ = makeRowVector(children1_);
    children2_.insert((children2_.begin()), makeFlatVector<int32_t>({2, 2}));
    hashInputVector2_ = makeRowVector(children2_);
  }

  std::shared_ptr<ShuffleWriterOptions> defaultShuffleWriterOptions() override {
    return createShuffleWriterOptions(Partitioning::kHash, 4);
  }

  std::vector<uint32_t> hashPartitionIds_{1, 2};

  RowVectorPtr hashInputVector1_;
  RowVectorPtr hashInputVector2_;
};

class GpuRangePartitioningShuffleWriterTest : public GpuVeloxShuffleWriterTest {
 protected:
  void SetUp() override {
    GpuVeloxShuffleWriterTest::SetUp();

    auto pid1 = makeRowVector({makeFlatVector<int32_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1})});
    auto rangeVector1 = makeRowVector(inputVector1_->children());
    compositeBatch1_ = VeloxColumnarBatch::compose(
        pool(), {std::make_shared<VeloxColumnarBatch>(pid1), std::make_shared<VeloxColumnarBatch>(rangeVector1)});

    auto pid2 = makeRowVector({makeFlatVector<int32_t>({0, 1})});
    auto rangeVector2 = makeRowVector(inputVector2_->children());
    compositeBatch2_ = VeloxColumnarBatch::compose(
        pool(), {std::make_shared<VeloxColumnarBatch>(pid2), std::make_shared<VeloxColumnarBatch>(rangeVector2)});
  }

  std::shared_ptr<ShuffleWriterOptions> defaultShuffleWriterOptions() override {
    return createShuffleWriterOptions(Partitioning::kRange, 4);
  }

  std::shared_ptr<ColumnarBatch> compositeBatch1_;
  std::shared_ptr<ColumnarBatch> compositeBatch2_;
};

class GpuRoundRobinPartitioningShuffleWriterTest : public GpuVeloxShuffleWriterTest {
 protected:
  std::shared_ptr<ShuffleWriterOptions> defaultShuffleWriterOptions() override {
    return createShuffleWriterOptions(Partitioning::kRoundRobin, 4096);
  }
};

TEST_P(GpuSinglePartitioningShuffleWriterTest, single) {
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }

  // Split 1 RowVector.
  {
    auto shuffleWriter = createShuffleWriter(1);
    testShuffleRoundTrip(*shuffleWriter, {inputVector1_}, 1, {{inputVector1_}});
  }
  // Split > 1 RowVector.
  {
    auto shuffleWriter = createShuffleWriter(1);
    testShuffleRoundTrip(
        *shuffleWriter,
        {inputVector1_, inputVector2_, inputVector1_},
        1,
        {{inputVector1_, inputVector2_, inputVector1_}});
  }
  // Split null RowVector.
  {
    auto shuffleWriter = createShuffleWriter(1);
    auto vector = makeRowVector({
        makeNullableFlatVector<int32_t>({std::nullopt}),
        makeNullableFlatVector<StringView>({std::nullopt}),
    });
    testShuffleRoundTrip(*shuffleWriter, {vector}, 1, {{vector}});
  }
}

TEST_P(GpuHashPartitioningShuffleWriterTest, hashPart1Vector) {
  // Fixed-length input.
  {
    auto shuffleWriter = createShuffleWriter(2);

    // Remove the decimal type because cudf to velox is not supported.
    std::vector<VectorPtr> data = {
        makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt}),
        makeNullableFlatVector<int16_t>({1, 2, 3, 4}),
        makeFlatVector<int64_t>({1, 2, 3, 4}),
        makeFlatVector<int64_t>({232, 34567235, 1212, 4567}),
        makeFlatVector<int32_t>({232, 34567235, 1212, 4567}),
        makeFlatVector<int32_t>(
            4, [](vector_size_t row) { return row % 2; }, nullEvery(5), DATE()),
        makeFlatVector<Timestamp>(4, [](vector_size_t row) { return Timestamp{row % 2, 0}; }, nullEvery(5))};

    const auto vector = makeRowVector(data);

    const auto blocksPid0 = takeRows({vector}, {{1, 3}});
    const auto blocksPid1 = takeRows({vector}, {{0, 2}});

    // Add partition id as the first column.
    data.insert(data.begin(), makeFlatVector<int32_t>({1, 2, 1, 2}));
    const auto input = makeRowVector(data);

    testShuffleRoundTrip(*shuffleWriter, {input}, 2, {blocksPid0, blocksPid1});
  }

  // Variable-length input.
  {
    auto shuffleWriter = createShuffleWriter(2);

    std::vector<VectorPtr> data = {
        makeFlatVector<StringView>({"nn", "", "fr", "juiu"}),
        makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt}),
        makeNullableFlatVector<StringView>({std::nullopt, "de", "10 I'm not inline string", "de"})};

    const auto vector = makeRowVector(data);

    const auto blocksPid0 = takeRows({vector}, {{0, 1, 3}});
    const auto blocksPid1 = takeRows({vector}, {{2}});

    // Add partition id as the first column.
    data.insert(data.begin(), makeFlatVector<int32_t>({2, 2, 1, 2}));
    const auto input = makeRowVector(data);

    testShuffleRoundTrip(*shuffleWriter, {input}, 2, {blocksPid0, blocksPid1});
  }
}

TEST_P(GpuHashPartitioningShuffleWriterTest, hashPart3Vectors) {
  auto shuffleWriter = createShuffleWriter(2);

  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 2, 3, 4, 8}, {0, 1}, {1, 2, 3, 4, 8}});
  auto blockPid1 = takeRows({inputVector1_, inputVector1_}, {{0, 5, 6, 7, 9}, {0, 5, 6, 7, 9}});

  testShuffleRoundTrip(
      *shuffleWriter, {hashInputVector1_, hashInputVector2_, hashInputVector1_}, 2, {blockPid2, blockPid1});
}

TEST_P(GpuHashPartitioningShuffleWriterTest, hashLargeVectors) {
  const int32_t expectedMaxBatchSize = 8;
  auto shuffleWriter = createShuffleWriter(2);

  // calculate maxBatchSize_
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, hashInputVector1_));
  if (GetParam().shuffleWriterType == ShuffleWriterType::kHashShuffle) {
    VELOX_CHECK_EQ(shuffleWriter->maxBatchSize(), expectedMaxBatchSize);
  }

  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 2, 3, 4, 8}, {}, {1, 2, 3, 4, 8}});
  auto blockPid1 = takeRows({inputVector1_, inputVector1_}, {{0, 5, 6, 7, 9}, {0, 5, 6, 7, 9}});

  VELOX_CHECK(hashInputVector1_->size() > expectedMaxBatchSize);
  testShuffleRoundTrip(*shuffleWriter, {hashInputVector2_, hashInputVector1_}, 2, {blockPid2, blockPid1});
}

TEST_P(GpuRangePartitioningShuffleWriterTest, range) {
  auto shuffleWriter = createShuffleWriter(2);

  auto blockPid1 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{0, 2, 4, 6, 8}, {0}, {0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 3, 5, 7, 9}, {1}, {1, 3, 5, 7, 9}});

  testShuffleRoundTrip(
      *shuffleWriter, {compositeBatch1_, compositeBatch2_, compositeBatch1_}, 2, {blockPid1, blockPid2});
}

TEST_P(GpuRoundRobinPartitioningShuffleWriterTest, roundRobin) {
  auto shuffleWriter = createShuffleWriter(2);

  auto blockPid1 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{0, 2, 4, 6, 8}, {0}, {0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 3, 5, 7, 9}, {1}, {1, 3, 5, 7, 9}});

  testShuffleRoundTrip(*shuffleWriter, {inputVector1_, inputVector2_, inputVector1_}, 2, {blockPid1, blockPid2});
}

TEST_P(GpuRoundRobinPartitioningShuffleWriterTest, preAllocForceRealloc) {
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }

  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferSize = 4096;
  shuffleWriterOptions->splitBufferReallocThreshold = 0; // Force re-alloc on buffer size changed.
  auto shuffleWriter = createShuffleWriter(2, shuffleWriterOptions);

  // First spilt no null.
  auto inputNoNull = inputVectorNoNull_;

  // Second split has null. Continue filling current partition buffers.
  std::vector<VectorPtr> intHasNull = {
      makeNullableFlatVector<int8_t>({std::nullopt, 1}),
      makeNullableFlatVector<int8_t>({std::nullopt, -1}),
      makeNullableFlatVector<int32_t>({std::nullopt, 100}),
      makeNullableFlatVector<int64_t>({0, 1}),
      makeNullableFlatVector<float>({0, 0.142857}),
      makeNullableFlatVector<bool>({false, true}),
      makeNullableFlatVector<StringView>({"", "alice"}),
      makeNullableFlatVector<StringView>({"alice", ""}),
  };

  auto inputHasNull = makeRowVector(intHasNull);
  // Split first input no null.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputNoNull));
  // Split second input, continue filling but update null.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputHasNull));

  // Split first input again.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputNoNull));
  // Check when buffer is full, evict current buffers and reuse.
  auto cachedPayloadSize = shuffleWriter->cachedPayloadSize();
  auto partitionBufferBeforeEvict = shuffleWriter->partitionBufferSize();
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(cachedPayloadSize, &evicted));
  // Check only cached data being spilled.
  ASSERT_EQ(evicted, cachedPayloadSize);
  VELOX_CHECK_EQ(shuffleWriter->partitionBufferSize(), partitionBufferBeforeEvict);

  // Split more data with null. New buffer size is larger.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Split more data with null. New buffer size is smaller.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));

  // Split more data with null. New buffer size is larger and current data is preserved.
  // Evict cached data first.
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  // Set a large buffer size.
  shuffleWriter->setPartitionBufferSize(100);
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  // No data got evicted so the cached size is 0.
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_P(GpuRoundRobinPartitioningShuffleWriterTest, preAllocForceReuse) {
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }
  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>();
  shuffleWriterOptions->splitBufferSize = 4096;
  shuffleWriterOptions->splitBufferReallocThreshold = 1; // Force reuse on buffer size changed.
  auto shuffleWriter = createShuffleWriter(2, shuffleWriterOptions);

  // First spilt no null.
  auto inputNoNull = inputVectorNoNull_;
  // Second split has null int, null string and non-null string,
  auto inputFixedWidthHasNull = inputVector1_;
  // Third split has null string.
  std::vector<VectorPtr> stringHasNull = {
      makeNullableFlatVector<int8_t>({0, 1}),
      makeNullableFlatVector<int8_t>({0, -1}),
      makeNullableFlatVector<int32_t>({0, 100}),
      makeNullableFlatVector<int64_t>({0, 1}),
      makeNullableFlatVector<float>({0, 0.142857}),
      makeNullableFlatVector<bool>({false, true}),
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
  };
  auto inputStringHasNull = makeRowVector(stringHasNull);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputNoNull));
  // Split more data with null. Already filled + to be filled > buffer size, Buffer is resized larger.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputFixedWidthHasNull));
  // Split more data with null. Already filled + to be filled > buffer size, newSize is smaller so buffer is not
  // resized.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputStringHasNull));

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_P(GpuRoundRobinPartitioningShuffleWriterTest, spillVerifyResult) {
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }

  auto shuffleWriter = createShuffleWriter(2);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Clear buffers and evict payloads and cache.
  for (auto pid : {0, 1}) {
    ASSERT_NOT_OK(shuffleWriter->evictPartitionBuffers(pid, true));
  }

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Evict all payloads and spill.
  int64_t evicted;
  auto cachedPayloadSize = shuffleWriter->cachedPayloadSize();
  auto partitionBufferSize = shuffleWriter->partitionBufferSize();
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(cachedPayloadSize + partitionBufferSize, &evicted));

  ASSERT_EQ(evicted, cachedPayloadSize + partitionBufferSize);

  // No more cached payloads after spill.
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  auto blockPid1 =
      takeRows({inputVector1_, inputVector1_, inputVector1_}, {{0, 2, 4, 6, 8}, {0, 2, 4, 6, 8}, {0, 2, 4, 6, 8}});
  auto blockPid2 =
      takeRows({inputVector1_, inputVector1_, inputVector1_}, {{1, 3, 5, 7, 9}, {1, 3, 5, 7, 9}, {1, 3, 5, 7, 9}});

  // Stop and verify.
  shuffleWriteReadMultiBlocks(*shuffleWriter, 2, {blockPid1, blockPid2});
}

INSTANTIATE_TEST_SUITE_P(
    SinglePartitioningShuffleWriterGroup,
    GpuSinglePartitioningShuffleWriterTest,
    ::testing::ValuesIn(getTestParams()));

INSTANTIATE_TEST_SUITE_P(
    RoundRobinPartitioningShuffleWriterGroup,
    GpuRoundRobinPartitioningShuffleWriterTest,
    ::testing::ValuesIn(getTestParams()));

INSTANTIATE_TEST_SUITE_P(
    HashPartitioningShuffleWriterGroup,
    GpuHashPartitioningShuffleWriterTest,
    ::testing::ValuesIn(getTestParams()));

INSTANTIATE_TEST_SUITE_P(
    RangePartitioningShuffleWriterGroup,
    GpuRangePartitioningShuffleWriterTest,
    ::testing::ValuesIn(getTestParams()));

} // namespace gluten

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new gluten::GpuVeloxShuffleWriterTestEnvironment);
  return RUN_ALL_TESTS();
}
