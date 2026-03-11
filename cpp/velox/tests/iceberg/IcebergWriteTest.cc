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

#include "compute/iceberg/IcebergWriter.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
namespace gluten {

class VeloxIcebergWriteTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    parquet::registerParquetWriterFactory();
    Type::registerSerDe();
    dwio::common::registerFileSinks();
    filesystems::registerLocalFileSystem();
  }
  std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{exec::test::TempDirectoryPath::create()};

  std::shared_ptr<memory::MemoryPool> connectorPool_ = rootPool_->addAggregateChild("connector");
};

TEST_F(VeloxIcebergWriteTest, write) {
  auto vector = makeRowVector({makeFlatVector<int8_t>({1, 2}), makeFlatVector<int16_t>({1, 2})});
  auto tmpPath = tmpDir_->getPath();
  std::vector<connector::hive::iceberg::IcebergPartitionSpec::Field> fields;
  auto partitionSpec = std::make_shared<const connector::hive::iceberg::IcebergPartitionSpec>(0, fields);

  gluten::IcebergNestedField root;
  root.set_id(0);
  gluten::IcebergNestedField* child1 = root.add_children();
  child1->set_id(1);
  gluten::IcebergNestedField* child2 = root.add_children();
  child2->set_id(2);

  auto writer = std::make_unique<IcebergWriter>(
      asRowType(vector->type()),
      1,
      tmpPath + "/iceberg_write_test_table",
      common::CompressionKind::CompressionKind_ZSTD,
      0, // partitionId
      0, // taskId
      folly::to<std::string>(folly::Random::rand64()), // operationId
      partitionSpec,
      root,
      std::unordered_map<std::string, std::string>(),
      pool_,
      connectorPool_);
  auto batch = VeloxColumnarBatch(vector);
  writer->write(batch);
  auto commitMessage = writer->commit();
  EXPECT_EQ(commitMessage.size(), 1);
}
} // namespace gluten
