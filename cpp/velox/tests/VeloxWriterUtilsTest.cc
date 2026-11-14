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

#include <arrow/c/abi.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <optional>

#include "utils/VeloxWriterUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox;

namespace gluten {

class VeloxWriterUtilsTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::shared_ptr<memory::MemoryPool> pool_ = memory::memoryManager()->addLeafPool();

  void exportMicros(const RowVectorPtr& input) {
    ArrowOptions options;
    options.timestampUnit = TimestampUnit::kMicro;
    ArrowArray out{};
    exportToArrow(input, out, pool_.get(), options);
    out.release(&out);
  }

  RowVectorPtr nullableTimestampRows(const TypePtr& timestampType) {
    auto values = BaseVector::create<FlatVector<Timestamp>>(timestampType, 2, pool_.get());
    values->set(0, Timestamp::fromMicros(-1));
    values->set(1, Timestamp(Timestamp::kMaxSeconds, 0));
    auto rows = std::make_shared<RowVector>(
        pool_.get(), ROW({"ts"}, {timestampType}), nullptr, 2, std::vector<VectorPtr>{values});
    rows->setNull(1, true);
    return rows;
  }
};

TEST_F(VeloxWriterUtilsTest, timestampSchemaWithoutFieldMetadata) {
  const auto ntz = ::arrow::timestamp(::arrow::TimeUnit::MICRO);
  const auto ltz = ::arrow::timestamp(::arrow::TimeUnit::MICRO, "America/Los_Angeles");
  const auto pair = ::arrow::struct_({::arrow::field("ntz", ntz), ::arrow::field("ltz", ltz)});
  const auto schema = ::arrow::schema(
      {::arrow::field("ntz", ntz),
       ::arrow::field("ltz", ltz),
       ::arrow::field("nested", pair),
       ::arrow::field("items", ::arrow::list(pair)),
       ::arrow::field("mapping", ::arrow::map(ntz, pair)),
       ::arrow::field("reverse", ::arrow::map(ltz, ntz)),
       ::arrow::field("decimal", ::arrow::decimal128(12, 2))});
  const auto pairType = ROW({"ntz", "ltz"}, {TIMESTAMP_UTC(), TIMESTAMP()});
  const auto expected =
      ROW({"ntz", "ltz", "nested", "items", "mapping", "reverse", "decimal"},
          {TIMESTAMP_UTC(),
           TIMESTAMP(),
           pairType,
           ARRAY(pairType),
           MAP(TIMESTAMP_UTC(), pairType),
           MAP(TIMESTAMP(), TIMESTAMP_UTC()),
           DECIMAL(12, 2)});
  const auto actual = makeParquetWriteSchema(schema);
  EXPECT_TRUE(actual->equivalent(*expected)) << actual->toString();
}

TEST_F(VeloxWriterUtilsTest, partitionIdsPreserveTimestampMicroseconds) {
  const std::vector<std::optional<int64_t>> values{
      -1, 0, 1, 1730619000123456, 1730619000123457, std::nullopt, -1, std::nullopt};
  const auto size = static_cast<vector_size_t>(values.size());
  for (const auto& keyType : std::vector<TypePtr>{TIMESTAMP_UTC(), TIMESTAMP()}) {
    const auto rowType = ROW({"part", "ltz", "data"}, {keyType, TIMESTAMP(), TIMESTAMP_UTC()});
    for (auto dictionary : {false, true}) {
      auto input = BaseVector::create<RowVector>(rowType, size, pool_.get());
      for (auto& child : input->children()) {
        auto timestamps = child->as<FlatVector<Timestamp>>();
        for (auto row = 0; row < size; ++row) {
          if (values[row].has_value()) {
            timestamps->set(row, Timestamp::fromMicros(*values[row]));
          } else {
            timestamps->setNull(row, true);
          }
        }
      }
      if (dictionary) {
        auto indices = allocateIndices(size, pool_.get());
        auto rawIndices = indices->asMutable<vector_size_t>();
        for (auto row = 0; row < size; ++row) {
          rawIndices[row] = size - row - 1;
        }
        input->childAt(0) = BaseVector::wrapInDictionary(nullptr, indices, size, input->childAt(0));
      }
      const auto normalized = makePartitionIdInput(input, {0});
      EXPECT_TRUE(
          normalized->type()->equivalent(*ROW({"part", "ltz", "data"}, {BIGINT(), TIMESTAMP(), TIMESTAMP_UTC()})));
      EXPECT_EQ(normalized->childAt(1), input->childAt(1));
      EXPECT_EQ(normalized->childAt(2), input->childAt(2));
      EXPECT_TRUE(input->type()->equivalent(*rowType));
      EXPECT_TRUE(input->childAt(0)->type()->equivalent(*keyType));
      const auto micros = normalized->childAt(0)->as<FlatVector<int64_t>>();
      for (auto row = 0; row < size; ++row) {
        const auto& expected = values[dictionary ? size - row - 1 : row];
        EXPECT_EQ(micros->isNullAt(row), !expected.has_value());
        if (expected.has_value()) {
          EXPECT_EQ(micros->valueAt(row), *expected);
        }
      }
      EXPECT_EQ(makePartitionIdInput(input, {}), input);
    }
  }
}

TEST_F(VeloxWriterUtilsTest, nonTimestampPartitionInputIsUnchanged) {
  auto input = BaseVector::create<RowVector>(ROW({"part", "data"}, {BIGINT(), TIMESTAMP_UTC()}), 0, pool_.get());
  EXPECT_EQ(makePartitionIdInput(input, {0}), input);
}

TEST_F(VeloxWriterUtilsTest, emptyNtzPartitionInput) {
  auto input = BaseVector::create<RowVector>(ROW({"part"}, {TIMESTAMP_UTC()}), 0, pool_.get());
  auto normalized = makePartitionIdInput(input, {0});
  EXPECT_EQ(normalized->size(), 0);
  EXPECT_TRUE(normalized->childAt(0)->type()->isBigint());
}

TEST_F(VeloxWriterUtilsTest, nullStructPayloadsAreNotExported) {
  for (const auto& timestampType : std::vector<TypePtr>{TIMESTAMP_UTC(), TIMESTAMP()}) {
    auto nested = nullableTimestampRows(timestampType);
    auto unaffected = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 2, pool_.get());
    unaffected->set(0, 0);
    unaffected->set(1, 1);
    auto input = std::make_shared<RowVector>(
        pool_.get(),
        ROW({"nested", "id"}, {nested->type(), BIGINT()}),
        nullptr,
        2,
        std::vector<VectorPtr>{nested, unaffected});
    auto prepared = prepareParquetWriteInput(input);
    EXPECT_NE(prepared, input);
    EXPECT_EQ(prepared->childAt(1), unaffected);
    EXPECT_TRUE(prepared->type()->equivalent(*input->type()));
    EXPECT_FALSE(nested->childAt(0)->isNullAt(1));
    EXPECT_EQ(nested->childAt(0)->as<FlatVector<Timestamp>>()->valueAt(1), Timestamp(Timestamp::kMaxSeconds, 0));
    for (auto row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(prepared->equalValueAt(input.get(), row, row));
    }
    EXPECT_NO_THROW(exportMicros(prepared));

    nested->childAt(0)->as<FlatVector<Timestamp>>()->set(0, Timestamp(Timestamp::kMaxSeconds, 0));
    EXPECT_THROW(exportMicros(prepareParquetWriteInput(input)), VeloxUserError);

    nested->setNull(1, false);
    EXPECT_EQ(prepareParquetWriteInput(input), input);
    EXPECT_THROW(exportMicros(prepareParquetWriteInput(input)), VeloxUserError);
  }
}

TEST_F(VeloxWriterUtilsTest, nullStructsInArraysMapsAndDictionaries) {
  auto nested = nullableTimestampRows(TIMESTAMP_UTC());
  auto keys = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 2, pool_.get());
  keys->set(0, 0);
  keys->set(1, 1);
  auto arrays = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(nested->type()),
      nullptr,
      2,
      allocateOffsets(2, pool_.get()),
      allocateSizes(2, pool_.get()),
      nested);
  auto maps = std::make_shared<MapVector>(
      pool_.get(),
      MAP(BIGINT(), nested->type()),
      nullptr,
      2,
      allocateOffsets(2, pool_.get()),
      allocateSizes(2, pool_.get()),
      keys,
      nested);
  for (auto row = 0; row < 2; ++row) {
    arrays->setOffsetAndSize(row, row, 1);
    maps->setOffsetAndSize(row, row, 1);
  }
  auto indices = allocateIndices(2, pool_.get());
  indices->asMutable<vector_size_t>()[0] = 1;
  indices->asMutable<vector_size_t>()[1] = 0;
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 2, nested);
  auto input = std::make_shared<RowVector>(
      pool_.get(),
      ROW({"a", "m", "d"}, {arrays->type(), maps->type(), nested->type()}),
      nullptr,
      2,
      std::vector<VectorPtr>{arrays, maps, dictionary});
  auto prepared = prepareParquetWriteInput(input);
  for (auto row = 0; row < input->size(); ++row) {
    EXPECT_TRUE(prepared->equalValueAt(input.get(), row, row));
  }
  EXPECT_NO_THROW(exportMicros(prepared));
  EXPECT_FALSE(nested->childAt(0)->isNullAt(1));
  EXPECT_EQ(input->childAt(0), arrays);
  EXPECT_EQ(input->childAt(1), maps);
  EXPECT_EQ(input->childAt(2), dictionary);
}

TEST_F(VeloxWriterUtilsTest, nonNullOuterPreservesNullableInner) {
  for (const auto& timestampType : std::vector<TypePtr>{TIMESTAMP_UTC(), TIMESTAMP()}) {
    auto inner = nullableTimestampRows(timestampType);
    auto outer = std::make_shared<RowVector>(
        pool_.get(), ROW({"inner"}, {inner->type()}), nullptr, 2, std::vector<VectorPtr>{inner});
    auto input = std::make_shared<RowVector>(
        pool_.get(), ROW({"outer"}, {outer->type()}), nullptr, 2, std::vector<VectorPtr>{outer});
    ASSERT_EQ(outer->rawNulls(), nullptr);
    auto prepared = prepareParquetWriteInput(input);
    auto preparedOuter = prepared->childAt(0)->as<RowVector>();
    auto preparedInner = preparedOuter->childAt(0)->as<RowVector>();
    EXPECT_EQ(preparedOuter->rawNulls(), nullptr);
    EXPECT_FALSE(preparedInner->isNullAt(0));
    EXPECT_TRUE(preparedInner->isNullAt(1));
    EXPECT_TRUE(preparedInner->childAt(0)->isNullAt(1));
    EXPECT_EQ(preparedInner->childAt(0)->as<FlatVector<Timestamp>>()->valueAt(0), Timestamp::fromMicros(-1));
    for (auto row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(prepared->equalValueAt(input.get(), row, row));
    }
    EXPECT_NO_THROW(exportMicros(prepared));
    EXPECT_EQ(outer->rawNulls(), nullptr);
    EXPECT_EQ(outer->childAt(0), inner);
    EXPECT_FALSE(inner->childAt(0)->isNullAt(1));
  }
}

} // namespace gluten
