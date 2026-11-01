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

// This File includes common helper functions with Arrow dependency.

#pragma once

#include <arrow/type_fwd.h>

#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class GlutenParquetWriterFactory : public facebook::velox::parquet::ParquetWriterFactory {
 public:
  std::shared_ptr<facebook::velox::dwio::common::FormatSpecificOptions> createFormatOptions(
      const facebook::velox::config::ConfigBase& connectorConfig,
      const facebook::velox::config::ConfigBase& session) const override;
};

std::shared_ptr<facebook::velox::dwio::common::WriterOptions> makeParquetWriteOption(
    const std::unordered_map<std::string, std::string>& sparkConfs);

facebook::velox::RowTypePtr makeParquetWriteSchema(const std::shared_ptr<::arrow::Schema>& schema);

facebook::velox::RowVectorPtr prepareParquetWriteInput(const facebook::velox::RowVectorPtr& input);

facebook::velox::RowVectorPtr makePartitionIdInput(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<uint32_t>& partitionChannels);

} // namespace gluten
