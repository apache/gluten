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

#pragma once

#include <arrow/c/abi.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include "memory/ColumnarBatch.h"

namespace gluten {

class BoltDataSource {
 public:
  BoltDataSource(const std::string& filePath, std::shared_ptr<arrow::Schema> schema)
      : filePath_(filePath), schema_(schema) {}

  virtual ~BoltDataSource() = default;

  virtual void init(const std::unordered_map<std::string, std::string>& sparkConfs) {}
  virtual void inspectSchema(struct ArrowSchema* out) = 0;
  virtual void write(const std::shared_ptr<ColumnarBatch>& cb) {}
  virtual void close() {}
  virtual std::shared_ptr<arrow::Schema> getSchema() = 0;

 private:
  std::string filePath_;
  std::shared_ptr<arrow::Schema> schema_;
};

} // namespace gluten
