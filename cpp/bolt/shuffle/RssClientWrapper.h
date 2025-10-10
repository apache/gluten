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

#include "shuffle/rss/RssClient.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace gluten {
class RssClientWrapper : public bytedance::bolt::shuffle::sparksql::RssClient {
 public:
  RssClientWrapper(std::shared_ptr<::RssClient> rssClient) : rssClient_(std::move(rssClient)) {}

  virtual ~RssClientWrapper() {}

  // Push partition data to the RSS server.
  virtual int32_t pushPartitionData(int32_t partitionId, char* bytes, int64_t size) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return rssClient_->pushPartitionData(partitionId, bytes, size);
  }

  // Stop the RSS client.
  virtual void stop() override {
    std::lock_guard<std::mutex> lock(mutex_);
    rssClient_->stop();
  }

 private:
  std::shared_ptr<::RssClient> rssClient_;
  // [multi-thread spark]
  std::mutex mutex_;
};
} // namespace gluten