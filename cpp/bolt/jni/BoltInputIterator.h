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

#include "jni/JniCommon.h"

namespace gluten {

class BoltJniColumnarBatchIterator : public JniColumnarBatchIterator {
 public:
  explicit BoltJniColumnarBatchIterator(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      Runtime* runtime,
      bool parallelEnabled,
      std::optional<int32_t> iteratorIndex = std::nullopt);

  ~BoltJniColumnarBatchIterator() override;

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  void getContextClassLoader(JNIEnv* env);
  void getTaskContext(JNIEnv* env);
  void setClassLoader(JNIEnv* env);
  void setTaskContext(JNIEnv* env);

  JavaVM* vm_{nullptr};
  bool parallelEnabled_{false};
  jobject jSparkTaskContext_{nullptr};
  jclass jSparkTaskUtilClass_{nullptr};
  jobject jContextClassLoader_{nullptr};
};

class ShuffleReaderWrapperedIterator : public BoltJniColumnarBatchIterator {
 public:
  static std::unique_ptr<ShuffleReaderWrapperedIterator> tryFrom(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      Runtime* runtime,
      bool parallelEnabled,
      std::optional<int32_t> iteratorIndex = std::nullopt);

  explicit ShuffleReaderWrapperedIterator(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      Runtime* runtime,
      bool parallelEnabled,
      std::optional<int32_t> iteratorIndex = std::nullopt);

  ShuffleReaderWrapperedIterator(const ShuffleReaderWrapperedIterator&) = delete;
  ShuffleReaderWrapperedIterator(ShuffleReaderWrapperedIterator&&) = delete;
  ShuffleReaderWrapperedIterator& operator=(const ShuffleReaderWrapperedIterator&) = delete;
  ShuffleReaderWrapperedIterator& operator=(ShuffleReaderWrapperedIterator&&) = delete;

  ~ShuffleReaderWrapperedIterator() override;

  void markAsOffloaded();

  void updateMetrics(
      int64_t numRows,
      int64_t numBatchesTotal,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t totalReadTime);

  std::string getRawReaderInfo();

  std::shared_ptr<StreamReader> getStreamReader();

 private:
  struct ShuffleReaderHandles {
    jobject wrapper;
    jobject streamReader;
  };

  static ShuffleReaderHandles resolveShuffleReaderHandles(JNIEnv* env, jobject iterator);

  JavaVM* shuffleVm_{nullptr};
  jobject jShuffleReaderIteratorWrapper_{nullptr};
  jmethodID markAsOffloadedMethod_{nullptr};
  jmethodID updateMetricsMethod_{nullptr};
  jmethodID getReaderInfoMethod_{nullptr};
  std::shared_ptr<StreamReader> streamReader_{nullptr};
};

void registerBoltInputIteratorFactory();

} // namespace gluten
