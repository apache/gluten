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

#include "jni/VeloxAllocationListener.h"
#include "jni/VeloxJavaException.h"

namespace gluten {
namespace {

class VeloxJniAllocationListener final : public SparkAllocationListener {
 public:
  VeloxJniAllocationListener(JavaVM* vm, jobject listener, const facebook::velox::VeloxException& fallback)
      : SparkAllocationListener(vm, listener), fallback_(fallback) {}

  void applyChange(Change& change) override {
    wrapJavaException([&] { SparkAllocationListener::applyChange(change); }, &fallback_);
  }

  bool reconcileUnreserve(int64_t bytes) override {
    return wrapJavaException([&] { return SparkAllocationListener::reconcileUnreserve(bytes); }, &fallback_);
  }

 private:
  const facebook::velox::VeloxException& fallback_;
};

} // namespace

std::unique_ptr<AllocationListener> createVeloxJniAllocationListener(JavaVM* vm, jobject listener) {
  // Allocate before any reservation callback. Copies share immutable state; the cause is nested per throw.
  static const auto fallback = makeVeloxJavaException(nullptr, "Java memory reservation callback failed");
  return std::make_unique<VeloxJniAllocationListener>(vm, listener, fallback);
}

} // namespace gluten
