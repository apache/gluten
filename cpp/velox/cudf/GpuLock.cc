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

#include "GpuLock.h"
#include <glog/logging.h>
#include <condition_variable>
#include <mutex>
#include <stdexcept>

namespace gluten {

namespace {
thread_local bool gThreadGpuHolder = false;

struct GpuLockState {
  std::mutex gGpuMutex;
  std::condition_variable gGpuCv;
  size_t maxConcurrentTasks{1};
  size_t activeTasks{0};
};

GpuLockState& getGpuLockState() {
  static GpuLockState gGpuLockState;
  return gGpuLockState;
}
} // namespace

void configureGpuTaskConcurrency(size_t maxConcurrentTasks) {
  if (maxConcurrentTasks <= 0) {
    throw std::invalid_argument("configureGpuTaskConcurrency() requires maxConcurrentTasks > 0");
  }

  std::lock_guard<std::mutex> lock(getGpuLockState().gGpuMutex);
  getGpuLockState().maxConcurrentTasks = maxConcurrentTasks;
  getGpuLockState().gGpuCv.notify_all();
}

void lockGpu() {
  if (gThreadGpuHolder) {
    return;
  }

  std::unique_lock<std::mutex> lock(getGpuLockState().gGpuMutex);
  getGpuLockState().gGpuCv.wait(
      lock, [] { return getGpuLockState().activeTasks < getGpuLockState().maxConcurrentTasks; });

  ++getGpuLockState().activeTasks;
  gThreadGpuHolder = true;
}

bool tryLockGpu() {
  if (gThreadGpuHolder) {
    return true;
  }

  std::unique_lock<std::mutex> lock(getGpuLockState().gGpuMutex);

  // Check if a permit is available without blocking
  if (getGpuLockState().activeTasks < getGpuLockState().maxConcurrentTasks) {
    ++getGpuLockState().activeTasks;
    gThreadGpuHolder = true;
    return true;
  }

  return false;
}

void unlockGpu() {
  if (!gThreadGpuHolder) {
    LOG(INFO) << "unlockGpu() called by non-owner thread!" << std::endl;
    return;
  }

  gThreadGpuHolder = false;

  {
    std::lock_guard<std::mutex> lock(getGpuLockState().gGpuMutex);
    if (getGpuLockState().activeTasks == 0) {
      throw std::runtime_error("unlockGpu() called with no active GPU tasks!");
    }

    --getGpuLockState().activeTasks;
  }

  getGpuLockState().gGpuCv.notify_one();
}

} // namespace gluten
