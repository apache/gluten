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

#include <functional>
#include "jni/JniCommon.h"
#include "bolt/exec/Task.h"

namespace gluten {

class TaskStatusListener {
 public:
  ~TaskStatusListener();

  void addTask(int64_t taskAttemptId, std::weak_ptr<bytedance::bolt::exec::Task> task);

  void removeTask(int64_t taskAttemptId);

  void startListener();

  static TaskStatusListener* getInstance();

 private:
  static TaskStatusListener instance_;

  void listen();

  struct TaskContext {
    jobject jTaskContext;
    std::weak_ptr<bytedance::bolt::exec::Task> boltTask;

    TaskContext(jobject jTaskContext, std::weak_ptr<bytedance::bolt::exec::Task> boltTask)
        : jTaskContext(jTaskContext), boltTask(std::move(boltTask)) {}
  };


  std::once_flag threadStartedFlag_;
  std::thread listenerThread_;
  enum class ThreadStatus { UNSTARTED, RUNNING, STOPPED };
  std::atomic<ThreadStatus> status_{ThreadStatus::UNSTARTED};

  std::mutex lock_;
  std::map<int64_t, TaskContext> tasks_;

  std::map<int64_t, bytedance::bolt::ContinueFuture> cancelFutures_;
};

} // namespace gluten