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

#include <cstddef>
#include <thread>

namespace gluten {

/// Configure the maximum number of concurrent GPU tasks.
/// Must be greater than 0.
void configureGpuTaskConcurrency(size_t maxConcurrentTasks);

/// Acquire a GPU execution permit (reentrant within the same thread).
void lockGpu();

/// Try to acquire a GPU execution permit without blocking (reentrant within the same thread).
/// Returns true if the permit was acquired, false otherwise.
bool tryLockGpu();

/// Release a GPU execution permit held by the current thread.
void unlockGpu();

} // namespace gluten
