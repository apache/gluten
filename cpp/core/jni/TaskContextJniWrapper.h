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

#include <cstdint>

namespace gluten {

/**
 * Returns the current thread's Spark task attempt id by calling back into
 * org.apache.gluten.task.TaskContextJniWrapper#currentTaskAttemptId via JNI.
 *
 * Returns -1 when there is no Spark TaskContext on the calling thread (driver,
 * standalone native worker, etc.). Safe to call from any thread; the helper
 * attaches the current thread to the JVM as a daemon on demand.
 */
int64_t getCurrentSparkTaskAttemptId();

} // namespace gluten
