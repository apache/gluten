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
package org.apache.gluten.task;

import org.apache.spark.TaskContext;

/**
 * Callback surface that native code uses to obtain the current thread's Spark task attempt id via
 * JNI, without having to plumb it explicitly through every JNI entry point.
 *
 * <p>Native side reads the value from {@link TaskContext#get()} at the moment it is needed. Since
 * {@link TaskContext} is a per-thread ThreadLocal, this only returns a meaningful value when the
 * native call is running on the executor task thread (or on a JVM thread inheriting the same
 * ThreadLocal). It returns {@code -1L} on the driver or on a native worker thread that has no
 * associated Spark task.
 */
public final class TaskContextJniWrapper {
  private TaskContextJniWrapper() {}

  /**
   * Returns the current thread's Spark task attempt id, or {@code -1L} when there is no task
   * context on the calling thread.
   */
  public static long currentTaskAttemptId() {
    TaskContext tc = TaskContext.get();
    return tc == null ? -1L : tc.taskAttemptId();
  }
}
