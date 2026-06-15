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
package org.apache.gluten.threads;

import com.google.common.base.Preconditions;
import org.apache.spark.TaskContext;
import org.apache.spark.util.SparkTaskUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskChildThreadInitializer implements NativeThreadInitializer {
  private final TaskContext parentTaskContext;
  private final Map<String, String> childThreads = new ConcurrentHashMap<>();

  public TaskChildThreadInitializer(TaskContext parentTaskContext) {
    Preconditions.checkNotNull(parentTaskContext);
    this.parentTaskContext = parentTaskContext;
  }

  @Override
  public void initialize(String threadName) {
    final String javaThreadName = Thread.currentThread().getName();
    if (childThreads.put(threadName, javaThreadName) != null) {
      throw new IllegalStateException(
          String.format(
              "Task native child thread %s (Java name: %s) is already initialized",
              threadName, javaThreadName));
    }
    SparkTaskUtil.setTaskContext(parentTaskContext);
  }

  @Override
  public void destroy(String threadName) {
    if (childThreads.remove(threadName) == null) {
      throw new IllegalStateException(
          String.format("Task native thread %s is not initialized", threadName));
    }
    SparkTaskUtil.unsetTaskContext();
  }
}
