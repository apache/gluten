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

/**
 * Java-side callback invoked by native code when a managed worker thread is created or destroyed.
 *
 * <p>Implementations are responsible for installing per-thread context — such as {@link
 * org.apache.spark.TaskContext} — so that native worker threads behave as proper child threads of
 * the Spark task.
 *
 * <p>Implementations must be thread-safe; {@link #initialize} and {@link #destroy} may be called
 * concurrently from different native threads.
 */
public interface NativeThreadInitializer {
  /**
   * Called when a native worker thread is about to start executing tasks.
   *
   * @param threadName a human-readable name identifying the thread.
   */
  void initialize(String threadName);

  /**
   * Called when a native worker thread is about to be returned to the pool or destroyed. Must not
   * detach the JNI thread — the thread may be reused.
   *
   * @param threadName the same name passed to {@link #initialize}.
   */
  void destroy(String threadName);
}
