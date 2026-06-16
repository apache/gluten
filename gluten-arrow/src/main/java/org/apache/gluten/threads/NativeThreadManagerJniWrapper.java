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
 * JNI bridge for native ThreadManager creation and release.
 *
 * <p>Each Spark task creates one native ThreadManager (via {@link #create}) that wraps a {@link
 * NativeThreadInitializer}. The returned handle is passed to the native Runtime factory so it can
 * install thread lifecycle callbacks on its worker executors.
 */
public class NativeThreadManagerJniWrapper {
  private NativeThreadManagerJniWrapper() {}

  /**
   * Create a native ThreadManager for the given backend type.
   *
   * @param backendType the backend kind string (e.g., "velox").
   * @param initializer the Java-side callback invoked on worker thread create/destroy.
   * @return opaque native handle.
   */
  public static native long create(String backendType, NativeThreadInitializer initializer);

  /**
   * Release a native ThreadManager previously created with {@link #create}.
   *
   * @param handle the native handle returned by {@link #create}.
   */
  public static native void release(long handle);
}
