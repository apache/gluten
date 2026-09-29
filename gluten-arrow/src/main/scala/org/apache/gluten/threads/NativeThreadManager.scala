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
package org.apache.gluten.threads

import org.apache.gluten.exception.GlutenException

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Scala wrapper around a native ThreadManager handle.
 *
 * Created once per Spark task by [[org.apache.gluten.runtime.Runtime]]. The ThreadManager wraps a
 * [[NativeThreadInitializer]] that propagates task context to native worker threads spawned by
 * folly executors.
 */
trait NativeThreadManager {

  /** @return opaque native handle passed to RuntimeJniWrapper#createRuntime. */
  def getHandle(): Long

  /** Release the native ThreadManager handle. Called by Runtime during task completion. */
  def release(): Unit
}

object NativeThreadManager {
  private class Impl(
      private val backendName: String,
      private val initializer: NativeThreadInitializer)
    extends NativeThreadManager {
    private val handle = NativeThreadManagerJniWrapper.create(backendName, initializer)
    private val released = new AtomicBoolean(false)

    override def getHandle(): Long = handle

    override def release(): Unit = {
      if (!released.compareAndSet(false, true)) {
        throw new GlutenException(
          s"Thread manager instance already released: $handle")
      }
      NativeThreadManagerJniWrapper.release(handle)
    }
  }

  /**
   * Create a new NativeThreadManager. The caller (typically Runtime) is responsible for calling
   * `release()` when the manager is no longer needed.
   *
   * @param backendName
   *   the backend kind string (e.g., "velox").
   * @param initializer
   *   callback invoked when native worker threads are created / destroyed.
   */
  def apply(backendName: String, initializer: NativeThreadInitializer): NativeThreadManager = {
    new Impl(backendName, initializer)
  }
}
