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

import org.apache.spark.task.{TaskResource, TaskResources}

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Scala wrapper around a native ThreadManager handle.
 *
 * Created once per Spark task and registered as a [[TaskResource]] so it is automatically released
 * when the task completes. The ThreadManager wraps a [[NativeThreadInitializer]] that propagates
 * task context to native worker threads spawned by folly executors.
 */
trait NativeThreadManager {

  /** @return opaque native handle passed to RuntimeJniWrapper#createRuntime. */
  def getHandle(): Long
}

object NativeThreadManager {
  private class Impl(
      private val backendName: String,
      private val initializer: NativeThreadInitializer)
    extends NativeThreadManager
    with TaskResource {
    private val handle = NativeThreadManagerJniWrapper.create(backendName, initializer)
    private val released = new AtomicBoolean(false)

    override def getHandle(): Long = handle

    override def release(): Unit = {
      if (!released.compareAndSet(false, true)) {
        throw new GlutenException(
          s"Thread manager instance already released: $handle, ${resourceName()}, ${priority()}")
      }
      NativeThreadManagerJniWrapper.release(handle)
    }

    // Release before MemoryManager (10) but after most other resources.
    override def priority(): Int = 20

    override def resourceName(): String = "ntm"
  }

  /**
   * Create a new NativeThreadManager and register it with the current Spark task's
   * [[TaskResources]] so it is automatically released when the task finishes.
   *
   * @param backendName
   *   the backend kind string (e.g., "velox").
   * @param initializer
   *   callback invoked when native worker threads are created / destroyed.
   */
  def apply(backendName: String, initializer: NativeThreadInitializer): NativeThreadManager = {
    TaskResources.addAnonymousResource(new Impl(backendName, initializer))
  }
}
