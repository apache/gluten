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
package org.apache.gluten.runtime

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.NativeMemoryManager
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.sql.internal.{GlutenConfigUtil, SQLConf}
import org.apache.spark.task.TaskResource

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._ // for 2.12

trait Runtime {
  def memoryManager(): NativeMemoryManager
  def getHandle(): Long
}

object Runtime {
  private[runtime] def apply(backendName: String, name: String): Runtime with TaskResource = {
    new RuntimeImpl(backendName, name, new util.HashMap[String, String]())
  }

  private[runtime] def apply(
      backendName: String,
      name: String,
      extraConf: util.Map[String, String]): Runtime with TaskResource = {
    new RuntimeImpl(backendName, name, extraConf)
  }

  private class RuntimeImpl(backendName: String, name: String, extraConf: util.Map[String, String])
    extends Runtime
    with TaskResource {

    private val nmm: NativeMemoryManager = NativeMemoryManager(backendName, name)
    private val handle = RuntimeJniWrapper.createRuntime(
      backendName,
      nmm.getHandle(),
      ConfigUtil.serialize(
        (GlutenConfig
          .getNativeSessionConf(
            backendName,
            GlutenConfigUtil.parseConfig(SQLConf.get.getAllConfs)) ++ extraConf.asScala).asJava)
    )

    private val released: AtomicBoolean = new AtomicBoolean(false)

    override def getHandle(): Long = handle

    override def memoryManager(): NativeMemoryManager = nmm

    override def release(): Unit = {
      if (!released.compareAndSet(false, true)) {
        throw new GlutenException(
          s"Runtime instance already released: $handle, ${resourceName()}, ${priority()}")
      }
      RuntimeJniWrapper.releaseRuntime(handle)

    }

    override def priority(): Int = 30

    override def resourceName(): String = s"runtime"
  }
}
