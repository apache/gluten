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
package org.apache.gluten.utils

import org.apache.gluten.jni.BoltJniLibLoader
import org.apache.gluten.spi.SharedLibraryLoader
import org.apache.gluten.utils.MySharedLibraryLoader.LOADED

import java.util.concurrent.atomic.AtomicBoolean

class MySharedLibraryLoader extends SharedLibraryLoader {
  override def accepts(osName: String, osVersion: String): Boolean = {
    osName == "My OS" && osVersion == "1.0"
  }

  /**
   * Load the required shared libraries using the given JniLibLoader.
   *
   * @param loader
   *   JniLibLoader to load the shared libraries
   */
  override def loadLib(loader: BoltJniLibLoader): Unit = {
    LOADED.set(true)
  }
}

object MySharedLibraryLoader {
  val LOADED = new AtomicBoolean(false)
}
