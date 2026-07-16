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
package org.apache.gluten.jni

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

class BoltNativeLoaderSuite extends AnyFunSuite {
  private lazy val loaderPath = {
    val configuredPath = sys.props
      .get("bolt.native.loader.path")
      .getOrElse(cancel("bolt.native.loader.path is required"))
    val path = Paths.get(configuredPath)
    if (!Files.isRegularFile(path)) {
      cancel(s"Bolt native loader does not exist: $configuredPath")
    }
    val canonicalPath = path.toRealPath().toString
    System.load(canonicalPath)
    canonicalPath
  }

  test("promote a JVM-loaded library to global visibility") {
    BoltJniLibLoader.nativePromoteLibrary(loaderPath)
    BoltJniLibLoader.nativePromoteLibrary(loaderPath)
  }

  test("reject promotion of a library that is not loaded") {
    val missingPath = s"$loaderPath.does-not-exist"
    intercept[UnsatisfiedLinkError] {
      BoltJniLibLoader.nativePromoteLibrary(missingPath)
    }
  }

  test("reject a null promotion path") {
    val _ = loaderPath
    intercept[NullPointerException] {
      BoltJniLibLoader.nativePromoteLibrary(null)
    }
  }
}
