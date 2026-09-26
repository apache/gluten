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
package org.apache.spark.util

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

class SparkDirectoryUtilSuite extends AnyFunSuite {

  test("namespace fails fast naming the configured local dirs when none can be created") {
    // A regular file cannot host a local directory, so directory creation fails
    // for every configured root. Build an isolated instance so the test does not
    // depend on the process-wide singleton's init() ordering.
    val notADir = Files.createTempFile("gluten-not-a-dir", ".tmp")
    notADir.toFile.deleteOnExit()
    val util = SparkDirectoryUtil.createForTesting(Array(notADir.toAbsolutePath.toString))
    val exception = intercept[IllegalStateException] {
      util.namespace("test-namespace")
    }
    assert(exception.getMessage.contains("test-namespace"))
    assert(exception.getMessage.contains(notADir.toAbsolutePath.toString))
  }

  test("Namespace rejects an empty parent list") {
    val exception = intercept[IllegalStateException] {
      new Namespace(Array.empty[File], "test-namespace")
    }
    assert(exception.getMessage.contains("test-namespace"))
  }

  test("Namespace creates child directories under an available root") {
    val root = Files.createTempDirectory("gluten-usable-root").toFile
    try {
      val namespace = new Namespace(Array(root), "test-namespace")
      val child = namespace.mkChildDirRoundRobin("child-dir")
      assert(child.isDirectory)
      assert(child.getCanonicalPath.startsWith(root.getCanonicalPath))
    } finally {
      Utils.deleteRecursively(root)
    }
  }
}
