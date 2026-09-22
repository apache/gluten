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
package org.apache.spark.sql

import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

class GlutenTestsBaseTraitSuite extends AnyFunSuite {

  private class TestPaths extends GlutenSQLTestsTrait {
    def defaultDirectory: String = rootPath + "unit-tests-working-home"
    def directories: (String, String, String) = (basePath, warehouse, metaStorePathAbsolute)
  }

  private def withTestDirectory(directory: Option[String])(f: TestPaths => Unit): Unit = {
    val previous = sys.props.remove("gluten.test.dir")
    try {
      directory.foreach(value => sys.props.put("gluten.test.dir", value))
      f(new TestPaths)
    } finally {
      previous match {
        case Some(value) => sys.props.put("gluten.test.dir", value)
        case None => sys.props.remove("gluten.test.dir")
      }
    }
  }

  test("keep the default test directory when gluten.test.dir is unset") {
    withTestDirectory(None) {
      paths =>
        val base = paths.defaultDirectory
        assert(paths.directories == ((base, base + "/spark-warehouse", base + "/meta")))
    }
  }

  test("create the test working directory under gluten.test.dir") {
    withTestDirectory(Some("custom-test-directory")) {
      paths =>
        assert(paths.directories == ((
          "custom-test-directory/unit-tests-working-home",
          "custom-test-directory/unit-tests-working-home/spark-warehouse",
          "custom-test-directory/unit-tests-working-home/meta")))
    }
  }

  test("reset only the test working directory and preserve other files in gluten.test.dir") {
    val parent = Files.createTempDirectory("gluten-test-dir").toFile
    try {
      val retained = new File(parent, "keep")
      val working = new File(parent, "unit-tests-working-home")
      val stale = new File(working, "stale")
      FileUtils.touch(retained)
      FileUtils.touch(stale)
      withTestDirectory(Some(parent.getAbsolutePath)) {
        paths =>
          paths.prepareWorkDir()
          assert(retained.isFile)
          assert(!stale.exists())
          assert(new File(working, "spark-warehouse").isDirectory)
          assert(new File(working, "meta").isDirectory)
      }
    } finally {
      FileUtils.deleteDirectory(parent)
    }
  }
}
