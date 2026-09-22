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

import org.scalatest.funsuite.AnyFunSuite

class GlutenTestsBaseTraitSuite extends AnyFunSuite {

  private class TestPaths extends GlutenTestsBaseTrait {
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

  test("use gluten.test.dir for the test, warehouse and metastore directories") {
    withTestDirectory(Some("custom-test-directory")) {
      paths =>
        assert(paths.directories == ((
          "custom-test-directory",
          "custom-test-directory/spark-warehouse",
          "custom-test-directory/meta")))
    }
  }
}
