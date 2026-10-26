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
package org.apache.spark.sql.sources

import org.apache.spark.SparkException
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.util.Utils

import java.io.File

class GlutenCreateTableAsSelectSuite extends CreateTableAsSelectSuite with GlutenSQLTestsBaseTrait {

  // Same as the Spark test, but skipped when removing the write permission from a directory
  // does not stop writes to it, e.g. when the tests run as root like in Gluten's CI.
  // TODO: find a way to run this test in CI as well, see
  // https://github.com/apache/gluten/issues/13569
  testGluten("CREATE TABLE USING AS SELECT based on the file without write permission") {
    // setWritable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)
    val dir = Utils.createTempDir()
    val childPath = new File(dir, "child")
    dir.setWritable(false)
    try {
      assume(
        !childPath.mkdir(),
        "The write permission of a directory is not enforced for this user, e.g. root")

      withTable("jsonTable") {
        val e = intercept[SparkException] {
          sql(s"""
                 |CREATE TABLE jsonTable
                 |USING json
                 |OPTIONS (
                 |  path '${childPath.toURI}'
                 |) AS
                 |SELECT a, b FROM jt
           """.stripMargin)
          sql("SELECT a, b FROM jsonTable").collect()
        }
        assert(e.getMessage().contains("Job aborted"))
      }
    } finally {
      dir.setWritable(true)
      Utils.deleteRecursively(dir)
    }
  }
}
