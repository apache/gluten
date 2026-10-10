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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.GlutenSQLTestsTrait

import org.apache.hadoop.fs.Path

class GlutenDataSourceScanExecRedactionSuite
  extends DataSourceScanExecRedactionSuite
  with GlutenSQLTestsTrait {

  // Gluten replaces FileSourceScanExec with FileSourceScanExecTransformer,
  // so "FileScan" is not in the explain output.
  testGluten("explain is redacted using SQLConf") {
    withTempDir {
      dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
        val df = spark.read.orc(basePath)
        val replacement = "*********"

        assert(isIncluded(df.queryExecution, replacement))
        assert(isIncluded(df.queryExecution, "FileSourceScanExecTransformer"))
        assert(!isIncluded(df.queryExecution, "file:/"))
    }
  }

  // Gluten replaces FileSourceScanExec with FileSourceScanExecTransformer
  testGluten("SPARK-31793: FileSourceScanExec metadata should contain limited file paths") {
    withTempPath {
      path =>
        val dataDirName = scala.util.Random.alphanumeric.take(100).toList.mkString
        val dataDir = new java.io.File(path, dataDirName)
        dataDir.mkdir()

        val partitionCol = "partitionCol"
        spark
          .range(10)
          .select("id", "id")
          .toDF("value", partitionCol)
          .write
          .partitionBy(partitionCol)
          .orc(dataDir.getCanonicalPath)
        val paths =
          (0 to 9).map(i => new java.io.File(dataDir, s"$partitionCol=$i").getCanonicalPath)
        val plan = spark.read.orc(paths: _*).queryExecution.executedPlan
        val location = plan.collectFirst {
          case f: FileSourceScanExecTransformer => f.metadata("Location")
        }
        assert(location.isDefined)
        assert(location.get.contains(paths.head))
        assert(location.get.contains("(10 paths)"))
        assert(location.get.indexOf('[') > -1)
        assert(location.get.indexOf(']') > -1)

        val pathsInLocation = location.get
          .substring(location.get.indexOf('[') + 1, location.get.indexOf(']'))
          .split(", ")
          .toSeq
        assert(pathsInLocation.size == 2)
        assert(pathsInLocation.exists(_.contains("...")))
    }
  }
}

class GlutenDataSourceV2ScanExecRedactionSuite
  extends DataSourceV2ScanExecRedactionSuite
  with GlutenSQLTestsTrait {

  // Gluten replaces BatchScanExec, so "BatchScan orc" is not in explain output.
  testGluten("explain is redacted using SQLConf") {
    withTempDir {
      dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
        val df = spark.read.orc(basePath)
        val replacement = "*********"

        assert(isIncluded(df.queryExecution, replacement))
        assert(isIncluded(df.queryExecution, "BatchScanExecTransformer"))
        assert(!isIncluded(df.queryExecution, "file:/"))
    }
  }

  // Gluten replaces BatchScanExec with BatchScanExecTransformer (orc/parquet only, json falls back)
  testGluten("FileScan description") {
    Seq("orc", "parquet").foreach {
      format =>
        withTempPath {
          path =>
            val dir = path.getCanonicalPath
            spark.range(0, 10).write.format(format).save(dir)
            val df = spark.read.format(format).load(dir)
            withClue(s"Source '$format':") {
              assert(isIncluded(df.queryExecution, "ReadSchema"))
              assert(isIncluded(df.queryExecution, "BatchScanExecTransformer"))
              assert(isIncluded(df.queryExecution, "PushedFilters"))
              assert(isIncluded(df.queryExecution, "Location"))
            }
        }
    }
  }
}
