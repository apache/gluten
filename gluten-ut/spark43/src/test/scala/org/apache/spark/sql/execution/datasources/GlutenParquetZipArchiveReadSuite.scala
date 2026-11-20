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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait}
import org.apache.spark.sql.execution.FileSourceScanExec

import java.io.File

class GlutenParquetZipArchiveReadSuite
  extends ParquetZipArchiveReadSuite
  with GlutenSQLTestsTrait {

  private def scans(df: DataFrame): (Int, Int) = {
    val plans = getExecutedPlan(df)
    (
      plans.count(_.isInstanceOf[FileSourceScanExec]),
      plans.count(_.isInstanceOf[FileSourceScanExecTransformer]))
  }

  testGluten("archive file scans fall back to Spark's archive reader") {
    // Keep the global ANSI fallback from masking which scan the plan uses.
    withSQLConf(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      val df = sampleDf((1, "Alice"), (2, "Bob"))
      withTempDir {
        root =>
          // Read the root directory, so the archive is only found among the listed files.
          val subDir = new File(root, "nested")
          subDir.mkdir()
          writeArchive(
            new File(subDir, s"data.${archiveExtensions.head}"),
            Seq(entryName(0) -> encodeFile(df)))
          val archiveRead = read(root.getCanonicalPath, Map("recursiveFileLookup" -> "true"))
          checkAnswer(archiveRead, df)
          assert(scans(archiveRead) == ((1, 0)), archiveRead.queryExecution.executedPlan)
      }

      // A directory of loose Parquet files is still scanned natively.
      withTempDir {
        root =>
          df.write.mode("overwrite").parquet(root.getCanonicalPath)
          val looseRead = read(root.getCanonicalPath)
          checkAnswer(looseRead, df)
          assert(scans(looseRead) == ((0, 1)), looseRead.queryExecution.executedPlan)
      }
    }
  }
}
