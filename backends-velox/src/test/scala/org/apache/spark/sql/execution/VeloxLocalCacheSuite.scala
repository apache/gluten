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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.{BasicScanExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.SparkConf

import java.io.File

class VeloxLocalCacheSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/parquet-for-read"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(VeloxConfig.LOAD_QUANTUM.key, "8m")
      .set(VeloxConfig.COLUMNAR_VELOX_CACHE_ENABLED.key, "true")
      .set(VeloxConfig.COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED.key, "true")
  }

  testWithSpecifiedSparkVersion("read example parquet files", "3.5") {
    withTable("test_table") {
      val dir = new File(getClass.getResource(resourcePath).getFile)
      val files = dir.listFiles
      if (files != null) {
        files.foreach {
          file =>
            // Exclude parquet files failed to read by velox for now
            if (file.getName != "test-file-with-no-column-indexes-1.parquet") {
              val df = spark.read.parquet(file.getAbsolutePath)
              df.createOrReplaceTempView("test_table")
              runQueryAndCompare("select * from test_table") {
                checkGlutenPlan[BasicScanExecTransformer]
              }
            }
        }
      }
    }
  }
}
