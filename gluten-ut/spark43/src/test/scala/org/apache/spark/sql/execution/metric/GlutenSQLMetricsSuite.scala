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
package org.apache.spark.sql.execution.metric

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.execution.FileSourceScanLike
import org.apache.spark.sql.functions.col

class GlutenSQLMetricsSuite extends GlutenSQLTestsBaseTrait {

  test("numFiles metric should reflect partition pruning") {
    withTempDir {
      tempDir =>
        val testPath = tempDir.getCanonicalPath

        // Generate two files in two partitions
        spark
          .range(2)
          .withColumn("part", col("id") % 2)
          .write
          .format("parquet")
          .partitionBy("part")
          .mode("append")
          .save(testPath)

        // Read only one partition
        val query = spark.read.format("parquet").load(testPath).where("part = 1")
        val fileScans = query.queryExecution.executedPlan.collect {
          case f: FileSourceScanLike => f
        }

        // Force the query to read files and generate metrics
        query.queryExecution.executedPlan.execute().count()

        // Verify only one file was read
        assert(fileScans.size == 1)
        val numFilesAfterPartitionSkipping = fileScans.head.metrics.get("numFiles")
        assert(numFilesAfterPartitionSkipping.nonEmpty)
        assert(numFilesAfterPartitionSkipping.get.value == 1)
        assert(query.collect().toSeq == Seq(Row(1, 1)))
    }
  }
}
