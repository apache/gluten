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
package org.apache.gluten.execution

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions.col

class VeloxDeltaEmptyWriteSuite extends QueryTest with DeltaSQLCommandTest {

  Seq(true, false).foreach {
    collectStats =>
      test(s"an empty native write writes a readable file, collectStats=$collectStats") {
        withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> collectStats.toString) {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              // An input without partitions, for which Spark still runs one write task.
              spark.range(10).toDF("id").limit(0).write.format("delta").save(path)
              // An input whose partitions are all empty.
              spark
                .range(0, 10, 1, 2)
                .toDF("id")
                .where(col("id") < 0)
                .write
                .format("delta")
                .mode("append")
                .save(path)
              spark.range(0, 10, 1, 2).toDF("id").write.format("delta").mode("append").save(path)

              // As in Spark, the first task of each empty write writes a file that carries the
              // schema: two such files and two data files.
              val parquetFiles = dir.listFiles().filter(_.getName.endsWith(".parquet")).toSeq
              assert(
                parquetFiles.size == 4 && parquetFiles.forall(_.length > 0),
                parquetFiles.map(f => f.getName -> f.length))
              // As in Delta, the log keeps the empty files only without statistics to tell that
              // they hold no rows.
              val tableFiles = DeltaLog.forTable(spark, path).update().allFiles.collect()
              assert(tableFiles.length == (if (collectStats) 2 else 4), tableFiles.toSeq)
              checkAnswer(spark.read.format("delta").load(path), (0 until 10).map(Row(_)))
              // Every Parquet file in the table directory can be read, not only those in the log.
              checkAnswer(spark.read.parquet(path), (0 until 10).map(Row(_)))
          }
        }
      }
  }
}
