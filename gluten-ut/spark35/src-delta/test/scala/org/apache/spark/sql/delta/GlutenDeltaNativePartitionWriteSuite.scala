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
package org.apache.spark.sql.delta

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class GlutenDeltaNativePartitionWriteSuite extends GlutenQueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite", "true")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.optimizeWrite.enabled", "false")
      .set("spark.databricks.delta.stats.collect", "false")
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.maxConcurrentOutputFileWriters", "0")
  }

  test("native Delta writes respect file limits for null partition keys") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    val schema = new StructType().add("id", LongType).add("part", IntegerType)
    val expected = Seq(
      Row(0L, null),
      Row(0L, null),
      Row(1L, null),
      Row(2L, null),
      Row(2L, null),
      Row(3L, 7),
      Row(3L, 7),
      Row(4L, 7))

    for (limit <- Seq(1, 2)) {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val plans = new ConcurrentLinkedQueue[SparkPlan]()
          val listener = new QueryExecutionListener {
            override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit =
              plans.add(qe.executedPlan)
            override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = {}
          }
          spark.listenerManager.register(listener)
          try {
            spark.createDataFrame(spark.sparkContext.parallelize(expected, 1), schema)
              .write.format("delta").partitionBy("part")
              .option("maxRecordsPerFile", limit.toString).save(path)
            spark.sparkContext.listenerBus.waitUntilEmpty(10000)
          } finally {
            spark.listenerManager.unregister(listener)
          }
          // This source also compiles for other backends, which do not provide these classes.
          assert(
            plans.asScala.exists(_.exists {
              case ExecutedCommandExec(command) =>
                Set("GlutenDeltaLeafRunnableCommand", "GlutenDeltaRunnableCommand")
                  .contains(command.getClass.getSimpleName)
              case plan => plan.getClass.getSimpleName == "GlutenDeltaLeafV2CommandExec"
            }),
            s"Expected a native Delta write:\n${plans.asScala.map(_.treeString).mkString}"
          )

          withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
            val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
            assert(files.exists(_.partitionValues("part") == null))
            val physicalRows = files.toSeq.flatMap {
              file =>
                val filePath = DeltaFileOperations.absolutePath(path, file.path).toString
                val rows = spark.read.parquet(filePath).select("id").collect().toSeq
                assert(rows.nonEmpty && rows.size <= limit, s"$filePath: ${rows.size} rows")
                val part = Option(file.partitionValues("part")).map(v => Int.box(v.toInt)).orNull
                rows.map(row => Row(row.getLong(0), part))
            }
            assert(rowCounts(physicalRows) == rowCounts(expected))
            val deltaRows = spark.read.format("delta").load(path).select("id", "part").collect()
            assert(rowCounts(deltaRows.toSeq) == rowCounts(expected))
          }
      }
    }
  }

  private def rowCounts(rows: Seq[Row]): Map[Row, Int] =
    rows.groupBy(identity).map { case (row, copies) => row -> copies.size }
}
