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
import org.apache.gluten.execution.HashAggregateExecTransformer
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.task.TaskResources

import org.apache.hadoop.fs.Path

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class GlutenDeltaStatsLifecycleSuite extends GlutenQueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", "2")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite", "true")
      .set("spark.databricks.delta.stats.collect", "true")
      .set("spark.databricks.delta.optimizeWrite.enabled", "false")
      .set("spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet", "false")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
  }

  private def statisticsThreads: Set[Long] =
    Thread.getAllStackTraces.keySet().asScala
      .filter(_.getName.startsWith("Gluten Delta Statistics Writer - "))
      .map(_.getId)
      .toSet

  private def withNativeStatistics(path: String)(write: => Unit): Unit = {
    val before = statisticsThreads
    val plans = new ConcurrentLinkedQueue[SparkPlan]()
    val completionChecks = new ConcurrentLinkedQueue[() => Unit]()
    val observer: (Path, SparkPlan) => Unit = (statsPath, plan) => {
      if (statsPath.toUri.getPath == path) {
        plans.add(plan)
        val context = TaskContext.get()
        val usage = TaskResources.getSharedUsage()
        completionChecks.add(
          () => {
            assert(context.isCompleted())
            assert(usage.current() == 0)
          })
      }
    }
    // The shared Delta source directory also compiles for backends without this Velox class.
    val moduleClass = Class.forName("org.apache.spark.sql.delta.stats.GlutenDeltaJobStatsTracker$")
    val module = moduleClass.getField("MODULE$").get(null)
    val observe = moduleClass.getMethod(
      "withStatsPlanObserver",
      classOf[Function2[Path, SparkPlan, Unit]],
      classOf[Function0[Unit]])
    val operation: () => Unit = () => write
    try {
      observe.invoke(module, observer, operation)
    } catch {
      case e: InvocationTargetException if e.getCause != null => throw e.getCause
    }
    assert(!plans.isEmpty, "The write did not exercise native Delta statistics")
    plans.asScala.foreach {
      plan =>
        assert(
          plan.exists(_.isInstanceOf[HashAggregateExecTransformer]),
          s"Expected a native statistics aggregate, but got:\n${plan.treeString}")
    }
    eventually(timeout(15.seconds), interval(100.millis)) {
      completionChecks.asScala.foreach(_())
      assert(statisticsThreads.diff(before).isEmpty, "Statistics workers survived task completion")
    }
  }

  test("aborted Delta writes close native statistics before task runtimes") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        val schema = new StructType().add("id", LongType, nullable = false)
        DeltaLog.forTable(spark, path).startTransaction()
          .commit(Seq(Metadata(schemaString = schema.json)), DeltaOperations.ManualUpdate)
        spark.read.format("delta").load(path).write.format("delta").mode("overwrite").save(path)
        val invalid = spark.createDataFrame(Seq(Row(1L), Row(null)).asJava, schema.asNullable)
        withNativeStatistics(path) {
          val failure = intercept[Exception] {
            invalid.write.format("delta").mode("append").save(path)
          }
          assert(
            Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
              .exists(_.isInstanceOf[InvariantViolationException]),
            failure.toString)
        }
        assert(spark.read.format("delta").load(path).count() == 0)
        withNativeStatistics(path) {
          Seq(2L, 3L).toDF("id").write.format("delta").mode("append").save(path)
        }
        assert(spark.read.format("delta").load(path).collect().toSet == Set(Row(2L), Row(3L)))
    }
  }

  test("multi-file Delta statistics close workers after a successful task") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        withNativeStatistics(path) {
          Seq((0L, 0), (1L, 0), (2L, 0), (3L, 0)).toDF("id", "part").coalesce(1)
            .write.format("delta").partitionBy("part").option("maxRecordsPerFile", "1")
            .save(path)
        }
        val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
        assert(files.length == 4)
        val records = spark.read.json(files.map(_.stats).toSeq.toDS())
          .select("numRecords").as[Long].collect()
        assert(records.length == 4 && records.forall(_ == 1))
    }
  }
}
