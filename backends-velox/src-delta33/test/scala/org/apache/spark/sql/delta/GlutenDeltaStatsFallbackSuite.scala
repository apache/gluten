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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{HashAggregateExecTransformer, RowToVeloxColumnarExec, VeloxColumnarToCarrierRowExec}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DeltaFileStatistics, DeltaJobStatisticsTracker, GlutenDeltaJobStatsTracker}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.json4s.{JInt, JNothing, JString}
import org.json4s.jackson.JsonMethods.parse

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class GlutenDeltaStatsFallbackSuite extends QueryTest with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "UTC")
      .set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "true")
      .set("spark.databricks.delta.optimizeWrite.enabled", "false")
  }

  // Disabling only aggregation leaves a native projection above a rejected Spark aggregate.
  for ((projectEnabled, aggregateEnabled) <- Seq((true, true), (false, true), (true, false))) {
    test(s"native writes preserve statistics: project=$projectEnabled, agg=$aggregateEnabled") {
      withSQLConf(
        GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> projectEnabled.toString,
        GlutenConfig.COLUMNAR_HASHAGG_ENABLED.key -> aggregateEnabled.toString,
        SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true"
      ) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            val schema = new StructType()
              .add("value", LongType)
              .add("all_null", LongType)
              .add("nested", new StructType().add("value", LongType))
            val rows = Seq(
              Row(3L, null, Row(3L)),
              Row(null, null, Row(null)),
              Row(-2L, null, Row(-2L)),
              Row(5L, null, Row(5L)),
              Row(null, null, null))
            val data = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
            val plans = new ConcurrentLinkedQueue[SparkPlan]()
            GlutenDeltaJobStatsTracker.withStatsPlanObserver {
              (statsPath, plan) =>
                if (statsPath.toUri.getPath == path) {
                  plans.add(plan)
                }
            } {
              data.write.format("delta").save(path)
              data.write.format("delta").mode("append").save(path)
            }
            assert(
              !plans.isEmpty == (projectEnabled && aggregateEnabled),
              s"Unexpected native statistics execution: ${plans.asScala.map(_.treeString).mkString}")
            plans.asScala.foreach {
              plan =>
                assert(plan.exists(_.isInstanceOf[HashAggregateExecTransformer]), plan.treeString)
                assert(!plan.exists(_.isInstanceOf[SortAggregateExec]), plan.treeString)
                assert(!plan.exists(_.isInstanceOf[ProjectExec]), plan.treeString)
            }

            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
              assert(files.length == 2)
              files.foreach {
                file =>
                  val filePath = DeltaFileOperations.absolutePath(path, file.path)
                  val reader = ParquetFileReader.open(
                    HadoopInputFile.fromPath(filePath, spark.sessionState.newHadoopConf()))
                  try {
                    assert(
                      !reader.getFooter.getFileMetaData.getCreatedBy.startsWith("parquet-mr"),
                      s"${file.path} did not use the native file writer")
                  } finally {
                    reader.close()
                  }
                  checkAnswer(spark.read.parquet(filePath.toString), rows)
                  val stats = parse(file.stats)
                  assert(stats \ "numRecords" == JInt(5), file.stats)
                  assert(stats \ "nullCount" \ "value" == JInt(2), file.stats)
                  assert(stats \ "nullCount" \ "all_null" == JInt(5), file.stats)
                  assert(stats \ "nullCount" \ "nested" \ "value" == JInt(2), file.stats)
                  assert(stats \ "minValues" \ "value" == JInt(-2), file.stats)
                  assert(stats \ "maxValues" \ "value" == JInt(5), file.stats)
                  assert(stats \ "minValues" \ "nested" \ "value" == JInt(-2), file.stats)
                  assert(stats \ "maxValues" \ "nested" \ "value" == JInt(5), file.stats)
                  assert(stats \ "minValues" \ "all_null" == JNothing, file.stats)
                  assert(stats \ "maxValues" \ "all_null" == JNothing, file.stats)
              }
              checkAnswer(spark.read.format("delta").load(path), rows ++ rows)
            }
        }
      }
    }
  }

  test("TIMESTAMP_NTZ statistics fall back when type validation rejects offload") {
    withSQLConf(VeloxConfig.ENABLE_TIMESTAMP_NTZ_VALIDATION.key -> "true") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val data = Seq("1969-12-31 23:59:59.999999", "2024-01-01 00:00:00.123456", null)
            .toDF("input").selectExpr("CAST(input AS TIMESTAMP_NTZ) AS ts")
          val aggregate = data
            .selectExpr(
              """to_json(named_struct(
                |  'numRecords', count(*),
                |  'minValues', named_struct('ts', min(ts)),
                |  'maxValues', named_struct('ts', max(ts)),
                |  'nullCount', named_struct('ts', sum(CASE WHEN ts IS NULL THEN 1 ELSE 0 END))
                |)) AS stats""".stripMargin)
            .queryExecution.analyzed.collectFirst { case a: Aggregate => a }.get
          val delegate = new DeltaJobStatisticsTracker(
            spark.sessionState.newHadoopConf(),
            new Path(path),
            aggregate.child.output,
            aggregate.aggregateExpressions.head)
          val jobTracker = GlutenDeltaJobStatsTracker(delegate)
          val plans = new ConcurrentLinkedQueue[SparkPlan]()

          // Exercise the statistics stream without bypassing Delta 4's NTZ file-writer gate.
          val taskStats = GlutenDeltaJobStatsTracker.withStatsPlanObserver {
            (_, plan) => plans.add(plan)
          } {
            SQLExecution.withNewExecutionId(data.queryExecution) {
              val carriers = VeloxColumnarToCarrierRowExec(
                RowToVeloxColumnarExec(data.queryExecution.executedPlan))
              carriers.execute().mapPartitions {
                rows =>
                  val tracker = jobTracker.newTaskInstance()
                  val file = s"$path/part.parquet"
                  tracker.newFile(file)
                  rows.foreach(tracker.newRow(file, _))
                  tracker.closeFile(file)
                  Iterator(tracker.getFinalStats(0L))
              }.collect()
            }
          }
          assert(plans.isEmpty, "The rejected NTZ statistics plan should not execute natively")
          assert(taskStats.length == 1)
          assert(taskStats.head.isInstanceOf[DeltaFileStatistics])
          jobTracker.processStats(taskStats.toSeq, 0L)
          assert(delegate.recordedStats.keySet == Set("part.parquet"))
          val stats = parse(delegate.recordedStats("part.parquet"))
          assert(stats \ "numRecords" == JInt(3))
          assert(stats \ "nullCount" \ "ts" == JInt(1))
          assert(stats \ "minValues" \ "ts" == JString("1969-12-31T23:59:59.999"))
          assert(stats \ "maxValues" \ "ts" == JString("2024-01-01T00:00:00.123"))
      }
    }
  }
}
