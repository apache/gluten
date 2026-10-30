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
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.json4s.{JInt, JNothing}
import org.json4s.jackson.JsonMethods.parse

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class GlutenDeltaNativePartitionWriteSuite extends GlutenQueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
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
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.maxConcurrentOutputFileWriters", "0")
      // Exercise omitted partition columns unless the writer feature overrides this property.
      // Older Delta ignores this unknown table property default.
      .set("spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet", "false")
  }

  // Small batches cross partition boundaries; a large batch requires splitting a single stripe.
  for (
    (batchSize, limit, collectStats) <- Seq((4, 4, false), (16, 4, true), (4, 0, true));
    materialize <- Seq(false, true)
  ) {
    test(s"native Delta partitioned files: batch=$batchSize, limit=$limit, " +
      s"stats=$collectStats, materialize=$materialize") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      assume(!materialize || spark.version.startsWith("4."))
      val schema = new StructType()
        .add("part", IntegerType).add("id", LongType).add("region", StringType)
      val input = Seq((null, 3), (Int.box(0), 4), (Int.box(1), 9)).flatMap {
        case (part, count) =>
          val region = if (part == null) "" else if (part == 0) "east/region=0" else null
          (0 until count).map(i => Row(part, if (i % 3 == 0) null else Long.box(i / 2), region))
      }
      val expected = input.map {
        row => Row(row(0), row(1), Option(row.getString(2)).filter(_.nonEmpty).orNull)
      }
      withSQLConf(
        GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> batchSize.toString,
        "spark.databricks.delta.stats.collect" -> collectStats.toString,
        "spark.sql.jsonGenerator.ignoreNullFields" -> "true"
      ) {
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
              val writer = spark.createDataFrame(spark.sparkContext.parallelize(input, 1), schema)
                .write.format("delta").partitionBy("region", "part")
                .option("maxRecordsPerFile", limit.toString)
              if (materialize) {
                writer.option("delta.feature.materializePartitionColumns", "supported")
              }
              writer.save(path)
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

            // Read the physical files with Spark to independently check layout and per-file stats.
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              val snapshot = DeltaLog.forTable(spark, path).update()
              if (materialize) {
                assert(snapshot.protocol.writerFeatures.exists(_.contains(
                  "materializePartitionColumns")))
                assert(snapshot.metadata.configuration("delta.writePartitionColumnsToParquet") ==
                  "false")
              }
              val files = snapshot.allFiles.collect()
              val fileSizes = files.toSeq.map {
                file =>
                  val filePath = DeltaFileOperations.absolutePath(path, file.path).toString
                  val reader = ParquetFileReader.open(
                    HadoopInputFile.fromPath(
                      DeltaFileOperations.absolutePath(path, file.path),
                      spark.sessionState.newHadoopConf()))
                  try {
                    // Vanilla Spark writes with parquet-mr; the Velox writer does not.
                    assert(
                      !reader.getFooter.getFileMetaData.getCreatedBy.startsWith("parquet-mr"),
                      s"${file.path} was not written by the native writer")
                    val expectedColumns =
                      if (materialize) Seq("id", "region", "part") else Seq("id")
                    assert(reader.getFooter.getFileMetaData.getSchema.getFields.asScala
                      .map(_.getName).toSeq == expectedColumns)
                  } finally {
                    reader.close()
                  }
                  val data = spark.read.option("recursiveFileLookup", "true").parquet(filePath)
                  if (materialize) {
                    data.select("region", "part").collect().foreach {
                      row =>
                        assert(row.getString(0) == file.partitionValues("region"))
                        val part = if (row.isNullAt(1)) null else row.getInt(1).toString
                        assert(part == file.partitionValues("part"))
                    }
                  }
                  val rows = data.select("id").collect().toSeq
                  if (collectStats) {
                    val stats = parse(file.stats)
                    for (
                      field <- Seq("minValues", "maxValues", "nullCount");
                      partition <- Seq("region", "part")
                    ) {
                      assert(stats \ field \ partition == JNothing, file.stats)
                    }
                    val values = rows.filterNot(_.isNullAt(0)).map(_.getLong(0))
                    assert((stats \ "numRecords") == JInt(rows.size), file.stats)
                    assert(
                      (stats \ "nullCount" \ "id") == JInt(rows.size - values.size),
                      file.stats)
                    assert(
                      (stats \ "minValues" \ "id") ==
                        (if (values.isEmpty) JNothing else JInt(values.min)),
                      file.stats)
                    assert(
                      (stats \ "maxValues" \ "id") ==
                        (if (values.isEmpty) JNothing else JInt(values.max)),
                      file.stats)
                  } else {
                    assert(file.stats == null || file.stats.isEmpty)
                  }
                  file.partitionValues("part") -> rows.size
              }.groupBy(_._1).map {
                case (part, sizes) => part -> sizes.map(_._2).sorted
              }
              // Exact sizes detect premature rotation as well as files exceeding the limit.
              assert(fileSizes == Map(
                (null, Seq(3)),
                "0" -> Seq(4),
                "1" -> (if (limit == 0) Seq(9) else Seq(1, 4, 4))))
              checkAnswer(
                spark.read.format("delta").load(path).select("part", "id", "region"),
                expected)
            }
        }
      }
    }
  }
}
