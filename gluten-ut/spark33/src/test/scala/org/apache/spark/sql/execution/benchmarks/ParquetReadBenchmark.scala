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
package org.apache.spark.sql.execution.benchmarks

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FileSourceScanExecTransformer, WholeStageTransformer}
import org.apache.gluten.extension.columnar.transition.Transitions
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

/**
 * Benchmark to measure native parquet read performance. To run this benchmark:
 * {{{
 *   1. Run in IDEA: run this class directly;
 *   2. Run without IDEA: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar>,<spark sql test jar>
 *        --conf xxxx=xxx
 *        gluten-ut-XXX-tests.jar
 *        parameters
 *
 *   Parameters:
 *     1. parquet files dir;
 *     2. the fields to read;
 *     3. the execution count;
 *     4. whether to run vanilla spark benchmarks;
 * }}}
 */
object ParquetReadBenchmark extends SqlBasedBenchmark {

  protected lazy val thrdNum = "1"
  protected lazy val memorySize = "4G"
  protected lazy val offheapSize = "4G"

  def beforeAll(): Unit = {}

  override def getSparkSession: SparkSession = {
    beforeAll()
    val conf = new SparkConf()
      .setAppName("ParquetReadBenchmark")
      .setIfMissing("spark.master", s"local[$thrdNum]")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .setIfMissing("spark.memory.offHeap.size", offheapSize)
      .setIfMissing("spark.sql.columnVector.offheap.enabled", "true")
      .set("spark.sql.adaptive.enabled", "false")
      .setIfMissing("spark.driver.memory", memorySize)
      .setIfMissing("spark.executor.memory", memorySize)
      .setIfMissing("spark.sql.files.maxPartitionBytes", "1G")
      .setIfMissing("spark.sql.files.openCostInBytes", "1073741824")

    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.io.compression.codec", "LZ4")
        .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
        .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .set("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")
        .set(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
        .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
        .set("spark.databricks.delta.snapshotPartitions", "1")
        .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
        .set("spark.databricks.delta.stalenessLimit", "3600000")
    }

    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val (parquetDir, scanSchema, executedCnt, executedVanilla) =
      if (mainArgs.isEmpty) {
        ("/data/tpch-data-sf10/lineitem", "l_orderkey,l_receiptdate", 5, true)
      } else {
        (mainArgs(0), mainArgs(1), mainArgs(2).toInt, mainArgs(3).toBoolean)
      }

    val parquetReadDf = spark.sql(s"""
                                     |select $scanSchema from parquet.`$parquetDir`
                                     |
                                     |""".stripMargin)
    // Get the `FileSourceScanExecTransformer`
    val fileScan = parquetReadDf.queryExecution.executedPlan.collect {
      case scan: FileSourceScanExecTransformer => scan
    }.head

    val filePartitions = fileScan.getPartitions
      .map(_.asInstanceOf[FilePartition])

    val wholeStageTransform = parquetReadDf.queryExecution.executedPlan.collect {
      case wholeStage: WholeStageTransformer => wholeStage
    }.head

    // remove ProjectExecTransformer
    val newWholeStage = wholeStageTransform.withNewChildren(Seq(fileScan))

    // generate ColumnarToRow
    val columnarToRowPlan = Transitions.toRowPlan(newWholeStage)

    val newWholeStageRDD = newWholeStage.executeColumnar()
    val newColumnarToRowRDD = columnarToRowPlan.execute()

    // Get the total row count
    val totalRowCnt = newWholeStageRDD
      .mapPartitionsInternal(
        batches => {
          batches.map(batch => batch.numRows().toLong)
        })
      .collect()
      .sum

    val parquetReadBenchmark =
      new Benchmark(
        s"Parquet Read files, fields: $scanSchema, total $totalRowCnt records",
        totalRowCnt,
        output = output)

    parquetReadBenchmark.addCase(s"Native Parquet Read", executedCnt) {
      _ =>
        val resultRDD: RDD[Long] = newWholeStageRDD.mapPartitionsInternal {
          batches =>
            batches.foreach(batch => batch.numRows().toLong)
            Iterator.empty
        }
        resultRDD.collect()
    }

    parquetReadBenchmark.addCase(s"Native Parquet Read to Rows", executedCnt) {
      _ =>
        val resultRDD: RDD[Int] = newColumnarToRowRDD.mapPartitionsInternal {
          rows =>
            rows.foreach(_.numFields)
            Iterator.empty
        }
        resultRDD.collect()
    }

    if (executedVanilla) {
      spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, "false")

      val vanillaParquet = spark.sql(s"""
                                        |select $scanSchema from parquet.`$parquetDir`
                                        |
                                        |""".stripMargin)

      val vanillaScanPlan = vanillaParquet.queryExecution.executedPlan.collect {
        case scan: FileSourceScanExec => scan
      }

      val fileScan = vanillaScanPlan.head
      val fileScanOutput = fileScan.output
      val relation = fileScan.relation
      val readFile: (PartitionedFile) => Iterator[InternalRow] =
        relation.fileFormat.buildReaderWithPartitionValues(
          sparkSession = relation.sparkSession,
          dataSchema = relation.dataSchema,
          partitionSchema = relation.partitionSchema,
          requiredSchema = fileScan.requiredSchema,
          filters = Seq.empty,
          options = relation.options,
          hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
        )

      val newFileScanRDD = new FileScanRDD(spark, readFile, filePartitions, fileScan.requiredSchema)
        .asInstanceOf[RDD[ColumnarBatch]]

      val rowCnt = newFileScanRDD
        .mapPartitionsInternal(batches => batches.map(batch => batch.numRows().toLong))
        .collect()
        .sum
      assert(totalRowCnt == rowCnt, "The row count of the benchmark is not equal.")

      parquetReadBenchmark.addCase(s"Vanilla Spark Parquet Read", executedCnt) {
        _ =>
          val resultRDD: RDD[Long] = newFileScanRDD.mapPartitionsInternal {
            batches =>
              batches.foreach(_.numRows().toLong)
              Iterator.empty
          }
          resultRDD.collect()
      }

      parquetReadBenchmark.addCase(s"Vanilla Spark Parquet Read to Rows", executedCnt) {
        _ =>
          val resultRDD: RDD[Long] = newFileScanRDD.mapPartitionsInternal {
            batches =>
              val toUnsafe = UnsafeProjection.create(fileScanOutput, fileScanOutput)
              batches.foreach(_.rowIterator().asScala.map(toUnsafe).foreach(_.numFields))
              Iterator.empty
          }
          resultRDD.collect()
      }
    }

    parquetReadBenchmark.run()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
