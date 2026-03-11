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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FileSourceScanExecTransformer, ProjectExecTransformer, WholeStageTransformer}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkEnv
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarShuffleExchangeExec, FileSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.benchmarks.utils.FakeFileOutputStream
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockId

/**
 * Benchmark to measure ClickHouse Backend Partial Agg performance. To run this benchmark:
 * {{{
 *   1. Run in IDEA: run this class directly;
 *   2. Run without IDEA: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar>,<spark sql test jar>
 *        --conf xxxx=xxx
 *        backends-clickhouse-XXX-tests.jar
 *        parameters
 *
 *   Parameters:
 *     1. file format: parquet or clickhouse;
 *     2. data files dir;
 *     3. the execution count;
 *     4. whether to run vanilla spark benchmarks;
 *     5. final output (Optional);
 *     6. group by keys (Optional);
 *     7. aggregate functions (Optional);
 *     8. filter conditions (Optional);
 * }}}
 */
object CHAggAndShuffleBenchmark extends SqlBasedBenchmark with CHSqlBasedBenchmark {

  protected lazy val appName = "CHAggAndShuffleBenchmark"
  protected lazy val thrdNum = "3"
  protected lazy val shufflePartition = "12"
  protected lazy val memorySize = "15G"
  protected lazy val offheapSize = "15G"

  def beforeAll(): Unit = {}

  override def getSparkSession: SparkSession = {
    beforeAll()
    val conf = getSparkConf
      .set(CHBackendSettings.GLUTEN_CLICKHOUSE_SEP_SCAN_RDD, "false")
      .setIfMissing("spark.sql.shuffle.partitions", shufflePartition)
      .setIfMissing("spark.shuffle.manager", "sort")
      .setIfMissing("spark.io.compression.codec", "SNAPPY")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val (
      fileFormat,
      parquetDir,
      executedCnt,
      executedVanilla,
      finalOutput,
      groupByKeys,
      aggFunctions,
      filters) =
      if (mainArgs.isEmpty) {
        (
          "parquet",
          "/data/tpch-data/parquet/lineitem/",
          3,
          true,
          "min(min_partkey), min(min_suppkey), min(sum_base_price)",
          "l_partkey",
          "min(l_partkey) as min_partkey,min(l_suppkey) as min_suppkey," +
            "sum(l_extendedprice) AS sum_base_price",
          "WHERE l_shipdate <= date'1996-09-02' - interval 1 year")
      } else {
        if (mainArgs.length == 4) {
          (
            mainArgs(0),
            mainArgs(1),
            mainArgs(2).toInt,
            mainArgs(3).toBoolean,
            "min(min_partkey), min(min_suppkey), min(sum_base_price)",
            "l_partkey",
            "min(l_partkey) as min_partkey,min(l_suppkey) as min_suppkey," +
              "sum(l_extendedprice) AS sum_base_price",
            "WHERE l_shipdate <= date'1996-09-02' - interval 1 year")
        } else {
          (
            mainArgs(0),
            mainArgs(1),
            mainArgs(2).toInt,
            mainArgs(3).toBoolean,
            mainArgs(4),
            mainArgs(5),
            mainArgs(6),
            mainArgs(7))
        }
      }

    def allStages = spark.sql(s"""
                                 | SELECT $finalOutput
                                 | FROM (
                                 | SELECT
                                 |     $groupByKeys
                                 |     ${if (aggFunctions.nonEmpty) "," + aggFunctions else ""}
                                 | FROM
                                 |     $fileFormat.`$parquetDir`
                                 | $filters
                                 | GROUP BY
                                 |     $groupByKeys) as a
                                 |""".stripMargin)

    // Default execution plan:
    // CHNativeColumnarToRow
    // +- *(6) HashAggregateTransformer
    //   +- ColumnarExchangeAdaptor SinglePartition
    //      +- *(5) HashAggregateTransformer
    //         +- *(5) HashAggregateTransformer
    //            +- ColumnarExchangeAdaptor hashpartitioning(l_partkey#65L, 32)
    //               +- *(4) HashAggregateTransformer
    //                  +- *(4) ProjectExecTransformer
    //                     +- *(4) FilterExecTransformerBase
    //                        +- *(4) FileScan parquet
    //
    // There are three `WholeStageTransformer`, two `ColumnarShuffleExchangeExec`
    // and one `FileSourceScanExecTransformer`.
    val executedPlan = allStages.queryExecution.executedPlan

    // Get the `FileSourceScanExecTransformer`
    val fileScan = executedPlan.collect { case scan: FileSourceScanExecTransformer => scan }.head
    val scanStage = WholeStageTransformer(fileScan)(
      ColumnarCollapseTransformStages
        .getTransformStageCounter(fileScan)
        .incrementAndGet()
    )
    val scanStageRDD = scanStage.executeColumnar()

    // Get the total row count
    val scanRowCnt = scanStageRDD
      .mapPartitionsInternal(
        batches => {
          batches.map(batch => batch.numRows().toLong)
        })
      .collect()
      .sum

    val chAllStagesBenchmark =
      new Benchmark(
        s"Gluten Stages Benchmark, total $scanRowCnt records",
        scanRowCnt,
        output = output)

    chAllStagesBenchmark.addCase(s"All Stages", executedCnt) {
      _ =>
        val resultRDD: RDD[Long] = allStages.queryExecution.executedPlan
          .execute()
          .mapPartitionsInternal {
            rows =>
              rows.foreach(_.numFields)
              Iterator.empty
          }
        resultRDD.collect()
    }

    chAllStagesBenchmark.addCase(s"Scan Stage", executedCnt) {
      _ =>
        val resultRDD: RDD[Long] = scanStageRDD.mapPartitionsInternal {
          batches =>
            batches.foreach(batch => batch.numRows().toLong)
            Iterator.empty
        }
        resultRDD.collect()
    }

    // Scan + Filter + Project Stages, if there is no filter or project, will not run.
    val projectFilter = executedPlan.collect { case project: ProjectExecTransformer => project }
    if (projectFilter.nonEmpty) {
      val projectFilterStage = WholeStageTransformer(projectFilter.head)(
        ColumnarCollapseTransformStages
          .getTransformStageCounter(projectFilter.head)
          .incrementAndGet())
      val projectFilterStageRDD = projectFilterStage.executeColumnar()

      chAllStagesBenchmark.addCase(s"Project Stage", executedCnt) {
        _ =>
          val resultRDD: RDD[Long] = projectFilterStageRDD.mapPartitionsInternal {
            batches =>
              batches.foreach(batch => batch.numRows().toLong)
              Iterator.empty
          }
          resultRDD.collect()
      }
    }

    // Scan + [ Filter + Project ] + Partial Agg Stage
    val wholeStage = executedPlan.collect { case stage: WholeStageTransformer => stage }
    val newWholeStageRDD = wholeStage(2).executeColumnar()

    chAllStagesBenchmark.addCase(s"Partial Agg Stage", executedCnt) {
      _ =>
        val resultRDD: RDD[Long] = newWholeStageRDD.mapPartitionsInternal {
          batches =>
            batches.foreach(batch => batch.numRows().toLong)
            Iterator.empty
        }
        resultRDD.collect()
    }

    // Get all `ColumnarShuffleExchangeExec` and run the second one.
    val shuffleExchange = executedPlan.collect {
      case shuffle: ColumnarShuffleExchangeExec => shuffle
    }(1)

    chAllStagesBenchmark.addCase(s"Shuffle Split Stage", executedCnt) {
      _ =>
        val shuffleSplit = ColumnarShuffleExchangeExec(
          shuffleExchange.outputPartitioning,
          shuffleExchange.child,
          ENSURE_REQUIREMENTS,
          shuffleExchange.child.output,
          shuffleExchange.advisoryPartitionSize)
        val resultRDD = shuffleSplit.columnarShuffleDependency.rdd.mapPartitionsInternal {
          batches =>
            batches.foreach(batch => (batch._1, batch._2.numRows().toLong))
            Iterator((0, 0))
        }
        resultRDD.collect()
    }

    chAllStagesBenchmark.addCase(s"Shuffle Write Stage ( Fake output to file )", executedCnt) {
      _ =>
        val shuffleWrite = ColumnarShuffleExchangeExec(
          shuffleExchange.outputPartitioning,
          shuffleExchange.child,
          ENSURE_REQUIREMENTS,
          shuffleExchange.child.output,
          shuffleExchange.advisoryPartitionSize)
        val serializer = shuffleWrite.columnarShuffleDependency.serializer
        val resultRDD = shuffleWrite.columnarShuffleDependency.rdd.mapPartitionsInternal {
          batches =>
            val serInstance = serializer.newInstance
            val bs = SparkEnv.get.serializerManager
              .wrapStream(ShuffleBlockId(0, 0L, 0), new FakeFileOutputStream)
            val objOut = serInstance.serializeStream(bs)
            batches.foreach(
              batch => {
                objOut.writeKey(batch._1)
                objOut.writeValue(batch._2)
              })
            objOut.flush()
            objOut.close()
            bs.close()
            Iterator((0, 0))
        }
        resultRDD.collect()
    }

    chAllStagesBenchmark.addCase(s"Shuffle Read Stage", executedCnt) {
      _ =>
        val shuffleRead = ColumnarShuffleExchangeExec(
          shuffleExchange.outputPartitioning,
          shuffleExchange.child,
          ENSURE_REQUIREMENTS,
          shuffleExchange.child.output,
          shuffleExchange.advisoryPartitionSize)

        val resultRDD: RDD[Long] = shuffleRead.executeColumnar().mapPartitionsInternal {
          batches =>
            batches.foreach(batch => batch.numRows().toLong)
            Iterator.empty
        }
        resultRDD.collect()
    }

    chAllStagesBenchmark.run()

    if (
      executedVanilla && fileFormat.equalsIgnoreCase("parquet") &&
      spark.conf
        .get("spark.shuffle.manager", "sort")
        .equalsIgnoreCase("sort")
    ) {
      // Get the file partitions for generating the `FileScanRDD`
      val filePartitions = fileScan.getPartitions
        .map(_.asInstanceOf[FilePartition])
      spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, "false")
      val sparkExecutedPlan = allStages.queryExecution.executedPlan

      // Get the `FileSourceScanExec`
      val vanillaScanPlan = sparkExecutedPlan.collect { case scan: FileSourceScanExec => scan }

      val sparkFileScan = vanillaScanPlan.head
      val relation = sparkFileScan.relation
      val readFile: PartitionedFile => Iterator[InternalRow] =
        relation.fileFormat.buildReaderWithPartitionValues(
          sparkSession = relation.sparkSession,
          dataSchema = relation.dataSchema,
          partitionSchema = relation.partitionSchema,
          requiredSchema = sparkFileScan.requiredSchema,
          filters = Seq.empty,
          options = relation.options,
          hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
        )

      val newFileScanRDD =
        SparkShimLoader.getSparkShims
          .generateFileScanRDD(spark, readFile, filePartitions, sparkFileScan)
          .asInstanceOf[RDD[ColumnarBatch]]

      // Get the total row count
      val rowCnt = newFileScanRDD
        .mapPartitionsInternal(batches => batches.map(batch => batch.numRows().toLong))
        .collect()
        .sum
      assert(scanRowCnt == rowCnt, "The row count of the benchmark is not equal.")

      val sparkAllStagesBenchmark =
        new Benchmark(
          s"Vanilla Spark Stages Benchmark, total $scanRowCnt records",
          scanRowCnt,
          output = output)

      sparkAllStagesBenchmark.addCase(s"All Stages", executedCnt) {
        _ =>
          val resultRDD: RDD[Long] = allStages.queryExecution.executedPlan
            .execute()
            .mapPartitionsInternal {
              rows =>
                rows.foreach(_.numFields)
                Iterator.empty
            }
          resultRDD.collect()
      }

      // Scan Stage
      sparkAllStagesBenchmark.addCase(s"Scan Stage", executedCnt) {
        _ =>
          val resultRDD: RDD[Long] = newFileScanRDD.mapPartitionsInternal {
            batches =>
              batches.foreach(_.numRows().toLong)
              Iterator.empty
          }
          resultRDD.collect()
      }

      // Scan + [ Filter + Project ] + Partial Agg Stage
      val wholeStage = sparkExecutedPlan.collect { case stage: WholeStageCodegenExec => stage }
      val newWholeStageRDD = wholeStage(2).execute()

      sparkAllStagesBenchmark.addCase(s"Partial Agg Stage", executedCnt) {
        _ =>
          val resultRDD: RDD[Long] = newWholeStageRDD.mapPartitionsInternal {
            rows =>
              rows.foreach(_.numFields)
              Iterator.empty
          }
          resultRDD.collect()
      }

      // Get all `ShuffleExchangeExec` and run the second one
      val shuffleStage =
        sparkExecutedPlan.collect { case shuffle: ShuffleExchangeExec => shuffle }(1)

      sparkAllStagesBenchmark.addCase(s"Shuffle Split Stage", executedCnt) {
        _ =>
          val shuffleSplit =
            ShuffleExchangeExec(shuffleStage.outputPartitioning, shuffleStage.child)
          val resultRDD = shuffleSplit.shuffleDependency.rdd.mapPartitionsInternal {
            rows =>
              rows.foreach(row => (row._1, row._2.numFields))
              Iterator((0, 0))
          }
          resultRDD.collect()
      }

      sparkAllStagesBenchmark.addCase(s"Shuffle Write Stage ( Fake output to file )", executedCnt) {
        _ =>
          val shuffleWrite =
            ShuffleExchangeExec(shuffleStage.outputPartitioning, shuffleStage.child)
          val serializer = shuffleWrite.shuffleDependency.serializer
          val resultRDD = shuffleWrite.shuffleDependency.rdd.mapPartitionsInternal {
            rows =>
              val serInstance = serializer.newInstance
              val bs = SparkEnv.get.serializerManager
                .wrapStream(ShuffleBlockId(0, 0L, 0), new FakeFileOutputStream)
              val objOut = serInstance.serializeStream(bs)
              rows.foreach(
                row => {
                  objOut.writeKey(row._1)
                  objOut.writeValue(row._2)
                })
              objOut.flush()
              objOut.close()
              bs.close()
              Iterator((0, 0))
          }
          resultRDD.collect()
      }

      sparkAllStagesBenchmark.addCase(s"Shuffle Read Stage", executedCnt) {
        _ =>
          val shuffleRead = ShuffleExchangeExec(shuffleStage.outputPartitioning, shuffleStage.child)

          val resultRDD: RDD[Long] = shuffleRead.execute().mapPartitionsInternal {
            rows =>
              rows.foreach(_.numFields)
              Iterator.empty
          }
          resultRDD.collect()
      }

      sparkAllStagesBenchmark.run()
    }
  }
}
