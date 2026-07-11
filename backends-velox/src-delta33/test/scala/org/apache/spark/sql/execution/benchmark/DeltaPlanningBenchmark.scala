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
package org.apache.spark.sql.execution.benchmark

import org.apache.gluten.delta.DeltaDeletionVectorScanInfo
import org.apache.gluten.extension.DeltaPostTransformRules

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import org.apache.hadoop.fs.Path

/**
 * Benchmark for Delta Lake planning-time operations in Gluten.
 *
 * Measures two hot paths that our performance optimizations target:
 *
 *   1. '''DV Materialization''' (`DeltaDeletionVectorScanInfo.normalize`): resolves table paths,
 *      loads DV bitmaps from storage, and serializes them into split metadata. Our optimizations
 *      (caching table path, Hadoop conf, DV store across files) target this path.
 *   2. '''Post-transform rule application''' (`DeltaPostTransformRules.rules`): traverses the
 *      physical plan to strip DV synthetic columns, push down input_file_name, and apply column
 *      mapping. Our optimizations (early-exit guard, shallow child check, pre-computed names,
 *      batched attribute mapping) target this path.
 *
 * To run:
 * {{{
 *   bin/spark-submit --class org.apache.spark.sql.execution.benchmark.DeltaPlanningBenchmark \
 *     --jars <spark-core-test-jar>,<gluten-backends-velox-jar>
 * }}}
 *
 * Or via Maven (from the backends-velox module):
 * {{{
 *   ./build/mvn test -pl backends-velox -Pspark-3.5 -Pbackends-velox -Pdelta -Pjava-17 \
 *     -Dtest=none -DfailIfNoTests=false \
 *     -Dsuites="org.apache.spark.sql.execution.benchmark.DeltaPlanningBenchmark"
 * }}}
 */
object DeltaPlanningBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName("DeltaPlanningBenchmark")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "1024MB")
      .config("spark.ui.enabled", "false")
      .config("spark.default.parallelism", "1")
      .getOrCreate()
  }

  private val numFiles =
    spark.sparkContext.conf.getInt("spark.gluten.benchmark.delta.numFiles", 100)
  private val rowsPerFile =
    spark.sparkContext.conf.getInt("spark.gluten.benchmark.delta.rowsPerFile", 10000)
  private val benchmarkIters =
    spark.sparkContext.conf.getInt("spark.gluten.benchmark.iterations", 5)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runDvMaterializationBenchmark()
    runPostTransformRulesBenchmark()
    runNonDeltaRulesOverheadBenchmark()
  }

  /**
   * Benchmarks DeltaDeletionVectorScanInfo.normalize() -- the critical path that loads DVs from
   * storage on the driver. Measures how caching table path + DV store reduces overhead.
   */
  private def runDvMaterializationBenchmark(): Unit = {
    val benchmark = new Benchmark(
      s"DV Materialization (normalize) - $numFiles files",
      numFiles.toLong,
      minNumIters = benchmarkIters,
      output = output)

    withDeltaTableWithDVs(numFiles, rowsPerFile) {
      (path, partitionedFiles) =>
        benchmark.addCase(s"normalize() - $numFiles DV files", benchmarkIters) {
          _ =>
            DeltaDeletionVectorScanInfo.normalize(
              partitionColumnCount = 0,
              partitionFiles = partitionedFiles)
        }

        benchmark.run()
    }
  }

  /**
   * Benchmarks DeltaPostTransformRules application on a Delta plan with DV columns. Measures the
   * combined cost of DV stripping, input_file pushdown, and column mapping.
   */
  private def runPostTransformRulesBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Post-transform rules (Delta plan)",
      1L,
      minNumIters = benchmarkIters,
      output = output)

    withDeltaTableWithDVs(numFiles = 10, rowsPerFile = 1000) {
      (path, _) =>
        val df = spark.read.format("delta").load(path)
        // Force planning to get the executed plan with DeltaScanTransformer
        val plan = df.queryExecution.executedPlan

        benchmark.addCase("apply rules (Delta plan with DV)", benchmarkIters) {
          _ =>
            val result = DeltaPostTransformRules.rules.foldLeft(plan) {
              (p, rule) => rule(p)
            }
            // Prevent dead code elimination
            assert(result != null)
        }

        benchmark.run()
    }
  }

  /**
   * Benchmarks post-transform rules on a non-Delta plan to verify zero overhead from the early-exit
   * guard. This is the "control" case showing that non-Delta queries don't pay for Delta rule
   * traversals.
   */
  private def runNonDeltaRulesOverheadBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Post-transform rules (non-Delta plan)",
      1L,
      minNumIters = benchmarkIters,
      output = output)

    withTempPath {
      p =>
        // Create a parquet table (not Delta)
        val path = p.getCanonicalPath
        spark
          .range(0, 100000, 1, numPartitions = 10)
          .selectExpr("id", "id * 2 as value", "cast(id as string) as name")
          .write
          .parquet(path)

        val df = spark.read.parquet(path)
        val plan = df.queryExecution.executedPlan

        benchmark.addCase("apply rules (non-Delta parquet plan)", benchmarkIters) {
          _ =>
            val result = DeltaPostTransformRules.rules.foldLeft(plan) {
              (p, rule) => rule(p)
            }
            assert(result != null)
        }

        benchmark.run()
    }
  }

  /**
   * Creates a Delta table with deletion vectors and provides the partitioned files for direct DV
   * materialization benchmarking.
   */
  private def withDeltaTableWithDVs(numFiles: Int, rowsPerFile: Int)(
      f: (String, Seq[org.apache.spark.sql.execution.datasources.PartitionedFile]) => Unit
  ): Unit = {
    withTempPath {
      p =>
        val path = p.getCanonicalPath
        val totalRows = numFiles.toLong * rowsPerFile

        // Write data across multiple files
        spark
          .range(0, totalRows, 1, numPartitions = numFiles)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable DVs and delete some rows to create DV entries
        spark.sql(s"""ALTER TABLE delta.`$path`
             SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)""")
        // Delete ~10% of rows to generate DVs on most files
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % 10 = 0")

        // Extract partitioned files with DV metadata
        val deltaLog = DeltaLog.forTable(spark, new Path(path))
        val snapshot = deltaLog.update()
        val allFiles = snapshot.allFiles.collect()

        import org.apache.spark.paths.SparkPath
        import org.apache.spark.sql.catalyst.InternalRow
        import org.apache.spark.sql.delta.GlutenDeltaParquetFileFormat
        import org.apache.spark.sql.execution.datasources.PartitionedFile

        val partitionedFiles = allFiles.map {
          dataFile =>
            val metadata: Map[String, Object] =
              if (dataFile.deletionVector != null) {
                Map(
                  GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED ->
                    dataFile.deletionVector.serializeToBase64(),
                  GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE -> "IF_CONTAINED"
                )
              } else {
                Map.empty[String, Object]
              }
            PartitionedFile(
              partitionValues = InternalRow.empty,
              filePath = SparkPath.fromPath(new Path(path, dataFile.path)),
              start = 0L,
              length = dataFile.size,
              fileSize = dataFile.size,
              otherConstantMetadataColumnValues = metadata
            )
        }.toSeq

        f(path, partitionedFiles)
    }
  }
}
