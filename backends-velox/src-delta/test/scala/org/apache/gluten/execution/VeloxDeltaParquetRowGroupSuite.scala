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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.File

/**
 * Reproduction for the Parquet row-group sizing bug behind Delta's
 * `DeletionVectorsWithPredicatePushdownSuite` abort.
 *
 * That suite sets `parquet.block.size = 2MB` on `spark.sparkContext.hadoopConfiguration`, writes a
 * 1M-row Delta table, and asserts the resulting data file has more than one row group. Under Gluten
 * the data file has a single row group, so `beforeAll` throws and the whole suite aborts.
 *
 * VeloxParquetRowGroupSuite already showed that a *plain* Parquet write (`INSERT OVERWRITE
 * DIRECTORY ... USING PARQUET`) DOES honor `parquet.block.size` from that same runtime Hadoop conf
 * (multiple row groups). These tests isolate the remaining difference -- the *Delta* write path --
 * using the same conf source and data. They assert the desired behavior (> 1 row group) and are
 * expected to FAIL today, reproducing the Delta abort; they should turn green once the Hadoop-conf
 * block size is plumbed through the Delta write path to the native writer. The deletion-vectors
 * variant matches the original suite (which enables DVs on the table); the initial write is
 * identical with or without DVs, so both cases pin down the write path rather than DVs or
 * checkpoints.
 */
class VeloxDeltaParquetRowGroupSuite extends DeltaSuite {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")

  // ~8MB of int64 (uncompressed): far above the 1MB block size used here, so a writer that honors
  // the block size produces several row groups.
  private val numRows: Long = 1000000L
  private val smallBlockSize: Long = 1L * 1024 * 1024 // 1MB

  private def listParquetFiles(dir: File): Seq[File] = {
    val entries = Option(dir.listFiles()).getOrElse(Array.empty[File])
    entries.filter(f => f.isFile && f.getName.endsWith(".parquet")).toSeq ++
      entries.filter(_.isDirectory).flatMap(listParquetFiles)
  }

  private def rowGroupCount(dir: File): Int = {
    val parquetFiles = listParquetFiles(dir)
    assert(parquetFiles.nonEmpty, s"no parquet file written under ${dir.getAbsolutePath}")
    parquetFiles.map {
      file =>
        val in = HadoopInputFile
          .fromPath(new Path(file.getAbsolutePath), spark.sessionState.newHadoopConf())
        val reader = ParquetFileReader.open(in)
        try {
          reader.getFooter.getBlocks.size().toInt
        } finally {
          reader.close()
        }
    }.sum
  }

  private def writeDelta(path: String, enableDeletionVectors: Boolean): Unit = {
    // scalastyle:off hadoopconfiguration
    // Mirror Delta's `hadoopConf().set("parquet.block.size", ...)`, which resolves to exactly this
    // conf object in DeletionVectorsWithPredicatePushdownSuite.
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration
    hadoopConf.set("parquet.block.size", smallBlockSize.toString)
    try {
      val writer = spark.range(0, numRows, 1, 1).toDF("id").write.format("delta")
      val withOptions =
        if (enableDeletionVectors) writer.option("delta.enableDeletionVectors", "true") else writer
      withOptions.save(path)
    } finally {
      hadoopConf.unset("parquet.block.size")
    }
  }

  test("delta write should respect parquet.block.size set on the runtime Hadoop conf") {
    withTempPath {
      dir =>
        writeDelta(dir.getCanonicalPath, enableDeletionVectors = false)
        assert(rowGroupCount(dir) > 1)
    }
  }

  test(
    "delta write with deletion vectors should respect parquet.block.size on the runtime Hadoop " +
      "conf") {
    withTempPath {
      dir =>
        writeDelta(dir.getCanonicalPath, enableDeletionVectors = true)
        assert(rowGroupCount(dir) > 1)
    }
  }
}
