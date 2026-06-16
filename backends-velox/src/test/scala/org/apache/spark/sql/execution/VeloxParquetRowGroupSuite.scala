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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.File

/**
 * Validates which configuration channel actually controls the Parquet row-group size of Gluten's
 * native (Velox) writer.
 *
 * Background: Delta's `DeletionVectorsWithPredicatePushdownSuite.beforeAll` writes a 1M-row
 * table with `hadoopConf().set("parquet.block.size", 2MB)` and asserts the resulting file has more
 * than one Parquet row group. Under Gluten the file has a single row group, so `beforeAll` throws
 * and the whole suite aborts. These tests pin down why: the native writer flushes a row group when
 * the accumulated uncompressed size reaches `maxRowGroupBytes` (default 128MB, from
 * `parquet.block.size`) or the row count reaches `maxRowGroupRows` (default 100M). ~8MB of `int64`
 * data is far below the 128MB default (one row group) but far above the 1MB block size used here
 * (several row groups) -- but only when the block size actually reaches the native writer.
 */
class VeloxParquetRowGroupSuite extends VeloxWholeStageTransformerSuite with WriteUtils {

  override protected val resourcePath: String = ""
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")

  // ~8MB of int64 data: << the 128MB default block size, >> the 1MB block size used below.
  private val numRows: Long = 1000000L
  private val smallBlockSize: Long = 1L * 1024 * 1024 // 1MB
  private val glutenBlockSizeKey = "spark.gluten.sql.columnar.parquet.write.blockSize"

  private def rowGroupCount(dir: File): Int = {
    val parquetFiles =
      Option(dir.listFiles((_, name) => name.endsWith(".parquet"))).getOrElse(Array.empty[File])
    assert(parquetFiles.nonEmpty, s"no parquet file written under ${dir.getAbsolutePath}")
    parquetFiles.map {
      file =>
        val in = HadoopInputFile
          .fromPath(new Path(file.getAbsolutePath), spark.sessionState.newHadoopConf())
        Utils.tryWithResource(ParquetFileReader.open(in))(_.getFooter.getBlocks.size())
    }.sum
  }

  // Writes `numRows` longs to `path` via the native Velox writer (checkNativeWrite asserts the plan
  // actually offloads to ColumnarWriteFilesExec, so we are exercising the native writer).
  private def writeRangeNatively(path: String): Unit = {
    withTempView("velox_rowgroup_src") {
      spark.range(0, numRows, 1, 1).toDF("id").createOrReplaceTempView("velox_rowgroup_src")
      checkNativeWrite(
        s"INSERT OVERWRITE DIRECTORY '$path' USING PARQUET SELECT * FROM velox_rowgroup_src")
    }
  }

  test("native writer emits a single row group at the default (128MB) block size") {
    withTempPath {
      f =>
        writeRangeNatively(f.getCanonicalPath)
        // Exactly what DeletionVectorsWithPredicatePushdownSuite hits: ~8MB under the 128MB default
        // is one row group, so its `> 1 row group` assert fails and the suite aborts.
        assert(rowGroupCount(f) === 1)
    }
  }

  test("native writer honors the Gluten block-size conf and emits multiple row groups") {
    withTempPath {
      f =>
        withSQLConf(glutenBlockSizeKey -> smallBlockSize.toString) {
          writeRangeNatively(f.getCanonicalPath)
        }
        // The Gluten conf is read directly by the native writer, so the 1MB block size takes effect
        // and the file is split into several row groups.
        assert(rowGroupCount(f) > 1)
    }
  }

  // Reproduction of the Delta suite abort -- this test currently FAILS by design.
  // `parquet.block.size` set on the Hadoop conf at runtime is honored by vanilla parquet-mr (it
  // produces multiple row groups) but does not reach Gluten's native writer, so today the file has
  // a single row group. The assertion states the DESIRED behavior (> 1 row group); it should turn
  // green once the Hadoop-conf block size is plumbed through to the native writer.
  test("native writer should respect parquet.block.size set on the runtime Hadoop conf") {
    withTempPath {
      f =>
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        hadoopConf.set("parquet.block.size", smallBlockSize.toString)
        try {
          writeRangeNatively(f.getCanonicalPath)
        } finally {
          hadoopConf.unset("parquet.block.size")
        }
        assert(rowGroupCount(f) > 1)
    }
  }
}
