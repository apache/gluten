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

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaLog}
import org.apache.spark.sql.delta.schema.DeltaInvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

class GlutenDeltaNativeConstraintSuite extends GlutenQueryTest with SharedSparkSession {

  private val nativeWriteKey = "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite"

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
      .set(nativeWriteKey, "true")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.optimizeWrite.enabled", "false")
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.maxConcurrentOutputFileWriters", "0")
      // Exercise JNI even before the native writer supports materialized partition columns.
      .set("spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet", "false")
  }

  private def input(rows: Seq[Row]): DataFrame = {
    val schema = new StructType().add("id", LongType).add("part", IntegerType)
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
  }

  private def append(path: String, rows: Seq[Row]): Unit =
    input(rows).write.format("delta").mode("append").save(path)

  private def assertNativeFiles(path: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val files = deltaLog.update().allFiles.collect()
    assert(files.nonEmpty)
    files.foreach {
      file =>
        val reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(
            DeltaFileOperations.absolutePath(path, file.path),
            deltaLog.newDeltaHadoopConf()))
        try {
          assert(
            reader.getFooter.getFileMetaData.getCreatedBy.startsWith("parquet-cpp-velox"),
            s"${file.path} was not written by Velox")
        } finally {
          reader.close()
        }
    }
  }

  private def withUnchangedTable[T](path: String)(action: => T): T = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val before = deltaLog.update()
    val beforeFiles = before.allFiles.collect().toSet
    val beforeRows = spark.read.format("delta").load(path).collect().toSeq

    val result = action
    val after = deltaLog.update()
    assert(after.version == before.version, "A rejected write committed a new Delta version")
    assert(after.allFiles.collect().toSet == beforeFiles)
    checkAnswer(spark.read.format("delta").load(path), beforeRows)
    result
  }

  private def rejectedWrite(
      path: String,
      rows: Seq[Row],
      errorClass: String): DeltaInvariantViolationException = {
    withUnchangedTable(path) {
      val error = intercept[DeltaInvariantViolationException] {
        append(path, rows)
      }
      assert(error.getErrorClass == errorClass)
      error
    }
  }

  private def assertSameError(
      actual: Exception with SparkThrowable,
      expected: Exception with SparkThrowable): Unit = {
    assert(actual.getClass == expected.getClass)
    assert(actual.getErrorClass == expected.getErrorClass)
    assert(actual.getMessageParameters == expected.getMessageParameters)
    assert(actual.getSqlState == expected.getSqlState)
    assert(actual.getMessage == expected.getMessage)
  }

  for (partitioned <- Seq(false, true); notNull <- Seq(false, true)) {
    val constraint = if (notNull) "NOT NULL" else "CHECK"
    test(s"native Delta $constraint preserves the exception and commits nothing, " +
      s"partitioned=$partitioned") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val nullability = if (notNull) " NOT NULL" else ""
          val partitions = if (partitioned) " PARTITIONED BY (part)" else ""
          sql(
            s"CREATE TABLE delta.`$path` (id BIGINT$nullability, part INT) USING delta$partitions")
          if (!notNull) {
            sql(s"ALTER TABLE delta.`$path` ADD CONSTRAINT positive_id CHECK (id > 0)")
          }
          append(path, Seq(Row(1L, 0), Row(2L, 1)))
          assertNativeFiles(path)

          val invalidId = if (notNull) null else Long.box(-1L)
          val invalidRows = Seq(Row(3L, 0), Row(invalidId, 1))
          val errorClass =
            if (notNull) "DELTA_NOT_NULL_CONSTRAINT_VIOLATED"
            else "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"
          val actual = rejectedWrite(path, invalidRows, errorClass)
          withSQLConf(nativeWriteKey -> "false") {
            val expected = rejectedWrite(path, invalidRows, errorClass)
            assertSameError(actual, expected)
          }
      }
    }
  }

  for (partitioned <- Seq(false, true)) {
    test(s"native Delta replaceWhere preserves the exception and commits nothing, " +
      s"partitioned=$partitioned") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withSQLConf(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            val partitions = if (partitioned) " PARTITIONED BY (part)" else ""
            sql(s"CREATE TABLE delta.`$path` (id BIGINT, part INT) USING delta$partitions")
            append(path, Seq(Row(1L, 0), Row(2L, 1)))
            assertNativeFiles(path)

            def rejectOverwrite(): DeltaAnalysisException = withUnchangedTable(path) {
              val error = intercept[DeltaAnalysisException] {
                input(Seq(Row(1L, 0), Row(4L, 1)))
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .option("replaceWhere", "id < 3")
                  .save(path)
              }
              assert(error.getErrorClass == "DELTA_REPLACE_WHERE_MISMATCH")
              error
            }

            val actual = rejectOverwrite()
            withSQLConf(nativeWriteKey -> "false") {
              val expected = rejectOverwrite()
              assertSameError(actual, expected)
              (actual.getCause, expected.getCause) match {
                case (
                      actualCause: DeltaInvariantViolationException,
                      expectedCause: DeltaInvariantViolationException) =>
                  assertSameError(actualCause, expectedCause)
                case _ => fail("Original Delta invariant violation cause was lost")
              }
            }
        }
      }
    }
  }
}
