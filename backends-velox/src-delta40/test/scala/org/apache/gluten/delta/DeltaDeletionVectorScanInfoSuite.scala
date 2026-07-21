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
package org.apache.gluten.delta

import org.apache.gluten.delta.DeltaDeletionVectorScanInfo.RowIndexFilterType

import org.apache.spark.SparkConf
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.{DeltaLog, GlutenDeltaParquetFileFormat}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path

@ExtendedSQLTest
class DeltaDeletionVectorScanInfoSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLTestUtils {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "2")
  }

  test("extracts essential Delta DV scan info from split metadata") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)")

        val dataFile = DeltaLog
          .forTable(spark, new Path(path))
          .update()
          .allFiles
          .collect()
          .find(_.deletionVector != null)
          .get
        val partitionedFile = partitionedFileWithMetadata(
          path,
          dataFile.path,
          dataFile.size,
          Map(
            GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED ->
              dataFile.deletionVector.serializeToBase64(),
            GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE -> "IF_CONTAINED",
            "kept_key" -> "kept_value"
          )
        )

        val scanInfo = DeltaDeletionVectorScanInfo.extract(spark, 0, partitionedFile)
        val dvInfo = scanInfo.deletionVectorInfo

        assert(dvInfo.hasDeletionVector)
        assert(dvInfo.rowIndexFilterType == RowIndexFilterType.IF_CONTAINED)
        assert(dvInfo.cardinality == dataFile.deletionVector.cardinality)
        assert(dvInfo.serializedDeletionVector.nonEmpty)
        assert(scanInfo.normalizedOtherMetadataColumns == Map("kept_key" -> "kept_value"))
    }
  }

  test("returns keep-all scan info when Delta DV metadata is absent") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a")).toDF("id", "value").coalesce(1).write.format("delta").save(path)

        val dataFile = DeltaLog.forTable(spark, new Path(path)).update().allFiles.collect().head
        val partitionedFile = partitionedFileWithMetadata(
          path,
          dataFile.path,
          dataFile.size,
          Map("kept_key" -> "kept_value"))

        val scanInfo = DeltaDeletionVectorScanInfo.extract(spark, 0, partitionedFile)
        val dvInfo = scanInfo.deletionVectorInfo

        assert(!dvInfo.hasDeletionVector)
        assert(dvInfo.rowIndexFilterType == RowIndexFilterType.KEEP_ALL)
        assert(dvInfo.cardinality == 0L)
        assert(dvInfo.serializedDeletionVector.isEmpty)
        assert(scanInfo.normalizedOtherMetadataColumns == Map("kept_key" -> "kept_value"))
    }
  }

  test("rejects partial Delta DV split metadata") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a")).toDF("id", "value").coalesce(1).write.format("delta").save(path)

        val dataFile = DeltaLog.forTable(spark, new Path(path)).update().allFiles.collect().head
        val partitionedFile = partitionedFileWithMetadata(
          path,
          dataFile.path,
          dataFile.size,
          Map(GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE -> "IF_CONTAINED"))

        val error = intercept[IllegalStateException] {
          DeltaDeletionVectorScanInfo.extract(spark, 0, partitionedFile)
        }
        assert(error.getMessage.contains("must either be present or absent"))
    }
  }

  test("normalize materializes the same DV whether the table path is supplied or derived") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)")

        val dataFile = DeltaLog
          .forTable(spark, new Path(path))
          .update()
          .allFiles
          .collect()
          .find(_.deletionVector != null)
          .get
        val partitionedFile = partitionedFileWithMetadata(
          path,
          dataFile.path,
          dataFile.size,
          Map(
            GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED ->
              dataFile.deletionVector.serializeToBase64(),
            GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE -> "IF_CONTAINED"
          )
        )

        // Supplying the table root (as DeltaScanTransformer does via TahoeFileIndex.path) must
        // yield exactly the same materialized DV as deriving it from the file path. This proves the
        // new tablePath parameter is honored and the fast path stays correct.
        val withSuppliedPath =
          DeltaDeletionVectorScanInfo.normalize(0, Seq(partitionedFile), Some(new Path(path)))
        val withDerivedPath =
          DeltaDeletionVectorScanInfo.normalize(0, Seq(partitionedFile), None)

        assert(withSuppliedPath.isDefined, "normalize should materialize DV options")
        assert(withDerivedPath.isDefined, "normalize should materialize DV options")

        val supplied = withSuppliedPath.get._2.head
        val derived = withDerivedPath.get._2.head
        assert(supplied.hasDeletionVector)
        assert(supplied.deletionVectorCardinality == dataFile.deletionVector.cardinality)
        assert(supplied.serializedDeletionVector.nonEmpty)
        assert(
          supplied.serializedDeletionVector.sameElements(derived.serializedDeletionVector),
          "supplied-path and derived-path DV payloads must be identical")
    }
  }

  private def partitionedFileWithMetadata(
      tablePath: String,
      relativeFilePath: String,
      fileSize: Long,
      metadata: Map[String, Object]): PartitionedFile = {
    PartitionedFile(
      partitionValues = InternalRow.empty,
      filePath = SparkPath.fromPath(new Path(tablePath, relativeFilePath)),
      start = 0L,
      length = fileSize,
      fileSize = fileSize,
      otherConstantMetadataColumnValues = metadata
    )
  }
}
