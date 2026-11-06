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

import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.DeltaFileReadOptions

import org.apache.spark.SparkConf
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.{DeltaLog, GlutenDeltaParquetFileFormat}
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path

import java.io.File
import java.nio.file.Files

/** Delta 4.4 allows on-disk deletion vectors without an offset (delta-io/delta#6436). */
class DeltaOffsetlessDeletionVectorSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLTestUtils {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
  }

  test("an on-disk DV without an offset is read from the start of its file") {
    withTempDir {
      tempDir =>
        val tablePath = new Path(tempDir.getCanonicalPath, "table")
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(tablePath.toString)
        spark.sql(
          s"ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id IN (3, 4)")

        val dataFile = DeltaLog
          .forTable(spark, tablePath)
          .update()
          .allFiles
          .collect()
          .find(_.deletionVector != null)
          .get
        val descriptor = dataFile.deletionVector
        // Copy the DV into a file of its own so that it starts at byte 0, then point an
        // offset-less descriptor at that file.
        val dvFileBytes = Files.readAllBytes(
          new File(descriptor.absolutePath(tablePath).toUri.getPath).toPath)
        val standaloneDvFile = new File(tempDir, "standalone.bin")
        Files.write(standaloneDvFile.toPath, dvFileBytes.drop(descriptor.offset.get))
        val offsetless = DeletionVectorDescriptor.onDiskWithAbsolutePath(
          standaloneDvFile.toURI.toString,
          descriptor.sizeInBytes,
          descriptor.cardinality)
        assert(offsetless.offset.isEmpty)

        def readOptions(dv: DeletionVectorDescriptor): DeltaFileReadOptions = {
          val partitionedFile = PartitionedFile(
            partitionValues = InternalRow.empty,
            filePath = SparkPath.fromPath(
              DeltaFileOperations.absolutePath(tablePath.toString, dataFile.path)),
            start = 0L,
            length = dataFile.size,
            fileSize = dataFile.size,
            otherConstantMetadataColumnValues = Map(
              GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED ->
                dv.serializeToBase64(),
              GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE -> "IF_CONTAINED"
            )
          )
          DeltaDeletionVectorScanInfo.normalize(Seq(partitionedFile), tablePath).get._2.head
        }

        val options = readOptions(offsetless)
        assert(options.hasDeletionVector)
        assert(options.deletionVectorCardinality == descriptor.cardinality)
        assert(
          options.serializedDeletionVector.sameElements(
            readOptions(descriptor).serializedDeletionVector))
    }
  }
}
