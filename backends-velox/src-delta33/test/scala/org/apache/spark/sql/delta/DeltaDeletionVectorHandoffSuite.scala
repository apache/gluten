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

import org.apache.gluten.backendsapi.velox.VeloxDeltaMetadataUtils
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.{RowIndexFilterType => GlutenRowIndexFilterType}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

@ExtendedSQLTest
class DeltaDeletionVectorHandoffSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  test("Spark 3.5 Delta DV handoff should materialize serialized payloads from scan metadata") {
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

        val log = DeltaLog.forTable(spark, new Path(path))
        val addFileWithDv = log.update().allFiles.collect().find(_.deletionVector != null)
        assert(addFileWithDv.nonEmpty)

        val dataFile = addFileWithDv.get
        val basePartitionedFile = PartitionedFile(
          partitionValues = InternalRow.empty,
          filePath = SparkPath.fromPath(new Path(path, dataFile.path)),
          start = 0L,
          length = dataFile.size,
          fileSize = dataFile.size)
        val partitionedFile = basePartitionedFile.copy(
          otherConstantMetadataColumnValues = Map[String, Object](
            GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED ->
              dataFile.deletionVector.serializeToBase64(),
            GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE -> "IF_CONTAINED"
          ))
        val normalized = VeloxDeltaMetadataUtils.normalizeSplitMetadata(
          partitionColumnCount = 0,
          files = Seq(partitionedFile).asJava)
        val metadata = normalized.otherMetadataColumns.get(0)
        val deltaReadOptions = normalized.deltaReadOptions.get(0)

        assert(normalized.deletionVectorPayloads.length == 1)
        assert(normalized.deletionVectorPayloads.head.nonEmpty)
        assert(deltaReadOptions.hasDeletionVector())
        assert(deltaReadOptions.deletionVectorPayloadIndex() == 0)
        assert(deltaReadOptions.deletionVectorCardinality() == dataFile.deletionVector.cardinality)
        assert(deltaReadOptions.rowIndexFilterType() == GlutenRowIndexFilterType.IF_CONTAINED)
        assert(!metadata.containsKey(GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED))
        assert(!metadata.containsKey(GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE))
    }
  }

  test("Spark 3.5 Delta DV handoff should skip payload materialization without scan metadata") {
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

        val log = DeltaLog.forTable(spark, new Path(path))
        val addFileWithDv = log.update().allFiles.collect().find(_.deletionVector != null)
        assert(addFileWithDv.nonEmpty)

        val dataFile = addFileWithDv.get
        val partitionedFile = PartitionedFile(
          partitionValues = InternalRow.empty,
          filePath = SparkPath.fromPath(new Path(path, dataFile.path)),
          start = 0L,
          length = dataFile.size,
          fileSize = dataFile.size)
        val normalized = VeloxDeltaMetadataUtils.normalizeSplitMetadata(
          partitionColumnCount = 0,
          files = Seq(partitionedFile).asJava)
        val metadata = normalized.otherMetadataColumns.get(0)

        assert(normalized.deletionVectorPayloads.isEmpty)
        assert(normalized.deltaReadOptions.isEmpty)
        assert(!metadata.containsKey(GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED))
        assert(!metadata.containsKey(GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE))
    }
  }
}
