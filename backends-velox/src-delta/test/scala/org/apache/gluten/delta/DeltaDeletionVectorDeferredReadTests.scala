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

import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.{DeltaFileReadOptions, InMemoryDeletionVectorPayload}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.hadoop.fs.Path

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.{CountDownLatch, Executors}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

final private[delta] case class TestDeletionVectorFile(
    relativePath: String,
    fileSize: Long,
    encodedDescriptor: String,
    storageType: String,
    absolutePath: String,
    offset: Long,
    payloadSize: Long,
    cardinality: Long)

/** Shared executor-side DV payload tests for the Delta 3.3 and Delta 4.0 source profiles. */
trait DeltaDeletionVectorDeferredReadTests {
  self: QueryTest with SharedSparkSession =>

  import testImplicits._

  protected def loadDeletionVectorFile(tablePath: Path): TestDeletionVectorFile

  protected def deletionVectorMetadata(encodedDescriptor: String): Map[String, Object]

  protected def encodeDeletionVectorDescriptor(descriptor: DeletionVectorDescriptor): String

  protected def partitionedFileWithMetadata(
      tablePath: String,
      relativeFilePath: String,
      fileSize: Long,
      metadata: Map[String, Object]): PartitionedFile

  protected def normalizeDeletionVectorOptions(
      partitionedFile: PartitionedFile,
      tablePath: Path,
      readTime: SQLMetric,
      readBytes: SQLMetric,
      readAttempts: SQLMetric): DeltaFileReadOptions

  protected def normalizeDeletionVectorOptions(
      partitionedFile: PartitionedFile,
      tablePath: Path): DeltaFileReadOptions

  test("eager DV payload owns its input bytes") {
    val input = Array[Byte](1, 2, 3)
    val payload = new InMemoryDeletionVectorPayload(input)

    input(0) = 9

    assert(payload.materialize().sameElements(Array[Byte](1, 2, 3)))
  }

  test("defers on-disk DV reads through serialization and coalesces concurrent materialization") {
    withTempDir {
      tempDir =>
        val tablePath = new Path(tempDir.getCanonicalPath, "table")
        val unrelatedPath = new Path(tempDir.getCanonicalPath, "unrelated")
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(tablePath.toString)

        spark.sql(
          s"ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id IN (3, 4)")

        val dataFile = loadDeletionVectorFile(tablePath)
        assert(dataFile.storageType == "u")
        val partitionedFile = partitionedFileWithMetadata(
          unrelatedPath.toString,
          dataFile.relativePath,
          dataFile.fileSize,
          deletionVectorMetadata(dataFile.encodedDescriptor)
        )

        val readTime = SQLMetrics.createNanoTimingMetric(spark.sparkContext, "DV read time")
        val readBytes = SQLMetrics.createSizeMetric(spark.sparkContext, "DV read bytes")
        val readAttempts = SQLMetrics.createMetric(spark.sparkContext, "DV read attempts")
        val options = normalizeDeletionVectorOptions(
          partitionedFile,
          tablePath,
          readTime,
          readBytes,
          readAttempts)
        assert(!options.isDeletionVectorPayloadMaterialized)

        val executorCopy = javaRoundTrip(options)
        assert(!executorCopy.isDeletionVectorPayloadMaterialized)
        assert(executorCopy.serializedDeletionVector.nonEmpty)
        assert(executorCopy.isDeletionVectorPayloadMaterialized)
        assert(!options.isDeletionVectorPayloadMaterialized)

        val start = new CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(8)
        implicit val executionContext: ExecutionContext =
          ExecutionContext.fromExecutorService(pool)
        val reads = (1 to 16).map {
          _ =>
            Future {
              start.await()
              options.serializedDeletionVector
            }
        }
        start.countDown()
        val payloads =
          try {
            Await.result(Future.sequence(reads), 30.seconds)
          } finally {
            pool.shutdownNow()
          }

        assert(payloads.head.nonEmpty)
        assert(payloads.forall(_ eq payloads.head))
        assert(options.isDeletionVectorPayloadMaterialized)
        assert(readAttempts.value == 1L)
        assert(readBytes.value == payloads.head.length.toLong)
        assert(readTime.value > 0L)
    }
  }

  test("keeps inline DV payloads eager without filesystem access") {
    val bitmap = new RoaringBitmapArray()
    bitmap.add(3L)
    bitmap.add(7L)
    val expectedPayload = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    val descriptor = DeletionVectorDescriptor.inlineInLog(expectedPayload, cardinality = 2L)
    val tablePath = new Path("unsupported-inline-dv-test://authority/table")
    val partitionedFile = partitionedFileWithMetadata(
      tablePath.toString,
      "data.parquet",
      fileSize = 0L,
      metadata = deletionVectorMetadata(encodeDeletionVectorDescriptor(descriptor)))

    val readTime = SQLMetrics.createNanoTimingMetric(spark.sparkContext, "DV read time")
    val readBytes = SQLMetrics.createSizeMetric(spark.sparkContext, "DV read bytes")
    val readAttempts = SQLMetrics.createMetric(spark.sparkContext, "DV read attempts")
    val options = normalizeDeletionVectorOptions(
      partitionedFile,
      tablePath,
      readTime,
      readBytes,
      readAttempts)

    assert(options.isDeletionVectorPayloadMaterialized)
    assert(options.serializedDeletionVector.sameElements(expectedPayload))
    assert(readAttempts.value == 0L)
    assert(readBytes.value == 0L)
    assert(readTime.value == 0L)
  }

  test("does not cache failed deferred DV reads") {
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

        val dataFile = loadDeletionVectorFile(tablePath)
        val partitionedFile = partitionedFileWithMetadata(
          tablePath.toString,
          dataFile.relativePath,
          dataFile.fileSize,
          deletionVectorMetadata(dataFile.encodedDescriptor)
        )

        val readTime = SQLMetrics.createNanoTimingMetric(spark.sparkContext, "DV read time")
        val readBytes = SQLMetrics.createSizeMetric(spark.sparkContext, "DV read bytes")
        val readAttempts = SQLMetrics.createMetric(spark.sparkContext, "DV read attempts")
        val options = normalizeDeletionVectorOptions(
          partitionedFile,
          tablePath,
          readTime,
          readBytes,
          readAttempts)

        val dvPath = new Path(dataFile.absolutePath)
        val backupPath = new Path(dvPath.toString + ".retry-test-backup")
        val fs = dvPath.getFileSystem(spark.sessionState.newHadoopConf())
        assert(fs.rename(dvPath, backupPath))
        try {
          intercept[Exception] {
            options.serializedDeletionVector
          }
          assert(!options.isDeletionVectorPayloadMaterialized)
          assert(readAttempts.value == 1L)
          assert(readBytes.value == 0L)
        } finally {
          assert(fs.rename(backupPath, dvPath))
        }

        val payload = options.serializedDeletionVector
        assert(payload.nonEmpty)
        assert(options.isDeletionVectorPayloadMaterialized)
        assert(readAttempts.value == 2L)
        assert(readBytes.value == payload.length.toLong)
        assert(readTime.value > 0L)
    }
  }

  private def javaRoundTrip(options: DeltaFileReadOptions): DeltaFileReadOptions = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try {
      output.writeObject(options)
    } finally {
      output.close()
    }

    val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try {
      input.readObject().asInstanceOf[DeltaFileReadOptions]
    } finally {
      input.close()
    }
  }

}
