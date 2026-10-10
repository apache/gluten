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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.datasource.BoltDataSourceJniWrapper
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.{ArrowAbiUtil, ConfigUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.bolt.BoltParquetWriterInjects
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Preconditions
import org.apache.arrow.c.ArrowSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import java.io.IOException
import java.net.URI

import scala.collection.JavaConverters._

/** The Parquet Writer implements the [[RecordWriter]] interface. */
class BoltParquetBatchWriter(
    path: String,
    options: Map[String, String],
    dataSchema: StructType,
    conf: Configuration)
  extends RecordWriter[Void, ColumnarBatch]
  with Logging {
  val originPath = path
  URI.create(originPath) // validate uri
  val arrowSchema = SparkArrowUtil.toArrowSchema(dataSchema, SQLConf.get.sessionLocalTimeZone)
  val cSchema = ArrowSchema.allocateNew(ArrowBufferAllocators.contextInstance())
  var dsHandle = -1L
  val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "BoltParquetWriter")
  val datasourceJniWrapper = BoltDataSourceJniWrapper.create(runtime)
  val allocator = ArrowBufferAllocators.contextInstance()
  val parquetOptions = new ParquetOptions(options, SQLConf.get)
  val nativeConf =
    new BoltParquetWriterInjects().nativeConf(options, parquetOptions.compressionCodecClassName)
  val queueSize = 16
  val (encryptionAlgorithm, encryptionConf) =
    ParquetEncryption.generateEncryptionOptionsFromProperties(
      ParquetEncryption.getFileEncryptionProperties(conf, new Path(path))
    )
  try {
    logWarning(
      s"path is $originPath, schema is ${dataSchema.toString()}, " +
        s"encryption conf is ${encryptionConf.asScala.map(t => s"${t._1}=${t._2}").mkString(";")}")
    ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
    dsHandle = datasourceJniWrapper.init(
      originPath,
      cSchema.memoryAddress(),
      ConfigUtil.serialize(nativeConf),
      encryptionAlgorithm,
      encryptionConf.keySet().toArray(Array[String]()),
      encryptionConf.values().toArray(Array[Array[Byte]]())
    )
  } catch {
    case e: IOException =>
      throw new RuntimeException(e)
  } finally {
    cSchema.close()
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def write(key: Void, batch: ColumnarBatch): Unit = {
    Preconditions.checkState(ColumnarBatches.isLightBatch(batch))
    ColumnarBatches.retain(batch)

    val batchHandler = {
      if (batch.numCols == 0) {
        // the operation will find a zero column batch from a task-local pool
        ColumnarBatchJniWrapper.create(runtime).getForEmptySchema(batch.numRows)
      } else {
        val offloaded =
          ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance, batch)
        ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, offloaded)
      }
    }
    datasourceJniWrapper.writeBatch(dsHandle, batchHandler)
    batch.close()
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def close(taskAttemptContext: TaskAttemptContext): Unit = {
    datasourceJniWrapper.close(dsHandle)
  }
}
