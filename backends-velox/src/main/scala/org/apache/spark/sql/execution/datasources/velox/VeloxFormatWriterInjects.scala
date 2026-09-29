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
package org.apache.spark.sql.execution.datasources.velox

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.datasource.{VeloxDataSourceJniWrapper, VeloxDataSourceUtil}
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.BatchCarrierRow
import org.apache.gluten.execution.datasource.GlutenRowSplitter
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.ArrowSchema
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.io.IOException

trait VeloxFormatWriterInjects extends GlutenFormatWriterInjectsBase {
  def createOutputWriter(
      filePath: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: java.util.Map[String, String]): OutputWriter = {
    // Create the hdfs path if not existed.
    val hdfsSchema = "hdfs://"
    if (filePath.startsWith(hdfsSchema)) {
      val hdfsPath = new Path(filePath)
      val fs = hdfsPath.getFileSystem(context.getConfiguration)
      if (!fs.exists(hdfsPath.getParent)) {
        fs.mkdirs(hdfsPath.getParent)
      }
    }

    val arrowSchema =
      SparkArrowUtil.toArrowSchema(dataSchema, SQLConf.get.sessionLocalTimeZone)
    val cSchema = ArrowSchema.allocateNew(ArrowBufferAllocators.contextInstance())
    var dsHandle = -1L
    val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "VeloxWriter")
    val datasourceJniWrapper = VeloxDataSourceJniWrapper.create(runtime)
    val allocator = ArrowBufferAllocators.contextInstance()
    try {
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      dsHandle = datasourceJniWrapper.init(filePath, cSchema.memoryAddress(), nativeConf)
    } catch {
      case e: IOException =>
        throw new GlutenException(e)
    } finally {
      cSchema.close()
    }

    new OutputWriter {
      override def write(row: InternalRow): Unit = {
        BatchCarrierRow.unwrap(row).foreach {
          batch =>
            ColumnarBatches.checkOffloaded(batch)
            ColumnarBatches.retain(batch)
            val batchHandle = {
              ColumnarBatches.checkOffloaded(batch)
              ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
            }
            datasourceJniWrapper.writeBatch(dsHandle, batchHandle)
            batch.close()
        }
      }

      override def close(): Unit = {
        datasourceJniWrapper.close(dsHandle)
      }

      override def path(): String = {
        filePath
      }
    }
  }

  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    VeloxDataSourceUtil.readSchema(files)
  }
}

class VeloxRowSplitter extends GlutenRowSplitter {
  def splitBlockByPartitionAndBucket(
      batch: ColumnarBatch,
      partitionColIndice: Array[Int],
      hasBucket: Boolean,
      reservePartitionColumns: Boolean = false): BlockStripes = {
    val handler = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val runtime =
      Runtimes.contextInstance(BackendsApiManager.getBackendName, "VeloxRowSplitter")
    val datasourceJniWrapper = VeloxDataSourceJniWrapper.create(runtime)
    new VeloxBlockStripes(
      datasourceJniWrapper
        .splitBlockByPartitionAndBucket(handler, partitionColIndice, hasBucket))
  }
}
