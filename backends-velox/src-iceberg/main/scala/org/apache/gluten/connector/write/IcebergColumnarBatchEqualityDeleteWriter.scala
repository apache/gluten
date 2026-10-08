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
package org.apache.gluten.connector.write

import org.apache.gluten.execution.IcebergWriteJniWrapper

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.{FileFormat, PartitionSpec, SortOrder}

class IcebergColumnarBatchEqualityDeleteWriter(
    writer: Long,
    jniWrapper: IcebergWriteJniWrapper,
    format: Int,
    partitionSpec: PartitionSpec,
    equalityFieldIds: Seq[Int],
    metricsPolicy: Option[IcebergEqualityDeleteMetrics] = None)
  extends DataWriter[ColumnarBatch] {

  private var closed = false

  private val delegate = IcebergColumnarBatchDataWriter(
    writer,
    jniWrapper,
    format,
    partitionSpec,
    SortOrder.unsorted())

  override def write(batch: ColumnarBatch): Unit = delegate.write(batch)

  override def commit(): IcebergEqualityDeleteCommitMessage = {
    val fileFormat = format match {
      case 0 => FileFormat.ORC
      case 1 => FileFormat.PARQUET
      case _ => throw new UnsupportedOperationException(s"Unsupported file format: $format")
    }
    IcebergEqualityDeleteCommitMessage.fromJson(
      jniWrapper.commit(writer),
      partitionSpec,
      fileFormat,
      equalityFieldIds,
      metricsPolicy)
  }

  override def abort(): Unit = {
    if (!closed) {
      jniWrapper.abort(writer)
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      jniWrapper.close(writer)
    }
  }
  override def currentMetricsValues(): Array[CustomTaskMetric] = delegate.currentMetricsValues()
}
