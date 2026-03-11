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

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite

import org.apache.iceberg.spark.source.IcebergWriteUtil.supportsWrite

case class VeloxIcebergWriteToDataSourceV2Exec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    override val batchWrite: BatchWrite,
    writeMetrics: Seq[CustomMetric]
) extends AbstractIcebergWriteExec {

  override val customMetrics: Map[String, SQLMetric] = {
    writeMetrics.map {
      customMetric =>
        customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
    }.toMap ++ BackendsApiManager.getMetricsApiInstance.genBatchWriteMetrics(sparkContext)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergWriteExec =
    copy(query = newChild)
}

object VeloxIcebergWriteToDataSourceV2Exec {

  private def extractOuterWrite(batchWrite: BatchWrite): Option[Write] = {
    batchWrite match {
      case microBatchWrite: MicroBatchWrite =>
        try {
          val streamWrite = microBatchWrite.writeSupport
          val outerClassField = streamWrite.getClass.getDeclaredField("this$0")
          outerClassField.setAccessible(true)
          outerClassField.get(streamWrite) match {
            case write: Write => Some(write)
            case _ => None
          }
        } catch {
          case _: Throwable => None
        }
      case _ => None
    }
  }

  def apply(original: WriteToDataSourceV2Exec): Option[VeloxIcebergWriteToDataSourceV2Exec] = {
    extractOuterWrite(original.batchWrite)
      .filter(supportsWrite)
      .map {
        write =>
          VeloxIcebergWriteToDataSourceV2Exec(
            original.query,
            original.refreshCache,
            write,
            original.batchWrite,
            original.writeMetrics
          )
      }
  }
}
