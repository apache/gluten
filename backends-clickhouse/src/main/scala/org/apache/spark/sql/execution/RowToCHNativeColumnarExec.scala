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

import org.apache.gluten.execution.{RowToColumnarExecBase, SparkRowIterator}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.vectorized.{CHBlockConverterJniWrapper, CHNativeBlock}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * this class is only for test use, not yet suggested to use in non-test code the performance is not
 * optimized
 *
 * @param child:
 *   the input plan
 */
case class RowToCHNativeColumnarExec(child: SparkPlan)
  extends RowToColumnarExecBase(child = child) {

  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = this.schema
    val fieldNames = output.map(ConverterUtils.genColumnNameWithExprId).toArray
    val fieldTypes = output
      .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable).toProtobuf.toByteArray)
      .toArray
    child.execute().mapPartitions {
      rowIterator =>
        val projection = UnsafeProjection.create(localSchema)
        if (rowIterator.hasNext) {
          val res = new Iterator[ColumnarBatch] {
            private val byteArrayIterator = rowIterator.map {
              case u: UnsafeRow => u.getBytes
              case i: InternalRow => projection.apply(i).getBytes
            }

            private var last_address: Long = 0

            override def hasNext: Boolean = {
              if (last_address != 0) {
                CHBlockConverterJniWrapper.freeBlock(last_address)
                last_address = 0
              }
              byteArrayIterator.hasNext
            }

            override def next(): ColumnarBatch = {
              val block = GlutenTimeMetric.millis(convertTime) {
                _ =>
                  val slice = byteArrayIterator.take(8192)
                  val sparkRowIterator = new SparkRowIterator(slice)
                  last_address = CHBlockConverterJniWrapper
                    .convertSparkRowsToCHColumn(sparkRowIterator, fieldNames, fieldTypes)
                  new CHNativeBlock(last_address)
              }
              numInputRows += block.numRows()
              numOutputBatches += 1
              block.toColumnarBatch
            }
          }
          res
        } else {
          Iterator.empty
        }
    }
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.executeBroadcast[T]()
  }

  protected def withNewChildInternal(newChild: SparkPlan): RowToCHNativeColumnarExec =
    copy(child = newChild)
}
