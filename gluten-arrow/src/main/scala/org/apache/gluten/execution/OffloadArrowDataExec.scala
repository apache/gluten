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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.{ArrowJavaBatchType, ArrowNativeBatchType}
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Converts input data with batch type [[ArrowJavaBatchType]] to type [[ArrowNativeBatchType]]. */
case class OffloadArrowDataExec(override val child: SparkPlan)
  extends ColumnarToColumnarExec(child)
  with GlutenColumnarToColumnarTransition {

  override protected val from: Convention.BatchType = ArrowJavaBatchType

  override protected val to: Convention.BatchType = ArrowNativeBatchType

  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] =
    new OffloadArrowDataIterator(in)

  override protected def closeIterator(out: Iterator[ColumnarBatch]): Unit = out match {
    case offload: OffloadArrowDataIterator => offload.close()
    case _ =>
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

/**
 * Offloads each batch of `in`. [[ColumnarBatches.offload]] moves the data of a Gluten batch into
 * the input batch and returns it, so its producer still closes it. A batch of Spark
 * `ArrowColumnVector`s instead stays with its producer, and offload returns a new batch: that one
 * belongs to this iterator, which closes it when the next batch is requested or on [[close]].
 */
class OffloadArrowDataIterator(in: Iterator[ColumnarBatch])
  extends Iterator[ColumnarBatch]
  with AutoCloseable {
  private var owned: ColumnarBatch = _

  override def hasNext: Boolean = in.hasNext

  override def next(): ColumnarBatch = {
    release()
    val batch = in.next()
    val offloaded = ColumnarBatches.offload(ArrowBufferAllocators.contextInstance, batch)
    if (offloaded ne batch) {
      owned = offloaded
    }
    offloaded
  }

  override def close(): Unit = release()

  private def release(): Unit = {
    if (owned != null) {
      owned.close()
      owned = null
    }
  }
}
