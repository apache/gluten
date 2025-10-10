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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowNativeBatchType
import org.apache.gluten.backendsapi.bolt.BoltBatchType
import org.apache.gluten.columnarbatch.BoltColumnarBatches
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ArrowColumnarToBoltColumnarExec(override val child: SparkPlan)
  extends ColumnarToColumnarExec(child)
  with GlutenColumnarToColumnarTransition {

  override protected val from: Convention.BatchType = ArrowNativeBatchType

  override protected val to: Convention.BatchType = BoltBatchType

  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    in.map(b => BoltColumnarBatches.toBoltBatch(b))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
