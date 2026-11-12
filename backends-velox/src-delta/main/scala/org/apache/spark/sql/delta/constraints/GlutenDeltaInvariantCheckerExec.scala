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
package org.apache.spark.sql.delta.constraints

import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.execution.ColumnarToColumnarExec
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Gluten's columnar version of Delta's DeltaInvariantCheckerExec, for writes whose constraints are
 * all top-level NOT NULL constraints. It checks the Velox batches in place, so the write plan needs
 * no conversion to rows and back around it.
 */
case class GlutenDeltaInvariantCheckerExec(
    override val child: SparkPlan,
    checker: GlutenDeltaInvariantChecker)
  extends ColumnarToColumnarExec(child) {

  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] =
    in.map {
      batch =>
        checker.check(batch)
        batch
    }

  override def batchType(): Convention.BatchType = VeloxBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
