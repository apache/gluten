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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{LocalTableScanExec, LocalTableScanTransformer, RowToCHNativeColumnarExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Converts an already computed Paimon count result into a CH native batch. */
case class CHPaimonCountTransformer(
    outputAttributes: Seq[Attribute],
    @transient rows: Seq[InternalRow],
    override val outputPartitioning: Partitioning,
    override val outputOrdering: Seq[SortOrder])
  extends LocalTableScanTransformer(outputAttributes, outputPartitioning, outputOrdering) {

  override protected def doValidateInternal(): ValidationResult = {
    if (rows == null) {
      ValidationResult.failed("Deserialized Paimon count scan has no rows")
    } else {
      ValidationResult.succeeded
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (rows == null) {
      throw new IllegalStateException("Deserialized Paimon count scan has no rows")
    }
    RowToCHNativeColumnarExec(LocalTableScanExec(output, rows)).executeColumnar()
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    assert(newChildren.isEmpty)
    copy(outputAttributes, rows, outputPartitioning, outputOrdering)
  }
}

object CHPaimonCountTransformer {
  def replace(plan: LocalTableScanExec): LocalTableScanTransformer =
    CHPaimonCountTransformer(
      plan.output,
      plan.rows,
      plan.outputPartitioning,
      plan.outputOrdering)
}
