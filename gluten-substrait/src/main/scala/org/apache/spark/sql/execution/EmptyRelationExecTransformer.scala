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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{ValidatablePlan, ValidationResult}
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Columnar-aware replacement for Spark's EmptyRelationExec (Spark 4.0+). It produces an empty
 * RDD[ColumnarBatch] so that surrounding columnar operators do not need to be wrapped in
 * unnecessary ColumnarToRow / RowToColumnar transitions when AQE propagates an empty relation
 * through the plan.
 */
case class EmptyRelationExecTransformer(output: Seq[Attribute]) extends ValidatablePlan {

  override def rowType(): Convention.RowType = Convention.RowType.None

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override protected def doValidateInternal(): ValidationResult = ValidationResult.succeeded

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(
      "EmptyRelationExecTransformer does not support row execution.")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.emptyRDD[ColumnarBatch]

  override def children: Seq[SparkPlan] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): SparkPlan = this
}

object EmptyRelationExecTransformer {

  /**
   * Whether the backend supports offloading the given empty-relation plan to native. The plan is
   * typed as [[SparkPlan]] because EmptyRelationExec only exists on Spark 4.0+; callers must first
   * confirm the type through `SparkShims.isEmptyRelationExec`.
   */
  def isSupportEmptyRelationExec(plan: SparkPlan): Boolean =
    BackendsApiManager.getSparkPlanExecApiInstance.isSupportEmptyRelationExec(plan)

  def getEmptyRelationExecTransform(plan: SparkPlan): EmptyRelationExecTransformer =
    BackendsApiManager.getSparkPlanExecApiInstance.getEmptyRelationExecTransform(plan)
}
