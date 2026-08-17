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
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Columnar-aware replacement for Spark's EmptyRelationExec (Spark 4.0+).
 *
 * The node is dual-mode: it advertises both a columnar (primary backend batch) output and a vanilla
 * row output, and implements execution for both. This lets the transition framework consume it
 * directly from either a columnar or a row context, so an empty relation propagated by AQE never
 * forces surrounding operators into ColumnarToRow / RowToColumnar transitions.
 *
 * It carries no data, so it deliberately extends [[GlutenPlan]] rather than
 * [[org.apache.gluten.execution.ValidatablePlan]]: native schema validation is irrelevant for a
 * relation that never sends rows to the backend, and applying it would needlessly force fallback
 * whenever the (unused) output schema contains a type the backend cannot execute natively.
 */
case class EmptyRelationExecTransformer(output: Seq[Attribute])
  extends LeafExecNode
  with GlutenPlan {

  override def rowType(): Convention.RowType = Convention.RowType.VanillaRowType

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override protected def doExecute(): RDD[InternalRow] =
    sparkContext.emptyRDD[InternalRow]

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.emptyRDD[ColumnarBatch]
}

object EmptyRelationExecTransformer {

  /**
   * Whether the backend supports offloading the given empty-relation plan to a columnar
   * transformer. The plan is typed as [[SparkPlan]] because EmptyRelationExec only exists on Spark
   * 4.0+; callers must first confirm the type through `SparkShims.isEmptyRelationExec`.
   */
  def isSupportEmptyRelationExec(plan: SparkPlan): Boolean =
    BackendsApiManager.getSparkPlanExecApiInstance.isSupportEmptyRelationExec(plan)

  def getEmptyRelationExecTransform(plan: SparkPlan): EmptyRelationExecTransformer =
    BackendsApiManager.getSparkPlanExecApiInstance.getEmptyRelationExecTransform(plan)
}
