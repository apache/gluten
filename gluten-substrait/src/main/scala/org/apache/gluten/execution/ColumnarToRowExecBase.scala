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
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}

abstract class ColumnarToRowExecBase(child: SparkPlan)
  extends ColumnarToRowTransition
  with ValidatablePlan {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genColumnarToRowMetrics(sparkContext)

  final override def output: Seq[Attribute] = child.output

  final override def outputPartitioning: Partitioning = child.outputPartitioning

  final override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def batchType(): Convention.BatchType = Convention.BatchType.None

  override def rowType(): Convention.RowType = Convention.RowType.VanillaRowType

  override def requiredChildConvention(): Seq[ConventionReq] = {
    List(
      ConventionReq.ofBatch(
        ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType)))
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    // Require for explicit implementation, otherwise throw error.
    super.doExecuteBroadcast[T]()
  }

  def doExecuteInternal(): RDD[InternalRow]

  override def doExecute(): RDD[InternalRow] = {
    doExecuteInternal()
  }
}
