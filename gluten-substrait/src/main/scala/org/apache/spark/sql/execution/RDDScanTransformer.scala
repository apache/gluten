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
import org.apache.gluten.execution.ValidatablePlan
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}

abstract class RDDScanTransformer(
    outputAttributes: Seq[Attribute],
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val outputOrdering: Seq[SortOrder]
) extends ValidatablePlan {

  override def rowType(): Convention.RowType = Convention.RowType.None
  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType
  override def output: Seq[Attribute] = {
    outputAttributes
  }

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def children: Seq[SparkPlan] = Seq.empty
}

object RDDScanTransformer {
  def isSupportRDDScanExec(plan: RDDScanExec): Boolean =
    BackendsApiManager.getSparkPlanExecApiInstance.isSupportRDDScanExec(plan)

  def getRDDScanTransform(plan: RDDScanExec): RDDScanTransformer =
    BackendsApiManager.getSparkPlanExecApiInstance.getRDDScanTransform(plan)
}
