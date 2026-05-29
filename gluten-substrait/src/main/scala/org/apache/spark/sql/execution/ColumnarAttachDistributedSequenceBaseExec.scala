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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.python.AttachDistributedSequenceExec

/**
 * Base class for [[AttachDistributedSequenceExec]] transformation that can be implemented by
 * supported backends. The exec prepends a contiguous, globally increasing `Long` id column to the
 * child output. Used by pandas-on-Spark distributed-sequence default index and
 * `DataFrame.zipWithIndex`.
 */
abstract class ColumnarAttachDistributedSequenceBaseExec(
    sequenceAttr: Attribute,
    override val child: SparkPlan)
  extends UnaryExecNode
  with ValidatablePlan {

  override def producedAttributes: AttributeSet = AttributeSet(sequenceAttr)

  override val output: Seq[Attribute] = sequenceAttr +: child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def rowType(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val indexColumn = s"Index: $sequenceAttr"
    s"$nodeName$truncatedOutputString $indexColumn"
  }

  /**
   * Hook for backend implementations to release any resources (e.g. cached RDDs) that were
   * materialized during execution. Called from [[cleanupResources]] after children have been
   * cleaned up. The default implementation is a no-op.
   */
  protected def doColumnarCleanup(): Unit = {}

  override protected[sql] def cleanupResources(): Unit = {
    try {
      doColumnarCleanup()
    } finally {
      super.cleanupResources()
    }
  }
}

/**
 * Companion object for ColumnarAttachDistributedSequenceBaseExec, provides factory methods to
 * create instance from existing AttachDistributedSequenceExec plan.
 */
object ColumnarAttachDistributedSequenceBaseExec {
  def from(plan: AttachDistributedSequenceExec): ColumnarAttachDistributedSequenceBaseExec = {
    BackendsApiManager.getSparkPlanExecApiInstance
      .genColumnarAttachDistributedSequenceExec(plan)
  }
}
