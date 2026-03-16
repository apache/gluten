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
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Converts a vanilla Spark plan node into Gluten plan node. Gluten plan is supposed to be executed
 * in native, and the internals of execution is subject by backend's implementation.
 *
 * Note: Only the current plan node is supposed to be open to modification. Do not access or modify
 * the children node. Tree-walking is done by caller of this trait.
 */
trait OffloadSingleNode extends Logging {
  def offload(plan: SparkPlan): SparkPlan
}

object OffloadSingleNode {
  implicit class OffloadSingleNodeOps(rule: OffloadSingleNode) {

    /**
     * Converts the [[OffloadSingleNode]] rule to a strict version.
     *
     * In the strict version of the rule, all children of the input query plan node will be replaced
     * with 'DummyLeafExec' nodes so they are not accessible from the rule body.
     */
    def toStrcitRule(): OffloadSingleNode = {
      new StrictRule(rule);
    }
  }

  private class StrictRule(delegate: OffloadSingleNode) extends OffloadSingleNode {
    override def offload(plan: SparkPlan): SparkPlan = {
      val planWithChildrenHidden = hideChildren(plan)
      val applied = delegate.offload(planWithChildrenHidden)
      val out = restoreHiddenChildren(applied)
      out
    }

    /**
     * Replaces the children with 'DummyLeafExec' nodes so they become inaccessible afterward. Used
     * when the children plan nodes can be dropped because not interested.
     */
    private def hideChildren[T <: SparkPlan](plan: T): T = {
      plan
        .withNewChildren(
          plan.children.map {
            child =>
              val dummyLeaf = DummyLeafExec(child)
              child.logicalLink.foreach(dummyLeaf.setLogicalLink)
              dummyLeaf
          }
        )
        .asInstanceOf[T]
    }

    /** Restores hidden children from the replaced 'DummyLeafExec' nodes. */
    private def restoreHiddenChildren[T <: SparkPlan](plan: T): T = {
      plan
        .transformDown {
          case d: DummyLeafExec =>
            d.hiddenPlan
          case other => other
        }
        .asInstanceOf[T]
    }
  }

  /**
   * The plan node that hides the real child plan node during #applyOnNode call. This is used when
   * query planner doesn't allow a rule to access the child plan nodes from the input query plan
   * node.
   */
  private case class DummyLeafExec(hiddenPlan: SparkPlan) extends LeafExecNode with GlutenPlan {
    private lazy val conv: Convention = Convention.get(hiddenPlan)

    override def batchType(): Convention.BatchType = conv.batchType
    override def rowType(): Convention.RowType = conv.rowType
    override def output: Seq[Attribute] = hiddenPlan.output
    override def outputPartitioning: Partitioning = hiddenPlan.outputPartitioning
    override def outputOrdering: Seq[SortOrder] = hiddenPlan.outputOrdering

    override def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Not allowed in #applyOnNode call")
    override def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException("Not allowed in #applyOnNode call")
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException("Not allowed in #applyOnNode call")
  }
}
