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
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.extension.columnar.transition.Convention.BatchType

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.annotation.tailrec

case class InsertTransitions(convReq: ConventionReq) extends Rule[SparkPlan] {
  private val convFunc = ConventionFunc.create()

  override def apply(plan: SparkPlan): SparkPlan = {
    // Remove all transitions at first.
    val removed = RemoveTransitions.apply(plan)
    val filled = fillWithTransitions(removed)
    val out = Transitions.enforceReq(filled, convReq)
    out
  }

  private def fillWithTransitions(plan: SparkPlan): SparkPlan = plan.transformUp {
    case node if node.children.nonEmpty => applyForNode(node)
  }

  private def applyForNode(node: SparkPlan): SparkPlan = {
    val convReqs = convFunc.conventionReqOf(node)
    val newChildren = node.children.zip(convReqs).map {
      case (child, convReq) =>
        val from = convFunc.conventionOf(child)
        if (from.isNone) {
          // For example, a union op with row child and columnar child at the same time,
          // The plan is actually not executable, and we cannot tell about its convention.
          child
        } else {
          val transition =
            Transition.factory.findTransitionOrThrow(from, convReq)(Transition.notFound(node))
          val newChild = transition.apply(child)
          newChild
        }
    }
    node.withNewChildren(newChildren)
  }
}

object InsertTransitions {
  def create(outputsColumnar: Boolean, batchType: BatchType): InsertTransitions = {
    val conventionReq = if (outputsColumnar) {
      ConventionReq.ofBatch(ConventionReq.BatchType.Is(batchType))
    } else {
      ConventionReq.vanillaRow
    }
    InsertTransitions(conventionReq)
  }
}

object RemoveTransitions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown { case p => removeForNode(p) }

  @tailrec
  private[transition] def removeForNode(plan: SparkPlan): SparkPlan = plan match {
    case ColumnarToRowLike(child) => removeForNode(child)
    case RowToColumnarLike(child) => removeForNode(child)
    case ColumnarToColumnarLike(child) => removeForNode(child)
    case other => other
  }
}

object Transitions {
  def insert(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    InsertTransitions.create(outputsColumnar, BatchType.VanillaBatchType).apply(plan)
  }

  def toRowPlan(plan: SparkPlan): SparkPlan = {
    enforceReq(plan, ConventionReq.vanillaRow)
  }

  def toBatchPlan(plan: SparkPlan, toBatchType: Convention.BatchType): SparkPlan = {
    enforceReq(plan, ConventionReq.ofBatch(ConventionReq.BatchType.Is(toBatchType)))
  }

  def enforceReq(plan: SparkPlan, req: ConventionReq): SparkPlan = {
    val convFunc = ConventionFunc.create()
    val removed = RemoveTransitions.removeForNode(plan)
    val transition = Transition.factory
      .findTransitionOrThrow(convFunc.conventionOf(removed), req)(Transition.notFound(removed, req))
    val out = transition.apply(removed)
    out
  }
}
