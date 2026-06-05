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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.{ProjectExecTransformer, ShuffledHashJoinExecTransformerBase, SortAggregateExecTransformer, SortExecTransformer, WindowGroupLimitExecTransformer}

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan, UnaryExecNode}

/**
 * This rule is used to eliminate unnecessary local sort.
 *
 * This could happen if:
 *   - Convert sort merge join to shuffled hash join
 *   - Offload SortAggregate to native hash aggregate
 *   - Offload WindowGroupLimit to native TopNRowNumber
 *   - Offload Window which has date type range frame
 */
object EliminateLocalSort extends Rule[SparkPlan] {
  private def canEliminateLocalSort(p: SparkPlan): Boolean = p match {
    case _: SortAggregateExecTransformer => true
    case _: ShuffledHashJoinExecTransformerBase => true
    case _: WindowGroupLimitExecTransformer => true
    case s: SortExec if s.global == false => true
    case s: SortExecTransformer if s.global == false => true
    case _ => false
  }

  private def canThrough(p: SparkPlan): Boolean = p match {
    case _: ProjectExec => true
    case _: ProjectExecTransformer => true
    case _ => false
  }

  private def orderingSatisfies(gChild: SparkPlan, requiredOrdering: Seq[SortOrder]): Boolean = {
    SortOrder.orderingSatisfies(gChild.outputOrdering, requiredOrdering)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case p if canEliminateLocalSort(p) =>
        val requiredChildOrdering = p.requiredChildOrdering
        assert(requiredChildOrdering.size == p.children.size)
        val newChildren = p.children.zipWithIndex.map {
          case (SortWithChild(gChild), i) if orderingSatisfies(gChild, requiredChildOrdering(i)) =>
            gChild
          case (p: UnaryExecNode, i) if canThrough(p) =>
            // There may be more than one project between target operator and sort,
            // e.g., both hash aggregate and sort pull out project
            p.child match {
              case SortWithChild(gChild) if orderingSatisfies(gChild, requiredChildOrdering(i)) =>
                p.withNewChildren(gChild :: Nil)
              case _ => p
            }
          case p => p._1
        }
        p.withNewChildren(newChildren)
    }
  }
}

object SortWithChild {
  def unapply(plan: SparkPlan): Option[SparkPlan] = {
    plan match {
      case p1 @ ProjectExec(_, SortExecTransformer(_, false, p2: ProjectExec, _))
          if p1.outputSet == p2.child.outputSet =>
        Some(p2.child)
      case p1 @ ProjectExecTransformer(
            _,
            SortExecTransformer(_, false, p2: ProjectExecTransformer, _))
          if p1.outputSet == p2.child.outputSet =>
        Some(p2.child)
      case SortExec(_, false, child, _) =>
        Some(child)
      case SortExecTransformer(_, false, child, _) =>
        Some(child)
      case _ => None
    }
  }
}
