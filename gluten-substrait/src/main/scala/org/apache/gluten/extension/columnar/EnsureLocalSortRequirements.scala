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

import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}

/**
 * This rule is similar with `EnsureRequirements` but only handle local `SortExec`.
 *
 * The reason is that, during transform SparkPlan to GlutenPlan, some operators do not need local
 * sort anymore, e.g., convert SortAggregate to HashAggregateTransformer, and we remove local sort
 * eagerly. However, it may break the other operator's requirements, e.g., A SortMergeJoin on top of
 * SortAggregate with the same key. So, this rule adds local sort back if necessary.
 */
object EnsureLocalSortRequirements extends Rule[SparkPlan] {
  private lazy val transform: HeuristicTransform = HeuristicTransform.static()

  private def addLocalSort(
      originalChild: SparkPlan,
      requiredOrdering: Seq[SortOrder]): SparkPlan = {
    // FIXME: HeuristicTransform is costly. Re-applying it may cause performance issues.
    val newChild = SortExec(requiredOrdering, global = false, child = originalChild)
    newChild.copyTagsFrom(originalChild)
    transform.apply(newChild)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p =>
        val newChildren = p.children.zip(p.requiredChildOrdering).map {
          case (child, requiredOrdering) =>
            // If child.outputOrdering already satisfies the requiredOrdering,
            // we do not need to sort.
            if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
              child
            } else {
              addLocalSort(child, requiredOrdering)
            }
        }
        p.withNewChildren(newChildren)
    }
  }
}
