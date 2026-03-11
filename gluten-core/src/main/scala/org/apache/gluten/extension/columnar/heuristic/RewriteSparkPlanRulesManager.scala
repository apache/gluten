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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.extension.columnar.{FallbackTag, FallbackTags}
import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, ProjectExec, SparkPlan}

case class RewrittenNodeWall(originalChild: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def supportsColumnar: Boolean = originalChild.supportsColumnar
  override def output: Seq[Attribute] = originalChild.output
  override def outputOrdering: Seq[SortOrder] = originalChild.outputOrdering
  override def outputPartitioning: Partitioning = originalChild.outputPartitioning
}

/**
 * A rule that holds a batch of [[Rule]]s to rewrite spark plan. When an operator can not be
 * offloaded to native, we try to rewrite it, e.g., pull out the complex exprs, so that we have one
 * more chance to offload it. If the rewritten plan still can not be offloaded, fallback to origin.
 *
 * Note that, this rule does not touch and tag these operators who does not need to rewrite.
 */
class RewriteSparkPlanRulesManager private (
    validateRule: Rule[SparkPlan],
    rewriteRules: Seq[RewriteSingleNode])
  extends Rule[SparkPlan] {

  private def mayNeedRewrite(plan: SparkPlan): Boolean = {
    FallbackTags.maybeOffloadable(plan) && rewriteRules.exists(_.isRewritable(plan))
  }

  private def getFallbackTagBack(rewrittenPlan: SparkPlan): Option[FallbackTag] = {
    // The rewritten plan may contain more nodes than origin, for now it should only be
    // `ProjectExec`.
    // TODO: Find a better approach than checking `p.isInstanceOf[ProjectExec]` which is not
    //  general.
    val target = rewrittenPlan.collect {
      case p if !p.isInstanceOf[ProjectExec] && !p.isInstanceOf[RewrittenNodeWall] => p
    }
    assert(target.size == 1)
    FallbackTags.getOption(target.head)
  }

  private def applyRewriteRules(origin: SparkPlan): (SparkPlan, Option[String]) = {
    try {
      val rewrittenPlan = rewriteRules.foldLeft(origin) {
        case (plan, rule) =>
          // Some rewrite rules may generate new parent plan node, we should use transform to
          // rewrite the original plan. For example, PullOutPreProject and PullOutPostProject
          // will generate post-project plan node.
          plan.transformUp { case p => rule.rewrite(p) }
      }
      (rewrittenPlan, None)
    } catch {
      case e: Exception =>
        // TODO: Remove this catch block
        //  See https://github.com/apache/gluten/issues/7766
        (origin, Option(e.getMessage))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case origin if mayNeedRewrite(origin) =>
        // Add a wall to avoid transforming unnecessary nodes.
        val withWall = origin.mapChildren(RewrittenNodeWall)
        val (rewrittenPlan, error) = applyRewriteRules(withWall)
        if (error.isDefined) {
          // Return origin if there is an exception during rewriting rules.
          // Note, it is not expected, but it happens in CH backend when pulling out
          // aggregate.
          // TODO: Fix the exception and remove this branch
          FallbackTags.add(origin, error.get)
          origin
        } else if (withWall.fastEquals(rewrittenPlan)) {
          // Return origin if the rewrite rules do nothing.
          // We do not add tag and leave it to the outside `AddFallbackTagRule`.
          origin
        } else {
          validateRule.apply(rewrittenPlan)
          val tag = getFallbackTagBack(rewrittenPlan)
          if (tag.isDefined) {
            // If the rewritten plan is still not transformable, return the original plan.
            FallbackTags.add(origin, tag.get)
            origin
          } else {
            rewrittenPlan.transformUp {
              case wall: RewrittenNodeWall => wall.originalChild
              case p if p.logicalLink.isEmpty =>
                // Add logical link to pull out project to make fallback reason work,
                // see `GlutenFallbackReporter`.
                origin.logicalLink.foreach(p.setLogicalLink)
                p
            }
          }
        }
    }
  }
}

object RewriteSparkPlanRulesManager {
  def apply(
      validateRule: Rule[SparkPlan],
      rewriteRules: Seq[RewriteSingleNode]): Rule[SparkPlan] = {
    new RewriteSparkPlanRulesManager(validateRule, rewriteRules)
  }
}
