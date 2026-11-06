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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.VeloxBloomFilterMightContain
import org.apache.gluten.expression.aggregate.VeloxBloomFilterAggregate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, BinaryExpression, BloomFilterMightContain, Cast, Expression, IsNotNull, NamedExpression, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType}

case class BloomFilterMightContainJointRewriteRule(
    spark: SparkSession,
    isBloomFilterStatFunction: Boolean)
  extends Rule[SparkPlan] {
  import BloomFilterMightContainJointRewriteRule._

  override def apply(plan: SparkPlan): SparkPlan = {
    if (isBloomFilterStatFunction || !GlutenConfig.get.enableNativeBloomFilter) {
      return plan
    }
    val out = plan.transformWithSubqueries {
      case p =>
        applyForNode(p)
    }
    out
  }

  private def replaceBloomFilterAggregate[T](
      expr: Expression,
      bloomFilterAggReplacer: (
          Expression,
          Expression,
          Expression,
          Int,
          Int) => TypedImperativeAggregate[T]): Expression = expr match {
    case bf @ BloomFilterAggregate(
          child,
          estimatedNumItemsExpression,
          numBitsExpression,
          mutableAggBufferOffset,
          inputAggBufferOffset) if !isKeepVanilla(bf) =>
      bloomFilterAggReplacer(
        child,
        estimatedNumItemsExpression,
        numBitsExpression,
        mutableAggBufferOffset,
        inputAggBufferOffset)
    case other => other
  }

  private def replaceMightContain[T](
      expr: Expression,
      mightContainReplacer: (Expression, Expression) => BinaryExpression): Expression = expr match {
    case mc @ BloomFilterMightContain(bloomFilterExpression, valueExpression)
        if !isKeepVanilla(mc) =>
      mightContainReplacer(bloomFilterExpression, valueExpression)
    case other => other
  }

  private def applyForNode(p: SparkPlan) = {
    p.transformExpressions {
      case e =>
        replaceMightContain(
          replaceBloomFilterAggregate(e, VeloxBloomFilterAggregate.apply),
          VeloxBloomFilterMightContain.apply)
    }
  }
}

object BloomFilterMightContainJointRewriteRule {
  private val KEEP_VANILLA_BLOOM_FILTER_TAG =
    TreeNodeTag[Unit]("org.apache.gluten.keepVanillaBloomFilter")

  private def tagKeepVanilla(expr: Expression): Expression = {
    expr.setTagValue(KEEP_VANILLA_BLOOM_FILTER_TAG, ())
    expr
  }

  private def isKeepVanilla(expr: Expression): Boolean = {
    expr.getTagValue(KEEP_VANILLA_BLOOM_FILTER_TAG).isDefined
  }

  class Logical(spark: SparkSession) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!GlutenConfig.get.enableNativeBloomFilter) {
        return plan
      }
      rewritePlan(plan)
    }

    private def rewritePlan(plan: LogicalPlan): LogicalPlan = {
      plan.mapChildren(rewritePlan).mapExpressions(rewriteExpr)
    }

    private def rewriteExpr(expr: Expression): Expression = {
      val rewritten = expr.mapChildren(rewriteExpr) match {
        case sub: SubqueryExpression =>
          sub.withNewPlan(rewritePlan(sub.plan))
        case other =>
          other
      }
      rewritten match {
        case mc: BloomFilterMightContain =>
          rewriteMightContain(mc)
        case bfAgg: BloomFilterAggregate =>
          tagKeepVanilla(bfAgg)
        case other =>
          other
      }
    }

    private def rewriteMightContain(mc: BloomFilterMightContain): Expression = {
      mc.left match {
        case sub: ScalarSubquery =>
          rewritePairedSubquery(sub.plan)
            .map(
              rewrittenPlan =>
                VeloxBloomFilterMightContain(sub.withNewPlan(rewrittenPlan), mc.right))
            .getOrElse(tagKeepVanilla(mc))
        case _ =>
          tagKeepVanilla(mc)
      }
    }

    private def rewritePairedSubquery(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case agg: Aggregate =>
        val head = agg.aggregateExpressions.head
        head.children.headOption.flatMap {
          case aggExpr: AggregateExpression =>
            aggExpr.aggregateFunction match {
              case bfAgg: BloomFilterAggregate
                  if isSupportedInputType(bfAgg.child.dataType) =>
                val newChild = widenToLongIfNeeded(bfAgg.child)
                val newFilter = addNotNullFilterIfNeeded(bfAgg.child, aggExpr.filter)
                val newBfAgg = VeloxBloomFilterAggregate(
                  newChild,
                  bfAgg.estimatedNumItemsExpression,
                  bfAgg.numBitsExpression,
                  bfAgg.mutableAggBufferOffset,
                  bfAgg.inputAggBufferOffset)
                val newAggExpr =
                  aggExpr.copy(aggregateFunction = newBfAgg, filter = newFilter)
                val newHead =
                  head.withNewChildren(Seq(newAggExpr)).asInstanceOf[NamedExpression]
                Some(agg.copy(aggregateExpressions = Seq(newHead)))
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }

    private def isSupportedInputType(dataType: DataType): Boolean = dataType match {
      case ByteType | ShortType | IntegerType | LongType => true
      case _ => false
    }

    private def widenToLongIfNeeded(expr: Expression): Expression = expr.dataType match {
      case LongType => expr
      case _ => Cast(expr, LongType)
    }

    private def addNotNullFilterIfNeeded(
        child: Expression,
        filter: Option[Expression]): Option[Expression] = {
      if (!child.nullable) {
        filter
      } else {
        val notNull = IsNotNull(child)
        Some(filter.map(And(_, notNull)).getOrElse(notNull))
      }
    }
  }

  object Logical {
    def apply(spark: SparkSession): Logical = new Logical(spark)
  }
}
