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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.execution._
import org.apache.gluten.utils._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

import scala.collection.mutable

/*
 * It will add a projection before hash join for
 * 1. The join keys which are not attribute references.
 * 2. If the join could rewrite into multiple join contiditions, also replace all the join keys with
 * attribute references.
 *
 * JoinUtils.createPreProjectionIfNeeded has add a pre-projection for join keys, but it doesn't
 * handle the post filter.
 */
case class AddPreProjectionForHashJoin(session: SparkSession)
  extends Rule[SparkPlan]
  with PullOutProjectHelper
  with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!CHBackendSettings.enablePreProjectionForJoinConditions) {
      return plan
    }

    plan.transformUp {
      case hashJoin: CHShuffledHashJoinExecTransformer =>
        val leftReplacedExpressions = new mutable.HashMap[Expression, NamedExpression]
        val rightReplacedExpressions = new mutable.HashMap[Expression, NamedExpression]

        val newLeftKeys = hashJoin.leftKeys.map {
          case e => replaceExpressionWithAttribute(e, leftReplacedExpressions, false, false)
        }

        val newRightKeys = hashJoin.rightKeys.map {
          case e => replaceExpressionWithAttribute(e, rightReplacedExpressions, false, false)
        }

        val newCondition = replaceExpressionInCondition(
          hashJoin.condition,
          hashJoin.left,
          leftReplacedExpressions,
          hashJoin.right,
          rightReplacedExpressions)
        val leftProjectExprs =
          eliminateProjectList(hashJoin.left.outputSet, leftReplacedExpressions.values.toSeq)
        val rightProjectExprs =
          eliminateProjectList(hashJoin.right.outputSet, rightReplacedExpressions.values.toSeq)
        val newHashJoin = hashJoin.copy(
          leftKeys = newLeftKeys,
          rightKeys = newRightKeys,
          condition = newCondition,
          left = if (leftReplacedExpressions.size > 0) {
            ProjectExecTransformer(leftProjectExprs, hashJoin.left)
          } else { hashJoin.left },
          right = if (rightReplacedExpressions.size > 0) {
            ProjectExecTransformer(rightProjectExprs, hashJoin.right)
          } else { hashJoin.right }
        )
        if (leftReplacedExpressions.size > 0 || rightReplacedExpressions.size > 0) {
          ProjectExecTransformer(hashJoin.output, newHashJoin)
        } else {
          newHashJoin
        }
    }
  }

  private def replaceExpressionInCondition(
      condition: Option[Expression],
      leftPlan: SparkPlan,
      leftReplacedExpressions: mutable.HashMap[Expression, NamedExpression],
      rightPlan: SparkPlan,
      rightReplacedExpressions: mutable.HashMap[Expression, NamedExpression])
      : Option[Expression] = {
    if (!condition.isDefined) {
      return condition
    }

    def replaceExpression(
        e: Expression,
        exprMap: mutable.HashMap[Expression, NamedExpression]): Expression = {
      e match {
        case or @ Or(left, right) =>
          Or(replaceExpression(left, exprMap), replaceExpression(right, exprMap))
        case and @ And(left, right) =>
          And(replaceExpression(left, exprMap), replaceExpression(right, exprMap))
        case equalTo @ EqualTo(left, right) =>
          EqualTo(replaceExpression(left, exprMap), replaceExpression(right, exprMap))
        case _ =>
          exprMap.getOrElseUpdate(e.canonicalized, null) match {
            case null => e
            case ne: NamedExpression => ne.toAttribute
          }
      }
    }

    val leftExprs = new mutable.ArrayBuffer[Expression]
    val rightExps = new mutable.ArrayBuffer[Expression]
    if (isMultipleOrEqualsCondition(condition.get, leftPlan, leftExprs, rightPlan, rightExps)) {
      leftExprs.foreach {
        e => replaceExpressionWithAttribute(e, leftReplacedExpressions, false, false)
      }
      rightExps.foreach {
        e => replaceExpressionWithAttribute(e, rightReplacedExpressions, false, false)
      }
      Some(replaceExpression(condition.get, leftReplacedExpressions ++ rightReplacedExpressions))
    } else {
      condition
    }
  }

  private def isMultipleOrEqualsCondition(
      e: Expression,
      leftPlan: SparkPlan,
      leftExpressions: mutable.ArrayBuffer[Expression],
      rightPlan: SparkPlan,
      rightExpressions: mutable.ArrayBuffer[Expression]): Boolean = {
    def splitIntoOrExpressions(e: Expression, result: mutable.ArrayBuffer[Expression]): Unit = {
      e match {
        case Or(left, right) =>
          splitIntoOrExpressions(left, result)
          splitIntoOrExpressions(right, result)
        case _ =>
          result += e
      }
    }
    def splitIntoAndExpressions(e: Expression, result: mutable.ArrayBuffer[Expression]): Unit = {
      e match {
        case And(left, right) =>
          splitIntoAndExpressions(left, result)
          splitIntoAndExpressions(right, result)
        case _ =>
          result += e
      }
    }

    val leftOutputSet = leftPlan.outputSet
    val rightOutputSet = rightPlan.outputSet
    val orExpressions = new mutable.ArrayBuffer[Expression]()
    splitIntoOrExpressions(e, orExpressions)
    orExpressions.foreach {
      orExpression =>
        val andExpressions = new mutable.ArrayBuffer[Expression]()
        splitIntoAndExpressions(orExpression, andExpressions)
        andExpressions.foreach {
          e =>
            if (!e.isInstanceOf[EqualTo]) {
              return false
            }
            val equalExpr = e.asInstanceOf[EqualTo]
            val leftPos = if (equalExpr.left.references.subsetOf(leftOutputSet)) {
              leftExpressions += equalExpr.left
              0
            } else if (equalExpr.left.references.subsetOf(rightOutputSet)) {
              rightExpressions += equalExpr.left
              1
            } else {
              return false
            }
            val rightPos = if (equalExpr.right.references.subsetOf(leftOutputSet)) {
              leftExpressions += equalExpr.right
              0
            } else if (equalExpr.right.references.subsetOf(rightOutputSet)) {
              rightExpressions += equalExpr.right
              1
            } else {
              return false
            }

            // they should come from different side
            if (leftPos == rightPos) {
              return false
            }
        }
    }
    true
  }
}
