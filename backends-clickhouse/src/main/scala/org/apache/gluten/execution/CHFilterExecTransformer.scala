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

import org.apache.gluten.expression.CHFlattenedExpression

import org.apache.spark.sql.catalyst.expressions.{And, Expression, ExprId, IsNotNull}
import org.apache.spark.sql.execution.SparkPlan

case class CHFilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecTransformerBase(condition, child) {
  override protected def getRemainingCondition: Expression = {
    val scanFilters = child match {
      // Get the filters including the manually pushed down ones.
      case basicScanTransformer: BasicScanExecTransformer =>
        basicScanTransformer.filterExprs()
      // In ColumnarGuardRules, the child is still row-based. Need to get the original filters.
      case _ =>
        FilterHandler.getScanFilters(child)
    }
    if (scanFilters.isEmpty) {
      condition
    } else {
      val remainingFilters =
        FilterHandler.subtractFilters(splitConjunctivePredicates(condition), scanFilters)
      remainingFilters.reduceLeftOption(And).orNull
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CHFilterExecTransformer =
    copy(child = newChild)
}

case class FilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecTransformerBase(condition, child) {
  override protected def getRemainingCondition: Expression = condition
  override protected def withNewChildInternal(newChild: SparkPlan): FilterExecTransformer =
    copy(child = newChild)
  override protected val notNullAttributes: Seq[ExprId] = condition match {
    case s: CHFlattenedExpression =>
      val (notNullPreds, _) = s.children.partition {
        case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
        case _ => false
      }
      notNullPreds.flatMap(_.references).distinct.map(_.exprId).toList
    case _ => notNullPreds.flatMap(_.references).distinct.map(_.exprId).toList
  }
}
