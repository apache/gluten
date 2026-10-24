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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.DeltaPostTransformRules.containsIncrementMetricExpr

import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, CreateArray, CreateNamedStruct, Expression, If, Literal, NamedExpression, Not}
import org.apache.spark.sql.types.BooleanType

/**
 * Decides whether a project or filter carrying Delta's `IncrementMetric` may be offloaded.
 *
 * `IncrementMetric` adds one to a SQL metric every time it is evaluated and returns its child. The
 * native side never evaluates it: the Delta project and filter transformers strip it from the plan
 * and the metrics updaters credit the metric with the operator's output row count once the task
 * finishes. That is exact only when Spark would have evaluated the expression once per output row:
 * at the root of a projection alias, or as a filter condition that keeps every row. An
 * `IncrementMetric` below a conditional expression, which is where MERGE keeps the counter of every
 * `WHEN` clause (`CASE WHEN clause THEN increment ... ELSE increment(copied)`), fires for the rows
 * that take its branch only. Offloading it inflated those counters to the full row count, which
 * corrupted the operation metrics of every MERGE commit and, through
 * `CDCReader.shouldSkipFileActionsInCommit`, produced phantom change-data-feed rows for no-op
 * merges (GLUTEN-9003).
 *
 * A backend that counts natively
 * ([[org.apache.gluten.backendsapi.BackendSettingsApi.supportNativeIncrementMetric]]) evaluates
 * each counter as a pass-through function named after its metric and reports how many rows that
 * function processed, which is the exact evaluation count: inside a `CASE WHEN` branch only the
 * rows that took the branch reach it. That holds only below the parents Delta's DML commands put
 * above a counter ([[countsLikeSpark]]); a counter below any other parent stays on Spark. Other
 * backends keep all conditional shapes on Spark.
 */
object IncrementMetricOffload {

  /** Delta's `IncrementMetric`, matched by name so the shared module needs no Delta class. */
  private[gluten] def isIncrementMetric(expr: Expression): Boolean = {
    expr.prettyName == "increment_metric" && expr.children.size == 1
  }

  /** Removes a stack of `IncrementMetric` wrappers and returns the expression they wrapped. */
  private[gluten] def peelIncrementMetrics(expr: Expression): Expression = {
    if (isIncrementMetric(expr)) peelIncrementMetrics(expr.children.head) else expr
  }

  /** Whether the backend can count an expression's evaluations natively. */
  def nativeCounting: Boolean = BackendsApiManager.getSettings.supportNativeIncrementMetric()

  /**
   * Native counters can be offloaded when every `IncrementMetric` is reached from its alias through
   * parents Delta's DML commands generate ([[countsLikeSpark]]). Without native counting, every
   * `IncrementMetric` must sit at the root of its alias (a stack of them counts as the root), so
   * crediting it with the output row count is exact.
   */
  def canOffloadProject(projectList: Seq[NamedExpression], nativeCounting: Boolean): Boolean = {
    if (nativeCounting) {
      projectList.forall(countsLikeSpark)
    } else {
      projectList.forall {
        case alias: Alias => !containsIncrementMetricExpr(peelIncrementMetrics(alias.child))
        case other => !containsIncrementMetricExpr(other)
      }
    }
  }

  /**
   * Whether a native backend evaluates every `IncrementMetric` in `expr` on the rows Spark does.
   * That holds below the parents Delta's UPDATE, DELETE and MERGE put above a counter: aliases,
   * other counters, the values of `IF` and `CASE WHEN` (never their conditions), `named_struct`,
   * `array` and `NOT`. Each passes every row it receives to its counter-bearing children, or routes
   * rows to a branch the way Spark does. Under any other parent the counts can differ: a
   * null-intolerant function such as `a + b` lets Velox drop the rows where another input is null
   * before the counter runs, `AND` and `OR` reorder their operands, a lambda body is missing from
   * the exported statistics, Spark skips the dividend of a `divide` by zero while Velox evaluates
   * it, and Velox merges identical `coalesce` inputs into one call. The subtree below a counter is
   * unrestricted unless it holds another counter.
   */
  private def countsLikeSpark(expr: Expression): Boolean = expr match {
    case e if !containsIncrementMetricExpr(e) => true
    case e if isIncrementMetric(e) => countsLikeSpark(e.children.head)
    case If(predicate, trueValue, falseValue) =>
      !containsIncrementMetricExpr(predicate) && countsLikeSpark(trueValue) &&
      countsLikeSpark(falseValue)
    case CaseWhen(branches, elseValue) =>
      branches.forall {
        case (condition, value) => !containsIncrementMetricExpr(condition) && countsLikeSpark(value)
      } && elseValue.forall(countsLikeSpark)
    case _: Alias | _: CreateNamedStruct | _: CreateArray | _: Not =>
      expr.children.forall(countsLikeSpark)
    case _ => false
  }

  /**
   * True when `condition` is a stack of `IncrementMetric` over a literal `true`. Such a filter
   * keeps every input row, so the operator's output rows equal Spark's evaluation count.
   */
  def canOffloadFilter(condition: Expression): Boolean = {
    isIncrementMetric(condition) && (peelIncrementMetrics(condition) match {
      case Literal(true, BooleanType) => true
      case _ => false
    })
  }

  private[gluten] val conditionalProjectReason: String =
    "IncrementMetric below a conditional expression must be evaluated by Spark; the native " +
      "project would credit it with every output row (GLUTEN-9003)"

  private[gluten] val nativeCounterPlacementReason: String =
    "IncrementMetric below a parent other than an alias, another IncrementMetric, an IF or " +
      "CASE WHEN value, named_struct, array or NOT must be evaluated by Spark; native evaluation " +
      "could count it on different rows (GLUTEN-9003)"

  private[gluten] val conditionalFilterReason: String =
    "IncrementMetric filter condition is not a literal true, so the native filter's output rows " +
      "would not match Spark's evaluation count (GLUTEN-9003)"
}
