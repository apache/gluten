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
package org.apache.gluten.delta

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.{DeltaFilterExecTransformer, DeltaProjectExecTransformer}
import org.apache.gluten.extension.IncrementMetricOffload

import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, CaseWhen, EqualTo, Expression, GreaterThan, If, Literal, NamedExpression}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

/**
 * Delta's `IncrementMetric` may only be offloaded where Spark would evaluate it once per output
 * row: at the root of a projection alias or as a filter condition that keeps every row. Anything
 * conditional stays on Spark (GLUTEN-9003).
 */
class IncrementMetricOffloadSuite extends AnyFunSuite {

  private val id = AttributeReference("id", IntegerType)()

  private def increment(child: Expression, metric: SQLMetric): IncrementMetric =
    IncrementMetric(child, metric)

  private def newMetric(): SQLMetric = new SQLMetric("sum")

  private def alias(child: Expression): NamedExpression = Alias(child, "col")()

  test("an IncrementMetric at the root of a projection alias is offloadable") {
    val metric = newMetric()
    val projectList = Seq(id, alias(increment(Literal.TrueLiteral, metric)))
    assert(IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = false))

    val (stripped, metrics) =
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = false)
    assert(stripped.head eq id)
    assert(stripped(1).asInstanceOf[Alias].child === Literal.TrueLiteral)
    assert(stripped(1).exprId === projectList(1).exprId)
    assert(metrics === Seq(("increment_metric", metric)))
  }

  test("a stack of IncrementMetric at the alias root is offloadable and keeps every metric") {
    val inner = newMetric()
    val outer = newMetric()
    val projectList = Seq(alias(increment(increment(Literal.FalseLiteral, inner), outer)))
    assert(IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = false))

    val (stripped, metrics) =
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = false)
    assert(stripped.head.asInstanceOf[Alias].child === Literal.FalseLiteral)
    assert(metrics.map(_._2) === Seq(outer, inner))
  }

  test("an IncrementMetric inside a CASE WHEN branch is not offloadable") {
    // The shape MERGE generates: one counter per clause, the copied-row counter in the else.
    val updated = newMetric()
    val copied = newMetric()
    val rowDropped = CaseWhen(
      Seq((EqualTo(id, Literal(1)), increment(Literal.FalseLiteral, updated))),
      Some(increment(Literal.FalseLiteral, copied)))
    val projectList = Seq(id, alias(rowDropped))
    assert(!IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = false))
    // A backend that counts natively takes any shape; the counters become named function calls.
    assert(IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = true))
    intercept[GlutenNotSupportException] {
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = false)
    }
  }

  test("an IncrementMetric below the alias root stack is not offloadable") {
    val projectList = Seq(
      alias(
        increment(
          If(
            GreaterThan(id, Literal(1)),
            increment(Literal.TrueLiteral, newMetric()),
            Literal(false)),
          newMetric())))
    assert(!IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = false))
    intercept[GlutenNotSupportException] {
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = false)
    }
  }

  test("a project list without IncrementMetric is returned untouched") {
    val projectList = Seq(id, alias(GreaterThan(id, Literal(1))))
    assert(IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = false))
    val (stripped, metrics) =
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = false)
    assert(stripped.zip(projectList).forall { case (a, b) => a eq b })
    assert(metrics.isEmpty)
  }

  test("a filter on IncrementMetric over a literal true is offloadable") {
    val touched = newMetric()
    val condition = increment(Literal.TrueLiteral, touched)
    assert(IncrementMetricOffload.canOffloadFilter(condition))
    val (stripped, metrics) = DeltaFilterExecTransformer.stripIncrementMetrics(condition)
    assert(stripped === Literal.TrueLiteral)
    assert(metrics === Seq(("increment_metric", touched)))

    val stacked = increment(condition, newMetric())
    assert(IncrementMetricOffload.canOffloadFilter(stacked))
    assert(DeltaFilterExecTransformer.stripIncrementMetrics(stacked)._2.size === 2)
  }

  test("a filter on IncrementMetric over anything else is not offloadable") {
    Seq[Expression](
      increment(Literal.FalseLiteral, newMetric()),
      increment(GreaterThan(id, Literal(1)), newMetric()),
      And(increment(Literal.TrueLiteral, newMetric()), GreaterThan(id, Literal(1)))
    ).foreach {
      condition => assert(!IncrementMetricOffload.canOffloadFilter(condition), condition)
    }
    intercept[GlutenNotSupportException] {
      DeltaFilterExecTransformer.stripIncrementMetrics(increment(Literal.FalseLiteral, newMetric()))
    }
  }

  test("a counter function name is derived from the metric's display name") {
    assert(
      IncrementMetricOffload.nativeFunctionName(Some("number of target rows copied")) ===
        Some("increment_metric_number_of_target_rows_copied"))
    assert(
      IncrementMetricOffload.nativeFunctionName(Some("number of rows deleted.")) ===
        Some("increment_metric_number_of_rows_deleted"))
    assert(IncrementMetricOffload.nativeFunctionName(Some(" -- ")).isEmpty)
    assert(IncrementMetricOffload.nativeFunctionName(None).isEmpty)
  }

  test("native counting refuses a metric it cannot name and leaves other aliases untouched") {
    // An accumulator that was never registered has no name, so it cannot get a counter function.
    val projectList = Seq(id, alias(increment(Literal.TrueLiteral, newMetric())))
    intercept[GlutenNotSupportException] {
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = true)
    }
    val plain = Seq(id, alias(GreaterThan(id, Literal(1))))
    val (rewritten, metrics) =
      DeltaProjectExecTransformer.stripIncrementMetrics(plain, nativeCounting = true)
    assert(rewritten.zip(plain).forall { case (a, b) => a eq b })
    assert(metrics.isEmpty)
  }

  test("a filter condition without IncrementMetric is returned untouched") {
    val condition = GreaterThan(id, Literal(1))
    assert(!IncrementMetricOffload.canOffloadFilter(condition))
    val (stripped, metrics) = DeltaFilterExecTransformer.stripIncrementMetrics(condition)
    assert(stripped eq condition)
    assert(metrics.isEmpty)
  }
}
