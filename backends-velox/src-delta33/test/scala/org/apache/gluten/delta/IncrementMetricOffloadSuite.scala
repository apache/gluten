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
import org.apache.gluten.expression.IncrementMetricCall
import org.apache.gluten.extension.IncrementMetricOffload

import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, ArrayFilter, ArrayTransform, AttributeReference, CaseWhen, Cast, Coalesce, CreateArray, CreateNamedStruct, Divide, EqualNullSafe, EqualTo, Expression, GreaterThan, If, IsNotNull, IsNull, LambdaFunction, Literal, NamedExpression, NamedLambdaVariable, Not, Or}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, StringType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Delta's `IncrementMetric` needs either native evaluation counts or an unconditional shape whose
 * output row count is exact. Native counters are offloaded only below the parents Delta's DML
 * commands generate; any other parent can make the native count differ from Spark's (GLUTEN-9003).
 */
class IncrementMetricOffloadSuite extends AnyFunSuite {

  private val id = AttributeReference("id", IntegerType)()

  private val nullableId = AttributeReference("nullable_id", IntegerType, nullable = true)()

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
    // A backend that counts natively supports CASE branches as named counter function calls.
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

  test("native counters inside AND or OR fall back") {
    val counter = increment(Literal.TrueLiteral, newMetric())
    val predicate = GreaterThan(id, Literal(1))
    Seq[Expression](
      And(counter, predicate),
      Or(counter, predicate),
      And(predicate, If(predicate, counter, Literal.FalseLiteral)),
      increment(Or(predicate, counter), newMetric())
    ).foreach {
      expr =>
        val projectList = Seq(alias(expr))
        assert(!IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = true))
        // The shape is rejected before any slot is assigned.
        val error = intercept[GlutenNotSupportException] {
          DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = true)
        }
        assert(error.getMessage === IncrementMetricOffload.nativeCounterPlacementReason)
    }
  }

  test("native counters below any other parent fall back") {
    def counter: Expression = increment(Literal(1), newMetric())
    val x = NamedLambdaVariable("x", IntegerType, nullable = true)
    val items = AttributeReference("items", ArrayType(IntegerType))()
    val predicate = GreaterThan(id, Literal(1))
    val shared = increment(Literal(1), newMetric())
    Seq[Expression](
      // Velox drops the rows where `nullable_id` is null before the counter runs; Spark does not.
      Add(counter, nullableId),
      Add(increment(nullableId, newMetric()), nullableId),
      // Spark skips the dividend when the divisor is zero; Velox evaluates it.
      Divide(increment(Literal(1.0), newMetric()), Cast(id, DoubleType)),
      // Velox merges identical inputs into one call; Spark evaluates the second where the first
      // is null.
      Coalesce(Seq(shared, shared)),
      // A lambda body is evaluated per element but missing from the exported statistics.
      ArrayTransform(items, LambdaFunction(increment(x, newMetric()), Seq(x))),
      ArrayFilter(items, LambdaFunction(IsNotNull(increment(x, newMetric())), Seq(x))),
      // Conditions can be pruned by their branches; only branch values are counted exactly.
      If(increment(predicate, newMetric()), id, nullableId),
      CaseWhen(Seq(increment(predicate, newMetric()) -> id), nullableId),
      // A counter that is accepted on its own is still rejected below a parent that is not.
      CaseWhen(Seq(predicate -> Add(counter, nullableId)), counter)
    ).foreach {
      expr =>
        val projectList = Seq(id, alias(expr))
        assert(!IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = true), expr)
        val error = intercept[GlutenNotSupportException] {
          DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = true)
        }
        assert(error.getMessage === IncrementMetricOffload.nativeCounterPlacementReason)
    }
  }

  test("native counting accepts every counter placement Delta DML generates") {
    val sourcePresent = AttributeReference("_source_row_present_", BooleanType)()
    val targetPresent = AttributeReference("_target_row_present_", BooleanType)()
    val changeType = AttributeReference("_change_type", StringType)()
    val predicate = GreaterThan(id, Literal(1))
    def flag(value: Boolean): Expression = increment(Literal(value), newMetric())
    def stacked(value: Boolean): Expression = increment(flag(value), newMetric())
    // MERGE's _row_dropped_ column: the source/target dispatch, then one value per clause.
    val rowDropped = CaseWhen(
      Seq(
        IsNull(sourcePresent) -> If(predicate, stacked(true), flag(false)),
        IsNull(targetPresent) -> CaseWhen(Seq(predicate -> flag(false)), Literal.TrueLiteral)),
      CaseWhen(
        Seq(EqualTo(id, Literal(3)) -> stacked(true), predicate -> stacked(false)),
        flag(false))
    )
    def packed(dropped: Expression): Expression =
      CreateArray(Seq(CreateNamedStruct(Seq(Literal("_row_dropped_"), dropped))))
    Seq[Expression](
      // Source-row counters and the single clause of an insert-only MERGE: a counter at the
      // alias root, whatever it wraps.
      flag(true),
      increment(Add(nullableId, Literal(1)), newMetric()),
      rowDropped,
      // With change data feed on, the same column is packed into the change rows Delta explodes.
      CaseWhen(
        Seq(
          EqualNullSafe(changeType, Literal("insert")) -> packed(rowDropped),
          EqualNullSafe(changeType, Literal("delete")) -> packed(Not(rowDropped))),
        packed(rowDropped)
      )
    ).foreach {
      expr =>
        val projectList = Seq(id, alias(expr))
        assert(IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = true), expr)
        val (rewritten, metrics) =
          DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = true)
        assert(rewritten.flatMap(_.collect { case m: IncrementMetric => m }).isEmpty)
        assert(metrics.nonEmpty)
    }
  }

  test("a lambda function below a native counter stays offloadable") {
    val x = NamedLambdaVariable("x", IntegerType, nullable = true)
    val items = AttributeReference("items", ArrayType(IntegerType))()
    val metric = newMetric()
    val projectList =
      Seq(alias(increment(
        ArrayTransform(items, LambdaFunction(Add(x, Literal(1)), Seq(x))),
        metric)))
    assert(IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = true))
    val (rewritten, metrics) =
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = true)
    assert(rewritten.head.asInstanceOf[Alias].child.isInstanceOf[IncrementMetricCall])
    assert(metrics.map(_._1) === Seq("increment_metric_0"))
  }

  test("native counters around AND or OR remain offloadable") {
    val predicate = GreaterThan(id, Literal(1))
    Seq[Expression](And(predicate, predicate), Or(predicate, predicate)).foreach {
      expr =>
        val projectList = Seq(alias(increment(expr, newMetric())))
        assert(IncrementMetricOffload.canOffloadProject(projectList, nativeCounting = true))
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

  test("native counting gives each distinct metric one slot, in order of first appearance") {
    val updated = newMetric()
    val copied = newMetric()
    val predicate = GreaterThan(id, Literal(1))
    // `copied` appears twice: inside a branch and at another alias root. Both calls share a slot.
    val projectList = Seq(
      id,
      alias(CaseWhen(Seq(predicate -> increment(id, updated)), increment(id, copied))),
      alias(increment(Literal.TrueLiteral, copied)))
    val (rewritten, metrics) =
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = true)
    assert(rewritten.head eq id)
    val calls = rewritten.flatMap(_.collect { case call: IncrementMetricCall => call })
    assert(calls.map(_.functionName) === Seq(
      "increment_metric_0",
      "increment_metric_1",
      "increment_metric_1"))
    assert(rewritten.flatMap(_.collect { case m: IncrementMetric => m }).isEmpty)
    assert(metrics.map(_._1) === Seq("increment_metric_0", "increment_metric_1"))
    assert(metrics.map(_._2).zip(Seq(updated, copied)).forall { case (a, b) => a eq b })
  }

  test("native counting does not need a metric name") {
    // An accumulator that was never registered has no name; the slot stands in for it.
    val projectList = Seq(id, alias(increment(Literal.TrueLiteral, newMetric())))
    val (rewritten, metrics) =
      DeltaProjectExecTransformer.stripIncrementMetrics(projectList, nativeCounting = true)
    assert(rewritten.head eq id)
    assert(metrics.map(_._1) === Seq("increment_metric_0"))
    val plain = Seq(id, alias(GreaterThan(id, Literal(1))))
    val (untouched, none) =
      DeltaProjectExecTransformer.stripIncrementMetrics(plain, nativeCounting = true)
    assert(untouched.zip(plain).forall { case (a, b) => a eq b })
    assert(none.isEmpty)
  }

  test("native counting falls back above the slot limit") {
    val projectList = (0 until IncrementMetricCall.maxCounters).map {
      _ => alias(increment(Literal.TrueLiteral, newMetric()))
    }
    assert(
      DeltaProjectExecTransformer
        .stripIncrementMetrics(projectList, nativeCounting = true)
        ._2
        .size === IncrementMetricCall.maxCounters)
    val oneTooMany = projectList :+ alias(increment(Literal.TrueLiteral, newMetric()))
    intercept[GlutenNotSupportException] {
      DeltaProjectExecTransformer.stripIncrementMetrics(oneTooMany, nativeCounting = true)
    }
  }

  test("a filter condition without IncrementMetric is returned untouched") {
    val condition = GreaterThan(id, Literal(1))
    assert(!IncrementMetricOffload.canOffloadFilter(condition))
    val (stripped, metrics) = DeltaFilterExecTransformer.stripIncrementMetrics(condition)
    assert(stripped eq condition)
    assert(metrics.isEmpty)
  }
}
