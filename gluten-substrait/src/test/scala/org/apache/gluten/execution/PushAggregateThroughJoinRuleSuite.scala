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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.joinagg.{JoinAggregate, JoinAggregateFunctionWrapper, PushAggregateThroughJoinRule}
import org.apache.gluten.extension.joinagg.JoinAggregateFunctionWrapper.{FinalPhase, PartialMergePhase, PartialPhase}

import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualTo, GreaterThan, Literal, Rand}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LocalRelation, Project}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class PushAggregateThroughJoinRuleSuite extends PlanTest with SharedSparkSession {
  private val joinAggregateRule = PushAggregateThroughJoinRule(spark)

  test("pushPartialAggToJoinSide accepts only an eligible side") {
    val leftKey = AttributeReference("left_key", IntegerType)()
    val value = AttributeReference("value", IntegerType)()
    val rightKey = AttributeReference("right_key", IntegerType)()
    val join = Join(
      LocalRelation(Seq(leftKey, value)),
      LocalRelation(Seq(rightKey)),
      Inner,
      Some(EqualTo(leftKey, rightKey)),
      JoinHint.NONE)
    val leftPartialSum = Alias(
      JoinAggregateFunctionWrapper.wrapperPartial(Sum(value)).toAggregateExpression(),
      "left_partial_sum")()
    val rightPartialSum = Alias(
      JoinAggregateFunctionWrapper.wrapperPartial(Sum(rightKey)).toAggregateExpression(),
      "right_partial_sum")()

    val leftPushed = joinAggregateRule.pushPartialAggToJoinSide(
      Seq(leftKey),
      Seq(leftKey, leftPartialSum),
      join,
      Nil,
      joinAggregateRule.JoinLeft)
    assert(leftPushed.exists(_.child eq join.left))
    assert(leftPushed.get.aggregateExpressions.last == leftPartialSum)
    assert(leftPushed.get.phase == PartialPhase)
    assert(joinAggregateRule
      .pushPartialAggToJoinSide(
        Seq(leftKey),
        Seq(leftKey, leftPartialSum),
        join,
        Nil,
        joinAggregateRule.JoinRight)
      .isEmpty)
    val rightPushed = joinAggregateRule.pushPartialAggToJoinSide(
      Seq(leftKey),
      Seq(leftKey, rightPartialSum),
      join,
      Nil,
      joinAggregateRule.JoinRight)
    assert(rightPushed.exists(_.child eq join.right))
  }

  test("pushPartialAggToJoinSide supports grouping-only pushdown on either side") {
    val leftKey = AttributeReference("left_key", IntegerType)()
    val rightKey = AttributeReference("right_key", IntegerType)()
    val join = Join(
      LocalRelation(Seq(leftKey)),
      LocalRelation(Seq(rightKey)),
      Inner,
      Some(EqualTo(leftKey, rightKey)),
      JoinHint.NONE)
    val keys = Seq(leftKey, rightKey)

    Seq(joinAggregateRule.JoinLeft, joinAggregateRule.JoinRight).foreach {
      side =>
        val pushed = joinAggregateRule
          .pushPartialAggToJoinSide(keys, keys, join, Nil, side)
          .get
        assert(pushed.child eq side.plan(join))
        assert(pushed.groupingExpressions == side.plan(join).output)
        assert(pushed.aggregateExpressions == side.plan(join).output)
        assert(pushed.phase == PartialPhase)
    }
  }

  test("grouping-only pushdown rule is idempotent") {
    Seq(false, true).foreach {
      partialMergeEnabled =>
        withSQLConf(
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key -> "1",
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_PARTIAL_MERGE_ENABLED.key ->
            partialMergeEnabled.toString
        ) {
          val leftKey = AttributeReference("left_key", IntegerType)()
          val rightKey = AttributeReference("right_key", IntegerType)()
          val join = Join(
            LocalRelation(Seq(leftKey)),
            LocalRelation(Seq(rightKey)),
            Inner,
            Some(EqualTo(leftKey, rightKey)),
            JoinHint.NONE)
          val keys = Seq(leftKey, rightKey)
          val plan = Aggregate(
            keys,
            Seq(Alias(leftKey, "left_output")(), Alias(rightKey, "right_output")()),
            Project(keys, join))
          val rule = PushAggregateThroughJoinRule(spark)

          val once = rule(plan)
          assert(once.collect { case _: JoinAggregate => 1 }.size == 3)
          assert(once.output == plan.output)
          assert(once.resolved)
          assert(once.collect { case p if p.missingInput.nonEmpty => p }.isEmpty)
          assert(rule.getSuccessfulPushCount == 2)

          val phases = once.collect {
            case agg: JoinAggregate => agg.phase
          }
          assert(phases == Seq(FinalPhase, PartialPhase, PartialPhase))
          // Explicit phase nodes are not ordinary Aggregates, so later runs skip them.
          val twice = rule(once)
          assert(twice eq once)
          assert(rule.getSuccessfulPushCount == 2)

          // Replacing children preserves the phase stored in each node.
          val copied = once.transformUp {
            case leaf: LocalRelation => Project(leaf.output, leaf)
          }
          assert(copied.collect {
            case agg: JoinAggregate => agg.phase
          } == phases)
          assert(rule(copied) eq copied)
        }
    }
  }

  test("generated phases and idempotence across join depths") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val d = AttributeReference("d", IntegerType)()
    val leftJoin = Join(
      LocalRelation(Seq(a)),
      LocalRelation(Seq(b)),
      Inner,
      Some(EqualTo(a, b)),
      JoinHint.NONE)
    val rightJoin = Join(
      LocalRelation(Seq(c)),
      LocalRelation(Seq(d)),
      Inner,
      Some(EqualTo(c, d)),
      JoinHint.NONE)
    val join = Join(leftJoin, rightJoin, Inner, Some(EqualTo(a, c)), JoinHint.NONE)
    val keys = Seq(a, b, c, d)
    val plans = Seq(
      (Aggregate(keys, keys, join), 2),
      (
        Aggregate(
          Seq(b, d),
          Seq(b, d, Alias(Sum(a).toAggregateExpression(), "total")()),
          join),
        1))

    for {
      (plan, pushedSides) <- plans
      depth <- Seq(1, 2)
      partialMergeEnabled <- Seq(false, true)
    } {
      withClue(s"plan=$plan, depth=$depth, partialMergeEnabled=$partialMergeEnabled: ") {
        withSQLConf(
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key -> depth.toString,
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_PARTIAL_MERGE_ENABLED.key ->
            partialMergeEnabled.toString
        ) {
          val rule = PushAggregateThroughJoinRule(spark)
          val rewritten = rule(plan)
          val aggregates = rewritten.collect { case agg: JoinAggregate => agg }
          assert(aggregates.head.phase == FinalPhase)
          val mergeCount = aggregates.count(
            _.phase == PartialMergePhase)
          val expectedMergeCount = if (partialMergeEnabled && depth == 2) {
            pushedSides
          } else {
            0
          }
          assert(mergeCount == expectedMergeCount)
          assert(rewritten.output == plan.output)
          assert(rewritten.resolved)
          assert(rewritten.collect { case p if p.missingInput.nonEmpty => p }.isEmpty)
          val pushes = rule.getSuccessfulPushCount
          assert(pushes > 0)
          assert(rule(rewritten) eq rewritten)
          assert(rule.getSuccessfulPushCount == pushes)
        }
      }
    }
  }

  test("aggregate pushdown does not cross nondeterministic expressions") {
    withSQLConf(
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key -> "1") {
      val leftKey = AttributeReference("left_key", IntegerType)()
      val rightKey = AttributeReference("right_key", IntegerType)()
      val join = Join(
        LocalRelation(Seq(leftKey)),
        LocalRelation(Seq(rightKey)),
        Inner,
        Some(EqualTo(leftKey, rightKey)),
        JoinHint.NONE)
      val keys = Seq(leftKey, rightKey)
      val randomValue = Alias(Rand(0L), "random_value")()
      val children = Seq(
        Project(keys :+ randomValue, join),
        Filter(GreaterThan(Rand(0L), Literal(0.5)), join),
        join.copy(condition = Some(And(
          EqualTo(leftKey, rightKey),
          GreaterThan(Rand(0L), Literal(0.5)))))
      )
      val rule = PushAggregateThroughJoinRule(spark)

      children.foreach {
        child =>
          val plans = Seq(
            Aggregate(child.output, child.output, child),
            Aggregate(
              Seq(leftKey),
              Seq(leftKey, Alias(Sum(leftKey).toAggregateExpression(), "total")()),
              child))
          plans.foreach {
            plan => assert(rule(plan) eq plan)
          }
      }
    }
  }
}
