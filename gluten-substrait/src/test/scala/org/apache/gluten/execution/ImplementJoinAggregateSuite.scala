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
import org.apache.gluten.extension.joinagg.{ImplementJoinAggregate, JoinAggregate, PushAggregateThroughJoin}
import org.apache.gluten.extension.joinagg.JoinAggregateFunctionWrapper.{FinalPhase, PartialMergePhase, PartialPhase}

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, JoinHint, LocalRelation}
import org.apache.spark.sql.execution.PlanLater
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class ImplementJoinAggregateSuite extends PlanTest with SharedSparkSession {
  test("push strategy defers explicit phases and the planner resolves them recursively") {
    withSQLConf(
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_TWO_STAGE_MODE.key -> "none") {
      val leftKey = AttributeReference("left_key", IntegerType)()
      val rightKey = AttributeReference("right_key", IntegerType)()
      val join = Join(
        LocalRelation(Seq(leftKey)),
        LocalRelation(Seq(rightKey)),
        Inner,
        Some(EqualTo(leftKey, rightKey)),
        JoinHint.NONE)
      val agg = Aggregate(Seq(leftKey, rightKey), Seq(leftKey, rightKey), join)
      val push = PushAggregateThroughJoin(spark)
      val deferred = push(agg).head.asInstanceOf[PlanLater]
      val phases = deferred.plan.collect { case phase: JoinAggregate => phase }
      assert(phases.map(_.phase) == Seq(FinalPhase, PartialPhase, PartialPhase))
      assert(deferred.output == agg.output)
      assert(push(phases.head).isEmpty)
      assert(push(agg.copy(child = LocalRelation(Seq(leftKey, rightKey)))).isEmpty)

      val previous = spark.experimental.extraStrategies
      try {
        spark.experimental.extraStrategies = Seq(push, ImplementJoinAggregate(spark))
        val physical = spark.sessionState.planner.plan(agg).next()
        assert(physical.collect { case p: PlanLater => p }.isEmpty)
        assert(physical.collect { case a: HashAggregateExec => a }.size == 3)
        assert(physical.output == agg.output)
      } finally {
        spark.experimental.extraStrategies = previous
      }
    }
  }

  test("ordinary aggregates are left to Spark") {
    withSQLConf(GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true") {
      val key = AttributeReference("key", IntegerType)()
      val agg = Aggregate(Seq(key), Seq(key), LocalRelation(Seq(key)))
      assert(ImplementJoinAggregate(spark)(agg).isEmpty)
    }
  }

  test("grouping-only aggregates follow their explicit phase") {
    for {
      phase <- Seq(PartialPhase, PartialMergePhase, FinalPhase)
      twoStageMode <- Seq("none", "final")
    } {
      withClue(s"phase=$phase, twoStageMode=$twoStageMode: ") {
        withSQLConf(
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_TWO_STAGE_MODE.key -> twoStageMode) {
          val key = AttributeReference("key", IntegerType)()
          val agg = JoinAggregate(
            Seq(key),
            Seq(Alias(key, "output_key")()),
            LocalRelation(Seq(key)),
            phase)

          val alternatives = ImplementJoinAggregate(spark)(agg)
          assert(alternatives.size == 1)
          val plan = alternatives.head
          val aggregates = plan.collect { case hash: HashAggregateExec => hash }
          assert(plan.output == agg.output)
          assert(aggregates.forall(_.aggregateExpressions.isEmpty))

          if (phase == FinalPhase) {
            assert(aggregates.head.requiredChildDistributionExpressions.contains(Seq(key)))
            val expectedStages = if (twoStageMode == "final") {
              2
            } else {
              1
            }
            assert(aggregates.size == expectedStages)
            assert(aggregates.tail.forall(_.requiredChildDistributionExpressions.isEmpty))
          } else {
            assert(aggregates.size == 1)
            assert(aggregates.head.requiredChildDistributionExpressions.isEmpty)
          }
        }
      }
    }
  }
}
