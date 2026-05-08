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
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.component.Component
import org.apache.gluten.extension.columnar.transition.ConventionReq.KnownChildConvention

import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.util.SparkPlanUtil

/** ConventionFunc is a utility to derive [[Convention]] or [[ConventionReq]] from a query plan. */
sealed trait ConventionFunc {
  def conventionOf(plan: SparkPlan): Convention
  def conventionReqOf(plan: SparkPlan): Seq[ConventionReq]
}

object ConventionFunc {
  trait Override {
    def rowTypeOf: PartialFunction[SparkPlan, Convention.RowType] = PartialFunction.empty
    def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = PartialFunction.empty
    def conventionReqOf: PartialFunction[SparkPlan, Seq[ConventionReq]] = PartialFunction.empty
  }

  object Override {
    object Empty extends Override
  }

  def create(): ConventionFunc = {
    val batchOverride = newOverride()
    new BuiltinFunc(batchOverride)
  }

  private def newOverride(): Override = {
    // Components should override Backend's convention function. Hence, reversed injection order
    // is applied.
    val overrides = Component.sorted().reverse.map(_.convFuncOverride())
    if (overrides.isEmpty) {
      return Override.Empty
    }
    new Override {
      override val rowTypeOf: PartialFunction[SparkPlan, Convention.RowType] = {
        overrides.map(_.rowTypeOf).reduce((l, r) => l.orElse(r))
      }
      override val batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = {
        overrides.map(_.batchTypeOf).reduce((l, r) => l.orElse(r))
      }
      override val conventionReqOf: PartialFunction[SparkPlan, Seq[ConventionReq]] = {
        overrides.map(_.conventionReqOf).reduce((l, r) => l.orElse(r))
      }
    }
  }

  private class BuiltinFunc(o: Override) extends ConventionFunc {
    override def conventionOf(plan: SparkPlan): Convention = {
      val conv = conventionOf0(plan)
      conv
    }

    private def conventionOf0(plan: SparkPlan): Convention = plan match {
      case p if canPropagateConvention(p) =>
        val childrenConventions = p.children.map(conventionOf0).distinct
        if (childrenConventions.size > 1) {
          childrenConventions.reduce(_ && _)
        } else {
          assert(childrenConventions.size == 1)
          childrenConventions.head
        }
      case q: QueryStageExec => conventionOf0(q.plan)
      case r: ReusedExchangeExec => conventionOf0(r.child)
      case other =>
        val conv = Convention.of(rowTypeOf(other), batchTypeOf(other))
        conv
    }

    private def rowTypeOf(plan: SparkPlan): Convention.RowType = {
      val out = o.rowTypeOf.applyOrElse(plan, rowTypeOf0)
      out
    }

    private def rowTypeOf0(plan: SparkPlan): Convention.RowType = {
      val out = plan match {
        case k: Convention.KnownRowType =>
          k.rowType()
        case _ if SparkPlanUtil.supportsRowBased(plan) =>
          Convention.RowType.VanillaRowType
        case _ =>
          Convention.RowType.None
      }
      checkRowType(plan, out)
      out
    }

    private def checkRowType(plan: SparkPlan, rowType: Convention.RowType): Unit = {
      if (SparkPlanUtil.supportsRowBased(plan)) {
        assert(
          rowType != Convention.RowType.None,
          s"Plan ${plan.nodeName} supports row-based execution, " +
            s"however #rowTypeOf returns None")
      } else {
        assert(
          rowType == Convention.RowType.None,
          s"Plan ${plan.nodeName} doesn't support row-based " +
            s"execution, however #rowTypeOf returns $rowType")
      }
    }

    private def batchTypeOf(plan: SparkPlan): Convention.BatchType = {
      val out = o.batchTypeOf.applyOrElse(plan, batchTypeOf0)
      out
    }

    private def batchTypeOf0(plan: SparkPlan): Convention.BatchType = {
      val out = plan match {
        case k: Convention.KnownBatchType =>
          k.batchType()
        case _ if plan.supportsColumnar =>
          Convention.BatchType.VanillaBatchType
        case _ =>
          Convention.BatchType.None
      }
      checkBatchType(plan, out)
      out
    }

    private def checkBatchType(plan: SparkPlan, batchType: Convention.BatchType): Unit = {
      if (plan.supportsColumnar) {
        assert(
          batchType != Convention.BatchType.None,
          s"Plan ${plan.nodeName} supports columnar " +
            s"execution, however #batchTypeOf returns None")
      } else {
        assert(
          batchType == Convention.BatchType.None,
          s"Plan ${plan.nodeName} doesn't support " +
            s"columnar execution, however #batchTypeOf returns $batchType")
      }
    }

    override def conventionReqOf(plan: SparkPlan): Seq[ConventionReq] = {
      val req = o.conventionReqOf.applyOrElse(plan, conventionReqOf0)
      assert(req.size == plan.children.size)
      req
    }

    private def conventionReqOf0(plan: SparkPlan): Seq[ConventionReq] = plan match {
      case k: KnownChildConvention =>
        val reqs = k.requiredChildConvention()
        reqs
      case RowToColumnarLike(_) =>
        Seq(
          ConventionReq.of(
            ConventionReq.RowType.Is(Convention.RowType.VanillaRowType),
            ConventionReq.BatchType.Any))
      case ColumnarToRowExec(_) =>
        Seq(
          ConventionReq.of(
            ConventionReq.RowType.Any,
            ConventionReq.BatchType.Is(Convention.BatchType.VanillaBatchType)))
      case write: DataWritingCommandExec if SparkPlanUtil.isPlannedV1Write(write) =>
        // To align with ApplyColumnarRulesAndInsertTransitions#insertTransitions
        Seq(ConventionReq.any)
      case u: UnionExec =>
        // We force vanilla union to output row data to get the best compatibility with vanilla
        // Spark.
        // As a result it's a common practice to rewrite it with GlutenPlan for offloading.
        Seq.tabulate(u.children.size)(
          _ =>
            ConventionReq.of(
              ConventionReq.RowType.Is(Convention.RowType.VanillaRowType),
              ConventionReq.BatchType.Any))
      case other if other.supportsColumnar && SparkPlanUtil.supportsRowBased(other) =>
        // A dual-mode parent can keep columnar children and still satisfy a row-based output
        // requirement itself. Align with Spark's transition insertion behavior.
        val childReq = ConventionReq.ofBatch(ConventionReq.BatchType.Is(batchTypeOf(other)))
        Seq.tabulate(other.children.size)(
          _ => {
            childReq
          })
      case other =>
        // In the normal case, children's convention should follow parent node's convention.
        val childReq = conventionOf0(other).asReq()
        Seq.tabulate(other.children.size)(
          _ => {
            childReq
          })
    }
  }
}
