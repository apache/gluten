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
package org.apache.spark.sql.execution

import org.apache.gluten.execution._

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Final}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import scala.reflect.ClassTag

class GlutenLogicalPlanTagInSparkPlanSuite
  extends LogicalPlanTagInSparkPlanSuite
  with GlutenSQLTestsTrait {

  // Override to use Gluten-aware logical plan tag checking.
  // Gluten replaces Spark physical operators with Transformer nodes that don't match
  // the original Spark pattern matching in LogicalPlanTagInSparkPlanSuite.
  override protected def checkGeneratedCode(
      plan: SparkPlan,
      checkMethodCodeSize: Boolean = true): Unit = {
    // Skip parent's codegen check (Gluten doesn't use WholeStageCodegen).
    // Only run the Gluten-aware logical plan tag check.
    checkGlutenLogicalPlanTag(plan)
  }

  private def isFinalAgg(aggExprs: Seq[AggregateExpression]): Boolean = {
    aggExprs.nonEmpty && aggExprs.forall(ae => ae.mode == Complete || ae.mode == Final)
  }

  private def checkGlutenLogicalPlanTag(plan: SparkPlan): Unit = {
    plan match {
      // Joins (Gluten + Spark)
      case _: BroadcastHashJoinExecTransformerBase | _: ShuffledHashJoinExecTransformerBase |
          _: SortMergeJoinExecTransformerBase | _: CartesianProductExecTransformer |
          _: BroadcastNestedLoopJoinExecTransformer | _: joins.BroadcastHashJoinExec |
          _: joins.ShuffledHashJoinExec | _: joins.SortMergeJoinExec |
          _: joins.BroadcastNestedLoopJoinExec | _: joins.CartesianProductExec =>
        assertLogicalPlanType[Join](plan)

      // Aggregates - only final (Gluten + Spark)
      case agg: HashAggregateExecBaseTransformer if isFinalAgg(agg.aggregateExpressions) =>
        assertLogicalPlanType[Aggregate](plan)
      case agg: aggregate.HashAggregateExec if isFinalAgg(agg.aggregateExpressions) =>
        assertLogicalPlanType[Aggregate](plan)
      case agg: aggregate.ObjectHashAggregateExec if isFinalAgg(agg.aggregateExpressions) =>
        assertLogicalPlanType[Aggregate](plan)
      case agg: aggregate.SortAggregateExec if isFinalAgg(agg.aggregateExpressions) =>
        assertLogicalPlanType[Aggregate](plan)

      // Window
      case _: WindowExecTransformer | _: window.WindowExec =>
        assertLogicalPlanType[Window](plan)

      // Union
      case _: ColumnarUnionExec | _: UnionExec =>
        assertLogicalPlanType[Union](plan)

      // Sample
      case _: SampleExec =>
        assertLogicalPlanType[Sample](plan)

      // Generate
      case _: GenerateExecTransformerBase | _: GenerateExec =>
        assertLogicalPlanType[Generate](plan)

      // Exchange nodes should NOT have logical plan tags
      case _: ColumnarShuffleExchangeExec | _: ColumnarBroadcastExchangeExec |
          _: exchange.ShuffleExchangeExec | _: exchange.BroadcastExchangeExec |
          _: ReusedExchangeExec =>
        assert(
          plan.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isEmpty,
          s"${plan.getClass.getSimpleName} should not have a logical plan tag")

      // Subquery exec nodes don't have logical plan tags
      case _: SubqueryExec | _: ReusedSubqueryExec =>
        assert(plan.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isEmpty)

      // Gluten infrastructure nodes (no corresponding logical plan)
      case _: WholeStageTransformer | _: InputIteratorTransformer | _: ColumnarInputAdapter |
          _: VeloxResizeBatchesExec =>
      // These are Gluten-specific wrapper nodes without logical plan links.

      // Scan trees
      case _ if isGlutenScanPlanTree(plan) =>
        // For scan plan trees (leaf under Project/Filter), we check that the leaf node
        // has a correct logical plan link. The intermediate Project/Filter nodes may not
        // have tags if they were created by Gluten's rewrite rules.
        val physicalLeaves = plan.collectLeaves()
        assert(
          physicalLeaves.length == 1,
          s"Expected 1 physical leaf, got ${physicalLeaves.length}")

        val leafNode = physicalLeaves.head
        // Find the logical plan from the leaf or any ancestor with a tag
        val logicalPlanOpt = leafNode
          .getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          .orElse(leafNode.getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG))
          .orElse(findLogicalPlanInTree(plan))

        logicalPlanOpt.foreach {
          lp =>
            val logicalPlan = lp match {
              case w: WithCTE => w.plan
              case o => o
            }
            val logicalLeaves = logicalPlan.collectLeaves()
            assert(
              logicalLeaves.length == 1,
              s"Expected 1 logical leaf, got ${logicalLeaves.length}")
            physicalLeaves.head match {
              case _: RangeExec => assert(logicalLeaves.head.isInstanceOf[Range])
              case _: DataSourceScanExec | _: BasicScanExecTransformer =>
                assert(logicalLeaves.head.isInstanceOf[LogicalRelation])
              case _: InMemoryTableScanExec =>
                assert(logicalLeaves.head.isInstanceOf[columnar.InMemoryRelation])
              case _: LocalTableScanExec => assert(logicalLeaves.head.isInstanceOf[LocalRelation])
              case _: ExternalRDDScanExec[_] =>
                assert(logicalLeaves.head.isInstanceOf[ExternalRDD[_]])
              case _: datasources.v2.BatchScanExec =>
                assert(logicalLeaves.head.isInstanceOf[DataSourceV2Relation])
              case _ =>
            }
        }
        return

      case _ =>
    }

    plan.children.foreach(checkGlutenLogicalPlanTag)
    plan.subqueries.foreach(checkGlutenLogicalPlanTag)
  }

  @scala.annotation.tailrec
  private def isGlutenScanPlanTree(plan: SparkPlan): Boolean = plan match {
    case ColumnarToRowExec(i: InputAdapter) => isGlutenScanPlanTree(i.child)
    case p: ProjectExec => isGlutenScanPlanTree(p.child)
    case p: ProjectExecTransformer => isGlutenScanPlanTree(p.child)
    case f: FilterExec => isGlutenScanPlanTree(f.child)
    case f: FilterExecTransformerBase => isGlutenScanPlanTree(f.child)
    case _: LeafExecNode => true
    case _ => false
  }

  /** Find any node in the tree that has a LOGICAL_PLAN_TAG. */
  private def findLogicalPlanInTree(plan: SparkPlan): Option[LogicalPlan] = {
    plan
      .getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
      .orElse(plan.getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG))
      .orElse(plan.children.iterator.map(findLogicalPlanInTree).collectFirst {
        case Some(lp) => lp
      })
  }

  private def getGlutenLogicalPlan(node: SparkPlan): LogicalPlan = {
    node.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).getOrElse {
      node.getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG).getOrElse {
        fail(node.getClass.getSimpleName + " does not have a logical plan link")
      }
    }
  }

  private def assertLogicalPlanType[T <: LogicalPlan: ClassTag](node: SparkPlan): Unit = {
    val logicalPlan = getGlutenLogicalPlan(node)
    val expectedCls = implicitly[ClassTag[T]].runtimeClass
    assert(
      expectedCls == logicalPlan.getClass,
      s"Expected ${expectedCls.getSimpleName} but got ${logicalPlan.getClass.getSimpleName}" +
        s" for ${node.getClass.getSimpleName}"
    )
  }
}
