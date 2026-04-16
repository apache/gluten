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

import org.apache.gluten.execution.{GlutenPlan, WholeStageTransformer}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.ColumnarWriteFilesExec.NoopLeaf
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}

import java.util
import java.util.Collections.newSetFromMap

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, BitSet}

// This file is copied from Spark `ExplainUtils` and changes:
// 1. add function `collectFallbackNodes`
// 2. remove `plan.verboseStringWithOperatorId`
// 3. remove codegen id
object GlutenExplainUtils extends AdaptiveSparkPlanHelper {
  type FallbackInfo = (Int, Map[String, String])

  def addFallbackNodeWithReason(
      p: SparkPlan,
      reason: String,
      fallbackNodeToReason: mutable.HashMap[String, String]): Unit = {
    SparkShimLoader.getSparkShims.getOperatorId(p).foreach {
      opId =>
        // e.g., 002 project, it is used to help analysis by `substring(4)`
        val formattedNodeName = f"$opId%03d ${p.nodeName}"
        fallbackNodeToReason.put(formattedNodeName, reason)
    }
  }

  def handleVanillaSparkPlan(
      p: SparkPlan,
      fallbackNodeToReason: mutable.HashMap[String, String]
  ): Unit = {
    p.logicalLink.flatMap(FallbackTags.getOption) match {
      case Some(tag) => addFallbackNodeWithReason(p, tag.reason(), fallbackNodeToReason)
      case _ =>
        // If the SparkPlan does not have fallback reason, then there are two options:
        // 1. Gluten ignore that plan, and it's a kind of fallback
        // 2. Gluten does not support it without the fallback reason

        // Some SparkPlan nodes may lack a logicalLink (e.g., SortExec generated
        // by EnsureRequirements). Additionally, some Gluten rules might omit
        // `copyTagsFrom` during plan conversion, leading to the same issue.
        // When such a node falls back, the fallback reason might be stored in
        // its tags. We attempt to retrieve it here as a fallback mechanism.
        // However, this is not always reliable and merely increases our chances
        // of finding the correct reason, as AQE might create new SparkPlan
        // instances that do not preserve the tags.
        val reason = FallbackTags
          .getOption(p)
          .map(_.reason())
          .getOrElse("Gluten does not touch it or does not support it")
        addFallbackNodeWithReason(p, reason, fallbackNodeToReason)
    }
  }

  private def collectFallbackNodes(plan: QueryPlan[_]): FallbackInfo = {
    var numGlutenNodes = 0
    val fallbackNodeToReason = new mutable.HashMap[String, String]

    def collect(tmp: QueryPlan[_]): Unit = {
      tmp.foreachUp {
        case _: ExecutedCommandExec =>
        case cmd: CommandResultExec => collect(cmd.commandPhysicalPlan)
        case p: V2CommandExec
            if FallbackTags.nonEmpty(p) ||
              p.logicalLink.exists(FallbackTags.getOption(_).nonEmpty) =>
          handleVanillaSparkPlan(p, fallbackNodeToReason)
        case _: V2CommandExec =>
        case _: DataWritingCommandExec =>
        case _: WholeStageCodegenExec =>
        case _: WholeStageTransformer =>
        case _: InputAdapter =>
        case _: ColumnarInputAdapter =>
        case _: InputIteratorTransformer =>
        case _: ColumnarToRowTransition =>
        case _: RowToColumnarTransition =>
        case _: ReusedExchangeExec =>
        case _: NoopLeaf =>
        case w: WriteFilesExec if w.child.isInstanceOf[NoopLeaf] =>
        case sub: AdaptiveSparkPlanExec if sub.isSubquery => collect(sub.executedPlan)
        case _: AdaptiveSparkPlanExec =>
        case p: QueryStageExec => collect(p.plan)
        case p: GlutenPlan =>
          numGlutenNodes += 1
          p.innerChildren.foreach(collect)
        case i: InMemoryTableScanExec =>
          if (PlanUtil.isGlutenTableCache(i)) {
            numGlutenNodes += 1
          } else {
            addFallbackNodeWithReason(i, "Columnar table cache is disabled", fallbackNodeToReason)
          }
        case _: AQEShuffleReadExec => // Ignore
        case p: SparkPlan =>
          handleVanillaSparkPlan(p, fallbackNodeToReason)
          p.innerChildren.foreach(collect)
        case _ =>
      }
    }
    collect(plan)
    (numGlutenNodes, fallbackNodeToReason.toMap)
  }

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with each operator tagged with an unique
   *         identifier. 2. Second part explains each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   *
   * @param plan
   *   Input query plan to process
   * @param append
   *   function used to append the explain output
   * @param collectedOperators
   *   The IDs of the operators that are already collected and we shouldn't collect again.
   */
  private def processPlanSkippingSubqueries[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      collectedOperators: BitSet): Unit = {
    try {

      QueryPlan.append(plan, append, verbose = false, addSuffix = false, printOperatorId = true)

      append("\n")
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  // format: off
  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generates the explain output for the input plan excluding the subquery plans. 2. Generates
   *      the explain output for each subquery referenced in the plan.
   */
  // format: on
  def processPlan[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      collectFallbackFunc: Option[QueryPlan[_] => FallbackInfo] = None): FallbackInfo =
    synchronized {
      SparkShimLoader.getSparkShims.withOperatorIdMap(
        new java.util.IdentityHashMap[QueryPlan[_], Int]()) {
        try {
          // Initialize a reference-unique set of Operators to avoid accdiental overwrites and to
          // allow intentional overwriting of IDs generated in previous AQE iteration
          val operators = newSetFromMap[QueryPlan[_]](new util.IdentityHashMap())
          // Initialize an array of ReusedExchanges to help find Adaptively Optimized Out
          // Exchanges as part of SPARK-42753
          val reusedExchanges = ArrayBuffer.empty[ReusedExchangeExec]

          var currentOperatorID = 0
          currentOperatorID =
            generateOperatorIDs(plan, currentOperatorID, operators, reusedExchanges, true)

          val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, BaseSubqueryExec)]
          getSubqueries(plan, subqueries)

          currentOperatorID = subqueries.foldLeft(currentOperatorID) {
            (curId, plan) =>
              generateOperatorIDs(plan._3.child, curId, operators, reusedExchanges, true)
          }

          // SPARK-42753: Process subtree for a ReusedExchange with unknown child
          val optimizedOutExchanges = ArrayBuffer.empty[Exchange]
          reusedExchanges.foreach {
            reused =>
              val child = reused.child
              if (!operators.contains(child)) {
                optimizedOutExchanges.append(child)
                currentOperatorID =
                  generateOperatorIDs(child, currentOperatorID, operators, reusedExchanges, false)
              }
          }

          val collectedOperators = BitSet.empty
          processPlanSkippingSubqueries(plan, append, collectedOperators)

          var i = 0
          for (sub <- subqueries) {
            if (i == 0) {
              append("\n===== Subqueries =====\n\n")
            }
            i = i + 1
            append(
              s"Subquery:$i Hosting operator id = " +
                s"${getOpId(sub._1)} Hosting Expression = ${sub._2}\n")

            // For each subquery expression in the parent plan, process its child plan to compute
            // the explain output. In case of subquery reuse, we don't print subquery plan more
            // than once. So we skip [[ReusedSubqueryExec]] here.
            if (!sub._3.isInstanceOf[ReusedSubqueryExec]) {
              processPlanSkippingSubqueries(sub._3.child, append, collectedOperators)
            }
            append("\n")
          }

          i = 0
          optimizedOutExchanges.foreach {
            exchange =>
              if (i == 0) {
                append("\n===== Adaptively Optimized Out Exchanges =====\n\n")
              }
              i = i + 1
              append(s"Subplan:$i\n")
              processPlanSkippingSubqueries[SparkPlan](exchange, append, collectedOperators)
              append("\n")
          }

          (subqueries.filter(!_._3.isInstanceOf[ReusedSubqueryExec]).map(_._3.child) :+ plan)
            .map {
              plan =>
                if (collectFallbackFunc.isEmpty) {
                  collectFallbackNodes(plan)
                } else {
                  collectFallbackFunc.get.apply(plan)
                }
            }
            .reduce((a, b) => (a._1 + b._1, a._2 ++ b._2))
        } finally {
          removeTags(plan)
        }
      }
    }

  /**
   * Traverses the supplied input plan in a bottom-up fashion and records the operator id via
   * setting a tag in the operator. Note :
   *   - Operator such as WholeStageCodegenExec and InputAdapter are skipped as they don't appear in
   *     the explain output.
   *   - Operator identifier starts at startOperatorID + 1
   *
   * @param plan
   *   Input query plan to process
   * @param startOperatorID
   *   The start value of operation id. The subsequent operations will be assigned higher value.
   * @param visited
   *   A unique set of operators visited by generateOperatorIds. The set is scoped at the callsite
   *   function processPlan. It serves two purpose: Firstly, it is used to avoid accidentally
   *   overwriting existing IDs that were generated in the same processPlan call. Secondly, it is
   *   used to allow for intentional ID overwriting as part of SPARK-42753 where an Adaptively
   *   Optimized Out Exchange and its subtree may contain IDs that were generated in a previous AQE
   *   iteration's processPlan call which would result in incorrect IDs.
   * @param reusedExchanges
   *   A unique set of ReusedExchange nodes visited which will be used to idenitfy adaptively
   *   optimized out exchanges in SPARK-42753.
   * @param addReusedExchanges
   *   Whether to add ReusedExchange nodes to reusedExchanges set. We set it to false to avoid
   *   processing more nested ReusedExchanges nodes in the subtree of an Adpatively Optimized Out
   *   Exchange.
   * @return
   *   The last generated operation id for this input plan. This is to ensure we always assign
   *   incrementing unique id to each operator.
   */
  private def generateOperatorIDs(
      plan: QueryPlan[_],
      startOperatorID: Int,
      visited: util.Set[QueryPlan[_]],
      reusedExchanges: ArrayBuffer[ReusedExchangeExec],
      addReusedExchanges: Boolean): Int = {
    var currentOperationID = startOperatorID
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return currentOperationID
    }

    def setOpId(plan: QueryPlan[_]): Unit = if (!visited.contains(plan)) {
      plan match {
        case r: ReusedExchangeExec if addReusedExchanges =>
          reusedExchanges.append(r)
        case _ =>
      }
      visited.add(plan)
      currentOperationID += 1
      SparkShimLoader.getSparkShims.setOperatorId(plan, currentOperationID)
    }

    plan.foreachUp {
      case _: WholeStageCodegenExec =>
      case _: InputAdapter =>
      case cmd: CommandResultExec =>
        currentOperationID = generateOperatorIDs(
          cmd.commandPhysicalPlan,
          currentOperationID,
          visited,
          reusedExchanges,
          addReusedExchanges)
        setOpId(cmd)
      case p: AdaptiveSparkPlanExec =>
        currentOperationID = generateOperatorIDs(
          p.executedPlan,
          currentOperationID,
          visited,
          reusedExchanges,
          addReusedExchanges)
        if (!p.executedPlan.fastEquals(p.initialPlan)) {
          currentOperationID = generateOperatorIDs(
            p.initialPlan,
            currentOperationID,
            visited,
            reusedExchanges,
            addReusedExchanges)
        }
        setOpId(p)
      case p: QueryStageExec =>
        currentOperationID = generateOperatorIDs(
          p.plan,
          currentOperationID,
          visited,
          reusedExchanges,
          addReusedExchanges)
        setOpId(p)
      case other: QueryPlan[_] =>
        setOpId(other)
        currentOperationID = other.innerChildren.foldLeft(currentOperationID) {
          (curId, plan) =>
            generateOperatorIDs(plan, curId, visited, reusedExchanges, addReusedExchanges)
        }
    }
    currentOperationID
  }

  /**
   * Given a input plan, returns an array of tuples comprising of :
   *   1. Hosting operator id. 2. Hosting expression 3. Subquery plan
   */
  private def getSubqueries(
      plan: => QueryPlan[_],
      subqueries: ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)]): Unit = {
    plan.foreach {
      case a: AdaptiveSparkPlanExec =>
        getSubqueries(a.executedPlan, subqueries)
      case q: QueryStageExec =>
        getSubqueries(q.plan, subqueries)
      case cmd: CommandResultExec =>
        getSubqueries(cmd.commandPhysicalPlan, subqueries)
      case p: SparkPlan =>
        p.expressions.foreach(_.collect {
          case e: PlanExpression[_] =>
            e.plan match {
              case s: BaseSubqueryExec =>
                subqueries += ((p, e, s))
                getSubqueries(s, subqueries)
              case _ =>
            }
        })
    }
  }

  /**
   * Returns the operator identifier for the supplied plan by retrieving the `operationId` tag
   * value.
   */
  private def getOpId(plan: QueryPlan[_]): String = {
    SparkShimLoader.getSparkShims.getOperatorId(plan).map(v => s"$v").getOrElse("unknown")
  }

  private def removeTags(plan: QueryPlan[_]): Unit = {
    def remove(p: QueryPlan[_], children: Seq[QueryPlan[_]]): Unit = {
      SparkShimLoader.getSparkShims.unsetOperatorId(p)
      children.foreach(removeTags)
    }

    plan.foreach {
      case p: AdaptiveSparkPlanExec => remove(p, Seq(p.executedPlan, p.initialPlan))
      case p: QueryStageExec => remove(p, Seq(p.plan))
      case cmd: CommandResultExec => remove(cmd, Seq(cmd.commandPhysicalPlan))
      case plan: QueryPlan[_] => remove(plan, plan.innerChildren)
    }
  }
}
