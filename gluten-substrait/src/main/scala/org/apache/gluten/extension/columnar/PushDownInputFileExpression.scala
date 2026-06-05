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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.{BatchScanExecTransformerBase, FileSourceScanExecTransformer, ProjectExecTransformer}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.CollapseProjectShim
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{DeserializeToObjectExec, LeafExecNode, ProjectExec, SerializeFromObjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

import java.util.Locale

import scala.collection.mutable

/**
 * The Spark implementations of input_file_name/input_file_block_start/input_file_block_length uses
 * a thread local to stash the file name and retrieve it from the function. If there is a
 * transformer node between project input_file_function and scan, the result of input_file_name is
 * an empty string. So we should push down input_file_function to transformer scan or add fallback
 * project of input_file_function before fallback scan.
 *
 * Two rules are involved:
 *   - Before offload, add new project before leaf node and push down input file expression to the
 *     new project
 *   - After offload, push down input file expression into scan and remove project if scan be
 *     offloaded, collapse project if scan is fallback and the outer project is cheap or fallback
 */
object PushDownInputFileExpression {

  private val INPUT_FILE_ATTR_NAMES =
    Set("input_file_name", "input_file_block_start", "input_file_block_length")

  def containsInputFileFunctionExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileFunctionExpr)
    }
  }

  def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case a: AttributeReference =>
        INPUT_FILE_ATTR_NAMES.contains(a.name.toLowerCase(Locale.ROOT))
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  def addFallbackTag(plan: SparkPlan): SparkPlan = {
    FallbackTags.add(plan, "fallback input file expression")
    plan
  }

  private def rewriteExpr(expr: Expression, replacedExprs: mutable.Map[String, Alias]): Expression =
    expr match {
      case _: InputFileName =>
        replacedExprs
          .getOrElseUpdate(expr.prettyName, Alias(InputFileName(), expr.prettyName)())
          .toAttribute
      case _: InputFileBlockStart =>
        replacedExprs
          .getOrElseUpdate(expr.prettyName, Alias(InputFileBlockStart(), expr.prettyName)())
          .toAttribute
      case _: InputFileBlockLength =>
        replacedExprs
          .getOrElseUpdate(expr.prettyName, Alias(InputFileBlockLength(), expr.prettyName)())
          .toAttribute
      case other =>
        other.withNewChildren(other.children.map(child => rewriteExpr(child, replacedExprs)))
    }

  object PreOffload extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case ProjectExec(projectList, child) if projectList.exists(containsInputFileRelatedExpr) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newProjectList = projectList.map {
          expr => rewriteExpr(expr, replacedExprs).asInstanceOf[NamedExpression]
        }
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(newProjectList, newChild)
    }

    private def addMetadataCol(
        plan: SparkPlan,
        replacedExprs: mutable.Map[String, Alias]): SparkPlan =
      plan match {
        case p: BatchScanExecTransformerBase =>
          // For BatchScanExecTransformerBase (includes Iceberg scans), add fallback tag
          // to prevent offloading when input_file expressions are present
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        case p: LeafExecNode =>
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        // Output of SerializeFromObjectExec's child and output of DeserializeToObjectExec must be
        // a single-field row.
        case p @ (_: SerializeFromObjectExec | _: DeserializeToObjectExec) =>
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        case p: ProjectExec =>
          p.copy(
            projectList = p.projectList ++ replacedExprs.values.toSeq.map(_.toAttribute),
            child = addMetadataCol(p.child, replacedExprs))
        case u @ UnionExec(children) =>
          val newFirstChild = addMetadataCol(children.head, replacedExprs)
          val newOtherChildren = children.tail.map {
            child =>
              // Make sure exprId is unique in each child of Union.
              val newReplacedExprs = replacedExprs.map {
                expr => (expr._1, Alias(expr._2.child, expr._2.name)())
              }
              addMetadataCol(child, newReplacedExprs)
          }
          u.copy(children = newFirstChild +: newOtherChildren)
        case p => p.withNewChildren(p.children.map(child => addMetadataCol(child, replacedExprs)))
      }
  }

  object PostOffload extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case p @ ProjectExec(projectList, child: FileSourceScanExecTransformer)
          if projectList.exists(containsInputFileRelatedExpr) =>
        child.copy(output = p.output)
      case p @ ProjectExec(projectList, child: HiveTableScanExecTransformer)
          if projectList.exists(containsInputFileRelatedExpr) =>
        child.copy(
          requestedAttributes = p.output,
          relation = child.relation,
          partitionPruningPred = child.partitionPruningPred,
          prunedOutput = child.prunedOutput
        )(child.session)
      case p @ ProjectExec(projectList, child: BatchScanExecTransformerBase)
          if projectList.exists(containsInputFileFunctionExpr) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newProjectList = projectList.map {
          expr => rewriteExpr(expr, replacedExprs).asInstanceOf[NamedExpression]
        }
        val existingNames = child.output.map(_.name.toLowerCase(Locale.ROOT)).toSet
        val inputFileAttrs = replacedExprs.values.toSeq
          .map(_.toAttribute.asInstanceOf[AttributeReference])
          .filterNot(attr => existingNames.contains(attr.name.toLowerCase(Locale.ROOT)))
        p.copy(
          projectList = newProjectList,
          child = child.withOutput(child.output ++ inputFileAttrs))
      case p1 @ ProjectExec(_, ProjectExec(childProjectList, scan: BatchScanExecTransformerBase))
          if childProjectList.exists(containsInputFileRelatedExpr) =>
        val newOutput = childProjectList.map(_.toAttribute.asInstanceOf[AttributeReference])
        p1.copy(child = scan.withOutput(newOutput))
      case p1 @ ProjectExec(_, p2: ProjectExec) if canCollapseProject(p2) =>
        addFallbackTag(
          p2.copy(projectList =
            CollapseProjectShim.buildCleanedProjectList(p1.projectList, p2.projectList)))
      case p1 @ ProjectExecTransformer(_, p2: ProjectExec) if canCollapseProject(p1, p2) =>
        addFallbackTag(
          p2.copy(projectList =
            CollapseProjectShim.buildCleanedProjectList(p1.projectList, p2.projectList)))
    }

    private def canCollapseProject(project: ProjectExec): Boolean = {
      project.projectList.forall {
        case Alias(_: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength, _) => true
        case _: Attribute => true
        case _ => false
      }
    }

    private def canCollapseProject(p1: ProjectExecTransformer, p2: ProjectExec): Boolean = {
      canCollapseProject(p2) && p1.projectList.forall {
        case Alias(_: Attribute, _) => true
        case _: Attribute => true
        case _ => false
      }
    }
  }
}
