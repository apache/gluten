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
import org.apache.gluten.expression.ConverterUtils

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.CollapseProjectShim
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{DeserializeToObjectExec, FileSourceScanExec, FilterExec, LeafExecNode, ProjectExec, SerializeFromObjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

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
        INPUT_FILE_ATTR_NAMES.contains(ConverterUtils.normalizeColName(a.name))
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
      case ProjectExec(projectList, child)
          if projectList.exists(containsInputFileRelatedExpr) && hasInputFileRelatedSource(child) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newProjectList = projectList.map {
          expr => rewriteExpr(expr, replacedExprs).asInstanceOf[NamedExpression]
        }
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(newProjectList, newChild)
      case f @ FilterExec(condition, child)
          if containsInputFileRelatedExpr(condition) && hasInputFileRelatedSource(child) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newCondition = rewriteExpr(condition, replacedExprs)
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(f.output, FilterExec(newCondition, newChild))
    }

    /**
     * Returns true when any of the injected metadata attribute names (all lowercase) matches a
     * column already present in the scan output at the Velox/case-insensitive level.
     *
     * Velox is case-insensitive. If the scan has a user data column named e.g. "Input_File_Name"
     * its lowercase form "input_file_name" collides with the metadata function of the same name.
     * Velox rejects a TableScan that maps the same lowercase column name to both a Regular handle
     * (data column) and a PartitionKey/metadata handle (file-path metadata). When this conflict is
     * detected the scan must fall back to Vanilla so that Spark's own FilePartitionReader sets the
     * InputFileBlockHolder thread-local and input_file_name() returns the correct file path.
     */
    private def hasVeloxColumnNameConflict(
        scanOutput: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
        replacedExprs: mutable.Map[String, Alias]): Boolean = {
      val scanOutputLowerNames = scanOutput.map(_.name.toLowerCase(java.util.Locale.ROOT)).toSet
      replacedExprs.keys.exists(k => scanOutputLowerNames.contains(k))
    }

    private def addMetadataCol(
        plan: SparkPlan,
        replacedExprs: mutable.Map[String, Alias]): SparkPlan =
      plan match {
        case p: BatchScanExecTransformerBase =>
          // For BatchScanExecTransformerBase (includes Iceberg scans), add fallback tag
          // to prevent offloading when input_file expressions are present.
          // Also fall back the scan itself if a Velox name conflict would occur.
          if (hasVeloxColumnNameConflict(p.output, replacedExprs)) {
            addFallbackTag(p)
          }
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        case p: LeafExecNode if shouldAddInputFileExpr(p) =>
          if (hasVeloxColumnNameConflict(p.output, replacedExprs)) {
            // The scan has a user data column whose lowercase name equals a metadata function
            // name. Velox cannot map the same case-insensitive column to both Regular (data)
            // and PartitionKey (metadata) handles.  Fall back the scan so that Vanilla Spark's
            // FilePartitionReader correctly sets InputFileBlockHolder for input_file_name().
            addFallbackTag(p)
          }
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        case p: LeafExecNode =>
          p
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

    private def hasInputFileRelatedSource(plan: SparkPlan): Boolean = {
      plan match {
        case _: BatchScanExecTransformerBase => true
        case p: LeafExecNode => shouldAddInputFileExpr(p)
        case _ => plan.children.exists(hasInputFileRelatedSource)
      }
    }

    private def shouldAddInputFileExpr(plan: SparkPlan): Boolean = {
      plan match {
        case _: FileSourceScanExec => true
        case _: BatchScanExec => true
        case p if HiveTableScanExecTransformer.isHiveTableScan(p) => true
        case _ => false
      }
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
        // Use expression ID to determine whether the injected metadata attribute is already
        // present in the scan output. Name-based dedup via toLowerCase is incorrect under
        // caseSensitive=true: a user column named e.g. "Input_File_Name" would collapse to
        // "input_file_name" and be treated as a duplicate of the injected metadata attribute,
        // causing the metadata attr to be dropped while the rewritten project list still holds
        // a reference to it, producing a dangling-attribute IllegalStateException.
        // The injected attributes are freshly created (new exprId) so identity is reliable.
        val existingExprIds = child.output.map(_.exprId).toSet
        val inputFileAttrs = replacedExprs.values.toSeq
          .map(_.toAttribute.asInstanceOf[AttributeReference])
          .filterNot(attr => existingExprIds.contains(attr.exprId))
        // Velox's native scan is case-insensitive: if a user data column in the scan output
        // has the same lowercase name as a metadata attribute being injected, Velox will see
        // two conflicting column handles (Regular vs PartitionKey/metadata) for the same
        // physical column and reject the plan with INVALID_STATE.  When such a name collision
        // exists the project cannot be collapsed into the scan; fall back instead so that
        // Velox only sees one representation of the column.
        val existingLowercaseNames =
          child.output.map(_.name.toLowerCase(java.util.Locale.ROOT)).toSet
        val hasVeloxNameConflict = inputFileAttrs.exists {
          attr => existingLowercaseNames.contains(attr.name.toLowerCase(java.util.Locale.ROOT))
        }
        if (hasVeloxNameConflict) {
          addFallbackTag(p)
        } else {
          p.copy(
            projectList = newProjectList,
            child = child.withOutput(child.output ++ inputFileAttrs))
        }
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
