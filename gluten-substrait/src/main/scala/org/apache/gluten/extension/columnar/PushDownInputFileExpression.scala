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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{BatchScanExecTransformerBase, FileSourceScanExecTransformer, ProjectExecTransformer}
import org.apache.gluten.expression.ConverterUtils

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExprId, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.CollapseProjectShim
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{DeserializeToObjectExec, FileSourceScanExec, FilterExec, LeafExecNode, ProjectExec, SerializeFromObjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.internal.SQLConf

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

  /**
   * Tag placed on a scan node (before offloading) when an injected input-file attribute would share
   * a normalized name with a user data column in the scan output. The native scan cannot
   * simultaneously produce two columns with the same normalized name, so the scan must fall back to
   * vanilla Spark (where the file-path thread-local is correctly populated by the JVM iterator).
   *
   * The tag survives the scan's conversion to a transformer (via
   * [[ScanTransformerFactory.createFileSourceScanTransformer]] calling copyTagsFrom), allowing
   * [[BasicScanExecTransformer.doValidateInternal]] to detect the conflict and fail validation.
   */
  val INPUT_FILE_NAME_COLLISION_TAG: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("org.apache.gluten.InputFileNameCollisionTag")

  private val INPUT_FILE_ATTR_NAMES =
    Set("input_file_name", "input_file_block_start", "input_file_block_length")

  /**
   * Whether `name` is the name of an attribute injected for an input file function, following
   * Spark's configured (case-sensitive or case-insensitive) resolution.
   */
  def isInputFileAttrName(name: String): Boolean =
    INPUT_FILE_ATTR_NAMES.exists(SQLConf.get.resolver(name, _))

  def containsInputFileFunctionExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileFunctionExpr)
    }
  }

  def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case a: AttributeReference => isInputFileAttrName(a.name)
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  def addFallbackTag(plan: SparkPlan): SparkPlan = {
    FallbackTags.add(plan, "fallback input file expression")
    plan
  }

  /** Whether `projectList` lists nothing but columns and input file function calls. */
  private def isColumnsAndInputFileCalls(projectList: Seq[NamedExpression]): Boolean =
    projectList.forall {
      case Alias(_: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength, _) => true
      case _: Attribute => true
      case _ => false
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
      // Only a call to an input file function needs rewriting: a column that is only named like
      // input_file_name has nothing to push down.
      case ProjectExec(projectList, child)
          if projectList.exists(containsInputFileFunctionExpr) && hasInputFileRelatedSource(
            child) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newProjectList = projectList.map {
          expr => rewriteExpr(expr, replacedExprs).asInstanceOf[NamedExpression]
        }
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(newProjectList, newChild)
      case f @ FilterExec(condition, child)
          if containsInputFileFunctionExpr(condition) && hasInputFileRelatedSource(child) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newCondition = rewriteExpr(condition, replacedExprs)
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(f.output, FilterExec(newCondition, newChild))
    }

    private def addMetadataCol(
        plan: SparkPlan,
        replacedExprs: mutable.Map[String, Alias]): SparkPlan =
      plan match {
        case p: BatchScanExecTransformerBase =>
          // For BatchScanExecTransformerBase (includes Iceberg scans), add fallback tag
          // to prevent offloading when input_file expressions are present
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        case p: LeafExecNode if shouldAddInputFileExpr(p) =>
          tagNameCollision(p, replacedExprs)
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        case p: LeafExecNode =>
          p
        // Output of SerializeFromObjectExec's child and output of DeserializeToObjectExec must be
        // a single-field row.
        case p @ (_: SerializeFromObjectExec | _: DeserializeToObjectExec) =>
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        // The project this rule added over the scan for a call below, recognized by its input file
        // aliases: the new aliases go into it too, and PostOffload folds it into the scan.
        case p @ ProjectExec(projectList, scan: LeafExecNode)
            if isInputFileSource(scan) && isColumnsAndInputFileCalls(projectList) &&
              projectList.exists(_.isInstanceOf[Alias]) =>
          if (!scan.isInstanceOf[BatchScanExecTransformerBase]) {
            tagNameCollision(scan, replacedExprs)
          }
          addFallbackTag(p.copy(projectList = projectList ++ replacedExprs.values))
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

    /**
     * Tags the scan when the injected attribute name would collide with a user data column under
     * the current case-sensitivity setting. The native scan cannot map two columns to the same
     * normalized name; BasicScanExecTransformer.doValidateInternal checks this tag and fails
     * validation, which causes the scan to remain on vanilla Spark where the file-path thread-local
     * is set by the JVM iterator.
     */
    private def tagNameCollision(
        scan: SparkPlan,
        replacedExprs: mutable.Map[String, Alias]): Unit = {
      val scanNames = scan.output.map(a => ConverterUtils.normalizeColName(a.name)).toSet
      val injectedNames = replacedExprs.values.map(a => ConverterUtils.normalizeColName(a.name))
      if (injectedNames.exists(scanNames)) {
        scan.setTagValue(INPUT_FILE_NAME_COLLISION_TAG, true)
      }
    }

    private def isInputFileSource(scan: SparkPlan): Boolean =
      scan.isInstanceOf[BatchScanExecTransformerBase] || shouldAddInputFileExpr(scan)

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
      // A project over the scan that lists nothing but columns and input file function calls, such
      // as the one PreOffload added: the scan, with a project over it for repeats, can produce its
      // output.
      case p @ ProjectExec(projectList, child: FileSourceScanExecTransformer)
          if projectList.exists(containsInputFileRelatedExpr) && canCollapseProject(p) =>
        foldIntoScan(p, output => child.copy(output = output))
      case p @ ProjectExec(projectList, child: HiveTableScanExecTransformer)
          if projectList.exists(containsInputFileRelatedExpr) && canCollapseProject(p) =>
        foldIntoScan(
          p,
          output =>
            child.copy(
              requestedAttributes = output,
              relation = child.relation,
              partitionPruningPred = child.partitionPruningPred,
              prunedOutput = child.prunedOutput
            )(child.session)
        )
      // The same for a DSv2 scan, so the input file attributes keep the exprIds PreOffload gave
      // them and the projects above can refer to them directly. The scan's own columns are kept as
      // the scan has them: Iceberg renames columns for the native reader.
      case p @ ProjectExec(projectList, child: BatchScanExecTransformerBase)
          if projectList.exists(containsInputFileFunctionExpr) && canCollapseProject(p) &&
            namesAliasesAfterFunctions(p) =>
        val scanAttrs = child.output.map(a => a.exprId -> a).toMap
        foldIntoScan(
          p,
          output =>
            child.withOutput(
              output.map(a => scanAttrs.getOrElse(a.exprId, a.asInstanceOf[AttributeReference]))))
      case p @ ProjectExec(projectList, child: BatchScanExecTransformerBase)
          if projectList.exists(containsInputFileFunctionExpr) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newProjectList = projectList.map {
          expr => rewriteExpr(expr, replacedExprs).asInstanceOf[NamedExpression]
        }
        // Dedup by exprId rather than by name, so that a user column whose name differs from the
        // injected attribute only by case is not mistaken for it.
        val existingExprIds = child.output.map(_.exprId).toSet
        val inputFileAttrs = replacedExprs.values.toSeq
          .map(_.toAttribute.asInstanceOf[AttributeReference])
          .filterNot(attr => existingExprIds.contains(attr.exprId))
        p.copy(
          projectList = newProjectList,
          child = child.withOutput(child.output ++ inputFileAttrs))
      case p1 @ ProjectExec(_, p2: ProjectExec) if canCollapseProject(p2) =>
        addFallbackTag(
          p2.copy(projectList =
            CollapseProjectShim.buildCleanedProjectList(p1.projectList, p2.projectList)))
      case p1 @ ProjectExecTransformer(_, p2: ProjectExec) if canCollapseProject(p1, p2) =>
        addFallbackTag(
          p2.copy(projectList =
            CollapseProjectShim.buildCleanedProjectList(p1.projectList, p2.projectList)))
    }

    /**
     * Folds `project` into the scan that `withOutput` makes with the given output. The scan reads
     * each column once, and each input file function once even when PreOffload added it for calls
     * at different levels, since the native scan maps a non-partition column to one output only. A
     * project over the scan gives the repeats their values.
     */
    private def foldIntoScan(
        project: ProjectExec,
        withOutput: Seq[Attribute] => SparkPlan): SparkPlan = {
      val firstByName = mutable.Map[String, Attribute]()
      val read = mutable.Set[ExprId]()
      // Left: read by the scan. Right: given by the project over it.
      val columns: Seq[Either[Attribute, NamedExpression]] = project.projectList.map {
        case a: Alias if firstByName.contains(a.name) =>
          Right(Alias(firstByName(a.name), a.name)(exprId = a.exprId))
        case a: Alias =>
          firstByName(a.name) = a.toAttribute
          read += a.exprId
          Left(a.toAttribute)
        case a if read.contains(a.exprId) => Right(a.toAttribute)
        case a =>
          read += a.exprId
          Left(a.toAttribute)
      }
      val scan = withOutput(columns.collect { case Left(attr) => attr })
      if (columns.forall(_.isLeft)) {
        scan
      } else {
        val scanAttrs = scan.output.map(a => a.exprId -> a).toMap
        BackendsApiManager.getSparkPlanExecApiInstance.genProjectExecTransformer(
          columns.map {
            case Left(attr) => scanAttrs(attr.exprId)
            case Right(attr: Attribute) => scanAttrs(attr.exprId)
            case Right(alias) => alias
          },
          scan)
      }
    }

    // PreOffload names each alias it adds after its function, which is the name the scan looks the
    // metadata column up by.
    private def namesAliasesAfterFunctions(project: ProjectExec): Boolean =
      project.projectList.forall {
        case Alias(f, name) => name == f.prettyName
        case _ => true
      }

    private def canCollapseProject(project: ProjectExec): Boolean =
      isColumnsAndInputFileCalls(project.projectList)

    private def canCollapseProject(p1: ProjectExecTransformer, p2: ProjectExec): Boolean = {
      canCollapseProject(p2) && p1.projectList.forall {
        case Alias(_: Attribute, _) => true
        case _: Attribute => true
        case _ => false
      }
    }
  }
}
