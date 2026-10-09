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
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

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
   * Metadata key used to mark AttributeReferences that were injected by PreOffload to carry
   * input-file function values (input_file_name, input_file_block_start, input_file_block_length).
   * BasicScanExecTransformer.makeColumnTypeNode checks this key and emits METADATA_COL for these
   * attributes so that Velox classifies them as kSynthesized (infoColumns) rather than kRegular,
   * avoiding a "Cannot map from same table column to different outputs in table scan" collision
   * when a user data column has the same case-insensitive name (e.g. Input_File_Name vs injected
   * input_file_name).
   */
  val GLUTEN_INPUT_FILE_COL_ATTR_KEY = "__gluten_input_file_col__"

  /**
   * Metadata key that stores the canonical (Spark) prettyName of the input-file function (e.g.
   * "input_file_name") for an injected attribute. When a caseSensitive=false name collision forces
   * us to give the injected attribute a mangled schema name, this key lets BasicScanExecTransformer
   * recover the canonical name and pass the correct value to the Velox split infoColumns map.
   */
  val GLUTEN_INPUT_FILE_CANON_KEY = "__gluten_input_file_canon__"

  /** Metadata instance placed on every injected input-file attribute. */
  val INPUT_FILE_COL_METADATA: Metadata =
    new MetadataBuilder().putBoolean(GLUTEN_INPUT_FILE_COL_ATTR_KEY, true).build()

  /** Returns true if the attribute was injected by PreOffload as an input-file metadata col. */
  def isInjectedInputFileAttr(attr: Attribute): Boolean =
    attr.metadata.contains(GLUTEN_INPUT_FILE_COL_ATTR_KEY)

  /**
   * Returns the canonical Spark prettyName stored in the injected attribute's metadata. Falls back
   * to attr.name when the attribute was created without the canon key (e.g. no collision path).
   */
  def injectedInputFileCanonName(attr: Attribute): String =
    if (attr.metadata.contains(GLUTEN_INPUT_FILE_CANON_KEY)) {
      attr.metadata.getString(GLUTEN_INPUT_FILE_CANON_KEY)
    } else {
      attr.name
    }

  def containsInputFileFunctionExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileFunctionExpr)
    }
  }

  def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  def addFallbackTag(plan: SparkPlan): SparkPlan = {
    FallbackTags.add(plan, "fallback input file expression")
    plan
  }

  /**
   * Create a tagged Alias for an input-file expression.
   *
   * The alias carries INPUT_FILE_COL_METADATA (so makeColumnTypeNode emits METADATA_COL) and, when
   * the schema name must be mangled to avoid a case-insensitive collision with a user column, also
   * stores the canonical prettyName under GLUTEN_INPUT_FILE_CANON_KEY so that
   * BasicScanExecTransformer can recover it for the Velox split infoColumns map.
   *
   * @param child
   *   The input-file function expression.
   * @param schemaName
   *   The name to give the alias in the scan output schema. Equals prettyName when there is no
   *   collision; is a mangled private name when a caseSensitive=false collision exists.
   * @param canonicalName
   *   The Spark prettyName (e.g. "input_file_name"). Stored in metadata only when it differs from
   *   schemaName.
   */
  private def makeInputFileAlias(
      child: Expression,
      schemaName: String,
      canonicalName: String): Alias = {
    val meta =
      if (schemaName == canonicalName) {
        INPUT_FILE_COL_METADATA
      } else {
        new MetadataBuilder()
          .withMetadata(INPUT_FILE_COL_METADATA)
          .putString(GLUTEN_INPUT_FILE_CANON_KEY, canonicalName)
          .build()
      }
    Alias(child, schemaName)(explicitMetadata = Some(meta))
  }

  /**
   * Rewrite input-file function expressions in `expr`, replacing each with an AttributeReference to
   * a fresh alias stored in `replacedExprs`.
   *
   * @param mangledPrettyNames
   *   Set of canonical prettyNames whose injected aliases must use a private mangled schema name
   *   (because the user table has a column that normalises to the same lowercase name under
   *   caseSensitive=false). An empty set means no mangling is needed.
   */
  private def rewriteExpr(
      expr: Expression,
      replacedExprs: mutable.Map[String, Alias],
      mangledPrettyNames: Set[String] = Set.empty): Expression =
    expr match {
      case _: InputFileName =>
        val pretty = expr.prettyName
        replacedExprs
          .getOrElseUpdate(
            pretty,
            makeInputFileAlias(
              InputFileName(),
              if (mangledPrettyNames.contains(pretty)) mangledSchemaName(pretty) else pretty,
              pretty))
          .toAttribute
      case _: InputFileBlockStart =>
        val pretty = expr.prettyName
        replacedExprs
          .getOrElseUpdate(
            pretty,
            makeInputFileAlias(
              InputFileBlockStart(),
              if (mangledPrettyNames.contains(pretty)) mangledSchemaName(pretty) else pretty,
              pretty))
          .toAttribute
      case _: InputFileBlockLength =>
        val pretty = expr.prettyName
        replacedExprs
          .getOrElseUpdate(
            pretty,
            makeInputFileAlias(
              InputFileBlockLength(),
              if (mangledPrettyNames.contains(pretty)) mangledSchemaName(pretty) else pretty,
              pretty))
          .toAttribute
      case other =>
        other.withNewChildren(
          other.children.map(child => rewriteExpr(child, replacedExprs, mangledPrettyNames)))
    }

  /**
   * Returns the private schema name used for an injected input-file alias when a
   * caseSensitive=false name collision is detected. The mangled name starts with the
   * GLUTEN_INPUT_FILE_COL_ATTR_KEY prefix, guaranteeing it can never collide with a real user
   * column (real columns cannot be named with double-underscore Gluten-internal identifiers).
   */
  private[columnar] def mangledSchemaName(prettyName: String): String =
    s"$GLUTEN_INPUT_FILE_COL_ATTR_KEY${prettyName}__"

  object PreOffload extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case ProjectExec(projectList, child)
          if projectList.exists(containsInputFileRelatedExpr) && hasInputFileRelatedSource(child) =>
        val mangledNames = collidingInputFilePrettyNames(child)
        val replacedExprs = mutable.Map[String, Alias]()
        val newProjectList = projectList.map {
          expr => rewriteExpr(expr, replacedExprs, mangledNames).asInstanceOf[NamedExpression]
        }
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(newProjectList, newChild)
      case f @ FilterExec(condition, child)
          if containsInputFileRelatedExpr(condition) && hasInputFileRelatedSource(child) =>
        val mangledNames = collidingInputFilePrettyNames(child)
        val replacedExprs = mutable.Map[String, Alias]()
        val newCondition = rewriteExpr(condition, replacedExprs, mangledNames)
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(f.output, FilterExec(newCondition, newChild))
    }

    /**
     * Returns the set of input-file function prettyNames (e.g. "input_file_name") whose injected
     * aliases must be given mangled schema names because a user column in the scan output
     * normalises (via ConverterUtils.normalizeColName) to the same lowercase string.
     *
     * Under caseSensitive=true, normalizeColName preserves case, so "Input_File_Name" and
     * "input_file_name" remain distinct -- no mangling needed, empty set returned. However, a user
     * column whose name is an exact lowercase match to the function prettyName (e.g.
     * "input_file_name") will still be detected as a collision even under caseSensitive=true,
     * because both names normalise to the same string. In that case the injected alias is mangled
     * AND the scan is fallback-tagged (in addMetadataCol) so the JVM scan sets the InputFileName
     * thread-local.
     *
     * Under caseSensitive=false, normalizeColName lowercases all names. A user column
     * "Input_File_Name" lowercases to "input_file_name", colliding with the injected alias of the
     * same name in the Velox NamedStruct schema. In this case we return {"input_file_name"} so that
     * the alias is given the private name mangledSchemaName("input_file_name") instead, avoiding
     * the Velox "Cannot map from same table column to different outputs" error.
     */
    private def collidingInputFilePrettyNames(plan: SparkPlan): Set[String] = {
      val inputFilePrettyNames = Set(
        InputFileName().prettyName,
        InputFileBlockStart().prettyName,
        InputFileBlockLength().prettyName)
      val userOutputNormNames: Set[String] = collectScanOutput(plan)
        .filterNot(isInjectedInputFileAttr)
        .map(attr => ConverterUtils.normalizeColName(attr.name))
        .toSet
      inputFilePrettyNames.filter(userOutputNormNames.contains)
    }

    /** Collect the output attributes of all leaf scan nodes reachable from plan. */
    private def collectScanOutput(plan: SparkPlan): Seq[Attribute] =
      plan match {
        case leaf: LeafExecNode => leaf.output
        case _ => plan.children.flatMap(collectScanOutput)
      }

    private def addMetadataCol(
        plan: SparkPlan,
        replacedExprs: mutable.Map[String, Alias]): SparkPlan =
      plan match {
        case p: BatchScanExecTransformerBase =>
          // For BatchScanExecTransformerBase (includes Iceberg scans), add fallback tag
          // to prevent offloading when input_file expressions are present.
          addFallbackTag(ProjectExec(p.output ++ replacedExprs.values, p))
        case p: LeafExecNode if shouldAddInputFileExpr(p) =>
          // When a name collision was detected (any injected alias carries
          // GLUTEN_INPUT_FILE_CANON_KEY, meaning its schema name was mangled), the native
          // scan cannot safely produce both the user data column and the metadata column under
          // the same effective name.  Adding a fallback tag to the scan node HERE (before
          // OffloadOthers runs) prevents the scan from being converted to a native transformer.
          // The JVM scan will then set the InputFileName thread-local so InputFileName()
          // returns the correct non-empty file path.
          //
          // When no collision exists, the injected aliases have already been given the correct
          // non-mangled schema names and the scan can be offloaded natively.
          // makeColumnTypeNode classifies injected attrs as METADATA_COL (kSynthesized)
          // via isInjectedInputFileAttr, and BasicScanExecTransformer.partitionToSplitInfo
          // uses injectedInputFileCanonName to populate the correct infoColumns value.
          val hasCollision =
            replacedExprs.values.exists(a => a.metadata.contains(GLUTEN_INPUT_FILE_CANON_KEY))
          if (hasCollision) addFallbackTag(p)
          ProjectExec(p.output ++ replacedExprs.values, p)
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
              // IMPORTANT: preserve the original alias's explicitMetadata (which carries
              // INPUT_FILE_COL_METADATA and, when mangling was needed, also
              // GLUTEN_INPUT_FILE_CANON_KEY).  The default Alias(...)() constructor uses
              // Metadata.empty, which strips these keys, causing isInjectedInputFileAttr to
              // return false for the tail-branch attributes and breaking the infoColumns lookup
              // in BasicScanExecTransformer.partitionToSplitInfo for every UNION branch after
              // the first (PR #12726 regression: input_file_name() + UNION ALL).
              val newReplacedExprs = replacedExprs.map {
                case (k, a) =>
                  k -> Alias(a.child, a.name)(explicitMetadata = Some(a.metadata))
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
        // Collect pre-existing injected aliases created by PreOffload (they carry
        // INPUT_FILE_COL_METADATA).  PreOffload may have given them mangled schema names
        // (e.g. "__gluten_input_file_col__input_file_name__") to avoid a Velox
        // Regular-vs-Synthesized name collision when the table has a physical column whose
        // normalised name equals the input-file function prettyName.
        val preInjectedAliases: Seq[Alias] = projectList.collect {
          case a: Alias if PushDownInputFileExpression.isInjectedInputFileAttr(a.toAttribute) => a
        }

        // Determine whether any injected attr (using its canonical prettyName) collides with a
        // physical Regular column in the scan output.  Under the BatchScan/Iceberg path:
        //   - IcebergScanTransformer.inputFileRelatedMetadataColumns recognises columns only by
        //     their exact (normalized) name ("input_file_name" etc.), not by the mangled schema
        //     name.  A mangled injected attr therefore won't be populated with the file path,
        //     producing a null value.
        //   - The correct resolution is to fall back the scan to JVM so that InputFileName()
        //     can be evaluated via the Spark thread-local set by the native JVM Iceberg iterator.
        //
        // A collision exists when the canonical prettyName of an injected alias (i.e. the
        // unmangled input-file function prettyName stored in GLUTEN_INPUT_FILE_CANON_KEY, or the
        // schema name when no mangling was needed) equals the name of a non-injected column in
        // the scan's physical output.  Under caseSensitive=true, name comparison is exact;
        // under caseSensitive=false, both sides have been lowercased by normalizeColName.
        val physicalOutputNames: Set[String] = child.output
          .filterNot(PushDownInputFileExpression.isInjectedInputFileAttr)
          .map(a => ConverterUtils.normalizeColName(a.name))
          .toSet
        val canonicalPrettyNames: Set[String] = preInjectedAliases
          .map(a => PushDownInputFileExpression.injectedInputFileCanonName(a.toAttribute))
          .map(ConverterUtils.normalizeColName)
          .toSet
        val hasNameCollision = canonicalPrettyNames.exists(physicalOutputNames.contains)

        if (hasNameCollision) {
          // Collision: the injected metadata attr's canonical name matches a physical Regular
          // column in the BatchScan/Iceberg scan.  We cannot give Velox both a Regular and a
          // Synthesized column with the same name.  Force the scan back to JVM so that Spark's
          // thread-local is set by the JVM scan iterator and InputFileName() returns the real
          // file path.  The inner ProjectExec is already fallback-tagged by PreOffload; tagging
          // the scan node too ensures the whole subtree evaluates on the JVM.
          addFallbackTag(child)
          p
        } else if (preInjectedAliases.nonEmpty) {
          // No collision. PreOffload already rewrote this node with (possibly mangled) aliases.
          // Reuse them: for each Alias(InputFileName(), schemaName, meta)(exprId=X), create a
          // fresh AttributeReference(schemaName, ..., meta)(exprId=Z) for the scan output, and
          // replace the Alias child with AttrRef(Z) while preserving the Alias exprId=X.
          // This mirrors rewriteExpr's behaviour (Alias(InputFileName,..) -> Alias(AttrRef,..))
          // and keeps Alias structure in the projectList so canCollapseProject does not fire.
          val existingExprIds = child.output.map(_.exprId).toSet
          val aliasWithScanAttr: Seq[(Alias, Alias, AttributeReference)] =
            preInjectedAliases.map {
              a =>
                val scanAttr = AttributeReference(a.name, a.dataType, a.nullable, a.metadata)()
                val newAlias = Alias(scanAttr, a.name)(
                  exprId = a.exprId,
                  qualifier = a.qualifier,
                  explicitMetadata = Some(a.metadata),
                  nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
                (a, newAlias, scanAttr)
            }
          val aliasRewrite: Map[Alias, Alias] = aliasWithScanAttr.map {
            case (orig, rewritten, _) => orig -> rewritten
          }.toMap
          val newProjList: Seq[NamedExpression] = projectList.map {
            case a: Alias if aliasRewrite.contains(a) => aliasRewrite(a)
            case other => other
          }
          val newAttrs = aliasWithScanAttr
            .map(_._3)
            .filterNot(attr => existingExprIds.contains(attr.exprId))
          p.copy(
            projectList = newProjList,
            child = child.withOutput(child.output ++ newAttrs))
        } else {
          // No pre-existing injected aliases -- create fresh ones as before.
          val replacedExprs = mutable.Map[String, Alias]()
          val newProjList = projectList.map {
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
          val newAttrs = replacedExprs.values.toSeq
            .map(_.toAttribute.asInstanceOf[AttributeReference])
            .filterNot(attr => existingExprIds.contains(attr.exprId))
          p.copy(
            projectList = newProjList,
            child = child.withOutput(child.output ++ newAttrs))
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
