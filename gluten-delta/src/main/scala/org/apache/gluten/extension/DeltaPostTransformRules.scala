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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{DeltaScanTransformer, FilterExecTransformerBase, ProjectExecTransformer, ProjectExecTransformerBase}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.transition.RemoveTransitions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, If, InputFileBlockLength, InputFileBlockStart, InputFileName, IsNull, LambdaFunction, Literal, NamedExpression, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, TransformKeys, TransformValues}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DeltaPostTransformRules {
  def rules: Seq[Rule[SparkPlan]] =
    RemoveTransitions ::
      keepDmlRowIndexFallbackSubtreeOnSpark ::
      deltaSpecificRules ::
      Nil

  /**
   * Combines the three Delta-specific post-transform rules into a single rule that first checks
   * whether the plan contains any DeltaScanTransformer. If not, the plan is returned unchanged,
   * eliminating the overhead of multiple full tree traversals for non-Delta queries.
   */
  private val deltaSpecificRules: Rule[SparkPlan] = (plan: SparkPlan) => {
    if (!plan.exists(_.isInstanceOf[DeltaScanTransformer])) {
      plan
    } else {
      val afterDv = nativeDeletionVectorRule(plan)
      val afterPushDown = pushDownInputFileExprRule(afterDv)
      columnMappingRule(afterPushDown)
    }
  }

  private val deletionVectorDeletedRowColumnName = "__delta_internal_is_row_deleted"
  private val deletionVectorRowIndexColumnName = "__delta_internal_row_index"
  // Spark 3.5+ exposes this as ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME.
  private val parquetTemporaryRowIndexColumnName = "_tmp_metadata_row_index"
  private val deletionVectorRowIndexColumnNames =
    Set(
      deletionVectorRowIndexColumnName,
      DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME,
      parquetTemporaryRowIndexColumnName)
  private val deletionVectorInternalColumnNames =
    Set(deletionVectorDeletedRowColumnName, deletionVectorRowIndexColumnName)

  private val COLUMN_MAPPING_RULE_TAG: TreeNodeTag[String] =
    TreeNodeTag[String]("org.apache.gluten.delta.column.mapping")
  private val PRESERVE_DELETION_VECTOR_ROW_INDEX_TAG: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("org.apache.gluten.delta.preserve.deletion.vector.row.index")

  private def notAppliedColumnMappingRule(plan: SparkPlan): Boolean = {
    plan.getTagValue(COLUMN_MAPPING_RULE_TAG).isEmpty
  }

  private def tagColumnMappingRule(plan: SparkPlan): Unit = {
    plan.setTagValue(COLUMN_MAPPING_RULE_TAG, null)
  }

  val columnMappingRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformWithSubqueries {
      // If it enables Delta Column Mapping(e.g. nameMapping and idMapping),
      // transform the metadata of Delta into Parquet's,
      // so that gluten can read Delta File using Parquet Reader.
      case p: DeltaScanTransformer
          if isDeltaColumnMappingFileFormat(p.relation.fileFormat) && notAppliedColumnMappingRule(
            p) =>
        transformColumnMappingPlan(p)
    }

  val pushDownInputFileExprRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case p @ ProjectExec(projectList, child: DeltaScanTransformer)
          if projectList.exists(containsInputFileRelatedExpr) =>
        child.copy(output = p.output)
    }

  /**
   * Native DELETE/UPDATE/MERGE DV support can deliberately keep the target row-index scan in Spark.
   * Keep only the contiguous scan-adjacent filter/project chain in Spark, avoiding an isolated
   * native island without propagating fallback through joins, exchanges, or aggregations. The
   * BitmapAggregator and the rest of the DML plan remain eligible for native execution.
   */
  val keepDmlRowIndexFallbackSubtreeOnSpark: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case project: ProjectExecTransformerBase
          if isDmlRowIndexFallbackSubtree(project.child) =>
        val sparkProject = ProjectExec(project.list, project.child)
        sparkProject.copyTagsFrom(project)
        sparkProject
      case filter: FilterExecTransformerBase if isDmlRowIndexFallbackSubtree(filter.child) =>
        val sparkFilter = FilterExec(filter.cond, filter.child)
        sparkFilter.copyTagsFrom(filter)
        sparkFilter
    }

  /**
   * Spark Delta injects synthetic deletion-vector predicates and columns into the plan (via
   * `PreprocessTableWithDVsStrategy`). Those drive the JVM reader path; for the native Delta scan
   * path they must be stripped, since Velox applies the DV as a per-file mask from the split info
   * and would otherwise double-apply it with incompatible semantics.
   *
   * {{{
   * Input (scan offloaded, shape injected by Delta during planning):
   *   ProjectExecTransformer [key, value]
   *   +- FilterExecTransformer (__delta_internal_is_row_deleted = 0)
   *      +- DeltaScanTransformer [key, value, __delta_internal_is_row_deleted]
   *
   * Output:
   *   DeltaScanTransformer [key, value]
   * }}}
   *
   * `__delta_internal_row_index` is preserved when an upstream operator still references it.
   * Non-offloaded scans are untouched and keep vanilla JVM filtering.
   */
  val nativeDeletionVectorRule: Rule[SparkPlan] = (plan: SparkPlan) => {
    tagRowIndexRequiredSubtrees(plan)
    plan.transformUp {
      case scan: DeltaScanTransformer =>
        val cleanedDataFilters = scan.dataFilters.flatMap(stripDeletionVectorPredicate)
        val cleanedPushDownFilters =
          scan.pushDownFilters.map(_.flatMap(stripDeletionVectorPredicate))
        val preserveRowIndex = shouldPreserveDeletionVectorRowIndex(scan)
        val cleanedOutput = stripDeletionVectorInternalOutput(scan.output, preserveRowIndex)
        val cleanedRequiredSchema =
          stripDeletionVectorInternalSchema(scan.requiredSchema, preserveRowIndex)
        if (
          cleanedDataFilters == scan.dataFilters &&
          cleanedPushDownFilters == scan.pushDownFilters &&
          cleanedOutput == scan.output &&
          cleanedRequiredSchema == scan.requiredSchema
        ) {
          scan
        } else {
          scan.copy(
            output = cleanedOutput,
            requiredSchema = cleanedRequiredSchema,
            dataFilters = cleanedDataFilters,
            pushDownFilters = cleanedPushDownFilters)
        }
      case project: ProjectExecTransformer if containsNativeDeltaScan(project.child) =>
        val cleanedProjectList = stripDeletionVectorInternalProjectList(
          project.projectList,
          shouldPreserveDeletionVectorRowIndex(project))
        if (cleanedProjectList == project.projectList) {
          project
        } else if (cleanedProjectList.isEmpty) {
          project.child
        } else {
          ProjectExecTransformer(cleanedProjectList, project.child)
        }
      case project: ProjectExec if containsNativeDeltaScan(project.child) =>
        val cleanedProjectList = stripDeletionVectorInternalProjectList(
          project.projectList,
          shouldPreserveDeletionVectorRowIndex(project))
        if (cleanedProjectList == project.projectList) {
          project
        } else if (cleanedProjectList.isEmpty) {
          project.child
        } else {
          ProjectExec(cleanedProjectList, project.child)
        }
      case filter: FilterExecTransformerBase if containsNativeDeltaScan(filter.child) =>
        stripDeletionVectorPredicate(filter.cond) match {
          case Some(cleanCondition) if cleanCondition != filter.cond =>
            BackendsApiManager.getSparkPlanExecApiInstance
              .genFilterExecTransformer(cleanCondition, filter.child)
          case Some(_) =>
            filter
          case None =>
            filter.child
        }
      case filter: FilterExec if containsNativeDeltaScan(filter.child) =>
        stripDeletionVectorPredicate(filter.condition) match {
          case Some(cleanCondition) if cleanCondition != filter.condition =>
            FilterExec(cleanCondition, filter.child)
          case Some(_) =>
            filter
          case None =>
            filter.child
        }
    }
  }

  /**
   * Checks whether a plan subtree contains a DeltaScanTransformer. Uses a shallow check (direct
   * child or grandchild) rather than a full subtree traversal, which is safe because transformUp
   * processes bottom-up and the DV-related Filter/Project nodes sit directly above the scan in
   * Delta's injected plan shape.
   */
  private def containsNativeDeltaScan(plan: SparkPlan): Boolean = {
    plan match {
      case _: DeltaScanTransformer => true
      case _ => plan.children.exists {
          case _: DeltaScanTransformer => true
          case child => child.children.exists(_.isInstanceOf[DeltaScanTransformer])
        }
    }
  }

  private def isDmlRowIndexFallbackSubtree(plan: SparkPlan): Boolean = {
    plan match {
      case scan: FileSourceScanExec =>
        DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan) &&
        FallbackTags.nonEmpty(scan)
      case project: ProjectExecTransformerBase =>
        isDmlRowIndexFallbackSubtree(project.child)
      case ProjectExec(_, child) =>
        isDmlRowIndexFallbackSubtree(child)
      case filter: FilterExecTransformerBase =>
        isDmlRowIndexFallbackSubtree(filter.child)
      case FilterExec(_, child) =>
        isDmlRowIndexFallbackSubtree(child)
      case _ => false
    }
  }

  private def isDeltaColumnMappingFileFormat(fileFormat: FileFormat): Boolean = fileFormat match {
    case d: DeltaParquetFileFormat if d.columnMappingMode != NoMapping =>
      true
    case _ =>
      false
  }

  private def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  private def referencesDeletionVectorInternalColumn(expr: Expression): Boolean = {
    expr.references.exists(attr => deletionVectorInternalColumnNames.contains(attr.name))
  }

  private def referencesDeletionVectorRowIndex(expr: Expression): Boolean = {
    expr.references.exists(attr => deletionVectorRowIndexColumnNames.contains(attr.name))
  }

  private def tagRowIndexRequiredSubtrees(plan: SparkPlan): Unit = {
    def tagSubtree(subtree: SparkPlan): Unit = {
      subtree.foreach(_.setTagValue(PRESERVE_DELETION_VECTOR_ROW_INDEX_TAG, true))
    }

    def visit(node: SparkPlan): Unit = {
      val shouldPreserveRowIndex =
        node.expressions.exists(containsIncrementMetricExpr) ||
          node.expressions.exists(referencesDeletionVectorRowIndex)
      if (shouldPreserveRowIndex) {
        node.children.foreach(tagSubtree)
      }
      node.children.foreach(visit)
    }

    visit(plan)
  }

  private def shouldPreserveDeletionVectorRowIndex(plan: SparkPlan): Boolean = {
    isDeletionVectorDmlRowIndexScan(plan) ||
    plan.getTagValue(PRESERVE_DELETION_VECTOR_ROW_INDEX_TAG).contains(true) ||
    plan.expressions.exists(containsIncrementMetricExpr) ||
    plan.expressions.exists(referencesDeletionVectorRowIndex)
  }

  private def isDeletionVectorDmlRowIndexScan(plan: SparkPlan): Boolean = {
    DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(plan)
  }

  private def shouldStripDeletionVectorInternalColumn(
      columnName: String,
      preserveRowIndex: Boolean): Boolean = {
    columnName == deletionVectorDeletedRowColumnName ||
    (!preserveRowIndex && columnName == deletionVectorRowIndexColumnName)
  }

  private def stripDeletionVectorInternalOutput(
      output: Seq[Attribute],
      preserveRowIndex: Boolean): Seq[Attribute] = {
    output.filterNot(attr => shouldStripDeletionVectorInternalColumn(attr.name, preserveRowIndex))
  }

  private def stripDeletionVectorInternalProjectList(
      projectList: Seq[NamedExpression],
      preserveRowIndex: Boolean): Seq[NamedExpression] = {
    projectList.filterNot(
      expr => shouldStripDeletionVectorInternalColumn(expr.name, preserveRowIndex))
  }

  private def stripDeletionVectorInternalSchema(
      schema: StructType,
      preserveRowIndex: Boolean): StructType = {
    StructType(
      schema.filterNot(
        field => shouldStripDeletionVectorInternalColumn(field.name, preserveRowIndex)))
  }

  private def stripDeletionVectorPredicate(expr: Expression): Option[Expression] = {
    expr match {
      case And(left, right) =>
        (stripDeletionVectorPredicate(left), stripDeletionVectorPredicate(right)) match {
          case (Some(cleanLeft), Some(cleanRight)) => Some(And(cleanLeft, cleanRight))
          case (Some(cleanLeft), None) => Some(cleanLeft)
          case (None, Some(cleanRight)) => Some(cleanRight)
          case (None, None) => None
        }
      case other if referencesDeletionVectorInternalColumn(other) =>
        None
      case other =>
        Some(other)
    }
  }

  private val inputFileRelatedNames: Set[String] =
    Set(InputFileName(), InputFileBlockStart(), InputFileBlockLength()).map(_.prettyName)

  private def isInputFileRelatedAttribute(attr: Attribute): Boolean = {
    attr match {
      case AttributeReference(name, _, _, _) => inputFileRelatedNames.contains(name)
      case _ => false
    }
  }

  private[gluten] def containsIncrementMetricExpr(expr: Expression): Boolean = {
    expr match {
      case e if e.prettyName == "increment_metric" => true
      case _ => expr.children.exists(containsIncrementMetricExpr)
    }
  }

  /**
   * Checks whether two structurally compatible DataTypes have different struct field names at any
   * nesting level.
   */
  private def nestedFieldNamesDiffer(logical: DataType, physical: DataType): Boolean = {
    (logical, physical) match {
      case (l: StructType, p: StructType) if l.length == p.length =>
        l.zip(p).exists {
          case (lf, pf) =>
            lf.name != pf.name || nestedFieldNamesDiffer(lf.dataType, pf.dataType)
        }
      case (l: ArrayType, p: ArrayType) =>
        nestedFieldNamesDiffer(l.elementType, p.elementType)
      case (l: MapType, p: MapType) =>
        nestedFieldNamesDiffer(l.keyType, p.keyType) ||
        nestedFieldNamesDiffer(l.valueType, p.valueType)
      case _ => false
    }
  }

  /**
   * Rebuilds an expression tree so that nested struct field names match the logical schema. Uses
   * positional extraction (GetStructField) and reconstruction (CreateNamedStruct) instead of Cast,
   * so correctness does not depend on Velox's cast_match_struct_by_name config.
   */
  private def reconcileFieldNames(
      expr: Expression,
      logical: DataType,
      physical: DataType): Expression = {
    (logical, physical) match {
      case (l: StructType, p: StructType) if l.length == p.length =>
        val rebuiltFields = l.zip(p).zipWithIndex.flatMap {
          case ((lf, pf), i) =>
            val extracted = GetStructField(expr, i, None)
            val reconciled = reconcileFieldNames(extracted, lf.dataType, pf.dataType)
            Seq(Literal(lf.name), reconciled)
        }
        val rebuilt = CreateNamedStruct(rebuiltFields)
        If(IsNull(expr), Literal.create(null, l), rebuilt)
      case (l: ArrayType, p: ArrayType) if nestedFieldNamesDiffer(l.elementType, p.elementType) =>
        val lambdaVar = NamedLambdaVariable("element", p.elementType, p.containsNull)
        val body = reconcileFieldNames(lambdaVar, l.elementType, p.elementType)
        ArrayTransform(expr, LambdaFunction(body, Seq(lambdaVar)))
      case (l: MapType, p: MapType) =>
        val needKeys = nestedFieldNamesDiffer(l.keyType, p.keyType)
        val needValues = nestedFieldNamesDiffer(l.valueType, p.valueType)
        var result = expr
        if (needValues) {
          val keyVar = NamedLambdaVariable("key", p.keyType, false)
          val valueVar = NamedLambdaVariable("value", p.valueType, p.valueContainsNull)
          val body = reconcileFieldNames(valueVar, l.valueType, p.valueType)
          result = TransformValues(result, LambdaFunction(body, Seq(keyVar, valueVar)))
        }
        if (needKeys) {
          val keyVar = NamedLambdaVariable("key", p.keyType, false)
          val valueVar = NamedLambdaVariable(
            "value",
            if (needValues) l.valueType else p.valueType,
            p.valueContainsNull)
          val body = reconcileFieldNames(keyVar, l.keyType, p.keyType)
          result = TransformKeys(result, LambdaFunction(body, Seq(keyVar, valueVar)))
        }
        result
      case _ => expr
    }
  }

  /**
   * Used for Delta ColumnMapping FileFormat (nameMapping and idMapping). Each plan is transformed
   * at most once; the first run is tagged so re-runs are no-ops.
   *
   * Background: with column mapping, Delta files are written with PHYSICAL column names while
   * Delta's metadata (partition schema, column stats) keeps LOGICAL names. Vanilla Spark + Delta
   * resolves this asymmetry inside `DeltaParquetFileFormat.buildReaderWithPartitionValues`:
   * everything on the scan node stays logical, and physical translation happens just-in-time when
   * handing data and filters to the parquet reader. Gluten bypasses that hook (it goes to native
   * via Substrait), so the translation has to live somewhere on our side.
   *
   * What this rule produces -- the parts that diverge from vanilla Spark are commented at each
   * site. The split-by-consumer is asymmetric on purpose:
   *
   *   - `output`, `dataSchema`, and the data fields of `requiredSchema` ==> PHYSICAL. These flow
   *     into the substrait `NamedStruct` that Velox uses to look up columns in the parquet file.
   *     The parquet column name is the physical name, so Velox needs the physical name on the
   *     schema side. A `ProjectExecTransformer` is added below to alias these back to logical names
   *     for downstream Spark operators.
   *   - `partitionSchema`, `partitionFilters`, `dataFilters`, partition fields of `requiredSchema`
   *     ==> LOGICAL. These are consumed by Delta's `PreparedDeltaFileIndex.matchingFiles` and
   *     `Snapshot.filesForScan`, which resolve filters and partition values against
   *     `metadata.partitionSchema` and the column-stats schema -- both LOGICAL. Rewriting any of
   *     these to physical names was the cause of issue #10511 (partition pruning silently no-op'd)
   *     and would also disable file-level stats skipping.
   *   - `DeltaScanTransformer.scanFilters` (override) ==> PHYSICAL, translated from `dataFilters`
   *     by exprId match against `output`. Substrait binds filters by exprId rather than name, so it
   *     would be tempting to pass logical-named filters straight through; but
   *     `BasicScanExecTransformer.filterExprs()` does a name-and-exprId equality check
   *     (`scanFilters.partition(pushDownFilters.contains(_))`) against the physical-named
   *     `pushDownFilters` from the upstream `Filter`. The override ensures both sides match.
   *
   * Future cleanup (out of scope for this fix): the cleaner shape is to mirror vanilla Spark
   * exactly -- keep EVERYTHING on the scan node logical, and do physical translation only at
   * substrait emission time (e.g. inside the `NamedStruct`/`ReadRel` build in
   * `BasicScanExecTransformer.doTransform`). That removes the alias-back project below and the
   * `scanFilters` override, but it requires plumbing Delta-specific physical-name lookup into the
   * substrait emitter and is a multi-module refactor.
   */
  private def transformColumnMappingPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: DeltaScanTransformer =>
      val fmt = plan.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]

      val relation = plan.relation
      val partitionColNames = relation.partitionSchema.fields.iterator.map(_.name).toSet
      def isPartitionCol(name: String): Boolean = partitionColNames.contains(name)

      // transform HadoopFsRelation: only `dataSchema` needs physical names (those are the
      // columns actually stored in parquet). `partitionSchema` stays logical.
      val newFsRelation = relation.copy(
        dataSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.dataSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode)
      )(SparkSession.active)
      // transform output's name into physical name so Reader can read data correctly
      // should keep the columns order the same as the origin output
      case class ColumnMapping(logicalName: String, logicalType: DataType, physicalAttr: Attribute)
      val columnMappings = ListBuffer.empty[ColumnMapping]
      val seenNames = mutable.Set.empty[String]

      // Batch: collect all data attributes that need physical name mapping, call
      // createPhysicalAttributes once (instead of per-column), then build a lookup map.
      val dataAttrsNeedingMapping = plan.output.filter {
        attr =>
          !plan.isMetadataColumn(attr) &&
          !isInputFileRelatedAttribute(attr) &&
          !isPartitionCol(attr.name)
      }
      val physicalDataAttrs = if (dataAttrsNeedingMapping.nonEmpty) {
        DeltaColumnMapping.createPhysicalAttributes(
          dataAttrsNeedingMapping,
          fmt.referenceSchema,
          fmt.columnMappingMode)
      } else {
        Seq.empty
      }
      val physicalByExprId = dataAttrsNeedingMapping.zip(physicalDataAttrs)
        .map {
          case (logical, physical) =>
            logical.exprId -> physical
        }.toMap

      def mapAttribute(attr: Attribute) = {
        val newAttr = physicalByExprId.getOrElse(attr.exprId, attr)
        if (seenNames.add(attr.name)) {
          columnMappings += ColumnMapping(attr.name, attr.dataType, newAttr)
        }
        newAttr
      }
      val newOutput = plan.output.map(o => mapAttribute(o))
      // dataFilters / partitionFilters: kept LOGICAL on the scan node so Delta's file index
      // (partition pruning + stats-based file skipping) resolves columns correctly. The native
      // (Velox) side gets physical-translated copies via `DeltaScanTransformer.scanFilters`.
      val newDataFilters = plan.dataFilters
      val newPartitionFilters = plan.partitionFilters

      // requiredSchema: rewrite data fields to physical, keep partition fields logical.
      val physicalRequiredSchema = DeltaColumnMapping.createPhysicalSchema(
        plan.requiredSchema,
        fmt.referenceSchema,
        fmt.columnMappingMode)
      val newRequiredSchema = StructType(
        physicalRequiredSchema.fields.zip(plan.requiredSchema.fields).map {
          case (_, logical) if isPartitionCol(logical.name) => logical
          case (physical, _) => physical
        })

      val scanExecTransformer = new DeltaScanTransformer(
        newFsRelation,
        plan.stream,
        newOutput,
        newRequiredSchema,
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        newDataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      scanExecTransformer.copyTagsFrom(plan)
      tagColumnMappingRule(scanExecTransformer)

      // Alias physical names back to logical names. For struct-typed columns, Delta column
      // mapping renames internal field names to physical UUIDs. A top-level Alias only restores
      // the column name, not the struct's internal field names. We rebuild the struct with
      // logical field names using positional extraction (GetStructField/CreateNamedStruct)
      // instead of Cast, so correctness does not depend on any Velox cast config.
      val expr = columnMappings.map {
        cm =>
          val projectedExpr: Expression =
            if (nestedFieldNamesDiffer(cm.logicalType, cm.physicalAttr.dataType)) {
              reconcileFieldNames(cm.physicalAttr, cm.logicalType, cm.physicalAttr.dataType)
            } else {
              cm.physicalAttr
            }
          Alias(projectedExpr, cm.logicalName)(exprId = cm.physicalAttr.exprId)
      }
      val projectExecTransformer = ProjectExecTransformer(expr.toSeq, scanExecTransformer)
      projectExecTransformer
    case _ => plan
  }
}
