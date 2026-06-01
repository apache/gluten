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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

object DeltaPostTransformRules {
  def rules: Seq[Rule[SparkPlan]] =
    RemoveTransitions ::
      keepDmlRowIndexFallbackSubtreeOnSpark ::
      nativeDeletionVectorRule ::
      pushDownInputFileExprRule ::
      columnMappingRule :: Nil

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
  private val deletionVectorPredicateColumnNames =
    deletionVectorInternalColumnNames ++ deletionVectorRowIndexColumnNames

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
   * In that mode, keeping the parent filter/project native adds Spark-row -> Velox-columnar ->
   * Spark-row transitions immediately before Delta's JVM bitmap path. Keep the small scan subtree
   * in Spark until the native DML row-index scan is fast enough to own the whole path.
   */
  val keepDmlRowIndexFallbackSubtreeOnSpark: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case project: ProjectExecTransformerBase if containsDmlRowIndexFallbackScan(project.child) =>
        ProjectExec(project.list, project.child)
      case filter: FilterExecTransformerBase if containsDmlRowIndexFallbackScan(filter.child) =>
        FilterExec(filter.cond, filter.child)
    }

  /**
   * Spark Delta injects synthetic deletion-vector predicates and columns into the plan. Those are
   * needed for the JVM reader path, but for the native Delta scan path they must be stripped or
   * they will be applied twice with incompatible semantics.
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

  private def containsNativeDeltaScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case _: DeltaScanTransformer => true
      case _ => false
    }
  }

  private def containsDmlRowIndexFallbackScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case scan: FileSourceScanExec =>
        FallbackTags
          .getOption(scan)
          .exists(_.reason().contains("fallback Delta DV DML row-index scan"))
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
    expr.references.exists(attr => deletionVectorPredicateColumnNames.contains(attr.name))
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

  private def isInputFileRelatedAttribute(attr: Attribute): Boolean = {
    attr match {
      case AttributeReference(name, _, _, _) =>
        Seq(InputFileName(), InputFileBlockStart(), InputFileBlockLength())
          .map(_.prettyName)
          .contains(name)
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
   * This method is only used for Delta ColumnMapping FileFormat(e.g. nameMapping and idMapping)
   * transform the metadata of Delta into Parquet's, each plan should only be transformed once.
   */
  private def transformColumnMappingPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: DeltaScanTransformer =>
      val fmt = plan.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]

      // transform HadoopFsRelation
      val relation = plan.relation
      val newFsRelation = relation.copy(
        partitionSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.partitionSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        dataSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.dataSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode)
      )(SparkSession.active)
      // transform output's name into physical name so Reader can read data correctly
      // should keep the columns order the same as the origin output
      val originColumnNames = ListBuffer.empty[String]
      val transformedAttrs = ListBuffer.empty[Attribute]
      def mapAttribute(attr: Attribute) = {
        val newAttr = if (plan.isMetadataColumn(attr)) {
          attr
        } else if (isInputFileRelatedAttribute(attr)) {
          attr
        } else {
          DeltaColumnMapping
            .createPhysicalAttributes(Seq(attr), fmt.referenceSchema, fmt.columnMappingMode)
            .head
        }
        if (!originColumnNames.contains(attr.name)) {
          transformedAttrs += newAttr
          originColumnNames += attr.name
        }
        newAttr
      }
      val newOutput = plan.output.map(o => mapAttribute(o))
      // transform dataFilters
      val newDataFilters = plan.dataFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // transform partitionFilters
      val newPartitionFilters = plan.partitionFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // replace tableName in schema with physicalName
      val scanExecTransformer = new DeltaScanTransformer(
        newFsRelation,
        plan.stream,
        newOutput,
        DeltaColumnMapping.createPhysicalSchema(
          plan.requiredSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        newDataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      scanExecTransformer.copyTagsFrom(plan)
      tagColumnMappingRule(scanExecTransformer)

      // alias physicalName into tableName
      val expr = (transformedAttrs, originColumnNames).zipped.map {
        (attr, columnName) => Alias(attr, columnName)(exprId = attr.exprId)
      }
      val projectExecTransformer = ProjectExecTransformer(expr.toSeq, scanExecTransformer)
      projectExecTransformer
    case _ => plan
  }
}
