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

import org.apache.gluten.execution.{DeltaScanTransformer, ProjectExecTransformer}
import org.apache.gluten.extension.columnar.transition.RemoveTransitions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, If, InputFileBlockLength, InputFileBlockStart, InputFileName, IsNull, LambdaFunction, Literal, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, TransformKeys, TransformValues}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DeltaPostTransformRules {
  def rules: Seq[Rule[SparkPlan]] =
    RemoveTransitions :: pushDownInputFileExprRule :: columnMappingRule :: Nil

  private val COLUMN_MAPPING_RULE_TAG: TreeNodeTag[String] =
    TreeNodeTag[String]("org.apache.gluten.delta.column.mapping")

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
      case class ColumnMapping(logicalName: String, logicalType: DataType, physicalAttr: Attribute)
      val columnMappings = ListBuffer.empty[ColumnMapping]
      val seenNames = mutable.Set.empty[String]
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
        if (seenNames.add(attr.name)) {
          columnMappings += ColumnMapping(attr.name, attr.dataType, newAttr)
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
