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
package org.apache.gluten.execution

import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.delta.{DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

case class DeltaScanTransformer(
    @transient override val relation: HadoopFsRelation,
    @transient stream: Option[SparkDataStream],
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false,
    override val pushDownFilters: Option[Seq[Expression]] = None)
  extends FileSourceScanExecTransformerBase(
    relation,
    stream,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan
  ) {

  override lazy val fileFormat: ReadFileFormat = ReadFileFormat.ParquetReadFormat

  // For Delta column-mapping tables, `dataFilters` are kept LOGICAL on the scan node so that
  // `PreparedDeltaFileIndex` (Delta's file index, which uses logical names for partition pruning
  // and stats-based file skipping) resolves them correctly. The native side, however, must see
  // PHYSICAL names — `output` and `dataSchema` are physical, and `BasicScanExecTransformer`
  // matches `scanFilters` against `pushDownFilters` (which are derived from a Filter referencing
  // the physical-named scan output) by AttributeReference equality, which checks names. Translate
  // the logical filter attrs to their physical counterparts in `output` so the two sets line up.
  override def scanFilters: Seq[Expression] = relation.fileFormat match {
    case d: DeltaParquetFileFormat if d.columnMappingMode != NoMapping =>
      val physicalByExprId = output.collect { case ar: AttributeReference => ar.exprId -> ar }.toMap
      dataFilters.map(_.transformDown {
        case ar: AttributeReference => physicalByExprId.getOrElse(ar.exprId, ar)
      })
    case _ => dataFilters
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (
      requiredSchema.fields.exists(
        _.name == "__delta_internal_is_row_deleted") || requiredSchema.fields.exists(
        _.name == "__delta_internal_row_index")
    ) {
      return ValidationResult.failed(s"Deletion vector is not supported in native.")
    }

    super.doValidateInternal()
  }

  override def doCanonicalize(): DeltaScanTransformer = {
    DeltaScanTransformer(
      relation,
      None,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan,
      pushDownFilters.map(QueryPlan.normalizePredicates(_, output))
    )
  }

  override def withNewPushdownFilters(filters: Seq[Expression]): BasicScanExecTransformer =
    copy(pushDownFilters = Some(filters))
}

object DeltaScanTransformer {

  def apply(scanExec: FileSourceScanExec): DeltaScanTransformer = {
    new DeltaScanTransformer(
      scanExec.relation,
      SparkShimLoader.getSparkShims.getFileSourceScanStream(scanExec),
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan
    )
  }

}
