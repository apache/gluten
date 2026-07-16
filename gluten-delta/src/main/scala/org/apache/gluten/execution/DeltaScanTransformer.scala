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

import org.apache.gluten.delta.DeltaDeletionVectorScanInfo
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.{DeltaLocalFilesBuilder, LocalFilesNode, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.delta.{DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import scala.collection.JavaConverters._

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

  // For Delta column-mapping tables, `dataFilters` on the scan node are LOGICAL-named so Delta's
  // file index (`PreparedDeltaFileIndex.matchingFiles`, `Snapshot.filesForScan`) can do partition
  // pruning and stats-based file skipping -- both resolve filter attrs against logical schemas.
  //
  // The native (Velox) side, however, must see PHYSICAL names: `output` and `dataSchema` are
  // physical (so the parquet reader finds the right column), and `BasicScanExecTransformer`
  // matches `scanFilters` against `pushDownFilters` (built from a `Filter` that references the
  // physical-named scan output) by `AttributeReference.equals`, which compares names. Without
  // this override, the logical-named `scanFilters` and physical-named `pushDownFilters` would
  // never match, causing duplicate filter evaluation in the substrait plan.
  //
  // Translate by exprId match against `output` rather than by re-running Delta's column-mapping
  // helpers; exprIds are stable across the post-transform rewrite and don't require a second
  // metadata lookup.
  //
  // See `DeltaPostTransformRules.transformColumnMappingPlan` for the full picture of which
  // fields stay logical vs. become physical, and the longer-term cleanup direction (do all
  // physical translation at substrait emission time so this override and the alias-back
  // ProjectExec both go away).
  override lazy val scanFilters: Seq[Expression] = relation.fileFormat match {
    case d: DeltaParquetFileFormat if d.columnMappingMode != NoMapping =>
      val physicalByExprId = output.collect { case ar: AttributeReference => ar.exprId -> ar }.toMap
      dataFilters.map(_.transformDown {
        case ar: AttributeReference => physicalByExprId.getOrElse(ar.exprId, ar)
      })
    case _ => dataFilters
  }

  /**
   * Decorates the generically built split infos with per-file deletion-vector read options so the
   * native Delta scan can apply DV filtering. Delta-specific extraction happens here -- where Delta
   * classes are directly linkable -- rather than in the backend iterator API, mirroring
   * `IcebergScanTransformer`. Splits without any DV keep the generic representation.
   */
  override def getSplitInfosFromPartitions(
      partitions: Seq[(Partition, ReadFileFormat)]): Seq[SplitInfo] = {
    val splitInfos = super.getSplitInfosFromPartitions(partitions)
    val partitionColumnCount = getPartitionSchema.fields.length
    splitInfos.zip(partitions).map {
      case (localFiles: LocalFilesNode, (filePartition: FilePartition, _)) =>
        DeltaDeletionVectorScanInfo
          .normalize(partitionColumnCount, filePartition.files.toSeq)
          .map {
            case (otherMetadataColumns, deltaReadOptions) =>
              DeltaLocalFilesBuilder.makeDeltaLocalFiles(
                localFiles,
                otherMetadataColumns.asJava,
                deltaReadOptions.asJava): SplitInfo
          }
          .getOrElse(localFiles)
      case (splitInfo, _) => splitInfo
    }
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
