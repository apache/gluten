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
package org.apache.gluten.backendsapi

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.hadoop.conf.Configuration

trait BackendSettingsApi {

  /** The default columnar-batch type of this backend. */
  def primaryBatchType: Convention.BatchType

  def validateScanExec(
      format: ReadFileFormat,
      fields: Array[StructField], // the fields to be output
      dataSchema: StructType, // the schema of the table
      rootPaths: Seq[String],
      properties: Map[String, String],
      hadoopConf: Configuration,
      partitionFileFormats: Set[ReadFileFormat]): ValidationResult =
    ValidationResult.succeeded

  def getSubstraitReadFileFormatV1(fileFormat: FileFormat): LocalFilesNode.ReadFileFormat

  def getSubstraitReadFileFormatV2(scan: Scan): LocalFilesNode.ReadFileFormat

  def supportWriteFilesExec(
      format: FileFormat,
      fields: Array[StructField],
      bucketSpec: Option[BucketSpec],
      isPartitionedTable: Boolean,
      options: Map[String, String]): ValidationResult = ValidationResult.succeeded

  def supportNativeWrite(fields: Array[StructField]): Boolean = true

  def supportNativeMetadataColumns(): Boolean = true

  def supportNativeRowIndexColumn(): Boolean = true

  def supportExpandExec(): Boolean = false

  def supportSortExec(): Boolean = false

  def supportSortMergeJoinExec(): Boolean = true

  def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    false
  }

  def supportWindowGroupLimitExec(rankLikeFunction: Expression): Boolean = {
    false
  }

  def supportColumnarShuffleExec(): Boolean = {
    GlutenConfig.get.enableColumnarShuffle
  }

  def enableJoinKeysRewrite(): Boolean = true

  def enableHashTableBuildOncePerExecutor(): Boolean = true

  def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    case _: InnerLike | RightOuter | FullOuter => true
    case _ => false
  }

  def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    case _: InnerLike | LeftOuter | FullOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    // LeftSingle is a Spark 4.0+ join type with same semantics as LeftOuter for build side.
    case leftSingle if SparkShimLoader.getSparkShims.isLeftSingleJoinType(leftSingle) => true
    case _ => false
  }

  def supportStructType(): Boolean = false

  def structFieldToLowerCase(): Boolean = true

  // Whether to fallback aggregate at the same time if its empty-output child is fallen back.
  def fallbackAggregateWithEmptyOutputChild(): Boolean = false

  def recreateJoinExecOnFallback(): Boolean = false

  def excludeScanExecFromCollapsedStage(): Boolean = false

  def rescaleDecimalArithmetic: Boolean = false

  /**
   * After https://github.com/apache/spark/pull/36698, every arithmetic should report the accurate
   * result decimal type and implement `CheckOverflow` by itself. <p/> Regardless of whether there
   * is 36698 or not, this option is used to indicate whether to transform `CheckOverflow`. `false`
   * means the backend will implement `CheckOverflow` by default and no need to transform it.
   */
  def transformCheckOverflow: Boolean = true

  def shuffleSupportedCodec(): Set[String]

  def needOutputSchemaForPlan(): Boolean = false

  def insertPostProjectForGenerate(): Boolean = false

  def skipNativeCtas(ctas: CreateDataSourceTableAsSelectCommand): Boolean = false

  def skipNativeInsertInto(insertInto: InsertIntoHadoopFsRelationCommand): Boolean = false

  def alwaysFailOnMapExpression(): Boolean = false

  def requiredChildOrderingForWindowGroupLimit(): Boolean = true

  def staticPartitionWriteOnly(): Boolean = false

  // TODO: Move this to test settings as used in UT only.
  def requireBloomFilterAggMightContainJointFallback(): Boolean = true

  def enableNativeWriteFiles(): Boolean

  def enableNativeArrowReadFiles(): Boolean = false

  def shouldRewriteCount(): Boolean = false

  def supportCartesianProductExec(): Boolean = false

  def supportCartesianProductExecWithCondition(): Boolean = true

  def supportSampleExec(): Boolean = false

  def supportColumnarArrowUdf(): Boolean = false

  def needPreComputeRangeFrameBoundary(): Boolean = false

  def supportIcebergEqualityDeleteRead(): Boolean = true

  def reorderColumnsForPartitionWrite(): Boolean = false

  def enableEnhancedFeatures(): Boolean = false

  def supportAppendDataExec(): Boolean = false

  def supportReplaceDataExec(): Boolean = false

  def supportOverwriteByExpression(): Boolean = false

  def supportOverwritePartitionsDynamic(): Boolean = false

  def supportWriteToDataSourceV2(): Boolean = false

  /** Whether the backend supports columnar shuffle with empty schema. */
  def supportEmptySchemaColumnarShuffle(): Boolean = true
}
