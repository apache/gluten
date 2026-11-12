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
package org.apache.gluten.utils.bolt

// Suites added or renamed in this Spark version, in addition to the shared regression inventory.
class BoltTestSettings extends BoltSpark4TestSettings {
  enableSuite[org.apache.spark.sql.GlutenAddMetadataColumnsSuite]
  enableSuite[org.apache.spark.sql.GlutenAlwaysPersistedConfigsSuite]
  enableSuite[org.apache.spark.sql.GlutenApproxTopKSuite]
  enableSuite[org.apache.spark.sql.GlutenCacheTableInKryoSuite]
  enableSuite[org.apache.spark.sql.GlutenGeographyDataFrameSuite]
  enableSuite[org.apache.spark.sql.GlutenGeometryDataFrameSuite]
  enableSuite[org.apache.spark.sql.GlutenLegacyParameterSubstitutionSuite]
  enableSuite[org.apache.spark.sql.GlutenMapStatusEndToEndSuite]
  enableSuite[org.apache.spark.sql.GlutenReplaceIntegerLiteralsWithOrdinalsDataframeSuite]
  enableSuite[org.apache.spark.sql.GlutenReplaceIntegerLiteralsWithOrdinalsSqlSuite]
  enableSuite[org.apache.spark.sql.GlutenSTExpressionsSuite]
  enableSuite[org.apache.spark.sql.GlutenSTFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenStringLiteralCoalescingSuite]
  enableSuite[org.apache.spark.sql.GlutenTimestampNtzAggregateSuite]
  enableSuite[org.apache.spark.sql.GlutenTimestampNtzLiteralSuite]
  enableSuite[org.apache.spark.sql.GlutenUnsafeRowChecksumSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenAttributeMapSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenBindReferencesSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenConstraintExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenTimeExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenApproxTopKSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenThetasketchesAggSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2EnhancedDeleteFilterSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2EnhancedPartitionFilterSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2EnhancedRuntimePartitionFilterSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenShowNamespacesParserSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenVariantInferShreddingSuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.v2.GlutenDSV2JoinPushDownAliasGenerationSuite]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenHashedRelationOffHeapSuite]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenHashedRelationOnHeapSuite]
  enableSuite[org.apache.spark.sql.execution.python.GlutenPythonWorkerLogsSuite]
  enableSuite[org.apache.spark.sql.gluten.GlutenRowBasedChecksumSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamRealTimeModeAllowlistSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamRealTimeModeE2ESuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamRealTimeModeSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingFullOuterWithVCFSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingFullOuterWithoutVCFSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingInnerWithVCFSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingInnerWithoutVCFSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingLeftSemiWithVCFSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingLeftSemiWithoutVCFSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingOuterWithVCFSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingOuterWithoutVCFSuite]
}
