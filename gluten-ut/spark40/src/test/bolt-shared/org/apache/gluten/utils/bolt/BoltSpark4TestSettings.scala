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

import org.apache.gluten.utils.{BackendTestSettings, SQLQueryTestSettings}
import org.apache.gluten.utils.velox.VeloxSQLQueryTestSettings

import java.nio.file.Paths

/**
 * Spark regression inventory shared by 4.0, 4.1 and 4.2. Version-specific suites are registered in
 * each module's BoltTestSettings. Keep this aligned with the common Velox suite inventory, but do
 * not inherit Velox result exclusions or modified SQL inputs/goldens. Newly exposed Bolt
 * compatibility failures must be reported by nightly CI, not silently filtered here.
 */
abstract class BoltSpark4TestSettings extends BackendTestSettings {
  enableSuite[org.apache.spark.GlutenSortShuffleSuite]
  enableSuite[org.apache.spark.sql.GlutenApproxCountDistinctForIntervalsQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenApproximatePercentileQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenBitmapExpressionsQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenBloomFilterAggregateQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenBloomFilterAggregateQuerySuiteCGOff]
  enableSuite[org.apache.spark.sql.GlutenBoltBackendSmokeSuite]
  enableSuite[org.apache.spark.sql.GlutenCTEHintSuite]
  enableSuite[org.apache.spark.sql.GlutenCTEInlineSuiteAEOff]
  enableSuite[org.apache.spark.sql.GlutenCTEInlineSuiteAEOn]
  enableSuite[org.apache.spark.sql.GlutenCacheManagerSuite]
  enableSuite[org.apache.spark.sql.GlutenCachedTableSuite]
  enableSuite[org.apache.spark.sql.GlutenColumnExpressionSuite]
  enableSuite[org.apache.spark.sql.GlutenConfigBehaviorSuite]
  enableSuite[org.apache.spark.sql.GlutenCountMinSketchAggQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenCsvFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenDSV2CharVarcharTestSuite]
  enableSuite[org.apache.spark.sql.GlutenDSV2SQLInsertTestSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameAggregateSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameAsOfJoinSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameComplexTypeSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameHintSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameImplicitsSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameJoinSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameNaFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFramePivotSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameRangeSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameSelfJoinSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameSessionWindowingSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameSetOperationsSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameShowSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameStatSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameSubquerySuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameTableValuedFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameTimeWindowingSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameToSchemaSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameTransposeSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameTungstenSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameWindowFramesSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameWindowFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenDataFrameWriterV2Suite]
  enableSuite[org.apache.spark.sql.GlutenDatasetAggregatorSuite]
  enableSuite[org.apache.spark.sql.GlutenDatasetCacheSuite]
  enableSuite[org.apache.spark.sql.GlutenDatasetOptimizationSuite]
  enableSuite[org.apache.spark.sql.GlutenDatasetPrimitiveSuite]
  enableSuite[org.apache.spark.sql.GlutenDatasetSerializerRegistratorSuite]
  enableSuite[org.apache.spark.sql.GlutenDatasetSuite]
  enableSuite[org.apache.spark.sql.GlutenDatasetUnpivotSuite]
  enableSuite[org.apache.spark.sql.GlutenDateFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenDeprecatedAPISuite]
  enableSuite[org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV1SuiteAEOff]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV1SuiteAEOffDisableScan]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV1SuiteAEOffWSCGOffDisableProject]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV1SuiteAEOffWSCGOnDisableProject]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV1SuiteAEOn]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV1SuiteAEOnDisableScan]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV2SuiteAEOff]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV2SuiteAEOffDisableScan]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV2SuiteAEOffWSCGOffDisableProject]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV2SuiteAEOffWSCGOnDisableProject]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV2SuiteAEOn]
  enableSuite[org.apache.spark.sql.GlutenDynamicPartitionPruningV2SuiteAEOnDisableScan]
  enableSuite[org.apache.spark.sql.GlutenEmptyInSuite]
  enableSuite[org.apache.spark.sql.GlutenExpressionsSchemaSuite]
  enableSuite[org.apache.spark.sql.GlutenExtraStrategiesSuite]
  enableSuite[org.apache.spark.sql.GlutenFileBasedDataSourceSuite]
  enableSuite[org.apache.spark.sql.GlutenFileScanSuite]
  enableSuite[org.apache.spark.sql.GlutenFileSourceCharVarcharTestSuite]
  enableSuite[org.apache.spark.sql.GlutenFileSourceSQLInsertTestSuite]
  enableSuite[org.apache.spark.sql.GlutenGeneratorFunctionSuite]
  enableSuite[org.apache.spark.sql.GlutenICUCollationsMapSuite]
  enableSuite[org.apache.spark.sql.GlutenInjectRuntimeFilterSuite]
  enableSuite[org.apache.spark.sql.GlutenInlineTableParsingImprovementsSuite]
  enableSuite[org.apache.spark.sql.GlutenIntervalFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenJoinHintSuite]
  enableSuite[org.apache.spark.sql.GlutenJoinSuite]
  enableSuite[org.apache.spark.sql.GlutenJsonFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenLateralColumnAliasSuite]
  enableSuite[org.apache.spark.sql.GlutenLogQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenMathFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenMetadataCacheSuite]
  enableSuite[org.apache.spark.sql.GlutenMiscFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenNestedDataSourceV1Suite]
  enableSuite[org.apache.spark.sql.GlutenNestedDataSourceV2Suite]
  enableSuite[org.apache.spark.sql.GlutenParametersSuite]
  enableSuite[org.apache.spark.sql.GlutenPercentileQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenProcessingTimeSuite]
  enableSuite[org.apache.spark.sql.GlutenProductAggSuite]
  enableSuite[org.apache.spark.sql.GlutenRandomDataGeneratorSuite]
  enableSuite[org.apache.spark.sql.GlutenReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[org.apache.spark.sql.GlutenResolveDefaultColumnsSuite]
  enableSuite[org.apache.spark.sql.GlutenRowJsonSuite]
  enableSuite[org.apache.spark.sql.GlutenRowSuite]
  enableSuite[org.apache.spark.sql.GlutenRuntimeConfigSuite]
  enableSuite[org.apache.spark.sql.GlutenRuntimeNullChecksV2Writes]
  enableSuite[org.apache.spark.sql.GlutenSQLQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenSQLQueryTestSuite]
  enableSuite[org.apache.spark.sql.GlutenSSBQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenScalaReflectionRelationSuite]
  enableSuite[org.apache.spark.sql.GlutenSerializationSuite]
  enableSuite[org.apache.spark.sql.GlutenSessionStateSuite]
  enableSuite[org.apache.spark.sql.GlutenSetCommandSuite]
  enableSuite[org.apache.spark.sql.GlutenSparkSessionBuilderSuite]
  enableSuite[org.apache.spark.sql.GlutenSparkSessionExtensionSuite]
  enableSuite[org.apache.spark.sql.GlutenSparkSessionJobTaggingAndCancellationSuite]
  enableSuite[org.apache.spark.sql.GlutenStatisticsCollectionSuite]
  enableSuite[org.apache.spark.sql.GlutenStringFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenSubqueryHintPropagationSuite]
  enableSuite[org.apache.spark.sql.GlutenSubquerySuite]
  enableSuite[org.apache.spark.sql.GlutenTPCDSCollationQueryTestSuite]
  enableSuite[org.apache.spark.sql.GlutenTPCDSQueryANSISuite]
  enableSuite[org.apache.spark.sql.GlutenTPCDSQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenTPCDSQueryTestSuite]
  enableSuite[org.apache.spark.sql.GlutenTPCDSQueryWithStatsSuite]
  enableSuite[org.apache.spark.sql.GlutenTPCHQuerySuite]
  enableSuite[org.apache.spark.sql.GlutenTableOptionsConstantFoldingSuite]
  enableSuite[org.apache.spark.sql.GlutenTypedImperativeAggregateSuite]
  enableSuite[org.apache.spark.sql.GlutenUDFSuite]
  enableSuite[org.apache.spark.sql.GlutenUDTRegistrationSuite]
  enableSuite[org.apache.spark.sql.GlutenUnsafeRowSuite]
  enableSuite[org.apache.spark.sql.GlutenUnwrapCastInComparisonEndToEndSuite]
  enableSuite[org.apache.spark.sql.GlutenUrlFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenUserDefinedTypeSuite]
  enableSuite[org.apache.spark.sql.GlutenVariantEndToEndSuite]
  enableSuite[org.apache.spark.sql.GlutenVariantShreddingSuite]
  enableSuite[org.apache.spark.sql.GlutenVariantSuite]
  enableSuite[org.apache.spark.sql.GlutenVariantWriteShreddingSuite]
  enableSuite[org.apache.spark.sql.GlutenXPathFunctionsSuite]
  enableSuite[org.apache.spark.sql.GlutenXmlFunctionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenArithmeticExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenAttributeResolutionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenAttributeSetSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenBitmapExpressionUtilsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenBitwiseExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCallMethodViaReflectionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCanonicalizeSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCastWithAnsiOffSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCodeGenerationSuite]
  enableSuite[
    org.apache.spark.sql.catalyst.expressions.GlutenCodeGeneratorWithInterpretedFallbackSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCollationExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCollectionExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenComplexTypeSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenConditionalExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCsvExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenDateExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenDecimalExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenDecimalPrecisionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenDynamicPruningSubquerySuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenExprIdSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenExpressionImplUtilsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenExpressionSQLBuilderSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenExpressionSetSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenExtractPredicatesWithinOutputSetSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenGeneratorExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenHashExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenHexSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenHigherOrderFunctionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenIntervalExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenJsonExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenLiteralExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenMathExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenMiscExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenMutableProjectionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenNamedExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenNondeterministicSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenNullExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenObjectExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenOrderingSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenPredicateSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenRandomSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenRegexpExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenScalaUDFSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenSchemaPruningSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenSelectedFieldSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenSortOrderExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenStringExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenSubexpressionEliminationSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenTimeWindowSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenToPrettyStringSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenTryCastSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenTryEvalSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenUnsafeRowConverterSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenUnwrapUDTExpressionSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenV2ExpressionUtilsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenValidateExternalTypeSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenXmlExpressionsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenAggregateExpressionSuite]
  enableSuite[
    org.apache.spark.sql.catalyst.expressions.aggregate.GlutenApproxCountDistinctForIntervalsSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenApproximatePercentileSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenCentralMomentAggSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenCountMinSketchAggSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenCovarianceAggSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenDatasketchesHllSketchSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenFirstLastTestSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenHistogramNumericSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenHyperLogLogPlusPlusSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.aggregate.GlutenProductAggSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2DataFrameSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2FunctionSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2MetricsSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2OptionSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2SQLSuiteV1Filter]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2SQLSuiteV2Filter]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2Suite]
  enableSuite[org.apache.spark.sql.connector.GlutenDataSourceV2UtilsSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDeleteFromTableSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDeltaBasedDeleteFromTableSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDeltaBasedMergeIntoTableSuite]
  enableSuite[
    org.apache.spark.sql.connector.GlutenDeltaBasedMergeIntoTableUpdateAsDeleteAndInsertSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDeltaBasedUpdateAsDeleteAndInsertTableSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenDeltaBasedUpdateTableSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenFileDataSourceV2FallBackSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenGroupBasedDeleteFromTableSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenGroupBasedMergeIntoTableSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenGroupBasedUpdateTableSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenKeyGroupedPartitioningSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenLocalScanSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenMergeIntoDataFrameSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenMetadataColumnSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenProcedureSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenPushablePredicateSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenSupportsCatalogOptionsSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenTableCapabilityCheckSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenV1ReadFallbackWithCatalogSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenV1ReadFallbackWithDataFrameReaderSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenV1WriteFallbackSessionCatalogSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenV1WriteFallbackSuite]
  enableSuite[org.apache.spark.sql.connector.GlutenV2CommandsCaseSensitivitySuite]
  enableSuite[org.apache.spark.sql.connector.GlutenWriteDistributionAndOrderingSuite]
  enableSuite[org.apache.spark.sql.errors.GlutenQueryCompilationErrorsDSv2Suite]
  enableSuite[org.apache.spark.sql.errors.GlutenQueryCompilationErrorsSuite]
  enableSuite[org.apache.spark.sql.errors.GlutenQueryContextSuite]
  enableSuite[org.apache.spark.sql.errors.GlutenQueryExecutionAnsiErrorsSuite]
  enableSuite[org.apache.spark.sql.errors.GlutenQueryExecutionErrorsSuite]
  enableSuite[org.apache.spark.sql.errors.GlutenQueryParsingErrorsSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenAggregatingAccumulatorSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenBroadcastExchangeSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenCoGroupedIteratorSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenCoalesceShufflePartitionsSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenColumnarRulesSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenDataSourceScanExecRedactionSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenDataSourceV2ScanExecRedactionSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenExchangeSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenExecuteImmediateEndToEndSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenExternalAppendOnlyUnsafeRowArraySuite]
  enableSuite[org.apache.spark.sql.execution.GlutenGlobalTempViewSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenGlobalTempViewTestSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenGroupedIteratorSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenHiveResultSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenInsertSortForLimitAndOffsetSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenLocalBroadcastExchangeSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenLocalTempViewTestSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenLogicalPlanTagInSparkPlanSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenOptimizeMetadataOnlyQuerySuite]
  enableSuite[org.apache.spark.sql.execution.GlutenPersistedViewTestSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenQueryExecutionSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenQueryPlanningTrackerEndToEndSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenRemoveRedundantProjectsSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenRemoveRedundantSortsSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenRemoveRedundantWindowGroupLimitsSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenReplaceHashWithSortAggSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenReuseExchangeAndSubquerySuite]
  enableSuite[org.apache.spark.sql.execution.GlutenRowToColumnConverterSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSQLAggregateFunctionSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSQLCollectLimitExecSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSQLExecutionSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSQLFunctionSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSQLJsonProtocolSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSQLWindowFunctionSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSameResultSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenShufflePartitionsUtilSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSimpleSQLViewSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSortSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSparkPlanSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSparkPlannerSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSparkScriptTransformationSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenSparkSqlParserSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenTakeOrderedAndProjectSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenUnsafeFixedWidthAggregationMapSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenUnsafeKVExternalSorterSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenUnsafeRowSerializerSuite]
  enableSuite[org.apache.spark.sql.execution.GlutenWholeStageCodegenSuite]
    // Rewrite with Gluten-aware native whole-stage plan assertions.
    .exclude("range/filter should be combined")
    .exclude("HashAggregate should be included in WholeStageCodegen")
    .exclude("SortAggregate should be included in WholeStageCodegen")
    .exclude("GenerateExec should be included in WholeStageCodegen (whole-stage-codegen on)")
    .exclude("HashAggregate with grouping keys should be included in WholeStageCodegen")
    .exclude("BroadcastHashJoin should be included in WholeStageCodegen")
    .exclude("Inner ShuffledHashJoin should be included in WholeStageCodegen")
    .exclude(
      "Full Outer ShuffledHashJoin and SortMergeJoin should be included in WholeStageCodegen")
    .exclude("SPARK-44060 Code-gen for build side outer shuffled hash join")
    .exclude("Left/Right Outer SortMergeJoin should be included in WholeStageCodegen")
    .exclude("Left Semi SortMergeJoin should be included in WholeStageCodegen")
    .exclude("Left Anti SortMergeJoin should be included in WholeStageCodegen")
    .exclude("Inner/Cross BroadcastNestedLoopJoinExec should be included in WholeStageCodegen")
    .exclude("Left/Right outer BroadcastNestedLoopJoinExec should be included in WholeStageCodegen")
    .exclude("Left semi/anti BroadcastNestedLoopJoinExec should be included in WholeStageCodegen")
    .exclude("Sort should be included in WholeStageCodegen")
    .exclude("Control splitting consume function by operators with config")
    .exclude("Skip splitting consume function when parameter number exceeds JVM limit")
    .exclude(
      "including codegen stage ID in generated class name should not regress codegen caching")
    .exclude("SPARK-26572: evaluate non-deterministic expressions for aggregate results")
    .exclude("SPARK-28520: WholeStageCodegen does not work properly for LocalTableScanExec")
    .exclude("Give up splitting aggregate code if a parameter length goes over the limit")
    .exclude("Give up splitting subexpression code if a parameter length goes over the limit")
    .exclude("SPARK-47238: Test broadcast threshold for generated code")
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenBasicWriteJobStatsTrackerMetricSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenBasicWriteTaskStatsTrackerSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenBucketingUtilsSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenCSVReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenCustomWriteTaskStatsTrackerSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenDataSourceManagerSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenDataSourceResolverSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenDataSourceStrategySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenDataSourceSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenFileFormatWriterSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenFileIndexSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenFileMetadataStructSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenFileResolverSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenFileSourceCustomMetadataStructSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenFileSourceStrategySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenHadoopFileLinesReaderSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenHeaderCSVReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenInMemoryTableMetricSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenJsonReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenMergedOrcReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenMergedParquetReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenOrcCodecSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenOrcReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenOrcV1AggregatePushDownSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenOrcV2AggregatePushDownSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenParquetCodecSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenParquetReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenParquetV1AggregatePushDownSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenParquetV2AggregatePushDownSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenPathFilterStrategySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenPathFilterSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenPruneFileSourcePartitionsSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenPushVariantIntoScanSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenRowDataSourceStrategySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenSaveIntoDataSourceCommandSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenTableLocationSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenV1WriteCommandSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenVectorizedOrcReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.GlutenVectorizedParquetReadSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.binaryfile.GlutenBinaryFileFormatSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.csv.GlutenCSVLegacyTimeParserSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.csv.GlutenCSVParsingOptionsSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.csv.GlutenCSVv1Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.csv.GlutenCSVv2Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.json.GlutenJsonLegacyTimeParserSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.json.GlutenJsonParsingOptionsSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.json.GlutenJsonV1Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.json.GlutenJsonV2Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcColumnarBatchReaderSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcEncryptionSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcFilterSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcPartitionDiscoverySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcSourceSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcV1FilterSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcV1PartitionDiscoverySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcV1QuerySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcV1SchemaPruningSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcV2QuerySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.orc.GlutenOrcV2SchemaPruningSuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetAvroCompatibilitySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetColumnIndexSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetCommitterSuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetCompressionCodecPrecedenceSuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetDeltaByteArrayEncodingSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetDeltaEncodingInteger]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetDeltaEncodingLong]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetDeltaLengthByteArrayEncodingSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetEncodingSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetFieldIdIOSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetFieldIdSchemaSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetFileFormatV1Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetFileFormatV2Suite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetFileMetadataStructRowIndexSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetIOSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetInteroperabilitySuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetProtobufCompatibilitySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetRebaseDatetimeV1Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetRebaseDatetimeV2Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetRowIndexSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetSchemaInferenceSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetSchemaSuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetThriftCompatibilitySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetTypeWideningSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV1FilterSuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV1PartitionDiscoverySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV1QuerySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV1SchemaPruningSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV2FilterSuite]
  enableSuite[
    org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV2PartitionDiscoverySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV2QuerySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetV2SchemaPruningSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetVariantShreddingSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.parquet.GlutenParquetVectorizedSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.text.GlutenTextV1Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.text.GlutenTextV2Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.text.GlutenWholeTextFileV1Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.text.GlutenWholeTextFileV2Suite]
  enableSuite[org.apache.spark.sql.execution.datasources.v2.GlutenDataSourceV2StrategySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.v2.GlutenFileTableSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.v2.GlutenFileWriterFactorySuite]
  enableSuite[org.apache.spark.sql.execution.datasources.v2.GlutenV2PredicateSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.v2.GlutenV2SessionCatalogNamespaceSuite]
  enableSuite[org.apache.spark.sql.execution.datasources.v2.GlutenV2SessionCatalogTableSuite]
  enableSuite[org.apache.spark.sql.execution.exchange.GlutenEnsureRequirementsSuite]
  enableSuite[org.apache.spark.sql.execution.exchange.GlutenValidateRequirementsSuite]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenBroadcastJoinSuite]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenExistenceJoinSuite]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenInnerJoinSuiteForceShjOff]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenInnerJoinSuiteForceShjOn]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenOuterJoinSuiteForceShjOff]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenOuterJoinSuiteForceShjOn]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenSingleJoinSuite]
  enableSuite[org.apache.spark.sql.execution.metric.GlutenCustomMetricsSuite]
  enableSuite[org.apache.spark.sql.execution.metric.GlutenSQLMetricsSuite]
  enableSuite[org.apache.spark.sql.execution.python.GlutenBatchEvalPythonExecSuite]
  enableSuite[org.apache.spark.sql.execution.python.GlutenExtractPythonUDFsSuite]
  enableSuite[org.apache.spark.sql.extension.GlutenCollapseProjectExecTransformerSuite]
  enableSuite[org.apache.spark.sql.extension.GlutenSessionExtensionSuite]
  enableSuite[org.apache.spark.sql.gluten.GlutenFallbackStrategiesSuite]
  enableSuite[org.apache.spark.sql.gluten.GlutenFallbackSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHashAggregationQuerySuite]
  enableSuite[
    org.apache.spark.sql.hive.execution.GlutenHashAggregationQueryWithControlledFallbackSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHashUDAQuerySuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHashUDAQueryWithControlledFallbackSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveCommandSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveDDLSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveExplainSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHivePlanTest]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveQuerySuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveResolutionSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveSQLQuerySuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveSQLViewSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveScriptTransformationSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveSerDeReadWriteSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveSerDeSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveTableScanSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveTypeCoercionSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveUDAFSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenHiveUDFSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenObjectHashAggregateSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenPruneHiveTablePartitionsSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenPruningSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenSQLMetricsSuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenSQLQuerySuite]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenSQLQuerySuiteAE]
  enableSuite[org.apache.spark.sql.hive.execution.GlutenWindowQuerySuite]
  enableSuite[org.apache.spark.sql.sources.GlutenBucketedReadWithoutHiveSupportSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenBucketedWriteWithoutHiveSupportSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenCreateTableAsSelectSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenDDLSourceLoadSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenDataSourceAnalysisSuite]
  enableSuite[
    org.apache.spark.sql.sources.GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE]
  enableSuite[org.apache.spark.sql.sources.GlutenExternalCommandRunnerSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenFilteredScanSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenFiltersSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenInsertSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenPartitionedWriteSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenPathOptionSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenPrunedScanSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenResolvedDataSourceSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenSaveLoadSuite]
  enableSuite[org.apache.spark.sql.sources.GlutenTableScanSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenAcceptsLatestSeenOffsetSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenCommitLogSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenEventTimeWatermarkSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFileStreamSinkV1Suite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFileStreamSinkV2Suite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFileStreamSourceStressTestSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFileStreamSourceSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFileStreamStressSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFlatMapGroupsInPandasWithStateDistributionSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFlatMapGroupsInPandasWithStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFlatMapGroupsWithStateDistributionSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFlatMapGroupsWithStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenFlatMapGroupsWithStateWithInitialStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenGroupStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenLongOffsetSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenMemorySourceStressSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenMultiStatefulOperatorsSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenReportSinkMetricsSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenRocksDBStateStoreFlatMapGroupsWithStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenRocksDBStateStoreStreamingAggregationSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenRocksDBStateStoreStreamingDeduplicationSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingAggregationDistributionSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingAggregationSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingDeduplicationDistributionSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingDeduplicationSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingDeduplicationWithinWatermarkSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingQueryHashPartitionVerifySuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingQueryListenerSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingQueryListenersConfSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingQueryManagerSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingQueryOptimizationCorrectnessSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingQueryStatusAndProgressSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingQuerySuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingSelfUnionSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingSessionWindowDistributionSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingSessionWindowSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingStateStoreFormatCompatibilitySuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingSymmetricHashJoinHelperSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithListStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithListStateTTLSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithMapStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithMapStateTTLSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithStateAvroSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithStateChainingSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithStateClusterSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithStateInitialStateSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithStateUnsafeRowSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithStateValidationSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTransformWithValueStateTTLSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenTriggerAvailableNowSuite]

  // Explicit implementation/harness boundaries; these are not native result failures.
  disableSuite[org.apache.spark.sql.GlutenExplainSuite](
    "Validates Spark-specific physical plans and JVM codegen output, neither of which applies to Gluten's native execution plan")
  disableSuite[org.apache.spark.sql.GlutenTPCDSModifiedPlanStabilitySuite](
    "Requires backend-specific approved plan files; Bolt goldens are not available.")
  disableSuite[org.apache.spark.sql.GlutenTPCDSModifiedPlanStabilityWithStatsSuite](
    "Requires backend-specific approved plan files; Bolt goldens are not available.")
  disableSuite[org.apache.spark.sql.GlutenTPCDSV1_4_PlanStabilitySuite](
    "Requires backend-specific approved plan files; Bolt goldens are not available.")
  disableSuite[org.apache.spark.sql.GlutenTPCDSV1_4_PlanStabilityWithStatsSuite](
    "Requires backend-specific approved plan files; Bolt goldens are not available.")
  disableSuite[org.apache.spark.sql.GlutenTPCDSV2_7_PlanStabilitySuite](
    "Requires backend-specific approved plan files; Bolt goldens are not available.")
  disableSuite[org.apache.spark.sql.GlutenTPCDSV2_7_PlanStabilityWithStatsSuite](
    "Requires backend-specific approved plan files; Bolt goldens are not available.")
  disableSuite[org.apache.spark.sql.GlutenTPCHPlanStabilitySuite](
    "Requires backend-specific approved plan files; Bolt goldens are not available.")
  disableSuite[org.apache.spark.sql.catalyst.expressions.GlutenExpressionEvalHelperSuite](
    "Validates Spark's ExpressionEvalHelper contract, while Gluten overrides checkEvaluation/checkExceptionInExpression")
  disableSuite[org.apache.spark.sql.catalyst.expressions.GlutenSubExprEvaluationRuntimeSuite](
    "Spark's test JAR uses unshaded Guava, while SubExprEvaluationRuntime uses shaded Guava")
  disableSuite[org.apache.spark.sql.execution.GlutenPlannerSuite](
    "Validates Spark planner implementation details")
  disableSuite[org.apache.spark.sql.execution.GlutenProjectedOrderingAndPartitioningSuite](
    "Validates Spark planner output ordering and partitioning metadata")
  disableSuite[org.apache.spark.sql.execution.GlutenWholeStageCodegenSparkSubmitSuite](
    "The SparkSubmit test launches Spark's main class without the Gluten plugin")
  disableSuite(
    "org.apache.spark.sql.execution.adaptive.velox.VeloxAdaptiveQueryExecSuite",
    "Tests Velox-specific adaptive operators; no Spark 4 Bolt counterpart exists."
  )
  disableSuite[
    org.apache.spark.sql.sources.GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite](
    "GLUTEN-4893: Vanilla UT checks scan operator by exactly matching the class type")

  override def getSQLQueryTestSettings: SQLQueryTestSettings = BoltSQLQueryTestSettings
}

private object BoltSQLQueryTestSettings extends SQLQueryTestSettings {
  // Reuse only each Spark version's case inventory, including cases Velox rewrites. Execute
  // Spark's original input/output pair: Velox-specific expected results are not Bolt's contract.
  override def getResourceFilePath: String = {
    val home = sys.props.get("spark.test.home").orElse(sys.env.get("SPARK_HOME")).getOrElse(".")
    Paths.get(home, "sql/core/src/test/resources/sql-tests").toString
  }
  override def getSupportedSQLQueryTests: Set[String] = {
    val inventory = VeloxSQLQueryTestSettings.getSupportedSQLQueryTests ++
      VeloxSQLQueryTestSettings.getOverwriteSQLQueryTests ++
      Set("bitwise.sql", "conditional-functions.sql", "count.sql", "null-handling.sql")
    inventory.map {
      // These two Velox copies predate Spark 4's default ANSI mode. Use the release originals.
      case "ansi/conditional-functions.sql" => "conditional-functions.sql"
      case "ansi/decimalArithmeticOperations.sql" => "decimalArithmeticOperations.sql"
      // SQLQueryTestSuite registers UDF cases with a language suffix, not the bare filename.
      case name if name.startsWith("udf/") && !name.contains(" - ") => name + " - Scala UDF"
      case name => name
    } - "ignored.sql" // Spark's deliberate ignore-functionality fixture has no golden file.
  }
  override def getOverwriteSQLQueryTests: Set[String] = Set.empty
}
