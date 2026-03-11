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
package org.apache.gluten.utils.velox

import org.apache.gluten.utils.{BackendTestSettings, SQLQueryTestSettings}

import org.apache.spark.GlutenSortShuffleSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector._
import org.apache.spark.sql.errors.{GlutenQueryCompilationErrorsDSv2Suite, GlutenQueryCompilationErrorsSuite, GlutenQueryExecutionErrorsSuite, GlutenQueryParsingErrorsSuite}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.velox.VeloxAdaptiveQueryExecSuite
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.binaryfile.GlutenBinaryFileFormatSuite
import org.apache.spark.sql.execution.datasources.csv.{GlutenCSVLegacyTimeParserSuite, GlutenCSVv1Suite, GlutenCSVv2Suite}
import org.apache.spark.sql.execution.datasources.exchange.GlutenValidateRequirementsSuite
import org.apache.spark.sql.execution.datasources.json.{GlutenJsonLegacyTimeParserSuite, GlutenJsonV1Suite, GlutenJsonV2Suite}
import org.apache.spark.sql.execution.datasources.orc._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.text.{GlutenTextV1Suite, GlutenTextV2Suite}
import org.apache.spark.sql.execution.datasources.v2.{GlutenDataSourceV2StrategySuite, GlutenFileTableSuite, GlutenV2PredicateSuite}
import org.apache.spark.sql.execution.exchange.GlutenEnsureRequirementsSuite
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.extension.{GlutenCollapseProjectExecTransformerSuite, GlutenSessionExtensionSuite, TestFileSourceScanExecTransformer}
import org.apache.spark.sql.gluten.GlutenFallbackSuite
import org.apache.spark.sql.hive.execution.GlutenHiveSQLQuerySuite
import org.apache.spark.sql.sources._

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class VeloxTestSettings extends BackendTestSettings {
  enableSuite[GlutenStringFunctionsSuite]
  enableSuite[GlutenBloomFilterAggregateQuerySuite]
  enableSuite[GlutenBloomFilterAggregateQuerySuiteCGOff]
  enableSuite[GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2DataFrameSuite]
  enableSuite[GlutenDataSourceV2FunctionSuite]
  enableSuite[GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSuiteV1Filter]
  enableSuite[GlutenDataSourceV2SQLSuiteV2Filter]
  enableSuite[GlutenDataSourceV2Suite]
    // Rewrite the following tests in GlutenDataSourceV2Suite.
    .exclude("partitioning reporting")
    .exclude("ordering and partitioning reporting")
  enableSuite[GlutenDeleteFromTableSuite]
  enableSuite[GlutenFileDataSourceV2FallBackSuite]
    // Rewritten
    .exclude("Fallback Parquet V2 to V1")
  enableSuite[GlutenKeyGroupedPartitioningSuite]
    // NEW SUITE: disable as they check vanilla spark plan
    .exclude("partitioned join: number of buckets mismatch should trigger shuffle")
    .exclude("partitioned join: only one side reports partitioning")
    .exclude("partitioned join: join with two partition keys and different # of partition keys")
    // disable due to check for SMJ node
    .excludeByPrefix("SPARK-41413: partitioned join:")
    .excludeByPrefix("SPARK-42038: partially clustered:")
    .exclude("SPARK-44641: duplicated records when SPJ is not triggered")
  enableSuite[GlutenLocalScanSuite]
  enableSuite[GlutenMetadataColumnSuite]
  enableSuite[GlutenSupportsCatalogOptionsSuite]
  enableSuite[GlutenTableCapabilityCheckSuite]
  enableSuite[GlutenWriteDistributionAndOrderingSuite]
  enableSuite[GlutenQueryCompilationErrorsDSv2Suite]
  enableSuite[GlutenQueryCompilationErrorsSuite]
  enableSuite[GlutenQueryExecutionErrorsSuite]
    // NEW SUITE: disable as it expects exception which doesn't happen when offloaded to gluten
    .exclude(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION: compatibility with Spark 2.4/3.2 in reading/writing dates")
    // Doesn't support unhex with failOnError=true.
    .exclude("CONVERSION_INVALID_INPUT: to_binary conversion function hex")
  enableSuite[GlutenQueryParsingErrorsSuite]
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude("SPARK-45786: Decimal multiply, divide, remainder, quot")
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    // Set timezone through config.
    .exclude("data type casting")
    // Revised by setting timezone through config and commented unsupported cases.
    .exclude("cast string to timestamp")
    .exclude("cast from timestamp II")
    .exclude("SPARK-36286: invalid string cast to timestamp")
    .exclude("SPARK-39749: cast Decimal to string")
  enableSuite[GlutenTryCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    .exclude("cast from timestamp II") // Rewrite test for Gluten not supported with ANSI mode
    .exclude("ANSI mode: Throw exception on casting out-of-range value to byte type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to short type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to int type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to long type")
    .exclude("cast from invalid string to numeric should throw NumberFormatException")
    .exclude("SPARK-26218: Fix the corner case of codegen when casting float to Integer")
    // Set timezone through config.
    .exclude("data type casting")
    // Revised by setting timezone through config and commented unsupported cases.
    .exclude("cast string to timestamp")
  enableSuite[GlutenCollectionExpressionsSuite]
    // Rewrite in Gluten to replace Seq with Array
    .exclude("Shuffle")
    .excludeGlutenTest("Shuffle")
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenDateExpressionsSuite]
    // Has exception in fallback execution when we use resultDF.collect in evaluation.
    .exclude("TIMESTAMP_MICROS")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("unix_timestamp")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("to_unix_timestamp")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("Hour")
    // Unsupported format: yyyy-MM-dd HH:mm:ss.SSS
    .exclude("SPARK-33498: GetTimestamp,UnixTimestamp,ToUnixTimestamp with parseError")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("DateFormat")
    // Legacy mode is not supported, assuming this mode is not commonly used.
    .exclude("to_timestamp exception mode")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("from_unixtime")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("months_between")
    // Vanilla Spark does not have a unified DST Timestamp fastTime. 1320570000000L and
    // 1320566400000L both represent 2011-11-06 01:00:00.
    .exclude("SPARK-42635: timestampadd near daylight saving transition")
    // https://github.com/facebookincubator/velox/pull/10563/files#diff-140dc50e6dac735f72d29014da44b045509df0dd1737f458de1fe8cfd33d8145
    .excludeGlutenTest("from_unixtime")
  enableSuite[GlutenDecimalExpressionSuite]
  enableSuite[GlutenDecimalPrecisionSuite]
  enableSuite[GlutenGeneratorExpressionSuite]
  enableSuite[GlutenHashExpressionsSuite]
  enableSuite[GlutenHigherOrderFunctionsSuite]
  enableSuite[GlutenIntervalExpressionsSuite]
  enableSuite[GlutenJsonExpressionsSuite]
    // https://github.com/apache/incubator-gluten/issues/8102
    .exclude("$.store.book")
    // https://github.com/apache/incubator-gluten/issues/10948
    .exclude("$['key with spaces']")
    .exclude("$")
    .exclude("$.store.book[0]")
    .exclude("$.store.book[*]")
    .exclude("$.store.book[*].category")
    .exclude("$.store.book[*].isbn")
    .exclude("$.store.book[*].reader")
    .exclude("$.store.basket[*]")
    .exclude("$.store.basket[*][0]")
    .exclude("$.store.basket[0][*]")
    .exclude("$.store.basket[*][*]")
    .exclude("$.store.basket[0][*].b")
    // Exception class different.
    .exclude("from_json - invalid data")
  enableSuite[GlutenJsonFunctionsSuite]
    // * in get_json_object expression not supported in velox
    .exclude("SPARK-42782: Hive compatibility check for get_json_object")
    // Velox does not support single quotes in get_json_object function.
    .exclude("function get_json_object - support single quotes")
  enableSuite[GlutenLiteralExpressionSuite]
    .exclude("default")
    // FIXME(yma11): ObjectType is not covered in RowEncoder/Serializer in vanilla spark
    .exclude("SPARK-37967: Literal.create support ObjectType")
  enableSuite[GlutenMathExpressionsSuite]
    // Spark round UT for round(3.1415,3) is not correct.
    .exclude("round/bround/floor/ceil")
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenNondeterministicSuite]
    .exclude("MonotonicallyIncreasingID")
    .exclude("SparkPartitionID")
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenPredicateSuite]
  enableSuite[GlutenRandomSuite]
    .exclude("random")
    .exclude("SPARK-9127 codegen with long seed")
  enableSuite[GlutenRegexpExpressionsSuite]
  enableSuite[GlutenSortShuffleSuite]
  enableSuite[GlutenSortOrderExpressionsSuite]
  enableSuite[GlutenStringExpressionsSuite]
  enableSuite[GlutenTryEvalSuite]
  enableSuite[VeloxAdaptiveQueryExecSuite]
    .includeAllGlutenTests()
    .includeByPrefix(
      "SPARK-30291",
      "SPARK-30403",
      "SPARK-30719",
      "SPARK-31384",
      "SPARK-31658",
      "SPARK-32717",
      "SPARK-32649",
      "SPARK-34533",
      "SPARK-34781",
      "SPARK-35585",
      "SPARK-32932",
      "SPARK-33494",
      "SPARK-33933",
      "SPARK-31220",
      "SPARK-35874",
      "SPARK-39551"
    )
    .include(
      "Union/Except/Intersect queries",
      "Subquery de-correlation in Union queries",
      "force apply AQE",
      "tree string output",
      "control a plan explain mode in listener vis SQLConf",
      "AQE should set active session during execution",
      "No deadlock in UI update",
      "SPARK-35455: Unify empty relation optimization between normal and AQE optimizer - multi join"
    )
  enableSuite[GlutenBinaryFileFormatSuite]
    // Exception.
    .exclude("column pruning - non-readable file")
  enableSuite[GlutenCSVv1Suite]
    // file cars.csv include null string, Arrow not support to read
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
    // Flaky and already excluded in other cases
    .exclude("Gluten - test for FAILFAST parsing mode")
  enableSuite[GlutenCSVv2Suite]
    .exclude("Gluten - test for FAILFAST parsing mode")
    // Rule org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown in batch
    // Early Filter and Projection Push-Down generated an invalid plan
    .exclude("SPARK-26208: write and read empty data to csv file with headers")
    // file cars.csv include null string, Arrow not support to read
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
  enableSuite[GlutenCSVLegacyTimeParserSuite]
    // file cars.csv include null string, Arrow not support to read
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
  enableSuite[GlutenJsonV1Suite]
  enableSuite[GlutenJsonV2Suite]
    // exception test
    .exclude("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
  enableSuite[GlutenJsonLegacyTimeParserSuite]
  enableSuite[GlutenValidateRequirementsSuite]
  enableSuite[GlutenOrcColumnarBatchReaderSuite]
  enableSuite[GlutenOrcEncryptionSuite]
    // Orc encryption not supported yet
    .exclude("Write and read an encrypted file")
    .exclude("Write and read an encrypted table")
    .exclude("SPARK-35325: Write and read encrypted nested columns")
    .exclude("SPARK-35992: Write and read fully-encrypted columns with default masking")
  enableSuite[GlutenOrcFilterSuite]
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcPartitionDiscoverySuite]
  enableSuite[GlutenOrcV1PartitionDiscoverySuite]
  enableSuite[GlutenOrcV1QuerySuite]
    // Expected exception org.apache.spark.sql.AnalysisException to be thrown
    // , but no exception was thrown
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
    // not supported ignoreCorruptFiles
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // Schema mismatch, From Kind: BIGINT, To Kind: VARCHAR
    .exclude("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
  enableSuite[GlutenOrcV2QuerySuite]
// feature not supported
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // Schema mismatch, From Kind: BIGINT, To Kind: VARCHAR
    .exclude("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
    // For exception test.
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
  enableSuite[GlutenOrcSourceSuite]
    // Rewrite to disable Spark's columnar reader.
    // date result miss match
    .exclude("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .exclude("SPARK-31238, SPARK-31423: rebasing dates in write")
    // Ignored to disable vectorized reading check.
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .exclude("create temporary orc table")
    .exclude("create temporary orc table as")
    .exclude("appending insert")
    .exclude("overwrite insert")
    .exclude("SPARK-34897: Support reconcile schemas based on index after nested column pruning")
    // date result miss match
    .excludeGlutenTest("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .excludeGlutenTest("SPARK-31238, SPARK-31423: rebasing dates in write")
    // exclude as struct not supported
    .exclude("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle a column name which consists of only numbers")
    .exclude("SPARK-37812: Reuse result row when deserializing a struct")
    // rewrite
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=true)")
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
  enableSuite[GlutenOrcV1FilterSuite]
    // Expected exception org.apache.spark.SparkException to be thrown, but no exception was thrown
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcV1SchemaPruningSuite]
  enableSuite[GlutenOrcV2SchemaPruningSuite]
  enableSuite[GlutenParquetColumnIndexSuite]
    // Rewrite by just removing test timestamp.
    .exclude("test reading unaligned pages - test all types")
    // Rewrite by converting smaller integral value to timestamp.
    .exclude("test reading unaligned pages - test all types (dict encode)")
  enableSuite[GlutenParquetCompressionCodecPrecedenceSuite]
  enableSuite[GlutenParquetDeltaByteArrayEncodingSuite]
  enableSuite[GlutenParquetDeltaEncodingInteger]
  enableSuite[GlutenParquetDeltaEncodingLong]
  enableSuite[GlutenParquetDeltaLengthByteArrayEncodingSuite]
  enableSuite[GlutenParquetEncodingSuite]
    // Velox does not support rle encoding, but it can pass when native writer enabled.
    .exclude("parquet v2 pages - rle encoding for boolean value columns")
  enableSuite[GlutenParquetFieldIdIOSuite]
  enableSuite[GlutenParquetFileFormatV1Suite]
  enableSuite[GlutenParquetFileFormatV2Suite]
  enableSuite[GlutenParquetV1FilterSuite]
    // Rewrite.
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    // Rewrite for supported INT96 - timestamp.
    .exclude("filter pushdown - timestamp")
    .exclude("filter pushdown - date")
    // Exception bebaviour.
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
    .exclude("SPARK-38825: in and notIn filters")
  enableSuite[GlutenParquetV2FilterSuite]
    // Rewrite.
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    // Rewrite for supported INT96 - timestamp.
    .exclude("filter pushdown - timestamp")
    .exclude("filter pushdown - date")
    // Exception bebaviour.
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
    .exclude("SPARK-38825: in and notIn filters")
  enableSuite[GlutenParquetInteroperabilitySuite]
    .exclude("parquet timestamp conversion")
  enableSuite[GlutenParquetIOSuite]
    // Velox doesn't write file metadata into parquet file.
    .exclude("Write Spark version into Parquet metadata")
    // Exception.
    .exclude("SPARK-35640: read binary as timestamp should throw schema incompatible error")
    // Exception msg.
    .exclude("SPARK-35640: int as long should throw schema incompatible error")
    // Velox parquet reader not allow offset zero.
    .exclude("SPARK-40128 read DELTA_LENGTH_BYTE_ARRAY encoded strings")
  enableSuite[GlutenParquetV1PartitionDiscoverySuite]
  enableSuite[GlutenParquetV2PartitionDiscoverySuite]
  enableSuite[GlutenParquetProtobufCompatibilitySuite]
  enableSuite[GlutenParquetV1QuerySuite]
    .exclude("row group skipping doesn't overflow when reading into larger type")
    // Unsupport spark.sql.files.ignoreCorruptFiles.
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // new added in spark-3.3 and need fix later, random failure may caused by memory free
    .exclude("SPARK-39833: pushed filters with project without filter columns")
    .exclude("SPARK-39833: pushed filters with count()")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV2QuerySuite]
    .exclude("row group skipping doesn't overflow when reading into larger type")
    // Unsupport spark.sql.files.ignoreCorruptFiles.
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV1SchemaPruningSuite]
  enableSuite[GlutenParquetV2SchemaPruningSuite]
  enableSuite[GlutenParquetRebaseDatetimeV1Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    .excludeByPrefix("SPARK-33160, SPARK-37705: write the metadata key")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ, rewrite some
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetRebaseDatetimeV2Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    .excludeByPrefix("SPARK-33160, SPARK-37705: write the metadata key")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetSchemaInferenceSuite]
  enableSuite[GlutenParquetSchemaSuite]
    // error message mismatch is accepted
    .exclude("schema mismatch failure error message for parquet reader")
    .exclude("schema mismatch failure error message for parquet vectorized reader")
    // [PATH_NOT_FOUND] Path does not exist:
    // file:/opt/spark331/sql/core/src/test/resources/test-data/timestamp-nanos.parquet
    // May require for newer spark.test.home
    .excludeByPrefix("SPARK-40819")
  enableSuite[GlutenParquetThriftCompatibilitySuite]
    // Rewrite for file locating.
    .exclude("Read Parquet file generated by parquet-thrift")
  enableSuite[GlutenParquetVectorizedSuite]
  enableSuite[GlutenTextV1Suite]
  enableSuite[GlutenTextV2Suite]
  enableSuite[GlutenDataSourceV2StrategySuite]
  enableSuite[GlutenFileTableSuite]
  enableSuite[GlutenV2PredicateSuite]
  enableSuite[GlutenBucketingUtilsSuite]
  enableSuite[GlutenDataSourceStrategySuite]
  enableSuite[GlutenDataSourceSuite]
  enableSuite[GlutenFileFormatWriterSuite]
  enableSuite[GlutenFileIndexSuite]
  enableSuite[GlutenFileMetadataStructSuite]
  enableSuite[GlutenParquetV1AggregatePushDownSuite]
  enableSuite[GlutenParquetV2AggregatePushDownSuite]
    // TODO: Timestamp columns stats will lost if using int64 in parquet writer.
    .exclude("aggregate push down - different data types")
  enableSuite[GlutenOrcV1AggregatePushDownSuite]
    .exclude("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenOrcV2AggregatePushDownSuite]
    .exclude("nested column: Max(top level column) not push down")
    .exclude("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenParquetCodecSuite]
    // codec not supported in native
    .exclude("write and read - file source parquet - codec: lz4_raw")
    .exclude("write and read - file source parquet - codec: lz4raw")
  enableSuite[GlutenOrcCodecSuite]
  enableSuite[GlutenFileSourceStrategySuite]
    // Plan comparison.
    .exclude("partitioned table - after scan filters")
  enableSuite[GlutenHadoopFileLinesReaderSuite]
  enableSuite[GlutenPathFilterStrategySuite]
  enableSuite[GlutenPathFilterSuite]
  enableSuite[GlutenPruneFileSourcePartitionsSuite]
  enableSuite[GlutenCSVReadSchemaSuite]
  enableSuite[GlutenHeaderCSVReadSchemaSuite]
    .exclude("change column type from int to long")
  enableSuite[GlutenJsonReadSchemaSuite]
  enableSuite[GlutenOrcReadSchemaSuite]
  enableSuite[GlutenVectorizedOrcReadSchemaSuite]
  enableSuite[GlutenMergedOrcReadSchemaSuite]
  enableSuite[GlutenParquetReadSchemaSuite]
  enableSuite[GlutenVectorizedParquetReadSchemaSuite]
  enableSuite[GlutenMergedParquetReadSchemaSuite]
  enableSuite[GlutenV1WriteCommandSuite]
    // Rewrite to match SortExecTransformer.
    .excludeByPrefix("SPARK-41914:")
  enableSuite[GlutenEnsureRequirementsSuite]

  enableSuite[GlutenBroadcastJoinSuite]
    .exclude("Shouldn't change broadcast join buildSide if user clearly specified")
    .exclude("Shouldn't bias towards build right if user didn't specify")
    .exclude("SPARK-23192: broadcast hint should be retained after using the cached data")
    .exclude("broadcast join where streamed side's output partitioning is HashPartitioning")

  enableSuite[GlutenExistenceJoinSuite]
  enableSuite[GlutenInnerJoinSuiteForceShjOn]
  enableSuite[GlutenInnerJoinSuiteForceShjOff]
  enableSuite[GlutenOuterJoinSuiteForceShjOn]
  enableSuite[GlutenOuterJoinSuiteForceShjOff]
  enableSuite[FallbackStrategiesSuite]
  enableSuite[GlutenBroadcastExchangeSuite]
  enableSuite[GlutenLocalBroadcastExchangeSuite]
  enableSuite[GlutenCoalesceShufflePartitionsSuite]
    // Rewrite for Gluten. Change details are in the inline comments in individual tests.
    .excludeByPrefix("determining the number of reducers")
  enableSuite[GlutenExchangeSuite]
    // ColumnarShuffleExchangeExec does not support doExecute() method
    .exclude("shuffling UnsafeRows in exchange")
    // This test will re-run in GlutenExchangeSuite with shuffle partitions > 1
    .exclude("Exchange reuse across the whole plan")
  enableSuite[GlutenReplaceHashWithSortAggSuite]
    .exclude("replace partial hash aggregate with sort aggregate")
    .exclude("replace partial and final hash aggregate together with sort aggregate")
    .exclude("do not replace hash aggregate if child does not have sort order")
    .exclude("do not replace hash aggregate if there is no group-by column")
  enableSuite[GlutenReuseExchangeAndSubquerySuite]
  enableSuite[GlutenSameResultSuite]
  enableSuite[GlutenSortSuite]
  enableSuite[GlutenSQLAggregateFunctionSuite]
  // spill not supported yet.
  enableSuite[GlutenSQLWindowFunctionSuite]
    .exclude("test with low buffer spill threshold")
  enableSuite[GlutenTakeOrderedAndProjectSuite]
  enableSuite[GlutenSessionExtensionSuite]
  enableSuite[TestFileSourceScanExecTransformer]
  enableSuite[GlutenBucketedReadWithoutHiveSupportSuite]
    // Exclude the following suite for plan changed from SMJ to SHJ.
    .exclude("avoid shuffle when join 2 bucketed tables")
    .exclude("avoid shuffle and sort when sort columns are a super set of join keys")
    .exclude("only shuffle one side when join bucketed table and non-bucketed table")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket number")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket keys")
    .exclude("shuffle when join keys are not equal to bucket keys")
    .exclude("shuffle when join 2 bucketed tables with bucketing disabled")
    .exclude("check sort and shuffle when bucket and sort columns are join keys")
    .exclude("only sort one side when sort columns are different")
    .exclude("only sort one side when sort columns are same but their ordering is different")
    .exclude("SPARK-17698 Join predicates should not contain filter clauses")
    .exclude("SPARK-19122 Re-order join predicates if they match with the child's" +
      " output partitioning")
    .exclude("SPARK-19122 No re-ordering should happen if set of join columns != set of child's " +
      "partitioning columns")
    .exclude("SPARK-29655 Read bucketed tables obeys spark.sql.shuffle.partitions")
    .exclude("SPARK-32767 Bucket join should work if SHUFFLE_PARTITIONS larger than bucket number")
    .exclude("bucket coalescing eliminates shuffle")
    .exclude("bucket coalescing is not satisfied")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("disable bucketing when the output doesn't contain all bucketing columns")
    .excludeByPrefix("bucket coalescing is applied when join expressions match")
  enableSuite[GlutenBucketedWriteWithoutHiveSupportSuite]
    .exclude("write bucketed data")
    .exclude("write bucketed data with sortBy")
    .exclude("write bucketed data without partitionBy")
    .exclude("write bucketed data without partitionBy with sortBy")
    .exclude("write bucketed data with bucketing disabled")
  enableSuite[GlutenCreateTableAsSelectSuite]
    // TODO Gluten can not catch the spark exception in Driver side.
    .exclude("CREATE TABLE USING AS SELECT based on the file without write permission")
    .exclude("create a table, drop it and create another one with the same name")
  enableSuite[GlutenDDLSourceLoadSuite]
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite]
    .disable(
      "DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type")
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE]
  enableSuite[GlutenExternalCommandRunnerSuite]
  enableSuite[GlutenFilteredScanSuite]
  enableSuite[GlutenFiltersSuite]
  enableSuite[GlutenInsertSuite]
    // the native write staing dir is differnt with vanilla Spark for coustom partition paths
    .exclude("SPARK-35106: Throw exception when rename custom partition paths returns false")
    .exclude("Stop task set if FileAlreadyExistsException was thrown")
    // Rewrite: Additional support for file scan with default values has been added in Spark-3.4.
    // It appends the default value in record if it is not present while scanning.
    // Velox supports default values for new records but it does not backfill the
    // existing records and provides null for the existing ones.
    .exclude("INSERT rows, ALTER TABLE ADD COLUMNS with DEFAULTs, then SELECT them")
    .exclude("SPARK-39557 INSERT INTO statements with tables with array defaults")
    .exclude("SPARK-39557 INSERT INTO statements with tables with struct defaults")
    .exclude("SPARK-39557 INSERT INTO statements with tables with map defaults")
  enableSuite[GlutenPartitionedWriteSuite]
  enableSuite[GlutenPathOptionSuite]
  enableSuite[GlutenPrunedScanSuite]
  enableSuite[GlutenResolvedDataSourceSuite]
  enableSuite[GlutenSaveLoadSuite]
  enableSuite[GlutenTableScanSuite]
  enableSuite[GlutenApproxCountDistinctForIntervalsQuerySuite]
  enableSuite[GlutenApproximatePercentileQuerySuite]
    // requires resource files from Vanilla spark jar
    .exclude("SPARK-32908: maximum target error in percentile_approx")
  enableSuite[GlutenCachedTableSuite]
    .exclude("A cached table preserves the partitioning and ordering of its cached SparkPlan")
    .exclude("InMemoryRelation statistics")
    // Extra ColumnarToRow is needed to transform vanilla columnar data to gluten columnar data.
    .exclude("SPARK-37369: Avoid redundant ColumnarToRow transition on InMemoryTableScan")
  enableSuite[GlutenFileSourceCharVarcharTestSuite]
    .exclude("length check for input string values: nested in array")
    .exclude("length check for input string values: nested in array")
    .exclude("length check for input string values: nested in map key")
    .exclude("length check for input string values: nested in map value")
    .exclude("length check for input string values: nested in both map key and value")
    .exclude("length check for input string values: nested in array of struct")
    .exclude("length check for input string values: nested in array of array")
    // Following tests are excluded as these are overridden in Gluten test suite..
    // The overridden tests assert against Velox-specific error messages for char/varchar
    // length validation, which differ from the original vanilla Spark tests.
    .exclude("length check for input string values: top-level columns")
    .exclude("length check for input string values: partitioned columns")
    .exclude("length check for input string values: nested in struct of array")
    .exclude("length check for input string values: with implicit cast")
    .exclude("char/varchar type values length check: partitioned columns of other types")
  enableSuite[GlutenDSV2CharVarcharTestSuite]
    // Following tests are excluded as these are overridden in Gluten test suite..
    // The overridden tests assert against Velox-specific error messages for char/varchar
    // length validation, which differ from the original vanilla Spark tests.
    .exclude("length check for input string values: top-level columns")
    .exclude("length check for input string values: nested in array")
    .exclude("length check for input string values: nested in struct of array")
    .exclude("length check for input string values: nested in array of struct")
    .exclude("length check for input string values: nested in array of array")
    .exclude("length check for input string values: with implicit cast")
    .exclude("char/varchar type values length check: partitioned columns of other types")
    .exclude("SPARK-42611: check char/varchar length in reordered structs within arrays")
  enableSuite[GlutenColumnExpressionSuite]
    // Velox raise_error('errMsg') throws a velox_user_error exception with the message 'errMsg'.
    // The final caught Spark exception's getCause().getMessage() contains 'errMsg' but does not
    // equal 'errMsg' exactly. The following two tests will be skipped and overridden in Gluten.
    .exclude("raise_error")
    .exclude("assert_true")
  enableSuite[GlutenComplexTypeSuite]
  enableSuite[GlutenConfigBehaviorSuite]
    // Will be fixed by cleaning up ColumnarShuffleExchangeExec.
    .exclude("SPARK-22160 spark.sql.execution.rangeExchange.sampleSizePerPartition")
    // Gluten columnar operator will have different number of jobs
    .exclude("SPARK-40211: customize initialNumPartitions for take")
  enableSuite[GlutenCountMinSketchAggQuerySuite]
  enableSuite[GlutenCsvFunctionsSuite]
  enableSuite[GlutenCTEHintSuite]
  enableSuite[GlutenCTEInlineSuiteAEOff]
  enableSuite[GlutenCTEInlineSuiteAEOn]
  enableSuite[GlutenDataFrameAggregateSuite]
    // Test for vanilla spark codegen, not apply for Gluten
    .exclude("SPARK-43876: Enable fast hashmap for distinct queries")
    .exclude(
      "zero moments", // [velox does not return NaN]
      "SPARK-26021: NaN and -0.0 in grouping expressions", // NaN case
      // incorrect result, distinct NaN case
      "SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate",
      // Replaced with another test.
      "SPARK-19471: AggregationIterator does not initialize the generated result projection" +
        " before using it",
      // Velox's collect_list / collect_set are by design declarative aggregate so plan check
      // for ObjectHashAggregateExec will fail.
      "SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle",
      "SPARK-31620: agg with subquery (whole-stage-codegen = true)",
      "SPARK-31620: agg with subquery (whole-stage-codegen = false)"
    )
  enableSuite[GlutenDataFrameAsOfJoinSuite]
  enableSuite[GlutenDataFrameComplexTypeSuite]
  enableSuite[GlutenDataFrameFunctionsSuite]
    // blocked by Velox-5768
    .exclude("aggregate function - array for primitive type containing null")
    .exclude("aggregate function - array for non-primitive type")
    // Rewrite this test because Velox sorts rows by key for primitive data types, which disrupts the original row sequence.
    .exclude("map_zip_with function - map of primitive types")
  enableSuite[GlutenDataFrameHintSuite]
  enableSuite[GlutenDataFrameImplicitsSuite]
  enableSuite[GlutenDataFrameJoinSuite]
  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .exclude(
      // NaN case
      "replace nan with float",
      "replace nan with double"
    )
  enableSuite[GlutenDataFramePivotSuite]
    // substring issue
    .exclude("pivot with column definition in groupby")
    // array comparison not supported for values that contain nulls
    .exclude(
      "pivot with null and aggregate type not supported by PivotFirst returns correct result")
  enableSuite[GlutenDataFrameRangeSuite]
    .exclude("SPARK-20430 Initialize Range parameters in a driver side")
    .excludeByPrefix("Cancelling stage in a query with Range")
  enableSuite[GlutenDataFrameSelfJoinSuite]
  enableSuite[GlutenDataFrameSessionWindowingSuite]
  enableSuite[GlutenDataFrameSetOperationsSuite]
    .exclude("SPARK-37371: UnionExec should support columnar if all children support columnar")
    // Result depends on the implementation for nondeterministic expression rand.
    // Not really an issue.
    .exclude("SPARK-10740: handle nondeterministic expressions correctly for set operations")
  enableSuite[GlutenDataFrameStatSuite]
  enableSuite[GlutenDataFrameSuite]
    // Rewrite these tests because it checks Spark's physical operators.
    .excludeByPrefix("SPARK-22520", "reuse exchange")
    .exclude(
      /**
       * Rewrite these tests because the rdd partition is equal to the configuration
       * "spark.sql.shuffle.partitions".
       */
      "repartitionByRange",
      "distributeBy and localSort",
      // Mismatch when max NaN and infinite value
      "NaN is greater than all other non-NaN numeric values",
      // Rewrite this test because the describe functions creates unmatched plan.
      "describe",
      // decimal failed ut.
      "SPARK-22271: mean overflows and returns null for some decimal variables",
      // Result depends on the implementation for nondeterministic expression rand.
      // Not really an issue.
      "SPARK-9083: sort with non-deterministic expressions"
    )
    // test for sort node not present but gluten uses shuffle hash join
    .exclude("SPARK-41048: Improve output partitioning and ordering with AQE cache")
    // Rewrite this test since it checks the physical operator which is changed in Gluten
    .exclude("SPARK-27439: Explain result should match collected result after view change")
  enableSuite[GlutenDataFrameTimeWindowingSuite]
  enableSuite[GlutenDataFrameTungstenSuite]
  enableSuite[GlutenDataFrameWindowFunctionsSuite]
    // does not support `spark.sql.legacy.statisticalAggregate=true` (null -> NAN)
    .exclude("corr, covar_pop, stddev_pop functions in specific window")
    .exclude("covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window")
    // does not support spill
    .exclude("Window spill with more than the inMemoryThreshold and spillThreshold")
    .exclude("SPARK-21258: complex object in combination with spilling")
    // rewrite `WindowExec -> WindowExecTransformer`
    .exclude(
      "SPARK-38237: require all cluster keys for child required distribution for window query")
  enableSuite[GlutenDataFrameWindowFramesSuite]
    // Local window fixes are not added.
    .exclude("range between should accept int/long values as boundary")
    .exclude("unbounded preceding/following range between with aggregation")
    .exclude("sliding range between with aggregation")
    .exclude("store and retrieve column stats in different time zones")
  enableSuite[GlutenDataFrameWriterV2Suite]
  enableSuite[GlutenDatasetAggregatorSuite]
  enableSuite[GlutenDatasetCacheSuite]
  enableSuite[GlutenDatasetOptimizationSuite]
  enableSuite[GlutenDatasetPrimitiveSuite]
  enableSuite[GlutenDatasetSerializerRegistratorSuite]
  enableSuite[GlutenDatasetSuite]
    // Rewrite the following two tests in GlutenDatasetSuite.
    .exclude("dropDuplicates: columns with same column name")
    .exclude("groupBy.as")
  enableSuite[GlutenDateFunctionsSuite]
    // The below two are replaced by two modified versions.
    .exclude("unix_timestamp")
    .exclude("to_unix_timestamp")
    // Unsupported datetime format: specifier X is not supported by velox.
    .exclude("to_timestamp with microseconds precision")
    // Legacy mode is not supported, assuming this mode is not commonly used.
    .exclude("SPARK-30668: use legacy timestamp parser in to_timestamp")
    // Legacy mode is not supported and velox getTimestamp function does not throw
    // exception when format is "yyyy-dd-aa".
    .exclude("function to_date")
  enableSuite[GlutenDeprecatedAPISuite]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOnDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOffDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOnDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOffDisableScan]
  enableSuite[GlutenExpressionsSchemaSuite]
  enableSuite[GlutenExtraStrategiesSuite]
  enableSuite[GlutenFileBasedDataSourceSuite]
    // test data path is jar path, rewrite
    .exclude("Option recursiveFileLookup: disable partition inferring")
    // gluten executor exception cannot get in driver, rewrite
    .exclude("Spark native readers should respect spark.sql.caseSensitive - parquet")
    // shuffle_partitions config is different, rewrite
    .excludeByPrefix("SPARK-22790")
    // plan is different cause metric is different, rewrite
    .excludeByPrefix("SPARK-25237")
    // error msg from velox is different & reader options is not supported, rewrite
    .exclude("Enabling/disabling ignoreMissingFiles using parquet")
    .exclude("Enabling/disabling ignoreMissingFiles using orc")
    .exclude("Spark native readers should respect spark.sql.caseSensitive - orc")
    .exclude("Return correct results when data columns overlap with partition columns")
    .exclude("Return correct results when data columns overlap with partition " +
      "columns (nested data)")
    .exclude("SPARK-31116: Select nested schema with case insensitive mode")
    // exclude as original metric not correct when task offloaded to velox
    .exclude("SPARK-37585: test input metrics for DSV2 with output limits")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("File source v2: support passing data filters to FileScan without partitionFilters")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("File source v2: support partition pruning")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("SPARK-41017: filter pushdown with nondeterministic predicates")
  enableSuite[GlutenFileScanSuite]
  enableSuite[GlutenGeneratorFunctionSuite]
    .exclude("SPARK-45171: Handle evaluated nondeterministic expression")
  enableSuite[GlutenInjectRuntimeFilterSuite]
    // FIXME: yan
    .exclude("Merge runtime bloom filters")
  enableSuite[GlutenIntervalFunctionsSuite]
  enableSuite[GlutenJoinSuite]
    // exclude as it check spark plan
    .exclude("SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join")
  enableSuite[GlutenMathFunctionsSuite]
  enableSuite[GlutenMetadataCacheSuite]
    .exclude("SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException")
  enableSuite[GlutenMiscFunctionsSuite]
  enableSuite[GlutenNestedDataSourceV1Suite]
  enableSuite[GlutenNestedDataSourceV2Suite]
  enableSuite[GlutenProcessingTimeSuite]
  enableSuite[GlutenProductAggSuite]
  enableSuite[GlutenReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[GlutenScalaReflectionRelationSuite]
  enableSuite[GlutenSerializationSuite]
  enableSuite[GlutenFileSourceSQLInsertTestSuite]
  enableSuite[GlutenDSV2SQLInsertTestSuite]
  enableSuite[GlutenSQLQuerySuite]
    // Decimal precision exceeds.
    .exclude("should be able to resolve a persistent view")
    // Unstable. Needs to be fixed.
    .exclude("SPARK-36093: RemoveRedundantAliases should not change expression's name")
    // Rewrite from ORC scan to Parquet scan because ORC is not well supported.
    .exclude("SPARK-28156: self-join should not miss cached view")
    .exclude("SPARK-33338: GROUP BY using literal map should not fail")
    // Rewrite to disable plan check for SMJ because SHJ is preferred in Gluten.
    .exclude("SPARK-11111 null-safe join should not use cartesian product")
    // Rewrite to change the information of a caught exception.
    .exclude("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    // Different exception.
    .exclude("run sql directly on files")
    // Not useful and time consuming.
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL")
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class")
    // exception test, rewritten in gluten
    .exclude("the escape character is not allowed to end with")
    // ORC related
    .exclude("SPARK-37965: Spark support read/write orc file with invalid char in field name")
    .exclude("SPARK-38173: Quoted column cannot be recognized correctly when quotedRegexColumnNames is true")
  enableSuite[GlutenSQLQueryTestSuite]
  enableSuite[GlutenStatisticsCollectionSuite]
    // The output byte size of Velox is different
    .exclude("SPARK-33687: analyze all tables in a specific database")
    .exclude("column stats collection for null columns")
    .exclude("analyze column command - result verification")
  enableSuite[GlutenSubquerySuite]
    .excludeByPrefix(
      "SPARK-26893" // Rewrite this test because it checks Spark's physical operators.
    )
    // exclude as it checks spark plan
    .exclude("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery")
  enableSuite[GlutenTypedImperativeAggregateSuite]
  enableSuite[GlutenUnwrapCastInComparisonEndToEndSuite]
    // Rewrite with NaN test cases excluded.
    .exclude("cases when literal is max")
  enableSuite[GlutenXPathFunctionsSuite]
  enableSuite[GlutenFallbackSuite]
  enableSuite[GlutenHiveSQLQuerySuite]
  enableSuite[GlutenCollapseProjectExecTransformerSuite]
  enableSuite[GlutenSparkSessionExtensionSuite]
  enableSuite[GlutenGroupBasedDeleteFromTableSuite]
  enableSuite[GlutenDeltaBasedDeleteFromTableSuite]
  enableSuite[GlutenDataFrameToSchemaSuite]
  enableSuite[GlutenDatasetUnpivotSuite]
  enableSuite[GlutenLateralColumnAliasSuite]
  enableSuite[GlutenParametersSuite]
  enableSuite[GlutenResolveDefaultColumnsSuite]
  enableSuite[GlutenSubqueryHintPropagationSuite]
  enableSuite[GlutenUrlFunctionsSuite]
  enableSuite[GlutenParquetRowIndexSuite]
    .excludeByPrefix("row index generation")
    .excludeByPrefix("invalid row index column type")
  enableSuite[GlutenBitmapExpressionsQuerySuite]
  enableSuite[GlutenEmptyInSuite]
  enableSuite[GlutenRuntimeNullChecksV2Writes]
  enableSuite[GlutenTableOptionsConstantFoldingSuite]
  enableSuite[GlutenDeltaBasedMergeIntoTableSuite]
  enableSuite[GlutenDeltaBasedMergeIntoTableUpdateAsDeleteAndInsertSuite]
  enableSuite[GlutenDeltaBasedUpdateAsDeleteAndInsertTableSuite]
    // FIXME: complex type result mismatch
    .exclude("update nested struct fields")
    .exclude("update char/varchar columns")
  enableSuite[GlutenDeltaBasedUpdateTableSuite]
  enableSuite[GlutenGroupBasedMergeIntoTableSuite]
  enableSuite[GlutenFileSourceCustomMetadataStructSuite]
  enableSuite[GlutenParquetFileMetadataStructRowIndexSuite]
  enableSuite[GlutenTableLocationSuite]
  enableSuite[GlutenRemoveRedundantWindowGroupLimitsSuite]
    // rewrite with Gluten test
    .exclude("remove redundant WindowGroupLimits")
  enableSuite[GlutenSQLCollectLimitExecSuite]
  enableSuite[GlutenBatchEvalPythonExecSuite]
    // Replaced with other tests that check for native operations
    .exclude("Python UDF: push down deterministic FilterExec predicates")
    .exclude("Nested Python UDF: push down deterministic FilterExec predicates")
    .exclude("Python UDF: no push down on non-deterministic")
    .exclude("Python UDF: push down on deterministic predicates after the first non-deterministic")
  enableSuite[GlutenExtractPythonUDFsSuite]
    // Replaced with test that check for native operations
    .exclude("Python UDF should not break column pruning/filter pushdown -- Parquet V1")
    .exclude("Chained Scalar Pandas UDFs should be combined to a single physical node")
    .exclude("Mixed Batched Python UDFs and Pandas UDF should be separate physical node")
    .exclude("Independent Batched Python UDFs and Scalar Pandas UDFs should be combined separately")
    .exclude("Dependent Batched Python UDFs and Scalar Pandas UDFs should not be combined")
    .exclude("Python UDF should not break column pruning/filter pushdown -- Parquet V2")
  enableSuite[GlutenStreamingQuerySuite]
    // requires test resources that don't exist in Gluten repo
    .exclude("detect escaped path and report the migration guide")
    .exclude("ignore the escaped path check when the flag is off")
    .excludeByPrefix("SPARK-51187")
  enableSuite[GlutenQueryExecutionSuite]
    // Rewritten to set root logger level to INFO so that logs can be parsed
    .exclude("Logging plan changes for execution")
    // Rewrite for transformed plan
    .exclude("dumping query execution info to a file - explainMode=formatted")

  override def getSQLQueryTestSettings: SQLQueryTestSettings = VeloxSQLQueryTestSettings
}
// scalastyle:on line.size.limit
