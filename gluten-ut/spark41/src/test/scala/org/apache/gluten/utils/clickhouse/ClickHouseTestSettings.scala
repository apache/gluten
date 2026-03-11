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
package org.apache.gluten.utils.clickhouse

import org.apache.gluten.utils.{BackendTestSettings, SQLQueryTestSettings}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector._
import org.apache.spark.sql.errors.{GlutenQueryCompilationErrorsDSv2Suite, GlutenQueryCompilationErrorsSuite, GlutenQueryExecutionErrorsSuite, GlutenQueryParsingErrorsSuite}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.clickhouse.ClickHouseAdaptiveQueryExecSuite
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.binaryfile.GlutenBinaryFileFormatSuite
import org.apache.spark.sql.execution.datasources.csv.{GlutenCSVLegacyTimeParserSuite, GlutenCSVv1Suite, GlutenCSVv2Suite}
import org.apache.spark.sql.execution.datasources.json.{GlutenJsonLegacyTimeParserSuite, GlutenJsonV1Suite, GlutenJsonV2Suite}
import org.apache.spark.sql.execution.datasources.orc._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.text.{GlutenTextV1Suite, GlutenTextV2Suite}
import org.apache.spark.sql.execution.datasources.v2.{GlutenDataSourceV2StrategySuite, GlutenFileTableSuite, GlutenV2PredicateSuite}
import org.apache.spark.sql.execution.exchange.{GlutenEnsureRequirementsSuite, GlutenValidateRequirementsSuite}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.extension.{GlutenCollapseProjectExecTransformerSuite, GlutenCustomerExtensionSuite, GlutenSessionExtensionSuite}
import org.apache.spark.sql.gluten.{GlutenFallbackStrategiesSuite, GlutenFallbackSuite}
import org.apache.spark.sql.hive.execution.GlutenHiveSQLQueryCHSuite
import org.apache.spark.sql.sources._

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class ClickHouseTestSettings extends BackendTestSettings {

  enableSuite[ClickHouseAdaptiveQueryExecSuite]
    .includeAllGlutenTests()
    .includeByPrefix(
      // exclude SPARK-29906 because gluten columnar operator will have different number of shuffle
      "SPARK-30291",
      "SPARK-30403",
      "SPARK-30719",
      "SPARK-31384",
      "SPARK-31658",
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
  enableSuite[GlutenFallbackStrategiesSuite]
  enableSuite[GlutenApproxCountDistinctForIntervalsQuerySuite]
    .excludeCH("test ApproxCountDistinctForIntervals with large number of endpoints")
  enableSuite[GlutenApproximatePercentileQuerySuite]
    // requires resource files from Vanilla spark jar
    .exclude("SPARK-32908: maximum target error in percentile_approx")
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude("SPARK-45786: Decimal multiply, divide, remainder, quot")
    .excludeCH("% (Remainder)")
    .excludeCH("SPARK-17617: % (Remainder) double % double on super big double")
    .excludeCH("pmod")
  enableSuite[GlutenBinaryFileFormatSuite]
    // Exception.
    .exclude("column pruning - non-readable file")
  enableSuite[GlutenBitmapExpressionsQuerySuite]
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenBloomFilterAggregateQuerySuite]
    .excludeCH("Test bloom_filter_agg and might_contain")
  enableSuite[GlutenBloomFilterAggregateQuerySuiteCGOff]
    .excludeCH("Test bloom_filter_agg and might_contain")
  enableSuite[GlutenBroadcastExchangeSuite]
  // TODO: fix the hanging problem in GLUTEN-8890 followup
//  enableSuite[GlutenBroadcastJoinSuite]
//    .includeCH("Shouldn't change broadcast join buildSide if user clearly specified")
//    .includeCH("Shouldn't bias towards build right if user didn't specify")
//    .includeCH("SPARK-23192: broadcast hint should be retained after using the cached data")
//    .includeCH("broadcast join where streamed side's output partitioning is HashPartitioning")
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
    .includeCH("write bucketed data")
    .includeCH("write bucketed data with sortBy")
    .includeCH("write bucketed data without partitionBy")
    .includeCH("write bucketed data without partitionBy with sortBy")
    .includeCH("write bucketed data with bucketing disabled")
  enableSuite[GlutenBucketingUtilsSuite]
  enableSuite[GlutenCSVLegacyTimeParserSuite]
    // file cars.csv include null string, Arrow not support to read
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with empty fields with user defined empty values")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
    .excludeCH("simple csv test")
    .excludeCH("simple csv test with calling another function to load")
    .excludeCH("simple csv test with type inference")
    .excludeCH("test with alternative delimiter and quote")
    .excludeCH("SPARK-24540: test with multiple character delimiter (comma space)")
    .excludeCH("SPARK-24540: test with multiple (crazy) character delimiter")
    .excludeCH("test different encoding")
    .excludeCH("crlf line separators in multiline mode")
    .excludeCH("test aliases sep and encoding for delimiter and charset")
    .excludeCH("test for DROPMALFORMED parsing mode")
    .excludeCH("test for blank column names on read and select columns")
    .excludeCH("test for FAILFAST parsing mode")
    .excludeCH("test for tokens more than the fields in the schema")
    .excludeCH("test with null quote character")
    .excludeCH("save csv with quote escaping, using charToEscapeQuoteEscaping option")
    .excludeCH("commented lines in CSV data")
    .excludeCH("inferring schema with commented lines in CSV data")
    .excludeCH("inferring timestamp types via custom date format")
    .excludeCH("load date types via custom date format")
    .excludeCH("nullable fields with user defined null value of \"null\"")
    .excludeCH("empty fields with user defined empty values")
    .excludeCH("old csv data source name works")
    .excludeCH("nulls, NaNs and Infinity values can be parsed")
    .excludeCH("SPARK-15585 turn off quotations")
    .excludeCH("Write timestamps correctly in ISO8601 format by default")
    .excludeCH("Write dates correctly in ISO8601 format by default")
    .excludeCH("Roundtrip in reading and writing timestamps")
    .excludeCH("SPARK-37326: Write and infer TIMESTAMP_LTZ values with a non-default pattern")
    .excludeCH("SPARK-37326: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ")
    .excludeCH("Write dates correctly with dateFormat option")
    .excludeCH("Write timestamps correctly with timestampFormat option")
    .excludeCH("Write timestamps correctly with timestampFormat option and timeZone option")
    .excludeCH("SPARK-18699 put malformed records in a `columnNameOfCorruptRecord` field")
    .excludeCH("Enabling/disabling ignoreCorruptFiles")
    .excludeCH("SPARK-19610: Parse normal multi-line CSV files")
    .excludeCH("SPARK-38523: referring to the corrupt record column")
    .excludeCH(
      "SPARK-17916: An empty string should not be coerced to null when nullValue is passed.")
    .excludeCH(
      "SPARK-25241: An empty string should not be coerced to null when emptyValue is passed.")
    .excludeCH("SPARK-24329: skip lines with comments, and one or multiple whitespaces")
    .excludeCH("SPARK-23786: Checking column names against schema in the multiline mode")
    .excludeCH("SPARK-23786: Checking column names against schema in the per-line mode")
    .excludeCH("SPARK-23786: Ignore column name case if spark.sql.caseSensitive is false")
    .excludeCH("SPARK-23786: warning should be printed if CSV header doesn't conform to schema")
    .excludeCH("SPARK-25134: check header on parsing of dataset with projection and column pruning")
    .excludeCH("SPARK-24676 project required data from parsed data when columnPruning disabled")
    .excludeCH("encoding in multiLine mode")
    .excludeCH("Support line separator - default value \\r, \\r\\n and \\n")
    .excludeCH("Support line separator in UTF-8 #0")
    .excludeCH("Support line separator in UTF-16BE #1")
    .excludeCH("Support line separator in ISO-8859-1 #2")
    .excludeCH("Support line separator in UTF-32LE #3")
    .excludeCH("Support line separator in UTF-8 #4")
    .excludeCH("Support line separator in UTF-32BE #5")
    .excludeCH("Support line separator in CP1251 #6")
    .excludeCH("Support line separator in UTF-16LE #8")
    .excludeCH("Support line separator in UTF-32BE #9")
    .excludeCH("Support line separator in US-ASCII #10")
    .excludeCH("Support line separator in utf-32le #11")
    .excludeCH("lineSep with 2 chars when multiLine set to true")
    .excludeCH("lineSep with 2 chars when multiLine set to false")
    .excludeCH("SPARK-26208: write and read empty data to csv file with headers")
    .excludeCH("Do not reuse last good value for bad input field")
    .excludeCH("SPARK-29101 test count with DROPMALFORMED mode")
    .excludeCH("return correct results when data columns overlap with partition columns")
    .excludeCH("filters push down - malformed input in PERMISSIVE mode")
    .excludeCH("case sensitivity of filters references")
    .excludeCH("SPARK-33566: configure UnescapedQuoteHandling to parse unescaped quotes and unescaped delimiter data correctly")
    .excludeCH("SPARK-36831: Support reading and writing ANSI intervals")
    .excludeCH("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
    .excludeCH("SPARK-39731: Handle date and timestamp parsing fallback")
    .excludeCH("SPARK-40215: enable parsing fallback for CSV in CORRECTED mode with a SQL config")
    .excludeCH("SPARK-40496: disable parsing fallback when the date/timestamp format is provided")
    .excludeCH("SPARK-42335: Pass the comment option through to univocity if users set it explicitly in CSV dataSource")
    .excludeCH("SPARK-46862: column pruning in the multi-line mode")
  enableSuite[GlutenCSVReadSchemaSuite]
  enableSuite[GlutenCSVv1Suite]
    // file cars.csv include null string, Arrow not support to read
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with empty fields with user defined empty values")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
    .excludeCH("simple csv test")
    .excludeCH("simple csv test with calling another function to load")
    .excludeCH("simple csv test with type inference")
    .excludeCH("test with alternative delimiter and quote")
    .excludeCH("SPARK-24540: test with multiple character delimiter (comma space)")
    .excludeCH("SPARK-24540: test with multiple (crazy) character delimiter")
    .excludeCH("test different encoding")
    .excludeCH("crlf line separators in multiline mode")
    .excludeCH("test aliases sep and encoding for delimiter and charset")
    .excludeCH("test for DROPMALFORMED parsing mode")
    .excludeCH("test for blank column names on read and select columns")
    .excludeCH("test for FAILFAST parsing mode")
    .excludeCH("test for tokens more than the fields in the schema")
    .excludeCH("test with null quote character")
    .excludeCH("save csv with quote escaping, using charToEscapeQuoteEscaping option")
    .excludeCH("commented lines in CSV data")
    .excludeCH("inferring schema with commented lines in CSV data")
    .excludeCH("inferring timestamp types via custom date format")
    .excludeCH("load date types via custom date format")
    .excludeCH("nullable fields with user defined null value of \"null\"")
    .excludeCH("empty fields with user defined empty values")
    .excludeCH("old csv data source name works")
    .excludeCH("nulls, NaNs and Infinity values can be parsed")
    .excludeCH("SPARK-15585 turn off quotations")
    .excludeCH("Write timestamps correctly in ISO8601 format by default")
    .excludeCH("Write dates correctly in ISO8601 format by default")
    .excludeCH("Roundtrip in reading and writing timestamps")
    .excludeCH("SPARK-37326: Write and infer TIMESTAMP_LTZ values with a non-default pattern")
    .excludeCH("SPARK-37326: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .excludeCH("SPARK-37326: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ")
    .excludeCH("Write dates correctly with dateFormat option")
    .excludeCH("Write timestamps correctly with timestampFormat option")
    .excludeCH("Write timestamps correctly with timestampFormat option and timeZone option")
    .excludeCH("SPARK-18699 put malformed records in a `columnNameOfCorruptRecord` field")
    .excludeCH("Enabling/disabling ignoreCorruptFiles")
    .excludeCH("SPARK-19610: Parse normal multi-line CSV files")
    .excludeCH("SPARK-38523: referring to the corrupt record column")
    .excludeCH(
      "SPARK-17916: An empty string should not be coerced to null when nullValue is passed.")
    .excludeCH(
      "SPARK-25241: An empty string should not be coerced to null when emptyValue is passed.")
    .excludeCH("SPARK-24329: skip lines with comments, and one or multiple whitespaces")
    .excludeCH("SPARK-23786: Checking column names against schema in the multiline mode")
    .excludeCH("SPARK-23786: Checking column names against schema in the per-line mode")
    .excludeCH("SPARK-23786: Ignore column name case if spark.sql.caseSensitive is false")
    .excludeCH("SPARK-23786: warning should be printed if CSV header doesn't conform to schema")
    .excludeCH("SPARK-25134: check header on parsing of dataset with projection and column pruning")
    .excludeCH("SPARK-24676 project required data from parsed data when columnPruning disabled")
    .excludeCH("encoding in multiLine mode")
    .excludeCH("Support line separator - default value \\r, \\r\\n and \\n")
    .excludeCH("Support line separator in UTF-8 #0")
    .excludeCH("Support line separator in UTF-16BE #1")
    .excludeCH("Support line separator in ISO-8859-1 #2")
    .excludeCH("Support line separator in UTF-32LE #3")
    .excludeCH("Support line separator in UTF-8 #4")
    .excludeCH("Support line separator in UTF-32BE #5")
    .excludeCH("Support line separator in CP1251 #6")
    .excludeCH("Support line separator in UTF-16LE #8")
    .excludeCH("Support line separator in UTF-32BE #9")
    .excludeCH("Support line separator in US-ASCII #10")
    .excludeCH("Support line separator in utf-32le #11")
    .excludeCH("lineSep with 2 chars when multiLine set to true")
    .excludeCH("lineSep with 2 chars when multiLine set to false")
    .excludeCH("SPARK-26208: write and read empty data to csv file with headers")
    .excludeCH("Do not reuse last good value for bad input field")
    .excludeCH("SPARK-29101 test count with DROPMALFORMED mode")
    .excludeCH("return correct results when data columns overlap with partition columns")
    .excludeCH("filters push down - malformed input in PERMISSIVE mode")
    .excludeCH("case sensitivity of filters references")
    .excludeCH("SPARK-33566: configure UnescapedQuoteHandling to parse unescaped quotes and unescaped delimiter data correctly")
    .excludeCH("SPARK-36831: Support reading and writing ANSI intervals")
    .excludeCH("SPARK-39469: Infer schema for columns with all dates")
    .excludeCH("SPARK-40474: Infer schema for columns with a mix of dates and timestamp")
    .excludeCH("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
    .excludeCH("SPARK-39731: Handle date and timestamp parsing fallback")
    .excludeCH("SPARK-40215: enable parsing fallback for CSV in CORRECTED mode with a SQL config")
    .excludeCH("SPARK-40496: disable parsing fallback when the date/timestamp format is provided")
    .excludeCH("SPARK-42335: Pass the comment option through to univocity if users set it explicitly in CSV dataSource")
    .excludeCH("SPARK-46862: column pruning in the multi-line mode")
    // Flaky and already excluded in other cases
    .exclude("Gluten - test for FAILFAST parsing mode")

  enableSuite[GlutenCSVv2Suite]
    .exclude("Gluten - test for FAILFAST parsing mode")
    // Rule org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown in batch
    // Early Filter and Projection Push-Down generated an invalid plan
    .exclude("SPARK-26208: write and read empty data to csv file with headers")
    // file cars.csv include null string, Arrow not support to read
    .exclude("old csv data source name works")
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with empty fields with user defined empty values")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
    .excludeCH("SPARK-36831: Support reading and writing ANSI intervals")
  enableSuite[GlutenCTEHintSuite]
  enableSuite[GlutenCTEInlineSuiteAEOff]
  enableSuite[GlutenCTEInlineSuiteAEOn]
  enableSuite[GlutenCachedTableSuite]
    .exclude("A cached table preserves the partitioning and ordering of its cached SparkPlan")
    .includeCH("InMemoryRelation statistics")
    // Extra ColumnarToRow is needed to transform vanilla columnar data to gluten columnar data.
    .includeCH("SPARK-37369: Avoid redundant ColumnarToRow transition on InMemoryTableScan")
    .excludeCH("Gluten - InMemoryRelation statistics")
  enableSuite[GlutenCastWithAnsiOffSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    // Set timezone through config.
    .exclude("data type casting")
    .excludeCH("null cast")
    .excludeCH("cast string to date")
    .excludeCH("cast string to timestamp")
    .excludeGlutenTest("cast string to timestamp")
    .excludeCH("SPARK-22825 Cast array to string")
    .excludeCH("SPARK-33291: Cast array with null elements to string")
    .excludeCH("SPARK-22973 Cast map to string")
    .excludeCH("SPARK-22981 Cast struct to string")
    .excludeCH("SPARK-33291: Cast struct with null elements to string")
    .excludeCH("SPARK-35111: Cast string to year-month interval")
    .excludeCH("Gluten - data type casting")
    .exclude("cast string to date #2")
    .exclude("casting to fixed-precision decimals")
    .exclude("SPARK-28470: Cast should honor nullOnOverflow property")
    .exclude("cast from array II")
    .exclude("cast from map II")
    .exclude("cast from struct II")
    .exclude("cast from date")
    .exclude("cast from timestamp II")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z")
    .exclude("SPARK-34727: cast from float II")
    .exclude("SPARK-39749: cast Decimal to string")
    .exclude("SPARK-42176: cast boolean to timestamp")
    .exclude("null cast #2")
    .exclude("cast from boolean to timestamp")
  enableSuite[GlutenCoalesceShufflePartitionsSuite]
    .excludeByPrefix("determining the number of reducers")
    .excludeCH("SPARK-46590 adaptive query execution works correctly with broadcast join and union")
    .excludeCH("SPARK-46590 adaptive query execution works correctly with cartesian join and union")
    .excludeCH("SPARK-24705 adaptive query execution works correctly when exchange reuse enabled")
    .excludeCH("Do not reduce the number of shuffle partition for repartition")
    .excludeCH("Union two datasets with different pre-shuffle partition number")
    .excludeCH("SPARK-34790: enable IO encryption in AQE partition coalescing")
    .excludeCH("Gluten - determining the number of reducers: aggregate operator(minNumPostShufflePartitions: 5)")
    .excludeCH(
      "Gluten - determining the number of reducers: join operator(minNumPostShufflePartitions: 5)")
    .excludeCH(
      "Gluten - determining the number of reducers: complex query 1(minNumPostShufflePartitions: 5)")
    .excludeCH(
      "Gluten - determining the number of reducers: complex query 2(minNumPostShufflePartitions: 5)")
    .excludeCH("Gluten - determining the number of reducers: plan already partitioned(minNumPostShufflePartitions: 5)")
    .excludeCH("Gluten - determining the number of reducers: aggregate operator")
    .excludeCH("Gluten - determining the number of reducers: join operator")
    .excludeCH("Gluten - determining the number of reducers: complex query 1")
    .excludeCH("Gluten - determining the number of reducers: complex query 2")
    .excludeCH("Gluten - determining the number of reducers: plan already partitioned")
  enableSuite[GlutenCollapseProjectExecTransformerSuite]
    .excludeCH("Gluten - Support ProjectExecTransformer collapse")
  enableSuite[GlutenCollectionExpressionsSuite]
    // Rewrite in Gluten to replace Seq with Array
    .exclude("Shuffle")
    .excludeGlutenTest("Shuffle")
    .excludeCH("Sequence of numbers")
    .excludeCH("Array Insert")
    .excludeCH("SPARK-36753: ArrayExcept should handle duplicated Double.NaN and Float.Nan")
    .excludeCH(
      "SPARK-36740: ArrayMin/ArrayMax/SortArray should handle NaN greater than non-NaN value")
    .excludeCH("SPARK-42401: Array insert of null value (explicit)")
    .excludeCH("SPARK-42401: Array insert of null value (implicit)")
  enableSuite[GlutenColumnExpressionSuite]
    // Velox raise_error('errMsg') throws a velox_user_error exception with the message 'errMsg'.
    // The final caught Spark exception's getCause().getMessage() contains 'errMsg' but does not
    // equal 'errMsg' exactly. The following two tests will be skipped and overridden in Gluten.
    .includeCH("raise_error")
    .includeCH("assert_true")
    .excludeCH("withField should add field with no name")
    .excludeCH("withField should replace all fields with given name in struct")
    .excludeCH("withField user-facing examples")
    .excludeCH("dropFields should drop field with no name in struct")
    .excludeCH("dropFields should drop all fields with given name in struct")
  enableSuite[GlutenComplexTypeSuite]
  enableSuite[GlutenComplexTypesSuite]
  enableSuite[GlutenConditionalExpressionSuite]
    .excludeCH("case when")
  enableSuite[GlutenConfigBehaviorSuite]
    // Will be fixed by cleaning up ColumnarShuffleExchangeExec.
    .exclude("SPARK-22160 spark.sql.execution.rangeExchange.sampleSizePerPartition")
    // Gluten columnar operator will have different number of jobs
    .exclude("SPARK-40211: customize initialNumPartitions for take")
  enableSuite[GlutenCountMinSketchAggQuerySuite]
  enableSuite[GlutenCreateTableAsSelectSuite]
    .exclude("CREATE TABLE USING AS SELECT based on the file without write permission")
    .exclude("create a table, drop it and create another one with the same name")
  enableSuite[GlutenCsvFunctionsSuite]
  enableSuite[GlutenCustomerExtensionSuite]
  enableSuite[GlutenDDLSourceLoadSuite]
  enableSuite[GlutenDSV2CharVarcharTestSuite]
  enableSuite[GlutenDSV2SQLInsertTestSuite]
  enableSuite[GlutenDataFrameAggregateSuite]
    // Test for vanilla spark codegen, not apply for Gluten
    .exclude("SPARK-43876: Enable fast hashmap for distinct queries")
    .exclude(
      "SPARK-26021: NaN and -0.0 in grouping expressions", // NaN case
      // incorrect result, distinct NaN case
      "SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate",
      // Replaced with another test.
      "SPARK-19471: AggregationIterator does not initialize the generated result projection" +
        " before using it"
    )
    .includeCH(
      "zero moments", // [velox does not return NaN]
      // Velox's collect_list / collect_set are by design declarative aggregate so plan check
      // for ObjectHashAggregateExec will fail.
      "SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle",
      "SPARK-31620: agg with subquery (whole-stage-codegen = true)",
      "SPARK-31620: agg with subquery (whole-stage-codegen = false)"
    )
    .excludeCH("linear regression")
    .excludeCH("collect functions")
    .excludeCH("collect functions structs")
    .excludeCH("SPARK-17641: collect functions should not collect null values")
    .excludeCH("collect functions should be able to cast to array type with no null values")
    .excludeCH("SPARK-45599: Neither 0.0 nor -0.0 should be dropped when computing percentile")
    .excludeCH("SPARK-34716: Support ANSI SQL intervals by the aggregate function `sum`")
    .excludeCH("SPARK-34837: Support ANSI SQL intervals by the aggregate function `avg`")
    .excludeCH("SPARK-35412: groupBy of year-month/day-time intervals should work")
    .excludeCH("SPARK-36054: Support group by TimestampNTZ column")
  enableSuite[GlutenDataFrameAsOfJoinSuite]
  enableSuite[GlutenDataFrameComplexTypeSuite]
  enableSuite[GlutenDataFrameFunctionsSuite]
    // blocked by Velox-5768
    .exclude("aggregate function - array for primitive type containing null")
    .exclude("aggregate function - array for non-primitive type")
    // Expected exception org.apache.spark.SparkException to be thrown, but no exception was thrown
    .exclude("map_concat function")
    // Rewrite this test because Velox sorts rows by key for primitive data types, which disrupts the original row sequence.
    .includeCH("map_zip_with function - map of primitive types")
    .excludeCH("map with arrays")
    .excludeCH("flatten function")
    .excludeCH("SPARK-41233: array prepend")
    .excludeCH("array_insert functions")
    .excludeCH("aggregate function - array for primitive type not containing null")
    .excludeCH("transform keys function - primitive data types")
    .excludeCH("transform values function - test primitive data types")
    .excludeCH("transform values function - test empty")
    .excludeCH("SPARK-14393: values generated by non-deterministic functions shouldn't change after coalesce or union")
    .excludeCH("mask function")
  enableSuite[GlutenDataFrameHintSuite]
  enableSuite[GlutenDataFrameImplicitsSuite]
  enableSuite[GlutenDataFrameJoinSuite]
    .excludeCH("SPARK-32693: Compare two dataframes with same schema except nullable property")
  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .includeCH(
      // NaN case
      "replace nan with float",
      "replace nan with double"
    )
  enableSuite[GlutenDataFramePivotSuite]
    // substring issue
    .includeCH("pivot with column definition in groupby")
    // array comparison not supported for values that contain nulls
    .includeCH(
      "pivot with null and aggregate type not supported by PivotFirst returns correct result")
    .excludeCH("SPARK-38133: Grouping by TIMESTAMP_NTZ should not corrupt results")
  enableSuite[GlutenDataFrameRangeSuite]
    .excludeCH("SPARK-20430 Initialize Range parameters in a driver side")
    .excludeByPrefix("Cancelling stage in a query with Range")
  enableSuite[GlutenDataFrameSelfJoinSuite]
  enableSuite[GlutenDataFrameSessionWindowingSuite]
    .excludeCH("simple session window with record at window start")
    .excludeCH("session window groupBy statement")
    .excludeCH("session window groupBy with multiple keys statement")
    .excludeCH("session window groupBy with multiple keys statement - two distinct")
    .excludeCH(
      "session window groupBy with multiple keys statement - keys overlapped with sessions")
    .excludeCH("SPARK-36465: filter out events with negative/zero gap duration")
    .excludeCH("SPARK-36724: Support timestamp_ntz as a type of time column for SessionWindow")
    // TODO: fix the hanging problem in GLUTEN-8890 followup
    .excludeCH(
      "SPARK-49836 using window fn with window as parameter should preserve parent operator")
  enableSuite[GlutenDataFrameSetOperationsSuite]
    .exclude("SPARK-37371: UnionExec should support columnar if all children support columnar")
    // Result depends on the implementation for nondeterministic expression rand.
    // Not really an issue.
    .exclude("SPARK-10740: handle nondeterministic expressions correctly for set operations")
    .excludeCH("union should union DataFrames with UDTs (SPARK-13410)")
    .excludeCH(
      "SPARK-35756: unionByName support struct having same col names but different sequence")
    .excludeCH("SPARK-36673: Only merge nullability for Unions of struct")
    .excludeCH("SPARK-36797: Union should resolve nested columns as top-level columns")
  enableSuite[GlutenDataFrameStatSuite]
    .excludeCH("SPARK-30532 stat functions to understand fully-qualified column name")
    .excludeCH("special crosstab elements (., '', null, ``)")
  enableSuite[GlutenDataFrameSuite]
    // Rewrite these tests because it checks Spark's physical operators.
    .excludeByPrefix("SPARK-22520", "reuse exchange")
    .exclude(
      /**
       * Rewrite these tests because the rdd partition is equal to the configuration
       * "spark.sql.shuffle.partitions".
       */
      "repartitionByRange",
      // Rewrite this test because the describe functions creates unmatched plan.
      "describe",
      // decimal failed ut.
      "SPARK-22271: mean overflows and returns null for some decimal variables",
      // Result depends on the implementation for nondeterministic expression rand.
      // Not really an issue.
      "SPARK-9083: sort with non-deterministic expressions"
    )
    .includeCH(
      // Mismatch when max NaN and infinite value
      "NaN is greater than all other non-NaN numeric values",
      "distributeBy and localSort"
    )
    // test for sort node not present but gluten uses shuffle hash join
    .exclude("SPARK-41048: Improve output partitioning and ordering with AQE cache")
    .exclude("SPARK-28224: Aggregate sum big decimal overflow")
    // Rewrite this test since it checks the physical operator which is changed in Gluten
    .excludeCH("SPARK-27439: Explain result should match collected result after view change")
    .excludeCH("SPARK-28067: Aggregate sum should not return wrong results for decimal overflow")
    .excludeCH("SPARK-35955: Aggregate avg should not return wrong results for decimal overflow")
    .excludeCH("summary")
    .excludeGlutenTest(
      "SPARK-27439: Explain result should match collected result after view change")
    .excludeCH(
      "SPARK-8608: call `show` on local DataFrame with random columns should return same value")
    .excludeCH(
      "SPARK-8609: local DataFrame with random columns should return same value after sort")
    .excludeCH("SPARK-10316: respect non-deterministic expressions in PhysicalOperation")
    .excludeCH("Uuid expressions should produce same results at retries in the same DataFrame")
    .excludeCH("Gluten - repartitionByRange")
    .excludeCH("Gluten - describe")
    .excludeCH("Gluten - Allow leading/trailing whitespace in string before casting")
  enableSuite[GlutenDataFrameTimeWindowingSuite]
    .excludeCH("simple tumbling window with record at window start")
    .excludeCH("SPARK-21590: tumbling window using negative start time")
    .excludeCH("tumbling window groupBy statement")
    .excludeCH("tumbling window groupBy statement with startTime")
    .excludeCH("SPARK-21590: tumbling window groupBy statement with negative startTime")
    .excludeCH("sliding window grouping")
    .excludeCH("time window joins")
    .excludeCH("millisecond precision sliding windows")
  enableSuite[GlutenDataFrameToSchemaSuite]
    .excludeCH("struct value: compatible field nullability")
    .excludeCH("map value: reorder inner fields by name")
  enableSuite[GlutenDataFrameTungstenSuite]
  enableSuite[GlutenDataFrameWindowFramesSuite]
    // Local window fixes are not added.
    .exclude("range between should accept int/long values as boundary")
    .includeCH("unbounded preceding/following range between with aggregation")
    .includeCH("sliding range between with aggregation")
    .exclude("store and retrieve column stats in different time zones")
    .excludeCH("rows between should accept int/long values as boundary")
    .excludeCH("reverse preceding/following range between with aggregation")
    .excludeCH(
      "SPARK-41793: Incorrect result for window frames defined by a range clause on large decimals")
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
    .excludeCH("SPARK-13860: corr, covar_pop, stddev_pop functions in specific window LEGACY_STATISTICAL_AGGREGATE off")
    .excludeCH("SPARK-13860: covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window LEGACY_STATISTICAL_AGGREGATE off")
    .excludeCH("lead/lag with ignoreNulls")
    .excludeCH("SPARK-37099: Insert window group limit node for top-k computation")
    .excludeCH("Gluten - corr, covar_pop, stddev_pop functions in specific window")
  enableSuite[GlutenDataFrameWriterV2Suite]
  enableSuite[GlutenDataSourceStrategySuite]
  enableSuite[GlutenDataSourceSuite]
  enableSuite[GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2DataFrameSuite]
  enableSuite[GlutenDataSourceV2FunctionSuite]
    .excludeCH("view should use captured catalog and namespace for function lookup")
    .excludeCH("aggregate function: lookup int average")
    .excludeCH("aggregate function: lookup long average")
    .excludeCH("aggregate function: lookup double average in Java")
    .excludeCH("aggregate function: lookup int average w/ expression")
    .excludeCH("SPARK-35390: aggregate function w/ type coercion")
  enableSuite[GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSuiteV1Filter]
    .excludeCH("DeleteFrom with v2 filtering: fail if has subquery")
    .excludeCH("DeleteFrom with v2 filtering: delete with unsupported predicates")
    .excludeCH("SPARK-33652: DeleteFrom should refresh caches referencing the table")
    .excludeCH("DeleteFrom: - delete with invalid predicate")
  enableSuite[GlutenDataSourceV2SQLSuiteV2Filter]
    .excludeCH("DeleteFrom with v2 filtering: fail if has subquery")
    .excludeCH("DeleteFrom with v2 filtering: delete with unsupported predicates")
    .excludeCH("SPARK-33652: DeleteFrom should refresh caches referencing the table")
  enableSuite[GlutenDataSourceV2StrategySuite]
  enableSuite[GlutenDataSourceV2Suite]
    // Rewrite the following tests in GlutenDataSourceV2Suite.
    .exclude("partitioning reporting")
    .exclude("ordering and partitioning reporting")
  enableSuite[GlutenDatasetAggregatorSuite]
  enableSuite[GlutenDatasetCacheSuite]
    // Disable this since coalesece union clauses rule will rewrite the query.
    .exclude("SPARK-44653: non-trivial DataFrame unions should not break caching")
  enableSuite[GlutenDatasetOptimizationSuite]
  enableSuite[GlutenDatasetPrimitiveSuite]
  enableSuite[GlutenDatasetSerializerRegistratorSuite]
  enableSuite[GlutenDatasetSuite]
    // Rewrite the following two tests in GlutenDatasetSuite.
    .exclude("dropDuplicates: columns with same column name")
    .exclude("groupBy.as")
    .exclude("dropDuplicates")
    .exclude("select 2, primitive and tuple")
    .exclude("SPARK-16853: select, case class and tuple")
    // TODO: SPARK-16995 may dead loop!!
    .exclude("SPARK-16995: flat mapping on Dataset containing a column created with lit/expr")
    .exclude("SPARK-24762: typed agg on Option[Product] type")
    .exclude("SPARK-40407: repartition should not result in severe data skew")
    .exclude("SPARK-40660: Switch to XORShiftRandom to distribute elements")
  enableSuite[GlutenDatasetUnpivotSuite]
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
    .exclude("SPARK-42635: timestampadd near daylight saving transition")
    // https://github.com/facebookincubator/velox/pull/10563/files#diff-140dc50e6dac735f72d29014da44b045509df0dd1737f458de1fe8cfd33d8145
    .excludeGlutenTest("from_unixtime")
    .excludeCH("DayOfYear")
    .excludeCH("Quarter")
    .excludeCH("Month")
    .excludeCH("Day / DayOfMonth")
    .excludeCH("DayOfWeek")
    .excludeCH("WeekDay")
    .excludeCH("WeekOfYear")
    .excludeCH("add_months")
    .excludeCH("months_between")
    .excludeCH("TruncDate")
    .excludeCH("unsupported fmt fields for trunc/date_trunc results null")
    .excludeCH("to_utc_timestamp")
    .excludeCH("from_utc_timestamp")
    .excludeCH("SPARK-31896: Handle am-pm timestamp parsing when hour is missing")
    .excludeCH("UNIX_SECONDS")
    .excludeCH("TIMESTAMP_SECONDS")
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
    .excludeCH("SPARK-30766: date_trunc of old timestamps to hours and days")
    .excludeCH("SPARK-30793: truncate timestamps before the epoch to seconds and minutes")
    .excludeCH("try_to_timestamp")
    .excludeCH("Gluten - to_unix_timestamp")
  enableSuite[GlutenDecimalExpressionSuite]
  enableSuite[GlutenDecimalPrecisionSuite]
  enableSuite[GlutenDeleteFromTableSuite]
  enableSuite[GlutenDeltaBasedDeleteFromTableSuite]
  enableSuite[GlutenDeltaBasedMergeIntoTableSuite]
  enableSuite[GlutenDeltaBasedMergeIntoTableUpdateAsDeleteAndInsertSuite]
  enableSuite[GlutenDeltaBasedUpdateAsDeleteAndInsertTableSuite]
    // FIXME: complex type result mismatch
    .includeCH("update nested struct fields")
    .includeCH("update char/varchar columns")
  enableSuite[GlutenDeltaBasedUpdateTableSuite]
  enableSuite[GlutenDeprecatedAPISuite]
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite]
    .disable(
      "DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type")
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff]
    .excludeGlutenTest("Subquery reuse across the whole plan")
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOffDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOnDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOffDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOnDisableScan]
  enableSuite[GlutenEmptyInSuite]
    .excludeCH("IN with empty list")
  enableSuite[GlutenEnsureRequirementsSuite]
  enableSuite[GlutenExchangeSuite]
    // ColumnarShuffleExchangeExec does not support doExecute() method
    .exclude("shuffling UnsafeRows in exchange")
    // ColumnarShuffleExchangeExec does not support SORT_BEFORE_REPARTITION
    .exclude("SPARK-23207: Make repartition() generate consistent output")
    // This test will re-run in GlutenExchangeSuite with shuffle partitions > 1
    .exclude("Exchange reuse across the whole plan")
  enableSuite[GlutenExistenceJoinSuite]
    .excludeCH("test single condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("test single condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("test single condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("test single condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left semi join using BroadcastHashJoin (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left semi join using BroadcastHashJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build left")
    .excludeCH("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
    .excludeCH("test composed condition (equal & non-equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("test composed condition (equal & non-equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("test composed condition (equal & non-equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("test composed condition (equal & non-equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("test single condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("test single condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("test single condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("test single condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left anti join using BroadcastHashJoin (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left anti join using BroadcastHashJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build left")
    .excludeCH("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .excludeCH("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
    .excludeCH("test composed condition (equal & non-equal) test for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("test composed condition (equal & non-equal) test for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("test composed condition (equal & non-equal) test for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("test composed condition (equal & non-equal) test for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("test composed unique condition (both non-equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("test composed unique condition (both non-equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("test composed unique condition (both non-equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("test composed unique condition (both non-equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
  enableSuite[GlutenExpressionsSchemaSuite]
  enableSuite[GlutenExternalCommandRunnerSuite]
  enableSuite[GlutenExtraStrategiesSuite]
  enableSuite[GlutenFallbackSuite]
    .excludeCH("Gluten - test fallback event")
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
    .includeCH("Return correct results when data columns overlap with partition columns")
    .includeCH("Return correct results when data columns overlap with partition " +
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
    .excludeCH("SPARK-23072 Write and read back unicode column names - csv")
    .excludeCH("Enabling/disabling ignoreMissingFiles using csv")
    .excludeCH("SPARK-30362: test input metrics for DSV2")
    .excludeCH("SPARK-35669: special char in CSV header with filter pushdown")
    .excludeCH("Gluten - Spark native readers should respect spark.sql.caseSensitive - parquet")
    .excludeCH("Gluten - SPARK-25237 compute correct input metrics in FileScanRDD")
    .excludeCH("Gluten - Enabling/disabling ignoreMissingFiles using orc")
    .excludeCH("Gluten - Enabling/disabling ignoreMissingFiles using parquet")
  enableSuite[GlutenFileDataSourceV2FallBackSuite]
    // Rewritten
    .exclude("Fallback Parquet V2 to V1")
  enableSuite[GlutenFileFormatWriterSuite]
    // TODO: fix "empty file should be skipped while write to file"
    .excludeCH("empty file should be skipped while write to file")
  enableSuite[GlutenFileIndexSuite]
  enableSuite[GlutenFileMetadataStructSuite]
  enableSuite[GlutenFileScanSuite]
  enableSuite[GlutenFileSourceCharVarcharTestSuite]
    .includeCH("length check for input string values: nested in array")
    .includeCH("length check for input string values: nested in array")
    .includeCH("length check for input string values: nested in map key")
    .includeCH("length check for input string values: nested in map value")
    .includeCH("length check for input string values: nested in both map key and value")
    .includeCH("length check for input string values: nested in array of struct")
    .includeCH("length check for input string values: nested in array of array")
  enableSuite[GlutenFileSourceCustomMetadataStructSuite]
  enableSuite[GlutenFileSourceSQLInsertTestSuite]
    .excludeCH("SPARK-33474: Support typed literals as partition spec values")
    .excludeCH(
      "SPARK-34556: checking duplicate static partition columns should respect case sensitive conf")
  enableSuite[GlutenFileSourceStrategySuite]
    // Plan comparison.
    .exclude("partitioned table - after scan filters")
    .excludeCH("unpartitioned table, single partition")
    .excludeCH("SPARK-32019: Add spark.sql.files.minPartitionNum config")
    .excludeCH(
      "SPARK-32352: Partially push down support data filter if it mixed in partition filters")
    .excludeCH("SPARK-44021: Test spark.sql.files.maxPartitionNum works as expected")
  enableSuite[GlutenFileTableSuite]
  enableSuite[GlutenFilteredScanSuite]
  enableSuite[GlutenFiltersSuite]
  enableSuite[GlutenGeneratorFunctionSuite]
    .exclude("SPARK-45171: Handle evaluated nondeterministic expression")
    .excludeCH("single explode_outer")
    .excludeCH("single posexplode_outer")
    .excludeCH("explode_outer and other columns")
    .excludeCH("aliased explode_outer")
    .excludeCH("explode_outer on map")
    .excludeCH("explode_outer on map with aliases")
    .excludeCH("SPARK-40963: generator output has correct nullability")
    .excludeCH("Gluten - SPARK-45171: Handle evaluated nondeterministic expression")
  enableSuite[GlutenGroupBasedDeleteFromTableSuite]
  enableSuite[GlutenGroupBasedMergeIntoTableSuite]
  enableSuite[GlutenHadoopFileLinesReaderSuite]
  enableSuite[GlutenHashExpressionsSuite]
    .excludeCH("sha2")
    .excludeCH("SPARK-30633: xxHash with different type seeds")
  enableSuite[GlutenHeaderCSVReadSchemaSuite]
    .excludeCH("append column at the end")
    .excludeCH("hide column at the end")
    .excludeCH("change column type from byte to short/int/long")
    .excludeCH("change column type from short to int/long")
    .excludeCH("change column type from int to long")
    .excludeCH("read byte, int, short, long together")
    .excludeCH("change column type from float to double")
    .excludeCH("read float and double together")
    .excludeCH("change column type from float to decimal")
    .excludeCH("change column type from double to decimal")
    .excludeCH("read float, double, decimal together")
    .excludeCH("read as string")
  enableSuite[GlutenHigherOrderFunctionsSuite]
    .excludeCH("ArraySort")
    .excludeCH("ArrayAggregate")
    .excludeCH("TransformKeys")
    .excludeCH("TransformValues")
    .excludeCH("SPARK-39419: ArraySort should throw an exception when the comparator returns null")
  enableSuite[GlutenHiveSQLQueryCHSuite]
  enableSuite[GlutenInjectRuntimeFilterSuite]
    // FIXME: yan
    .includeCH("Merge runtime bloom filters")
    .excludeGlutenTest("GLUTEN-9849: bloom filter applied to partition filter")
  enableSuite[GlutenInnerJoinSuiteForceShjOff]
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("inner join, one match per row using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("inner join, one match per row using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("inner join, multiple matches using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("inner join, multiple matches using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH(
      "inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH(
      "inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("inner join, null safe using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("inner join, null safe using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using CartesianProduct")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
  enableSuite[GlutenInnerJoinSuiteForceShjOn]
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("inner join, one match per row using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("inner join, one match per row using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("inner join, multiple matches using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("inner join, multiple matches using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH(
      "inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH(
      "inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH(
      "inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("inner join, null safe using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("inner join, null safe using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using CartesianProduct")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen on)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .excludeCH("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
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
    .excludeCH("Gluten - insert partition table")
    .excludeCH("Gluten - remove v1writes sort and project")
    .excludeCH("Gluten - remove v1writes sort")
    .excludeCH("Gluten - do not remove non-v1writes sort and project")
    .excludeCH(
      "Gluten - SPARK-35106: Throw exception when rename custom partition paths returns false")
    .excludeCH(
      "Gluten - Do not fallback write files if output columns contain Spark internal metadata")
    .excludeCH("Gluten - Add metadata white list to allow native write files")
    .excludeCH("Gluten - INSERT rows, ALTER TABLE ADD COLUMNS with DEFAULTs, then SELECT them")
  enableSuite[GlutenIntervalExpressionsSuite]
  enableSuite[GlutenIntervalFunctionsSuite]
  enableSuite[GlutenJoinSuite]
    // exclude as it check spark plan
    .exclude("SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join")
    .excludeCH(
      "SPARK-45882: BroadcastHashJoinExec propagate partitioning should respect CoalescedHashPartitioning")
  enableSuite[GlutenJsonExpressionsSuite]
    // https://github.com/apache/incubator-gluten/issues/8102
    .includeCH("$.store.book")
    .includeCH("$")
    .includeCH("$.store.book[0]")
    .includeCH("$.store.book[*]")
    .includeCH("$.store.book[*].category")
    .includeCH("$.store.book[*].isbn")
    .includeCH("$.store.book[*].reader")
    .includeCH("$.store.basket[*]")
    .includeCH("$.store.basket[*][0]")
    .includeCH("$.store.basket[0][*]")
    .includeCH("$.store.basket[*][*]")
    .exclude("$.store.basket[0][*].b")
    // Exception class different.
    .exclude("from_json - invalid data")
    .excludeCH("from_json - input=object, schema=array, output=array of single row")
    .excludeCH("from_json - input=empty object, schema=array, output=array of single row with null")
    .excludeCH("from_json - input=array of single object, schema=struct, output=single row")
    .excludeCH("from_json - input=array, schema=struct, output=single row")
    .excludeCH("from_json - input=empty array, schema=struct, output=single row with null")
    .excludeCH("from_json - input=empty object, schema=struct, output=single row with null")
    .excludeCH("SPARK-20549: from_json bad UTF-8")
    .excludeCH("from_json with timestamp")
    .excludeCH("to_json - struct")
    .excludeCH("to_json - array")
    .excludeCH("to_json - array with single empty row")
    .excludeCH("to_json with timestamp")
    .excludeCH("SPARK-21513: to_json support map[string, struct] to json")
    .excludeCH("SPARK-21513: to_json support map[struct, struct] to json")
    .excludeCH("parse date with locale")
    .excludeCH("parse decimals using locale")
  enableSuite[GlutenJsonFunctionsSuite]
    // * in get_json_object expression not supported in velox
    .exclude("SPARK-42782: Hive compatibility check for get_json_object")
    // Velox does not support single quotes in get_json_object function.
    .includeCH("function get_json_object - support single quotes")
    .excludeCH("from_json with option (allowComments)")
    .excludeCH("from_json with option (allowUnquotedFieldNames)")
    .excludeCH("from_json with option (allowSingleQuotes)")
    .excludeCH("from_json with option (allowNumericLeadingZeros)")
    .excludeCH("from_json with option (allowBackslashEscapingAnyCharacter)")
    .excludeCH("from_json with option (dateFormat)")
    .excludeCH("from_json with option (allowUnquotedControlChars)")
    .excludeCH("from_json with option (allowNonNumericNumbers)")
    .excludeCH("from_json missing columns")
    .excludeCH("from_json invalid json")
    .excludeCH("from_json array support")
    .excludeCH("to_json with option (timestampFormat)")
    .excludeCH("to_json with option (dateFormat)")
    .excludeCH("SPARK-19637 Support to_json in SQL")
    .excludeCH("pretty print - roundtrip from_json -> to_json")
    .excludeCH("from_json invalid json - check modes")
    .excludeCH("SPARK-36069: from_json invalid json schema - check field name and field value")
    .excludeCH("corrupt record column in the middle")
    .excludeCH("parse timestamps with locale")
    .excludeCH("SPARK-33134: return partial results only for root JSON objects")
    .excludeCH("SPARK-40646: return partial results for JSON arrays with objects")
    .excludeCH("SPARK-40646: return partial results for JSON maps")
    .excludeCH("SPARK-40646: return partial results for objects with values as JSON arrays")
    .excludeCH("SPARK-48863: parse object as an array with partial results enabled")
    .excludeCH("SPARK-33907: bad json input with json pruning optimization: GetStructField")
    .excludeCH("SPARK-33907: bad json input with json pruning optimization: GetArrayStructFields")
    .excludeCH("SPARK-33907: json pruning optimization with corrupt record field")
  enableSuite[GlutenJsonLegacyTimeParserSuite]
    .includeCH("Complex field and type inferring")
    .includeCH("SPARK-4228 DataFrame to JSON")
    .excludeCH("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .excludeCH("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .excludeCH("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenJsonReadSchemaSuite]
  enableSuite[GlutenJsonV1Suite]
    // FIXME: Array direct selection fails
    .includeCH("Complex field and type inferring")
    .includeCH("SPARK-4228 DataFrame to JSON")
    .excludeCH("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .excludeCH("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .excludeCH("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenJsonV2Suite]
    // exception test
    .exclude("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
    .includeCH("Complex field and type inferring")
    .includeCH("SPARK-4228 DataFrame to JSON")
    .excludeCH("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .excludeCH("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .excludeCH("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenKeyGroupedPartitioningSuite]
    // NEW SUITE: disable as they check vanilla spark plan
    .exclude("partitioned join: number of buckets mismatch should trigger shuffle")
    .exclude("partitioned join: only one side reports partitioning")
    .exclude("partitioned join: join with two partition keys and different # of partition keys")
    // disable due to check for SMJ node
    .excludeByPrefix("SPARK-41413: partitioned join:")
    .excludeByPrefix("SPARK-42038: partially clustered:")
    .exclude("SPARK-44641: duplicated records when SPJ is not triggered")
    .excludeCH("Gluten - partitioned join: only one side reports partitioning")
    .excludeCH("Gluten - SPARK-41413: partitioned join: partition values from one side are subset of those from the other side")
    .excludeCH("Gluten - SPARK-41413: partitioned join: partition values from both sides overlaps")
    .excludeCH(
      "Gluten - SPARK-41413: partitioned join: non-overlapping partition values from both sides")
    .excludeCH("Gluten - SPARK-42038: partially clustered: with different partition keys and both sides partially clustered")
    .excludeCH("Gluten - SPARK-42038: partially clustered: with different partition keys and missing keys on left-hand side")
    .excludeCH("Gluten - SPARK-42038: partially clustered: with different partition keys and missing keys on right-hand side")
    .excludeCH("Gluten - SPARK-42038: partially clustered: left outer join")
    .excludeCH("Gluten - SPARK-42038: partially clustered: right outer join")
    .excludeCH("Gluten - SPARK-42038: partially clustered: full outer join is not applicable")
    .excludeCH("Gluten - SPARK-44641: duplicated records when SPJ is not triggered")
    .excludeCH(
      "Gluten - partitioned join: join with two partition keys and different # of partition keys")
  enableSuite[GlutenLateralColumnAliasSuite]
    .excludeCH("Lateral alias conflicts with table column - Project")
    .excludeCH("Lateral alias conflicts with table column - Aggregate")
    .excludeCH("Lateral alias of a complex type")
    .excludeCH("Lateral alias reference works with having and order by")
    .excludeCH("Lateral alias basics - Window on Project")
    .excludeCH("Lateral alias basics - Window on Aggregate")
  enableSuite[GlutenLiteralExpressionSuite]
    .exclude("default")
    // FIXME(yma11): ObjectType is not covered in RowEncoder/Serializer in vanilla spark
    .exclude("SPARK-37967: Literal.create support ObjectType")
  enableSuite[GlutenLocalBroadcastExchangeSuite]
    .excludeCH("SPARK-39983 - Broadcasted relation is not cached on the driver")
  enableSuite[GlutenLocalScanSuite]
  enableSuite[GlutenMathExpressionsSuite]
    // Spark round UT for round(3.1415,3) is not correct.
    .exclude("round/bround/floor/ceil")
    .excludeCH("tanh")
    .excludeCH("unhex")
    .excludeCH("atan2")
    .excludeCH("SPARK-42045: integer overflow in round/bround")
    .excludeCH("Gluten - round/bround/floor/ceil")
  enableSuite[GlutenMathFunctionsSuite]
  enableSuite[GlutenMergedOrcReadSchemaSuite]
    .includeCH("append column into middle")
    .includeCH("add a nested column at the end of the leaf struct column")
    .includeCH("add a nested column in the middle of the leaf struct column")
    .includeCH("add a nested column at the end of the middle struct column")
    .includeCH("add a nested column in the middle of the middle struct column")
    .includeCH("hide a nested column at the end of the leaf struct column")
    .includeCH("hide a nested column in the middle of the leaf struct column")
    .includeCH("hide a nested column at the end of the middle struct column")
    .includeCH("hide a nested column in the middle of the middle struct column")
    .includeCH("change column type from boolean to byte/short/int/long")
    .includeCH("change column type from byte to short/int/long")
    .includeCH("change column type from short to int/long")
    .includeCH("change column type from int to long")
    .includeCH("read byte, int, short, long together")
    .includeCH("change column type from float to double")
    .includeCH("read float and double together")
  enableSuite[GlutenMergedParquetReadSchemaSuite]
  enableSuite[GlutenMetadataCacheSuite]
    .exclude("SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException")
  enableSuite[GlutenMetadataColumnSuite]
    .excludeCH("SPARK-34923: propagate metadata columns through Sort")
    .excludeCH("SPARK-34923: propagate metadata columns through RepartitionBy")
    .excludeCH("SPARK-40149: select outer join metadata columns with DataFrame API")
    .excludeCH("SPARK-42683: Project a metadata column by its logical name - column not found")
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenMiscFunctionsSuite]
  enableSuite[GlutenNestedDataSourceV1Suite]
  enableSuite[GlutenNestedDataSourceV2Suite]
  enableSuite[GlutenNondeterministicSuite]
    .exclude("MonotonicallyIncreasingID")
    .exclude("SparkPartitionID")
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenOrcCodecSuite]
  enableSuite[GlutenOrcColumnarBatchReaderSuite]
  enableSuite[GlutenOrcFilterSuite]
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcPartitionDiscoverySuite]
    .includeCH("read partitioned table - normal case")
    .includeCH("read partitioned table - with nulls")
  enableSuite[GlutenOrcReadSchemaSuite]
    .includeCH("append column into middle")
    .includeCH("hide column in the middle")
    .includeCH("change column position")
    .exclude("change column type from boolean to byte/short/int/long")
    .exclude("read as string")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("read byte, int, short, long together")
    .exclude("change column type from float to double")
    .exclude("read float and double together")
    .exclude("change column type from float to decimal")
    .exclude("change column type from double to decimal")
    .exclude("read float, double, decimal together")
    .includeCH("add a nested column at the end of the leaf struct column")
    .includeCH("add a nested column in the middle of the leaf struct column")
    .includeCH("add a nested column at the end of the middle struct column")
    .includeCH("add a nested column in the middle of the middle struct column")
    .includeCH("hide a nested column at the end of the leaf struct column")
    .includeCH("hide a nested column in the middle of the leaf struct column")
    .includeCH("hide a nested column at the end of the middle struct column")
    .includeCH("hide a nested column in the middle of the middle struct column")
  enableSuite[GlutenOrcSourceSuite]
    // Rewrite to disable Spark's columnar reader.
    .exclude("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .exclude("SPARK-31238, SPARK-31423: rebasing dates in write")
    .exclude("SPARK-31284: compatibility with Spark 2.4 in reading timestamps")
    .exclude("SPARK-31284, SPARK-31423: rebasing timestamps in write")
    .includeCH("SPARK-34862: Support ORC vectorized reader for nested column")
    // Ignored to disable vectorized reading check.
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .includeCH("create temporary orc table")
    .includeCH("create temporary orc table as")
    .includeCH("appending insert")
    .includeCH("overwrite insert")
    .includeCH("SPARK-34897: Support reconcile schemas based on index after nested column pruning")
    .excludeGlutenTest("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .excludeGlutenTest("SPARK-31238, SPARK-31423: rebasing dates in write")
    .excludeGlutenTest("SPARK-34862: Support ORC vectorized reader for nested column")
    // exclude as struct not supported
    .includeCH("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle a column name which consists of only numbers")
    .includeCH("SPARK-37812: Reuse result row when deserializing a struct")
    // rewrite
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=true)")
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
    .excludeCH("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false, spark.sql.orc.enableNestedColumnVectorizedReader=true)")
    .excludeCH("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
    .excludeCH("Gluten - SPARK-31284: compatibility with Spark 2.4 in reading timestamps")
    .excludeCH("Gluten - SPARK-31284, SPARK-31423: rebasing timestamps in write")
    .excludeCH("Gluten - SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
  enableSuite[GlutenOrcV1FilterSuite]
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcV1AggregatePushDownSuite]
    .includeCH("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenOrcV1PartitionDiscoverySuite]
    .includeCH("read partitioned table - normal case")
    .includeCH("read partitioned table - with nulls")
    .includeCH("read partitioned table - partition key included in orc file")
    .includeCH("read partitioned table - with nulls and partition keys are included in Orc file")
  enableSuite[GlutenOrcV1QuerySuite]
    // Rewrite to disable Spark's columnar reader.
    .includeCH("Simple selection form ORC table")
    .includeCH("simple select queries")
    .includeCH("overwriting")
    .includeCH("self-join")
    .includeCH("columns only referenced by pushed down filters should remain")
    .includeCH("SPARK-5309 strings stored using dictionary compression in orc")
    // For exception test.
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
    .includeCH("Read/write binary data")
    .includeCH("Read/write all types with non-primitive type")
    .includeCH("Creating case class RDD table")
    .includeCH("save and load case class RDD with `None`s as orc")
    .includeCH("SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when" +
      " compression is unset")
    .includeCH("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)")
    .includeCH("appending")
    .includeCH("nested data - struct with array field")
    .includeCH("nested data - array of struct")
    .includeCH("SPARK-9170: Don't implicitly lowercase of user-provided columns")
    .includeCH("SPARK-10623 Enable ORC PPD")
    .includeCH("SPARK-14962 Produce correct results on array type with isnotnull")
    .includeCH("SPARK-15198 Support for pushing down filters for boolean types")
    .includeCH("Support for pushing down filters for decimal types")
    .includeCH("Support for pushing down filters for timestamp types")
    .includeCH("column nullability and comment - write and then read")
    .includeCH("Empty schema does not read data from ORC file")
    .includeCH("read from multiple orc input paths")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .includeCH("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .includeCH("LZO compression options for writing to an ORC file")
    .includeCH("Schema discovery on empty ORC files")
    .includeCH("SPARK-21791 ORC should support column names with dot")
    .includeCH("SPARK-25579 ORC PPD should support column names with dot")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    .includeCH("SPARK-37728: Reading nested columns with ORC vectorized reader should not")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .includeCH("Read/write all timestamp types")
    .includeCH("SPARK-37463: read/write Timestamp ntz to Orc with different time zone")
    .includeCH("SPARK-39381: Make vectorized orc columar writer batch size configurable")
    .includeCH("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
    .excludeCH("SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException")
  enableSuite[GlutenOrcV1SchemaPruningSuite]
    .includeCH(
      "Spark vectorized reader - without partition data column - select only top-level fields")
    .includeCH(
      "Spark vectorized reader - with partition data column - select only top-level fields")
    .includeCH("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after join")
    .includeCH("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after join")
    .includeCH("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after outer join")
    .includeCH("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after outer join")
    // Vectorized reading.
    .includeCH("Spark vectorized reader - without partition data column - " +
      "select only expressions without references")
    .includeCH("Spark vectorized reader - with partition data column - " +
      "select only expressions without references")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH(
      "Spark vectorized reader - without partition data column - select a single complex field")
    .includeCH(
      "Spark vectorized reader - with partition data column - select a single complex field")
    .includeCH(
      "Non-vectorized reader - without partition data column - select a single complex field")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field and its parent struct")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field and its parent struct")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field and its parent struct")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field and its parent struct")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field and the partition column")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field and the partition column")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field and the partition column")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field and the partition column")
    .includeCH("Spark vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .includeCH("Spark vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .includeCH("Non-vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .includeCH("Non-vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .includeCH(
      "Spark vectorized reader - without partition data column - no unnecessary schema pruning")
    .includeCH(
      "Spark vectorized reader - with partition data column - no unnecessary schema pruning")
    .includeCH(
      "Non-vectorized reader - without partition data column - no unnecessary schema pruning")
    .includeCH("Non-vectorized reader - with partition data column - no unnecessary schema pruning")
    .includeCH(
      "Spark vectorized reader - without partition data column - empty schema intersection")
    .includeCH("Spark vectorized reader - with partition data column - empty schema intersection")
    .includeCH("Non-vectorized reader - without partition data column - empty schema intersection")
    .includeCH("Non-vectorized reader - with partition data column - empty schema intersection")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .includeCH("Spark vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Spark vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Non-vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Non-vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Spark vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .includeCH("Spark vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .includeCH("Non-vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .includeCH("Non-vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .includeCH("Spark vectorized reader - without partition data column - select explode of nested field of array of struct")
    .includeCH("Spark vectorized reader - with partition data column - select explode of nested field of array of struct")
    .includeCH("Non-vectorized reader - without partition data column - select explode of nested field of array of struct")
    .includeCH("Non-vectorized reader - with partition data column - select explode of nested field of array of struct")
    .includeCH("Non-vectorized reader - without partition data column - select one deep nested complex field after join")
    .includeCH("Non-vectorized reader - with partition data column - select one deep nested complex field after join")
    .includeCH("Non-vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .includeCH("Non-vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .includeCH("Spark vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Non-vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Spark vectorized reader - without partition data column - select nested field in window function")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in window function")
    .includeCH("Non-vectorized reader - without partition data column - select nested field in window function")
    .includeCH(
      "Non-vectorized reader - with partition data column - select nested field in window function")
    .includeCH("Spark vectorized reader - without partition data column - select nested field in window function and then order by")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in window function and then order by")
    .includeCH("Non-vectorized reader - without partition data column - select nested field in window function and then order by")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in window function and then order by")
    .includeCH(
      "Spark vectorized reader - without partition data column - select nested field in Sort")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in Sort")
    .includeCH(
      "Non-vectorized reader - without partition data column - select nested field in Sort")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in Sort")
    .includeCH(
      "Spark vectorized reader - without partition data column - select nested field in Expand")
    .includeCH(
      "Spark vectorized reader - with partition data column - select nested field in Expand")
    .includeCH(
      "Non-vectorized reader - without partition data column - select nested field in Expand")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in Expand")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Case-sensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .excludeCH(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .excludeCH("Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenOrcV2AggregatePushDownSuite]
    .includeCH("nested column: Max(top level column) not push down")
    .includeCH("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenOrcV2QuerySuite]
    .includeCH("Read/write binary data")
    .includeCH("Read/write all types with non-primitive type")
    // Rewrite to disable Spark's columnar reader.
    .includeCH("Simple selection form ORC table")
    .includeCH("Creating case class RDD table")
    .includeCH("save and load case class RDD with `None`s as orc")
    .includeCH(
      "SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when compression is unset")
    .includeCH("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)")
    .includeCH("appending")
    .includeCH("nested data - struct with array field")
    .includeCH("nested data - array of struct")
    .includeCH("SPARK-9170: Don't implicitly lowercase of user-provided columns")
    .includeCH("SPARK-10623 Enable ORC PPD")
    .includeCH("SPARK-14962 Produce correct results on array type with isnotnull")
    .includeCH("SPARK-15198 Support for pushing down filters for boolean types")
    .includeCH("Support for pushing down filters for decimal types")
    .includeCH("Support for pushing down filters for timestamp types")
    .includeCH("column nullability and comment - write and then read")
    .includeCH("Empty schema does not read data from ORC file")
    .includeCH("read from multiple orc input paths")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .includeCH("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .includeCH("LZO compression options for writing to an ORC file")
    .includeCH("Schema discovery on empty ORC files")
    .includeCH("SPARK-21791 ORC should support column names with dot")
    .includeCH("SPARK-25579 ORC PPD should support column names with dot")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    .includeCH("SPARK-37728: Reading nested columns with ORC vectorized reader should not")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .includeCH("Read/write all timestamp types")
    .includeCH("SPARK-37463: read/write Timestamp ntz to Orc with different time zone")
    .includeCH("SPARK-39381: Make vectorized orc columar writer batch size configurable")
    .includeCH("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
    .includeCH("simple select queries")
    .includeCH("overwriting")
    .includeCH("self-join")
    .includeCH("columns only referenced by pushed down filters should remain")
    .includeCH("SPARK-5309 strings stored using dictionary compression in orc")
    // For exception test.
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
    .excludeCH("SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException")
  enableSuite[GlutenOrcV2SchemaPruningSuite]
    .includeCH(
      "Spark vectorized reader - without partition data column - select only top-level fields")
    .includeCH(
      "Spark vectorized reader - with partition data column - select only top-level fields")
    .includeCH("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after join")
    .includeCH("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after join")
    .includeCH("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after outer join")
    .includeCH("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after outer join")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .includeCH(
      "Spark vectorized reader - without partition data column - select a single complex field")
    .includeCH(
      "Spark vectorized reader - with partition data column - select a single complex field")
    .includeCH(
      "Non-vectorized reader - without partition data column - select a single complex field")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field and its parent struct")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field and its parent struct")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field and its parent struct")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field and its parent struct")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field and the partition column")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field and the partition column")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field and the partition column")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field and the partition column")
    .includeCH("Spark vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .includeCH("Spark vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .includeCH("Non-vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .includeCH("Non-vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .includeCH(
      "Spark vectorized reader - without partition data column - no unnecessary schema pruning")
    .includeCH(
      "Spark vectorized reader - with partition data column - no unnecessary schema pruning")
    .includeCH(
      "Non-vectorized reader - without partition data column - no unnecessary schema pruning")
    .includeCH("Non-vectorized reader - with partition data column - no unnecessary schema pruning")
    .includeCH(
      "Spark vectorized reader - without partition data column - empty schema intersection")
    .includeCH("Spark vectorized reader - with partition data column - empty schema intersection")
    .includeCH("Non-vectorized reader - without partition data column - empty schema intersection")
    .includeCH("Non-vectorized reader - with partition data column - empty schema intersection")
    .includeCH("Spark vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .includeCH("Spark vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .includeCH("Non-vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .includeCH("Non-vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .includeCH("Spark vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Spark vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Non-vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Non-vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .includeCH("Spark vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .includeCH("Spark vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .includeCH("Non-vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .includeCH("Non-vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .includeCH("Spark vectorized reader - without partition data column - select explode of nested field of array of struct")
    .includeCH("Spark vectorized reader - with partition data column - select explode of nested field of array of struct")
    .includeCH("Non-vectorized reader - without partition data column - select explode of nested field of array of struct")
    .includeCH("Non-vectorized reader - with partition data column - select explode of nested field of array of struct")
    .includeCH("Non-vectorized reader - without partition data column - select one deep nested complex field after join")
    .includeCH("Non-vectorized reader - with partition data column - select one deep nested complex field after join")
    .includeCH("Non-vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .includeCH("Non-vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .includeCH("Spark vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Non-vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .includeCH("Spark vectorized reader - without partition data column - select nested field in window function")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in window function")
    .includeCH("Non-vectorized reader - without partition data column - select nested field in window function")
    .includeCH(
      "Non-vectorized reader - with partition data column - select nested field in window function")
    .includeCH("Spark vectorized reader - without partition data column - select nested field in window function and then order by")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in window function and then order by")
    .includeCH("Non-vectorized reader - without partition data column - select nested field in window function and then order by")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in window function and then order by")
    .includeCH(
      "Spark vectorized reader - without partition data column - select nested field in Sort")
    .includeCH("Spark vectorized reader - with partition data column - select nested field in Sort")
    .includeCH(
      "Non-vectorized reader - without partition data column - select nested field in Sort")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in Sort")
    .includeCH(
      "Spark vectorized reader - without partition data column - select nested field in Expand")
    .includeCH(
      "Spark vectorized reader - with partition data column - select nested field in Expand")
    .includeCH(
      "Non-vectorized reader - without partition data column - select nested field in Expand")
    .includeCH("Non-vectorized reader - with partition data column - select nested field in Expand")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .includeCH("Case-sensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .includeCH("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .includeCH("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .includeCH("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .includeCH("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .excludeCH("Spark vectorized reader - without partition data column - select a single complex field and in where clause")
    .excludeCH("Spark vectorized reader - with partition data column - select a single complex field and in where clause")
    .excludeCH("Non-vectorized reader - without partition data column - select a single complex field and in where clause")
    .excludeCH("Non-vectorized reader - with partition data column - select a single complex field and in where clause")
    .excludeCH("Spark vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Spark vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Non-vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Non-vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .excludeCH("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation")
    .excludeCH("Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenOuterJoinSuiteForceShjOff]
    .excludeCH("basic left outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("basic left outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("basic left outer join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("basic left outer join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("basic right outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("basic right outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("basic right outer join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("basic right outer join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("basic full outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("basic full outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("basic full outer join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("basic full outer join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("left outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("left outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("right outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("right outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("full outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("full outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("SPARK-32717: AQEOptimizer should respect excludedRules configuration")
    // TODO: fix the hanging problem in GLUTEN-8890 followup
    .excludeCH("SPARK-46037: ShuffledHashJoin build left with left outer join, codegen off (whole-stage-codegen off)")
    .excludeCH("SPARK-46037: ShuffledHashJoin build left with left outer join, codegen off (whole-stage-codegen on)")
  enableSuite[GlutenOuterJoinSuiteForceShjOn]
    .excludeCH("basic left outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("basic left outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("basic left outer join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("basic left outer join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("basic right outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("basic right outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("basic right outer join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("basic right outer join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("basic full outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("basic full outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("basic full outer join using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("basic full outer join using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("left outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("left outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("right outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("right outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .excludeCH("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .excludeCH("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .excludeCH("full outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .excludeCH("full outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    // TODO: fix the hanging problem in GLUTEN-8890 followup
    .excludeCH("SPARK-46037: ShuffledHashJoin build left with left outer join, codegen off (whole-stage-codegen off)")
    .excludeCH("SPARK-46037: ShuffledHashJoin build left with left outer join, codegen off (whole-stage-codegen on)")
  enableSuite[GlutenParametersSuite]
  enableSuite[GlutenParquetCodecSuite]
    // codec not supported in native
    .includeCH("write and read - file source parquet - codec: lz4_raw")
    .includeCH("write and read - file source parquet - codec: lz4raw")
  enableSuite[GlutenParquetColumnIndexSuite]
    // Rewrite by just removing test timestamp.
    .exclude("test reading unaligned pages - test all types")
    .excludeCH("test reading unaligned pages - test all types (dict encode)")
  enableSuite[GlutenParquetCompressionCodecPrecedenceSuite]
  enableSuite[GlutenParquetDeltaByteArrayEncodingSuite]
  enableSuite[GlutenParquetDeltaEncodingInteger]
  enableSuite[GlutenParquetDeltaEncodingLong]
  enableSuite[GlutenParquetDeltaLengthByteArrayEncodingSuite]
  enableSuite[GlutenParquetEncodingSuite]
    // Velox does not support rle encoding, but it can pass when native writer enabled.
    .includeCH("parquet v2 pages - rle encoding for boolean value columns")
    .excludeCH("All Types Dictionary")
    .excludeCH("All Types Null")
  enableSuite[GlutenParquetFieldIdIOSuite]
  enableSuite[GlutenParquetFileFormatV1Suite]
    .excludeCH(
      "SPARK-36825, SPARK-36854: year-month/day-time intervals written and read as INT32/INT64")
  enableSuite[GlutenParquetFileFormatV2Suite]
    .excludeCH(
      "SPARK-36825, SPARK-36854: year-month/day-time intervals written and read as INT32/INT64")
  enableSuite[GlutenParquetFileMetadataStructRowIndexSuite]
  enableSuite[GlutenParquetIOSuite]
    // Velox doesn't write file metadata into parquet file.
    .includeCH("Write Spark version into Parquet metadata")
    // Exception.
    .exclude("SPARK-35640: read binary as timestamp should throw schema incompatible error")
    // Exception msg.
    .exclude("SPARK-35640: int as long should throw schema incompatible error")
    // Velox parquet reader not allow offset zero.
    .includeCH("SPARK-40128 read DELTA_LENGTH_BYTE_ARRAY encoded strings")
  enableSuite[GlutenParquetInteroperabilitySuite]
    .exclude("parquet timestamp conversion")
  enableSuite[GlutenParquetProtobufCompatibilitySuite]
    .excludeCH("struct with unannotated array")
  enableSuite[GlutenParquetReadSchemaSuite]
  enableSuite[GlutenParquetRebaseDatetimeV1Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    .excludeByPrefix("SPARK-33160, SPARK-37705: write the metadata key")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ, rewrite some
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
    .excludeCH("Gluten - SPARK-31159: rebasing dates in write")
  enableSuite[GlutenParquetRebaseDatetimeV2Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    .excludeByPrefix("SPARK-33160, SPARK-37705: write the metadata key")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetRowIndexSuite]
    .excludeByPrefix("row index generation")
    .excludeByPrefix("invalid row index column type")
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
    .excludeCH("SPARK-10136 list of primitive list")
  enableSuite[GlutenParquetV1AggregatePushDownSuite]
  enableSuite[GlutenParquetV1FilterSuite]
    // Rewrite.
    .includeCH("SPARK-23852: Broken Parquet push-down for partially-written stats")
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
    .includeCH("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
    .excludeCH("filter pushdown - StringContains")
    .excludeCH("SPARK-36866: filter pushdown - year-month interval")
  // avoid Velox compile error
  enableSuite(
    "org.apache.gluten.execution.parquet.GlutenParquetV1FilterSuite2"
  )
    // Rewrite.
    .includeCH("SPARK-23852: Broken Parquet push-down for partially-written stats")
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
    .includeCH("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
    .excludeCH("filter pushdown - StringContains")
    .excludeCH("SPARK-36866: filter pushdown - year-month interval")
  enableSuite[GlutenParquetV1PartitionDiscoverySuite]
    .excludeCH("Various partition value types")
    .excludeCH("Various inferred partition value types")
    .excludeCH("Resolve type conflicts - decimals, dates and timestamps in partition column")
  enableSuite[GlutenParquetV1QuerySuite]
    .exclude("row group skipping doesn't overflow when reading into larger type")
    // Unsupport spark.sql.files.ignoreCorruptFiles.
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // new added in spark-3.3 and need fix later, random failure may caused by memory free
    .includeCH("SPARK-39833: pushed filters with project without filter columns")
    .includeCH("SPARK-39833: pushed filters with count()")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV1SchemaPruningSuite]
    .excludeCH("Case-insensitive parser - mixed-case schema - select with exact column names")
    .excludeCH("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .excludeCH(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .excludeCH(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .excludeCH(
      "Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .excludeCH("SPARK-36352: Spark should check result plan's output schema name")
    .excludeCH("Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenParquetV2AggregatePushDownSuite]
    // TODO: Timestamp columns stats will lost if using int64 in parquet writer.
    .includeCH("aggregate push down - different data types")
  enableSuite[GlutenParquetV2FilterSuite]
    // Rewrite.
    .includeCH("SPARK-23852: Broken Parquet push-down for partially-written stats")
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
    .includeCH("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
    .excludeCH("filter pushdown - StringContains")
    .excludeCH("SPARK-36866: filter pushdown - year-month interval")
    .excludeCH("Gluten - filter pushdown - date")
  enableSuite[GlutenParquetV2PartitionDiscoverySuite]
    .excludeCH("Various partition value types")
    .excludeCH("Various inferred partition value types")
    .excludeCH("Resolve type conflicts - decimals, dates and timestamps in partition column")
  enableSuite[GlutenParquetV2QuerySuite]
    .exclude("row group skipping doesn't overflow when reading into larger type")
    // Unsupport spark.sql.files.ignoreCorruptFiles.
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV2SchemaPruningSuite]
    .excludeCH("Spark vectorized reader - without partition data column - select a single complex field and in where clause")
    .excludeCH("Spark vectorized reader - with partition data column - select a single complex field and in where clause")
    .excludeCH("Non-vectorized reader - without partition data column - select a single complex field and in where clause")
    .excludeCH("Non-vectorized reader - with partition data column - select a single complex field and in where clause")
    .excludeCH("Spark vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Spark vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Non-vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Non-vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .excludeCH("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .excludeCH("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .excludeCH("Case-insensitive parser - mixed-case schema - select with exact column names")
    .excludeCH("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .excludeCH(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .excludeCH(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .excludeCH("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .excludeCH("SPARK-36352: Spark should check result plan's output schema name")
    .excludeCH("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation")
    .excludeCH("Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .excludeCH("Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenParquetVectorizedSuite]
  enableSuite[GlutenPartitionedWriteSuite]
    .excludeCH("SPARK-37231, SPARK-37240: Dynamic writes/reads of ANSI interval partitions")
  enableSuite[GlutenPathFilterStrategySuite]
  enableSuite[GlutenPathFilterSuite]
  enableSuite[GlutenPathOptionSuite]
  enableSuite[GlutenPredicateSuite]
    .excludeCH("basic IN/INSET predicate test")
    .excludeCH("IN with different types")
    .excludeCH("IN/INSET: binary")
    .excludeCH("IN/INSET: struct")
    .excludeCH("IN/INSET: array")
    .excludeCH("BinaryComparison: lessThan")
    .excludeCH("BinaryComparison: LessThanOrEqual")
    .excludeCH("BinaryComparison: GreaterThan")
    .excludeCH("BinaryComparison: GreaterThanOrEqual")
    .excludeCH("EqualTo on complex type")
    .excludeCH("SPARK-32764: compare special double/float values")
    .excludeCH("SPARK-32110: compare special double/float values in struct")
  enableSuite[GlutenProcessingTimeSuite]
  enableSuite[GlutenProductAggSuite]
  enableSuite[GlutenPruneFileSourcePartitionsSuite]
  enableSuite[GlutenPrunedScanSuite]
  enableSuite[GlutenQueryCompilationErrorsDSv2Suite]
  enableSuite[GlutenQueryCompilationErrorsSuite]
    .excludeCH("CREATE NAMESPACE with LOCATION for JDBC catalog should throw an error")
    .excludeCH(
      "ALTER NAMESPACE with property other than COMMENT for JDBC catalog should throw an exception")
  enableSuite[GlutenQueryExecutionErrorsSuite]
    // NEW SUITE: disable as it expects exception which doesn't happen when offloaded to gluten
    .exclude(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION: compatibility with Spark 2.4/3.2 in reading/writing dates")
    // Doesn't support unhex with failOnError=true.
    .exclude("CONVERSION_INVALID_INPUT: to_binary conversion function hex")
    .excludeCH("CONVERSION_INVALID_INPUT: to_binary conversion function base64")
    .excludeCH("UNSUPPORTED_FEATURE - SPARK-38504: can't read TimestampNTZ as TimestampLTZ")
    .excludeCH("CANNOT_PARSE_DECIMAL: unparseable decimal")
    .excludeCH("UNRECOGNIZED_SQL_TYPE: unrecognized SQL type DATALINK")
    .excludeCH("UNSUPPORTED_FEATURE.MULTI_ACTION_ALTER: The target JDBC server hosting table does not support ALTER TABLE with multiple actions.")
    .excludeCH("INVALID_BITMAP_POSITION: position out of bounds")
    .excludeCH("INVALID_BITMAP_POSITION: negative position")
  enableSuite[GlutenQueryParsingErrorsSuite]
  enableSuite[GlutenRandomSuite]
    .exclude("random")
    .exclude("SPARK-9127 codegen with long seed")
  enableSuite[GlutenRegexpExpressionsSuite]
    .excludeCH("LIKE Pattern")
    .excludeCH("LIKE Pattern ESCAPE '/'")
    .excludeCH("LIKE Pattern ESCAPE '#'")
    .excludeCH("LIKE Pattern ESCAPE '\"'")
    .excludeCH("RLIKE Regular Expression")
    .excludeCH("RegexReplace")
    .excludeCH("RegexExtract")
    .excludeCH("RegexExtractAll")
    .excludeCH("SPLIT")
  enableSuite[GlutenRemoveRedundantWindowGroupLimitsSuite]
    .excludeCH("remove redundant WindowGroupLimits")
    .excludeCH("Gluten - remove redundant WindowGroupLimits")
  enableSuite[GlutenReplaceHashWithSortAggSuite]
    .exclude("replace partial hash aggregate with sort aggregate")
    .exclude("replace partial and final hash aggregate together with sort aggregate")
    .exclude("do not replace hash aggregate if child does not have sort order")
    .exclude("do not replace hash aggregate if there is no group-by column")
  enableSuite[GlutenReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[GlutenResolveDefaultColumnsSuite]
  enableSuite[GlutenResolvedDataSourceSuite]
  enableSuite[GlutenReuseExchangeAndSubquerySuite]
  enableSuite[GlutenRuntimeNullChecksV2Writes]
  enableSuite[GlutenSQLAggregateFunctionSuite]
    .excludeGlutenTest("Return NaN or null when dividing by zero")
  enableSuite[GlutenSQLQuerySuite]
    // Decimal precision exceeds.
    .includeCH("should be able to resolve a persistent view")
    // Unstable. Needs to be fixed.
    .includeCH("SPARK-36093: RemoveRedundantAliases should not change expression's name")
    // Rewrite from ORC scan to Parquet scan because ORC is not well supported.
    .includeCH("SPARK-28156: self-join should not miss cached view")
    .includeCH("SPARK-33338: GROUP BY using literal map should not fail")
    // Rewrite to disable plan check for SMJ because SHJ is preferred in Gluten.
    .includeCH("SPARK-11111 null-safe join should not use cartesian product")
    // Rewrite to change the information of a caught exception.
    .exclude("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    // Different exception.
    .exclude("run sql directly on files")
    // Not useful and time consuming.
    .includeCH("SPARK-33084: Add jar support Ivy URI in SQL")
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class")
    // exception test, rewritten in gluten
    .exclude("the escape character is not allowed to end with")
    // ORC related
    .includeCH("SPARK-37965: Spark support read/write orc file with invalid char in field name")
    .includeCH("SPARK-38173: Quoted column cannot be recognized correctly when quotedRegexColumnNames is true")
    // Need to support MAP<NullType, NullType>
    .exclude(
      "SPARK-27619: When spark.sql.legacy.allowHashOnMapType is true, hash can be used on Maptype")
    .excludeCH("SPARK-6743: no columns from cache")
    .excludeCH("external sorting updates peak execution memory")
    .excludeCH("Struct Star Expansion")
    .excludeCH("Common subexpression elimination")
    .excludeCH("SPARK-24940: coalesce and repartition hint")
    .excludeCH("normalize special floating numbers in subquery")
    .excludeCH("SPARK-38548: try_sum should return null if overflow happens before merging")
    .excludeCH("SPARK-38589: try_avg should return null if overflow happens before merging")
    .excludeCH("Gluten - SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    .excludeCH("Gluten - the escape character is not allowed to end with")
  // enableSuite[GlutenSQLQueryTestSuite]
  enableSuite[GlutenSQLWindowFunctionSuite]
    // spill not supported yet.
    .exclude("test with low buffer spill threshold")
    .excludeCH(
      "window function: multiple window expressions specified by range in a single expression")
    .excludeCH("Gluten - Filter on row number")
    .excludeCH("Gluten - Filter on rank")
    .excludeCH("Gluten - Filter on dense_rank")
  enableSuite[GlutenSameResultSuite]
  enableSuite[GlutenSaveLoadSuite]
  enableSuite[GlutenScalaReflectionRelationSuite]
  enableSuite[GlutenSerializationSuite]
  enableSuite[GlutenSessionExtensionSuite]
  enableSuite[GlutenSortOrderExpressionsSuite]
  // TODO: fix the hanging problem in GLUTEN-8890 followup
//  enableSuite[GlutenSortShuffleSuite]
  enableSuite[GlutenSortSuite]
    .excludeCH("basic sorting using ExternalSort")
    .excludeCH("SPARK-33260: sort order is a Stream")
    .excludeCH("SPARK-40089: decimal values sort correctly")
    .excludeCH(
      "sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .excludeCH(
      "sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .excludeCH(
      "sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .excludeCH("sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .excludeCH("sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .excludeCH(
      "sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .excludeCH("sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .excludeCH("sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
  enableSuite[GlutenSparkSessionExtensionSuite]
  enableSuite[GlutenStatisticsCollectionSuite]
    // The output byte size of Velox is different
    .includeCH("SPARK-33687: analyze all tables in a specific database")
    .excludeCH("analyze empty table")
    .excludeCH("analyze column command - result verification")
    .excludeCH("column stats collection for null columns")
    .excludeCH("store and retrieve column stats in different time zones")
    .excludeCH("SPARK-42777: describe column stats (min, max) for timestamp_ntz column")
    .excludeCH("Gluten - store and retrieve column stats in different time zones")
    .excludeCH("statistics collection of a table with zero column")
  enableSuite[GlutenStringExpressionsSuite]
    .excludeCH("StringComparison")
    .excludeCH("Substring")
    .excludeCH("string substring_index function")
    .excludeCH("SPARK-40213: ascii for Latin-1 Supplement characters")
    .excludeCH("ascii for string")
    .excludeCH("Mask")
    .excludeCH("SPARK-42384: Mask with null input")
    .excludeCH("base64/unbase64 for string")
    .excludeCH("encode/decode for string")
    .excludeCH("SPARK-47307: base64 encoding without chunking")
    .excludeCH("Levenshtein distance threshold")
    .excludeCH("soundex unit test")
    .excludeCH("overlay for string")
    .excludeCH("overlay for byte array")
    .excludeCH("translate")
    .excludeCH("FORMAT")
    .excludeCH("LOCATE")
    .excludeCH("REPEAT")
    .excludeCH("ParseUrl")
    .excludeCH("SPARK-33468: ParseUrl in ANSI mode should fail if input string is not a valid url")
  enableSuite[GlutenStringFunctionsSuite]
    .excludeCH("string Levenshtein distance")
    .excludeCH("string regexp_count")
    .excludeCH("string regex_replace / regex_extract")
    .excludeCH("string regexp_extract_all")
    .excludeCH("string regexp_substr")
    .excludeCH("string overlay function")
    .excludeCH("binary overlay function")
    .excludeCH("string / binary length function")
    .excludeCH("SPARK-36751: add octet length api for scala")
    .excludeCH("SPARK-36751: add bit length api for scala")
    .excludeCH("str_to_map function")
    .excludeCH("SPARK-42384: mask with null input")
    .excludeCH("like & ilike function")
    .excludeCH("parse_url")
    .excludeCH("url_decode")
    .excludeCH("url_encode")
  enableSuite[GlutenSubqueryHintPropagationSuite]
  enableSuite[GlutenSubquerySuite]
    .excludeByPrefix(
      "SPARK-26893" // Rewrite this test because it checks Spark's physical operators.
    )
    // exclude as it checks spark plan
    .exclude("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery")
    .excludeCH("SPARK-39355: Single column uses quoted to construct UnresolvedAttribute")
    .excludeCH("SPARK-40800: always inline expressions in OptimizeOneRowRelationSubquery")
    .excludeCH("SPARK-40862: correlated one-row subquery with non-deterministic expressions")
  enableSuite[GlutenSupportsCatalogOptionsSuite]
  enableSuite[GlutenTableCapabilityCheckSuite]
  enableSuite[GlutenTableLocationSuite]
  enableSuite[GlutenTableOptionsConstantFoldingSuite]
  enableSuite[GlutenTableScanSuite]
  enableSuite[GlutenTakeOrderedAndProjectSuite]
    .excludeCH("TakeOrderedAndProject.doExecute without project")
    .excludeCH("TakeOrderedAndProject.doExecute with project")
    .excludeCH("TakeOrderedAndProject.doExecute with local sort")
  enableSuite[GlutenTextV1Suite]
  enableSuite[GlutenTextV2Suite]
  enableSuite[GlutenTryCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    .exclude("ANSI mode: Throw exception on casting out-of-range value to byte type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to short type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to int type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to long type")
    .exclude("cast from invalid string to numeric should throw NumberFormatException")
    .exclude("SPARK-26218: Fix the corner case of codegen when casting float to Integer")
    // Set timezone through config.
    .exclude("data type casting")
    .excludeCH("null cast")
    .excludeCH("cast string to date")
    .excludeCH("cast string to timestamp")
    .excludeGlutenTest("cast string to timestamp")
    .excludeCH("SPARK-22825 Cast array to string")
    .excludeCH("SPARK-33291: Cast array with null elements to string")
    .excludeCH("SPARK-22973 Cast map to string")
    .excludeCH("SPARK-22981 Cast struct to string")
    .excludeCH("SPARK-33291: Cast struct with null elements to string")
    .excludeCH("SPARK-35111: Cast string to year-month interval")
    .excludeCH("cast from timestamp II")
    .excludeCH("cast a timestamp before the epoch 1970-01-01 00:00:00Z II")
    .excludeCH("cast a timestamp before the epoch 1970-01-01 00:00:00Z")
    .excludeCH("cast from array II")
    .excludeCH("cast from array III")
    .excludeCH("cast from struct III")
    .excludeCH("ANSI mode: cast string to timestamp with parse error")
    .excludeCH("ANSI mode: cast string to date with parse error")
    .excludeCH("Gluten - data type casting")
  enableSuite[GlutenTryEvalSuite]
    .excludeCH("try_subtract")
  enableSuite[GlutenTypedImperativeAggregateSuite]
  enableSuite[GlutenUnwrapCastInComparisonEndToEndSuite]
    // Rewrite with NaN test cases excluded.
    .exclude("cases when literal is max")
  enableSuite[GlutenUrlFunctionsSuite]
    .excludeCH("url encode/decode function")
  enableSuite[GlutenV1WriteCommandSuite]
    // Rewrite to match SortExecTransformer.
    .excludeByPrefix("SPARK-41914:")
    .excludeCH(
      "Gluten - SPARK-41914: v1 write with AQE and in-partition sorted - non-string partition column")
    .excludeCH(
      "Gluten - SPARK-41914: v1 write with AQE and in-partition sorted - string partition column")
  enableSuite[GlutenV2PredicateSuite]
  enableSuite[GlutenValidateRequirementsSuite]
  enableSuite[GlutenVectorizedOrcReadSchemaSuite]
    // Rewrite to disable Spark's vectorized reading.
    .includeCH("change column position")
    .includeCH("read byte, int, short, long together")
    .includeCH("read float and double together")
    .includeCH("append column into middle")
    .includeCH("add a nested column at the end of the leaf struct column")
    .includeCH("add a nested column in the middle of the leaf struct column")
    .includeCH("add a nested column at the end of the middle struct column")
    .includeCH("add a nested column in the middle of the middle struct column")
    .includeCH("hide a nested column at the end of the leaf struct column")
    .includeCH("hide a nested column in the middle of the leaf struct column")
    .includeCH("hide a nested column at the end of the middle struct column")
    .includeCH("hide a nested column in the middle of the middle struct column")
    .includeCH("change column type from boolean to byte/short/int/long")
    .includeCH("change column type from byte to short/int/long")
    .includeCH("change column type from short to int/long")
    .includeCH("change column type from int to long")
    .includeCH("change column type from float to double")
  // .excludeGlutenTest("read byte, int, short, long together")
  // .excludeGlutenTest("read float and double together")
  enableSuite[GlutenVectorizedParquetReadSchemaSuite]
  enableSuite[GlutenWriteDistributionAndOrderingSuite]
  enableSuite[GlutenXPathFunctionsSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = ClickHouseSQLQueryTestSettings
}
// scalastyle:on line.size.limit
