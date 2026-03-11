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
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.connector._
import org.apache.spark.sql.errors._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.clickhouse.ClickHouseAdaptiveQueryExecSuite
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.binaryfile.GlutenBinaryFileFormatSuite
import org.apache.spark.sql.execution.datasources.exchange._
import org.apache.spark.sql.execution.datasources.json._
import org.apache.spark.sql.execution.datasources.orc._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.text.{GlutenTextV1Suite, GlutenTextV2Suite}
import org.apache.spark.sql.execution.datasources.v2.{GlutenDataSourceV2StrategySuite, GlutenFileTableSuite, GlutenV2PredicateSuite}
import org.apache.spark.sql.execution.exchange.GlutenEnsureRequirementsSuite
import org.apache.spark.sql.execution.joins.{GlutenBroadcastJoinSuite, GlutenExistenceJoinSuite, GlutenInnerJoinSuite, GlutenOuterJoinSuite}
import org.apache.spark.sql.extension.{GlutenCustomerExtensionSuite, GlutenSessionExtensionSuite}
import org.apache.spark.sql.gluten.GlutenFallbackSuite
import org.apache.spark.sql.hive.execution.GlutenHiveSQLQueryCHSuite
import org.apache.spark.sql.sources._
import org.apache.spark.sql.statistics.SparkFunctionStatistics

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class ClickHouseTestSettings extends BackendTestSettings {

  // disable tests that will break the whole UT
  override def shouldRun(suiteName: String, testName: String): Boolean = {
    val preCheck = suiteName.split("[.]").last match {
      case "GlutenCSVSuite" => !csvCoreDumpCases.contains(testName)
      case "GlutenCSVv1Suite" => !csvCoreDumpCases.contains(testName)
      case "GlutenCSVv2Suite" => !csvCoreDumpCases.contains(testName)
      case "GlutenCSVLegacyTimeParserSuite" => !csvCoreDumpCases.contains(testName)
      case "GlutenDataFrameSuite" => !dfCoreDumpCases.contains(testName)
      case "GlutenDatasetSuite" => !dsSlowCases.contains(testName)
      case "GlutenSQLQuerySuite" => !sqlQuerySlowCases.contains(testName)
      // Below 2 suites temporarily ignored because of gluten domain name change
      case "GlutenClickHouseMergeTreeWriteOnHDFSSuite" =>
        false
      case "GlutenClickHouseMergeTreeWriteOnS3Suite" =>
        false
      case "GlutenDataFrameWriterV2Suite" =>
        false // nativeDoValidate failed due to spark conf cleanup
      case "GlutenDataSourceV2DataFrameSuite" =>
        false // nativeDoValidate failed due to spark conf cleanup
      case "GlutenDataSourceV2FunctionSuite" =>
        false // nativeDoValidate failed due to spark conf cleanup
      case "GlutenDataSourceV2SQLSuite" =>
        false // nativeDoValidate failed due to spark conf cleanup
      case "GlutenMetadataColumnSuite" => false // nativeDoValidate failed due to spark conf cleanup
      case "GlutenQueryCompilationErrorsDSv2Suite" =>
        false // nativeDoValidate failed due to spark conf cleanup
      case "GlutenBloomFilterAggregateQuerySuite" =>
        !bloomFilterCases.contains(testName)
      case "GlutenSortShuffleSuite" => false
      case _ => true
    }
    preCheck && super.shouldRun(suiteName, testName)
  }

  private val csvCoreDumpCases: Seq[String] = Seq(
    "test with alternative delimiter and quote",
    "SPARK-24540: test with multiple character delimiter (comma space)",
    "DDL test with tab separated file",
    "test with null quote character",
    "SPARK-24540: test with multiple (crazy) character delimiter",
    "nullable fields with user defined null value of \"null\"",
    "SPARK-15585 turn off quotations",
    "SPARK-29101 test count with DROPMALFORMED mode"
  )

  private val dfCoreDumpCases: Seq[String] = Seq(
    "repartitionByRange",
    GLUTEN_TEST + "repartitionByRange"
  )

  private val dsSlowCases: Seq[String] = Seq(
    "SPARK-16995: flat mapping on Dataset containing a column created with lit/expr"
  )

  private val sqlQuerySlowCases: Seq[String] = Seq(
    "SPARK-33084: Add jar support Ivy URI in SQL"
  )

  private val bloomFilterCases: Seq[String] = Seq(
    // Currently return a empty set(same reason as sum(empty set),
    // both behaviors are acceptable.
    "Test that bloom_filter_agg produces a NULL with empty input"
  )

  enableSuite[GlutenApproxCountDistinctForIntervalsQuerySuite].exclude(
    "test ApproxCountDistinctForIntervals with large number of endpoints")
  enableSuite[GlutenApproximatePercentileQuerySuite].exclude(
    "SPARK-32908: maximum target error in percentile_approx")
  enableSuite[GlutenBloomFilterAggregateQuerySuite]
    .exclude("Test bloom_filter_agg and might_contain")
    .excludeGlutenTest("Test bloom_filter_agg with big RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS")
  enableSuite[GlutenCTEHintSuite]
  enableSuite[GlutenCTEInlineSuiteAEOff]
  enableSuite[GlutenCTEInlineSuiteAEOn]
  enableSuite[GlutenCachedTableSuite]
    .exclude("SPARK-37369: Avoid redundant ColumnarToRow transition on InMemoryTableScan")
    .exclude("analyzes column statistics in cached query")
    .excludeGlutenTest("InMemoryRelation statistics")
  enableSuite[GlutenColumnExpressionSuite]
    .exclude("input_file_name, input_file_block_start, input_file_block_length - FileScanRDD")
    .exclude("withField should add field with no name")
    .exclude("withField should add field to nullable struct")
    .exclude("withField should add field to nested nullable struct")
    .exclude("withField should add multiple fields to nullable struct")
    .exclude("withField should add multiple fields to nested nullable struct")
    .exclude("withField should replace field in nullable struct")
    .exclude("withField should replace field in nested nullable struct")
    .exclude("withField should replace multiple fields in nullable struct")
    .exclude("withField should replace multiple fields in nested nullable struct")
    .exclude("withField should replace all fields with given name in struct")
    .exclude("withField user-facing examples")
    .exclude("dropFields should drop field in nullable struct")
    .exclude("dropFields should drop field with no name in struct")
    .exclude("dropFields should drop field in nested nullable struct")
    .exclude("dropFields should drop multiple fields in nested nullable struct")
    .exclude("dropFields should drop all fields with given name in struct")
    .exclude("dropFields user-facing examples")
    .exclude("should move field up one level of nesting")
    .exclude("SPARK-36778: add ilike API for scala")
  enableSuite[GlutenComplexTypesSuite]
  enableSuite[GlutenConfigBehaviorSuite].exclude(
    "SPARK-22160 spark.sql.execution.rangeExchange.sampleSizePerPartition")
  enableSuite[GlutenCountMinSketchAggQuerySuite]
  enableSuite[GlutenCsvFunctionsSuite]
  enableSuite[GlutenDSV2CharVarcharTestSuite]
    // Excluded. The Gluten tests for char/varchar validation were rewritten for Velox.
    // ClickHouse backend doesn't support this feature and falls back to vanilla Spark,
    // causing mismatches in error messages.
    .excludeGlutenTest("length check for input string values: nested in struct of array")
  enableSuite[GlutenDSV2SQLInsertTestSuite]
  enableSuite[GlutenDataFrameAggregateSuite]
    .exclude("average")
    .exclude("zero average")
    .exclude("zero stddev")
    .exclude("collect functions")
    .exclude("collect functions structs")
    .exclude("SPARK-17641: collect functions should not collect null values")
    .exclude("collect functions should be able to cast to array type with no null values")
    .exclude("SPARK-17616: distinct aggregate combined with a non-partial aggregate")
    .exclude("SPARK-19471: AggregationIterator does not initialize the generated result projection before using it")
    .excludeGlutenTest("SPARK-19471: AggregationIterator does not initialize the generated" +
      " result projection before using it")
    .exclude("SPARK-26021: NaN and -0.0 in grouping expressions")
    .exclude("SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate")
    .exclude("SPARK-32136: NormalizeFloatingNumbers should work on null struct")
    .exclude("SPARK-34713: group by CreateStruct with ExtractValue")
    .exclude("SPARK-34716: Support ANSI SQL intervals by the aggregate function `sum`")
    .exclude("SPARK-34837: Support ANSI SQL intervals by the aggregate function `avg`")
    .exclude("SPARK-35412: groupBy of year-month/day-time intervals should work")
    .exclude("SPARK-36926: decimal average mistakenly overflow")
    .excludeGlutenTest("use gluten hash agg to replace vanilla spark sort agg")
  enableSuite[GlutenDataFrameAsOfJoinSuite]
  enableSuite[GlutenDataFrameComplexTypeSuite]
  enableSuite[GlutenDataFrameFunctionsSuite]
    // Expected exception org.apache.spark.SparkException to be thrown, but no exception was thrown
    .exclude("map_concat function")
    .exclude("map with arrays")
    .exclude("flatten function")
    .exclude("aggregate function - array for primitive type not containing null")
    .exclude("aggregate function - array for primitive type containing null")
    .exclude("aggregate function - array for non-primitive type")
    .exclude("SPARK-14393: values generated by non-deterministic functions shouldn't change after coalesce or union")
    .exclude("SPARK-24734: Fix containsNull of Concat for array type")
    .exclude("transform keys function - primitive data types")
    .exclude("transform keys function - Invalid lambda functions and exceptions")
    .exclude("transform values function - test primitive data types")
    .exclude("transform values function - test empty")
  enableSuite[GlutenDataFrameHintSuite]
  enableSuite[GlutenDataFrameImplicitsSuite]
  enableSuite[GlutenDataFrameJoinSuite].exclude(
    "SPARK-32693: Compare two dataframes with same schema except nullable property")
  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .exclude("replace nan with float")
    .exclude("replace nan with double")
  enableSuite[GlutenDataFramePivotSuite]
    .exclude("pivot with column definition in groupby")
    .exclude("pivot with timestamp and count should not print internal representation")
    .exclude("SPARK-38133: Grouping by TIMESTAMP_NTZ should not corrupt results")
  enableSuite[GlutenDataFrameRangeSuite]
  enableSuite[GlutenDataFrameSelfJoinSuite]
  enableSuite[GlutenDataFrameSessionWindowingSuite]
    .exclude("simple session window with record at window start")
    .exclude("session window groupBy statement")
    .exclude("SPARK-36465: filter out events with negative/zero gap duration")
    .exclude("session window groupBy with multiple keys statement")
    .exclude("session window groupBy with multiple keys statement - one distinct")
    .exclude("session window groupBy with multiple keys statement - two distinct")
    .exclude("session window groupBy with multiple keys statement - keys overlapped with sessions")
    .exclude("session window with multi-column projection")
    .exclude("SPARK-36724: Support timestamp_ntz as a type of time column for SessionWindow")
  enableSuite[GlutenDataFrameSetOperationsSuite]
    .exclude("SPARK-10740: handle nondeterministic expressions correctly for set operations")
    .exclude(
      "SPARK-34283: SQL-style union using Dataset, keep necessary deduplicate in multiple unions")
    .exclude("union should union DataFrames with UDTs (SPARK-13410)")
    .exclude(
      "SPARK-32376: Make unionByName null-filling behavior work with struct columns - simple")
    .exclude(
      "SPARK-32376: Make unionByName null-filling behavior work with struct columns - nested")
    .exclude("SPARK-32376: Make unionByName null-filling behavior work with struct columns - case-sensitive cases")
    .exclude(
      "SPARK-32376: Make unionByName null-filling behavior work with struct columns - edge case")
    .exclude("SPARK-35290: Make unionByName null-filling behavior work with struct columns - sorting edge case")
    .exclude(
      "SPARK-32376: Make unionByName null-filling behavior work with struct columns - deep expr")
    .exclude("SPARK-35756: unionByName support struct having same col names but different sequence")
    .exclude("SPARK-36797: Union should resolve nested columns as top-level columns")
    .exclude("SPARK-37371: UnionExec should support columnar if all children support columnar")
    .exclude(
      "SPARK-36673: Only merge nullability for Unions of struct"
    ) // disabled due to case-insensitive not supported in CH tuple
    .exclude("except all")
    .exclude("exceptAll - nullability")
    .exclude("intersectAll")
    .exclude("intersectAll - nullability")
  enableSuite[GlutenDataFrameStatSuite]
  enableSuite[GlutenDataFrameSuite]
    .exclude("SPARK-27439: Explain result should match collected result after view change")
    .exclude("Uuid expressions should produce same results at retries in the same DataFrame")
    .exclude("SPARK-28224: Aggregate sum big decimal overflow")
    .exclude("SPARK-28067: Aggregate sum should not return wrong results for decimal overflow")
    .exclude("SPARK-35955: Aggregate avg should not return wrong results for decimal overflow")
    .exclude("describe")
    .exclude("getRows: array")
    .exclude("showString: array")
    .exclude("showString: array, vertical = true")
    .exclude("SPARK-23023 Cast rows to strings in showString")
    .exclude("SPARK-18350 show with session local timezone")
    .exclude("SPARK-18350 show with session local timezone, vertical = true")
    .exclude("SPARK-6899: type should match when using codegen")
    .exclude("SPARK-7324 dropDuplicates")
    .exclude(
      "SPARK-8608: call `show` on local DataFrame with random columns should return same value")
    .exclude("SPARK-8609: local DataFrame with random columns should return same value after sort")
    .exclude("SPARK-9083: sort with non-deterministic expressions")
    .exclude("SPARK-10316: respect non-deterministic expressions in PhysicalOperation")
    .exclude("distributeBy and localSort")
    .exclude("reuse exchange")
    .exclude("SPARK-22271: mean overflows and returns null for some decimal variables")
    .exclude("SPARK-22520: support code generation for large CaseWhen")
    .exclude("SPARK-24165: CaseWhen/If - nullability of nested types")
    .exclude("SPARK-27671: Fix analysis exception when casting null in nested field in struct")
    .exclude("summary")
    .excludeGlutenTest(
      "SPARK-27439: Explain result should match collected result after view change")
    .excludeGlutenTest("distributeBy and localSort")
    .excludeGlutenTest("describe")
    .excludeGlutenTest("Allow leading/trailing whitespace in string before casting")
  enableSuite[GlutenDataFrameTimeWindowingSuite]
    .exclude("simple tumbling window with record at window start")
    .exclude("SPARK-21590: tumbling window using negative start time")
    .exclude("tumbling window groupBy statement")
    .exclude("tumbling window groupBy statement with startTime")
    .exclude("SPARK-21590: tumbling window groupBy statement with negative startTime")
    .exclude("tumbling window with multi-column projection")
    .exclude("sliding window grouping")
    .exclude("time window joins")
    .exclude("negative timestamps")
    .exclude("millisecond precision sliding windows")
  enableSuite[GlutenDataFrameTungstenSuite].excludeGlutenTest("Map type with struct type as key")
  enableSuite[GlutenDataFrameWindowFramesSuite]
    .exclude("rows between should accept int/long values as boundary")
    .exclude("range between should accept int/long values as boundary")
    .exclude("reverse preceding/following range between with aggregation")
  enableSuite[GlutenDataFrameWindowFunctionsSuite]
    .exclude("corr, covar_pop, stddev_pop functions in specific window")
    .exclude(
      "SPARK-13860: corr, covar_pop, stddev_pop functions in specific window LEGACY_STATISTICAL_AGGREGATE off")
    .exclude("covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window")
    .exclude("SPARK-13860: covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window LEGACY_STATISTICAL_AGGREGATE off")
    .exclude("lead/lag with ignoreNulls")
    .exclude("Window spill with more than the inMemoryThreshold and spillThreshold")
    .exclude("SPARK-21258: complex object in combination with spilling")
    .exclude(
      "SPARK-38237: require all cluster keys for child required distribution for window query")
    .excludeGlutenTest("corr, covar_pop, stddev_pop functions in specific window")
  enableSuite[GlutenDatasetAggregatorSuite]
  enableSuite[GlutenDatasetCacheSuite]
  enableSuite[GlutenDatasetOptimizationSuite]
    .exclude("Pruned nested serializers: map of map key")
    .exclude("Pruned nested serializers: map of complex key")
  enableSuite[GlutenDatasetPrimitiveSuite]
  enableSuite[GlutenDatasetSerializerRegistratorSuite]
  enableSuite[GlutenDatasetSuite]
    .exclude("SPARK-16853: select, case class and tuple")
    .exclude("select 2, primitive and tuple")
    .exclude("SPARK-15550 Dataset.show() should show inner nested products as rows")
    .exclude("dropDuplicates")
    .exclude("dropDuplicates: columns with same column name")
    .exclude("SPARK-24762: select Option[Product] field")
    .exclude("SPARK-24762: typed agg on Option[Product] type")
    .exclude("SPARK-26233: serializer should enforce decimal precision and scale")
    .exclude("groupBy.as")
    .exclude("SPARK-40407: repartition should not result in severe data skew")
    .exclude("SPARK-40660: Switch to XORShiftRandom to distribute elements")
  enableSuite[GlutenDateFunctionsSuite]
    .exclude("function to_date")
    .exclude("unix_timestamp")
    .exclude("to_unix_timestamp")
    .exclude("to_timestamp with microseconds precision")
    .exclude("SPARK-30668: use legacy timestamp parser in to_timestamp")
    .exclude("SPARK-30766: date_trunc of old timestamps to hours and days")
    .exclude("SPARK-30793: truncate timestamps before the epoch to seconds and minutes")
    .excludeGlutenTest("unix_timestamp")
    .excludeGlutenTest("to_unix_timestamp")
    .exclude("to_utc_timestamp with column zone")
    .exclude("from_utc_timestamp with column zone")
  enableSuite[GlutenDeprecatedAPISuite]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff].excludeGlutenTest(
    "SPARK-32659: Fix the data issue when pruning DPP on non-atomic type")
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOn].excludeGlutenTest(
    "SPARK-32659: Fix the data issue when pruning DPP on non-atomic type")
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOff].excludeGlutenTest(
    "SPARK-32659: Fix the data issue when pruning DPP on non-atomic type")
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOn].excludeGlutenTest(
    "SPARK-32659: Fix the data issue when pruning DPP on non-atomic type")
  enableSuite[GlutenExpressionsSchemaSuite]
  enableSuite[GlutenExtraStrategiesSuite]
  enableSuite[GlutenFileBasedDataSourceSuite]
    .exclude("SPARK-23072 Write and read back unicode column names - csv")
    .excludeByPrefix("Enabling/disabling ignoreMissingFiles using")
    .excludeGlutenTestsByPrefix("Enabling/disabling ignoreMissingFiles using")
    .exclude("Spark native readers should respect spark.sql.caseSensitive - parquet")
    .exclude("Spark native readers should respect spark.sql.caseSensitive - orc")
    .exclude("SPARK-25237 compute correct input metrics in FileScanRDD")
    .exclude("SPARK-30362: test input metrics for DSV2")
    .exclude("SPARK-37585: test input metrics for DSV2 with output limits")
    .exclude("UDF input_file_name()")
    .exclude("Option recursiveFileLookup: disable partition inferring")
    .exclude("SPARK-31116: Select nested schema with case insensitive mode")
    .exclude("SPARK-35669: special char in CSV header with filter pushdown")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("File source v2: support passing data filters to FileScan without partitionFilters")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("File source v2: support partition pruning")
    .excludeGlutenTest("Spark native readers should respect spark.sql.caseSensitive - parquet")
    .excludeGlutenTest("SPARK-25237 compute correct input metrics in FileScanRDD")
    .excludeGlutenTest("Option recursiveFileLookup: disable partition inferring")
  enableSuite[GlutenFileScanSuite]
  enableSuite[GlutenFileSourceCharVarcharTestSuite]
    .exclude("char type values should be padded or trimmed: partitioned columns")
    .exclude("varchar type values length check and trim: partitioned columns")
    .exclude("char/varchar type values length check: partitioned columns of other types")
    .exclude("char type comparison: partitioned columns")
    // Excluded. The Gluten tests for char/varchar validation were rewritten for Velox.
    // ClickHouse backend doesn't support this feature and falls back to vanilla Spark,
    // causing mismatches in error messages.
    .excludeGlutenTest("length check for input string values: nested in struct of array")
  enableSuite[GlutenFileSourceSQLInsertTestSuite]
    .exclude("SPARK-33474: Support typed literals as partition spec values")
    .exclude(
      "SPARK-34556: checking duplicate static partition columns should respect case sensitive conf")
  enableSuite[GlutenGeneratorFunctionSuite]
    .exclude("single explode_outer")
    .exclude("single posexplode")
    .exclude("single posexplode_outer")
    .exclude("explode_outer and other columns")
    .exclude("aliased explode_outer")
    .exclude("explode_outer on map")
    .exclude("explode_outer on map with aliases")
    .exclude("inline_outer")
    .exclude("SPARK-14986: Outer lateral view with empty generate expression")
    .exclude("outer explode()")
    .exclude("generator in aggregate expression")
    .exclude("SPARK-37947: lateral view <func>_outer()")
  enableSuite[GlutenInjectRuntimeFilterSuite].exclude("Merge runtime bloom filters")
  enableSuite[GlutenIntervalFunctionsSuite]
  enableSuite[GlutenJoinSuite].exclude(
    "SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join")
  enableSuite[GlutenJsonExpressionsSuite]
    .exclude(
      "$.store.basket[0][*].b"
    ) // issue: https://github.com/apache/incubator-gluten/issues/8529
    .exclude("from_json - invalid data")
    .exclude("from_json - input=object, schema=array, output=array of single row")
    .exclude("from_json - input=empty object, schema=array, output=array of single row with null")
    .exclude("from_json - input=array of single object, schema=struct, output=single row")
    .exclude("from_json - input=array, schema=struct, output=single row")
    .exclude("from_json - input=empty array, schema=struct, output=single row with null")
    .exclude("from_json - input=empty object, schema=struct, output=single row with null")
    .exclude("SPARK-20549: from_json bad UTF-8")
    .exclude("from_json with timestamp")
    .exclude("to_json - struct")
    .exclude("to_json - array")
    .exclude("to_json - array with single empty row")
    .exclude("to_json with timestamp")
    .exclude("SPARK-21513: to_json support map[string, struct] to json")
    .exclude("SPARK-21513: to_json support map[struct, struct] to json")
    .exclude("parse date with locale")
    .exclude("parse decimals using locale")
    // NOT use gluten
    .exclude("$..no_recursive")
    .exclude("non foldable literal")
    .exclude("some big value")
    .exclude("from/to json - interval support")
    .exclude("SPARK-24709: infer schema of json strings")
    .exclude("infer schema of JSON strings by using options")
    .exclude("inferring the decimal type using locale")
    .exclude("json_object_keys")
  enableSuite[GlutenJsonFunctionsSuite]
    .exclude("from_json with option")
    .exclude("from_json missing columns")
    .exclude("from_json invalid json")
    .exclude("from_json array support")
    .exclude("to_json with option")
    .exclude("roundtrip in to_json and from_json - array")
    .exclude("SPARK-19637 Support to_json in SQL")
    .exclude("SPARK-19967 Support from_json in SQL")
    .exclude("pretty print - roundtrip from_json -> to_json")
    .exclude("from_json invalid json - check modes")
    .exclude("SPARK-36069: from_json invalid json schema - check field name and field value")
    .exclude("corrupt record column in the middle")
    .exclude("parse timestamps with locale")
    .exclude("from_json - timestamp in micros")
    .exclude("SPARK-33134: return partial results only for root JSON objects")
    .exclude("SPARK-33907: bad json input with json pruning optimization: GetStructField")
    .exclude("SPARK-33907: json pruning optimization with corrupt record field")
    .exclude("SPARK-33907: bad json input with json pruning optimization: GetArrayStructFields")
  enableSuite[GlutenMathFunctionsSuite]
  enableSuite[GlutenMetadataCacheSuite].exclude(
    "SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException")
  enableSuite[GlutenMiscFunctionsSuite]
  enableSuite[GlutenNestedDataSourceV1Suite]
  enableSuite[GlutenNestedDataSourceV2Suite]
  enableSuite[GlutenProcessingTimeSuite]
  enableSuite[GlutenProductAggSuite]
  enableSuite[GlutenReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[GlutenSQLQuerySuite]
    .exclude("self join with alias in agg")
    .exclude("SPARK-3176 Added Parser of SQL LAST()")
    .exclude("SPARK-3173 Timestamp support in the parser")
    .exclude("SPARK-11111 null-safe join should not use cartesian product")
    .exclude("SPARK-3349 partitioning after limit")
    .exclude("aggregation with codegen updates peak execution memory")
    .exclude("SPARK-10215 Div of Decimal returns null")
    .exclude("precision smaller than scale")
    .exclude("external sorting updates peak execution memory")
    .exclude("run sql directly on files")
    .exclude("Struct Star Expansion")
    .exclude("Common subexpression elimination")
    .exclude(
      "SPARK-27619: When spark.sql.legacy.allowHashOnMapType is true, hash can be used on Maptype")
    .exclude("SPARK-24940: coalesce and repartition hint")
    .exclude("SPARK-25144 'distinct' causes memory leak")
    .exclude("SPARK-29239: Subquery should not cause NPE when eliminating subexpression")
    .exclude("normalize special floating numbers in subquery")
    .exclude("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    .exclude("SPARK-33593: Vector reader got incorrect data with binary partition value")
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class")
    .exclude("SPARK-27442: Spark support read/write parquet file with invalid char in field name")
    .exclude("SPARK-37965: Spark support read/write orc file with invalid char in field name")
    .exclude("SPARK-38548: try_sum should return null if overflow happens before merging")
    .exclude("SPARK-38589: try_avg should return null if overflow happens before merging")
    .exclude("SPARK-39548: CreateView will make queries go into inline CTE code path thustrigger a mis-clarified `window definition not found` issue")
    .excludeGlutenTest("SPARK-33593: Vector reader got incorrect data with binary partition value")
    .excludeGlutenTest(
      "SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
  enableSuite[GlutenSQLQueryTestSuite]
  enableSuite[GlutenScalaReflectionRelationSuite]
  enableSuite[GlutenSerializationSuite]
  enableSuite[GlutenStatisticsCollectionSuite]
    .exclude("analyze empty table")
    .exclude("analyze column command - result verification")
    .exclude("column stats collection for null columns")
    .exclude("store and retrieve column stats in different time zones")
    .excludeGlutenTest("store and retrieve column stats in different time zones")
    .excludeCH("statistics collection of a table with zero column")
  enableSuite[GlutenStringFunctionsSuite]
    .exclude("string regex_replace / regex_extract")
    .exclude("string overlay function")
    .exclude("binary overlay function")
    .exclude("string / binary length function")
    .exclude("SPARK-36751: add octet length api for scala")
    .exclude("SPARK-36751: add bit length api for scala")
  enableSuite[GlutenSubquerySuite]
    .exclude("SPARK-15370: COUNT bug in subquery in subquery in subquery")
    .exclude("SPARK-26893: Allow pushdown of partition pruning subquery filters to file source")
    .exclude("SPARK-28441: COUNT bug in nested subquery with non-foldable expr")
    .exclude("SPARK-28441: COUNT bug with non-foldable expression in Filter condition")
    .exclude("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery")
    .exclude("SPARK-36656: Do not collapse projects with correlate scalar subqueries")
    .exclude("Merge non-correlated scalar subqueries from different parent plans")
    .exclude("Merge non-correlated scalar subqueries with conflicting names")
  enableSuite[GlutenTypedImperativeAggregateSuite]
  enableSuite[GlutenUnwrapCastInComparisonEndToEndSuite].exclude("cases when literal is max")
  enableSuite[GlutenXPathFunctionsSuite]
  enableSuite[QueryTestSuite]
  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOff]
    .exclude("cast string to date")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35819: Support cast YearMonthIntervalType in different fields")
  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOn]
    .exclude("null cast")
    .exclude("cast string to date")
    .exclude("cast string to timestamp")
    .exclude("cast from boolean")
    .exclude("cast from int")
    .exclude("cast from long")
    .exclude("cast from float")
    .exclude("cast from double")
    .exclude("cast from timestamp")
    .exclude("data type casting")
    .exclude("cast and add")
    .exclude("from decimal")
    .exclude("cast from array")
    .exclude("cast from map")
    .exclude("cast from struct")
    .exclude("cast struct with a timestamp field")
    .exclude("cast between string and interval")
    .exclude("cast string to boolean")
    .exclude("SPARK-20302 cast with same structure")
    .exclude("SPARK-22500: cast for struct should not generate codes beyond 64KB")
    .exclude("SPARK-27671: cast from nested null type in struct")
    .exclude("Process Infinity, -Infinity, NaN in case insensitive manner")
    .exclude("SPARK-22825 Cast array to string")
    .exclude("SPARK-33291: Cast array with null elements to string")
    .exclude("SPARK-22973 Cast map to string")
    .exclude("SPARK-22981 Cast struct to string")
    .exclude("SPARK-33291: Cast struct with null elements to string")
    .exclude("SPARK-34667: cast year-month interval to string")
    .exclude("SPARK-34668: cast day-time interval to string")
    .exclude("SPARK-35698: cast timestamp without time zone to string")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35716: cast timestamp without time zone to date type")
    .exclude("SPARK-35718: cast date type to timestamp without timezone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("SPARK-35720: cast string to timestamp without timezone")
    .exclude("SPARK-35112: Cast string to day-time interval")
    .exclude("SPARK-35111: Cast string to year-month interval")
    .exclude("SPARK-35820: Support cast DayTimeIntervalType in different fields")
    .exclude("SPARK-35819: Support cast YearMonthIntervalType in different fields")
    .exclude("SPARK-35768: Take into account year-month interval fields in cast")
    .exclude("SPARK-35735: Take into account day-time interval fields in cast")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to byte type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to short type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to int type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to long type")
    .exclude("Fast fail for cast string type to decimal type in ansi mode")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z")
    .exclude("cast from array III")
    .exclude("cast from map II")
    .exclude("cast from map III")
    .exclude("cast from struct II")
    .exclude("cast from struct III")
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude("- (UnaryMinus)")
    .exclude("/ (Divide) basic")
    .exclude("/ (Divide) for Long and Decimal type")
    .exclude("% (Remainder)")
    .exclude("SPARK-17617: % (Remainder) double % double on super big double")
    .exclude("Abs")
    .exclude("pmod")
    .exclude("SPARK-28322: IntegralDivide supports decimal type")
    .exclude("SPARK-33008: division by zero on divide-like operations returns incorrect result")
    .exclude("SPARK-34920: error class")
    .exclude("SPARK-36920: Support year-month intervals by ABS")
    .exclude("SPARK-36920: Support day-time intervals by ABS")
    .exclude("SPARK-36921: Support YearMonthIntervalType by div")
    .exclude("SPARK-36921: Support DayTimeIntervalType by div")
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenCastSuite]
    .exclude("null cast")
    .exclude("cast string to date")
    .exclude("cast string to timestamp")
    .excludeGlutenTest("cast string to timestamp")
    .exclude("cast from boolean")
    .exclude("data type casting")
    .excludeGlutenTest("data type casting")
    .exclude("cast between string and interval")
    .exclude("SPARK-27671: cast from nested null type in struct")
    .exclude("Process Infinity, -Infinity, NaN in case insensitive manner")
    .exclude("SPARK-22825 Cast array to string")
    .exclude("SPARK-33291: Cast array with null elements to string")
    .exclude("SPARK-22973 Cast map to string")
    .exclude("SPARK-22981 Cast struct to string")
    .exclude("SPARK-33291: Cast struct with null elements to string")
    .exclude("SPARK-34667: cast year-month interval to string")
    .exclude("SPARK-34668: cast day-time interval to string")
    .exclude("SPARK-35698: cast timestamp without time zone to string")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35716: cast timestamp without time zone to date type")
    .exclude("SPARK-35718: cast date type to timestamp without timezone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("SPARK-35720: cast string to timestamp without timezone")
    .exclude("SPARK-35112: Cast string to day-time interval")
    .exclude("SPARK-35111: Cast string to year-month interval")
    .exclude("SPARK-35820: Support cast DayTimeIntervalType in different fields")
    .exclude("SPARK-35819: Support cast YearMonthIntervalType in different fields")
    .exclude("SPARK-35768: Take into account year-month interval fields in cast")
    .exclude("SPARK-35735: Take into account day-time interval fields in cast")
    .exclude("null cast #2")
    .exclude("cast string to date #2")
    .exclude("casting to fixed-precision decimals")
    .exclude("SPARK-28470: Cast should honor nullOnOverflow property")
    .exclude("cast string to boolean II")
    .exclude("cast from array II")
    .exclude("cast from map II")
    .exclude("cast from struct II")
    .exclude("cast from date")
    .exclude("cast from timestamp II")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z")
    .exclude("SPARK-32828: cast from a derived user-defined type to a base type")
    .exclude("SPARK-34727: cast from float II")
    .exclude("SPARK-35720: cast invalid string input to timestamp without time zone")
    .exclude("SPARK-36924: Cast DayTimeIntervalType to IntegralType")
    .exclude("SPARK-36924: Cast IntegralType to DayTimeIntervalType")
    .exclude("SPARK-36924: Cast YearMonthIntervalType to IntegralType")
    .exclude("SPARK-36924: Cast IntegralType to YearMonthIntervalType")
    .exclude("Cast should output null for invalid strings when ANSI is not enabled.")
    .exclude("cast timestamp to Int64 with floor division")
    .exclude("cast from boolean to timestamp")
  enableSuite[GlutenCastSuiteWithAnsiModeOn]
    .exclude("null cast")
    .exclude("cast string to date")
    .exclude("cast string to timestamp")
    .exclude("cast from boolean")
    .exclude("cast from int")
    .exclude("cast from long")
    .exclude("cast from float")
    .exclude("cast from double")
    .exclude("cast from timestamp")
    .exclude("data type casting")
    .exclude("cast and add")
    .exclude("from decimal")
    .exclude("cast from array")
    .exclude("cast from map")
    .exclude("cast from struct")
    .exclude("cast struct with a timestamp field")
    .exclude("cast between string and interval")
    .exclude("cast string to boolean")
    .exclude("SPARK-20302 cast with same structure")
    .exclude("SPARK-22500: cast for struct should not generate codes beyond 64KB")
    .exclude("SPARK-27671: cast from nested null type in struct")
    .exclude("Process Infinity, -Infinity, NaN in case insensitive manner")
    .exclude("SPARK-22825 Cast array to string")
    .exclude("SPARK-33291: Cast array with null elements to string")
    .exclude("SPARK-22973 Cast map to string")
    .exclude("SPARK-22981 Cast struct to string")
    .exclude("SPARK-33291: Cast struct with null elements to string")
    .exclude("SPARK-34667: cast year-month interval to string")
    .exclude("SPARK-34668: cast day-time interval to string")
    .exclude("SPARK-35698: cast timestamp without time zone to string")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35716: cast timestamp without time zone to date type")
    .exclude("SPARK-35718: cast date type to timestamp without timezone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("SPARK-35720: cast string to timestamp without timezone")
    .exclude("SPARK-35112: Cast string to day-time interval")
    .exclude("SPARK-35111: Cast string to year-month interval")
    .exclude("SPARK-35820: Support cast DayTimeIntervalType in different fields")
    .exclude("SPARK-35819: Support cast YearMonthIntervalType in different fields")
    .exclude("SPARK-35768: Take into account year-month interval fields in cast")
    .exclude("SPARK-35735: Take into account day-time interval fields in cast")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to byte type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to short type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to int type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to long type")
    .exclude("Fast fail for cast string type to decimal type in ansi mode")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z")
    .exclude("cast from array III")
    .exclude("cast from map II")
    .exclude("cast from map III")
    .exclude("cast from struct II")
    .exclude("cast from struct III")
  enableSuite[GlutenCollectionExpressionsSuite]
    .exclude("ArraysZip") // wait for https://github.com/ClickHouse/ClickHouse/pull/69576
    .exclude("Sequence of numbers")
    .exclude("Shuffle")
    .exclude("SPARK-33386: element_at ArrayIndexOutOfBoundsException")
    .exclude("SPARK-33460: element_at NoSuchElementException")
    .exclude("SPARK-36753: ArrayExcept should handle duplicated Double.NaN and Float.Nan")
    .exclude(
      "SPARK-36740: ArrayMin/ArrayMax/SortArray should handle NaN greater then non-NaN value")
    .excludeGlutenTest("Shuffle")
  enableSuite[GlutenComplexTypeSuite]
  enableSuite[GlutenConditionalExpressionSuite]
    .exclude("case when")
    .exclude("if/case when - null flags of non-primitive types")
  enableSuite[GlutenDateExpressionsSuite]
    .exclude("DayOfYear")
    .exclude("Year")
    .exclude("Quarter")
    .exclude("Month")
    .exclude("Day / DayOfMonth")
    .exclude("Seconds")
    .exclude("DayOfWeek")
    .exclude("WeekDay")
    .exclude("WeekOfYear")
    .exclude("DateFormat")
    .excludeGlutenTest("DateFormat")
    .exclude("Hour")
    .exclude("Minute")
    .exclude("date add interval")
    .exclude("time_add")
    .exclude("time_sub")
    .exclude("add_months")
    .exclude("SPARK-34721: add a year-month interval to a date")
    .exclude("months_between")
    .excludeGlutenTest("months_between")
    .exclude("next_day")
    .exclude("TruncDate")
    .exclude("TruncTimestamp")
    .exclude("unsupported fmt fields for trunc/date_trunc results null")
    .exclude("from_unixtime")
    .excludeGlutenTest("from_unixtime")
    .exclude("unix_timestamp")
    .exclude("to_unix_timestamp")
    .exclude("to_utc_timestamp")
    .exclude("from_utc_timestamp")
    .exclude("creating values of DateType via make_date")
    .exclude("creating values of Timestamp/TimestampNTZ via make_timestamp")
    .exclude("ISO 8601 week-numbering year")
    .exclude("extract the seconds part with fraction from timestamps")
    .exclude("SPARK-34903: timestamps difference")
    .exclude("SPARK-35916: timestamps without time zone difference")
    .exclude("SPARK-34896: subtract dates")
    .exclude("to_timestamp_ntz")
    .exclude("to_timestamp exception mode")
    .exclude("SPARK-31896: Handle am-pm timestamp parsing when hour is missing")
    .exclude("DATE_FROM_UNIX_DATE")
    .exclude("UNIX_SECONDS")
    .exclude("TIMESTAMP_SECONDS") // refer to https://github.com/ClickHouse/ClickHouse/issues/69280
    .exclude("TIMESTAMP_MICROS") // refer to https://github.com/apache/incubator-gluten/issues/7127
    .exclude("SPARK-33498: GetTimestamp,UnixTimestamp,ToUnixTimestamp with parseError")
    .exclude("SPARK-34739,SPARK-35889: add a year-month interval to a timestamp")
    .exclude("SPARK-34761,SPARK-35889: add a day-time interval to a timestamp")
    .exclude("SPARK-37552: convert a timestamp_ntz to another time zone")
    .exclude("SPARK-38195: add a quantity of interval units to a timestamp")
    .exclude("SPARK-38284: difference between two timestamps in units")
    .excludeGlutenTest("unix_timestamp")
    .excludeGlutenTest("to_unix_timestamp")
    .excludeGlutenTest("Hour")
  enableSuite[GlutenDecimalExpressionSuite]
  enableSuite[GlutenDecimalPrecisionSuite]
  enableSuite[GlutenHashExpressionsSuite]
    .exclude("sha2")
    .exclude("murmur3/xxHash64/hive hash: struct<null:void,boolean:boolean,byte:tinyint,short:smallint,int:int,long:bigint,float:float,double:double,bigDecimal:decimal(38,18),smallDecimal:decimal(10,0),string:string,binary:binary,date:date,timestamp:timestamp,udt:examplepoint>")
    .exclude("SPARK-30633: xxHash64 with long seed: struct<null:void,boolean:boolean,byte:tinyint,short:smallint,int:int,long:bigint,float:float,double:double,bigDecimal:decimal(38,18),smallDecimal:decimal(10,0),string:string,binary:binary,date:date,timestamp:timestamp,udt:examplepoint>")
    .exclude("murmur3/xxHash64/hive hash: struct<arrayOfNull:array<void>,arrayOfString:array<string>,arrayOfArrayOfString:array<array<string>>,arrayOfArrayOfInt:array<array<int>>,arrayOfStruct:array<struct<str:string>>,arrayOfUDT:array<examplepoint>>")
    .exclude("SPARK-30633: xxHash64 with long seed: struct<arrayOfNull:array<void>,arrayOfString:array<string>,arrayOfArrayOfString:array<array<string>>,arrayOfArrayOfInt:array<array<int>>,arrayOfStruct:array<struct<str:string>>,arrayOfUDT:array<examplepoint>>")
    .exclude("murmur3/xxHash64/hive hash: struct<structOfString:struct<str:string>,structOfStructOfString:struct<struct:struct<str:string>>,structOfArray:struct<array:array<string>>,structOfUDT:struct<udt:examplepoint>>")
    .exclude("SPARK-30633: xxHash64 with long seed: struct<structOfString:struct<str:string>,structOfStructOfString:struct<struct:struct<str:string>>,structOfArray:struct<array:array<string>>,structOfUDT:struct<udt:examplepoint>>")
    .exclude("SPARK-30633: xxHash with different type seeds")
    .exclude("SPARK-35113: HashExpression support DayTimeIntervalType/YearMonthIntervalType")
    .exclude("SPARK-35207: Compute hash consistent between -0.0 and 0.0")
  enableSuite[GlutenIntervalExpressionsSuite]
    .exclude("years")
    .exclude("months")
    .exclude("days")
    .exclude("hours")
    .exclude("minutes")
    .exclude("seconds")
    .exclude("multiply")
    .exclude("divide")
    .exclude("make interval")
    .exclude("ANSI mode: make interval")
    .exclude("SPARK-35130: make day time interval")
    .exclude("SPARK-34824: multiply year-month interval by numeric")
    .exclude("SPARK-34850: multiply day-time interval by numeric")
    .exclude("SPARK-34868: divide year-month interval by numeric")
    .exclude("SPARK-34875: divide day-time interval by numeric")
    .exclude("ANSI: extract years and months")
    .exclude("ANSI: extract days, hours, minutes and seconds")
    .exclude("SPARK-35129: make_ym_interval")
    .exclude("SPARK-35728: Check multiply/divide of day-time intervals of any fields by numeric")
    .exclude("SPARK-35778: Check multiply/divide of year-month intervals of any fields by numeric")
  enableSuite[GlutenLiteralExpressionSuite]
    .exclude("default")
    .exclude("SPARK-37967: Literal.create support ObjectType")
  enableSuite[GlutenMathExpressionsSuite]
    .exclude("unhex") // https://github.com/apache/incubator-gluten/issues/7232
    .exclude("round/bround/floor/ceil") // https://github.com/apache/incubator-gluten/issues/7233
    .exclude("atan2") // https://github.com/apache/incubator-gluten/issues/7233
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenNondeterministicSuite]
    .exclude("MonotonicallyIncreasingID")
    .exclude("SparkPartitionID")
    .exclude("InputFileName")
  enableSuite[GlutenNullExpressionsSuite]
    .exclude("AtLeastNNonNulls")
    .exclude("AtLeastNNonNulls should not throw 64KiB exception")
  enableSuite[GlutenPredicateSuite]
    .exclude("3VL Not")
    .exclude("3VL AND")
    .exclude("3VL OR")
    .exclude("3VL =")
    .exclude("basic IN/INSET predicate test")
    .exclude("IN with different types")
    .exclude("IN/INSET: binary")
    .exclude("IN/INSET: struct")
    .exclude("IN/INSET: array")
    .exclude("BinaryComparison: lessThan")
    .exclude("BinaryComparison: LessThanOrEqual")
    .exclude("BinaryComparison: GreaterThan")
    .exclude("BinaryComparison: GreaterThanOrEqual")
    .exclude("BinaryComparison: EqualTo")
    .exclude("BinaryComparison: EqualNullSafe")
    .exclude("BinaryComparison: null test")
    .exclude("EqualTo on complex type")
    .exclude("isunknown and isnotunknown")
    .exclude("SPARK-32764: compare special double/float values")
    .exclude("SPARK-32110: compare special double/float values in array")
    .exclude("SPARK-32110: compare special double/float values in struct")
  enableSuite[GlutenRandomSuite].exclude("random").exclude("SPARK-9127 codegen with long seed")
  enableSuite[GlutenRegexpExpressionsSuite]
    .exclude("LIKE Pattern")
    .exclude("LIKE Pattern ESCAPE '/'")
    .exclude("LIKE Pattern ESCAPE '#'")
    .exclude("LIKE Pattern ESCAPE '\"'")
    .exclude("RLIKE Regular Expression")
    .exclude("RegexReplace")
    .exclude("RegexExtract")
    .exclude("RegexExtractAll")
  enableSuite[GlutenSortOrderExpressionsSuite].exclude("SortPrefix")
  enableSuite[GlutenStringExpressionsSuite]
    .exclude("StringComparison")
    .exclude("Substring")
    .exclude("string substring_index function")
    .exclude("SPARK-40213: ascii for Latin-1 Supplement characters")
    .exclude("ascii for string")
    .exclude("base64/unbase64 for string")
    .exclude("encode/decode for string")
    .exclude("overlay for string")
    .exclude("overlay for byte array")
    .exclude("translate")
    .exclude("LOCATE")
    .exclude("REPEAT")
    .exclude("ParseUrl")
    .exclude("SPARK-33468: ParseUrl in ANSI mode should fail if input string is not a valid url")
    .exclude("FORMAT") // refer https://github.com/apache/incubator-gluten/issues/6765
    .exclude(
      "soundex unit test"
    ) // CH and spark returns different results when input non-ASCII characters
  enableSuite[GlutenTryCastSuite]
    .exclude("null cast")
    .exclude("cast string to date")
    .exclude("cast string to timestamp")
    .excludeGlutenTest("cast string to timestamp")
    .exclude("cast from boolean")
    .exclude("cast from int")
    .exclude("cast from long")
    .exclude("cast from float")
    .exclude("cast from double")
    .exclude("cast from timestamp")
    .exclude("data type casting")
    .exclude("cast and add")
    .exclude("from decimal")
    .exclude("cast from array")
    .exclude("cast from map")
    .exclude("cast from struct")
    .exclude("cast struct with a timestamp field")
    .exclude("cast between string and interval")
    .exclude("cast string to boolean")
    .exclude("SPARK-20302 cast with same structure")
    .exclude("SPARK-22500: cast for struct should not generate codes beyond 64KB")
    .exclude("SPARK-27671: cast from nested null type in struct")
    .exclude("Process Infinity, -Infinity, NaN in case insensitive manner")
    .exclude("SPARK-22825 Cast array to string")
    .exclude("SPARK-33291: Cast array with null elements to string")
    .exclude("SPARK-22973 Cast map to string")
    .exclude("SPARK-22981 Cast struct to string")
    .exclude("SPARK-33291: Cast struct with null elements to string")
    .exclude("SPARK-34667: cast year-month interval to string")
    .exclude("SPARK-34668: cast day-time interval to string")
    .exclude("SPARK-35698: cast timestamp without time zone to string")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35716: cast timestamp without time zone to date type")
    .exclude("SPARK-35718: cast date type to timestamp without timezone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("SPARK-35720: cast string to timestamp without timezone")
    .exclude("SPARK-35112: Cast string to day-time interval")
    .exclude("SPARK-35111: Cast string to year-month interval")
    .exclude("SPARK-35820: Support cast DayTimeIntervalType in different fields")
    .exclude("SPARK-35819: Support cast YearMonthIntervalType in different fields")
    .exclude("SPARK-35768: Take into account year-month interval fields in cast")
    .exclude("SPARK-35735: Take into account day-time interval fields in cast")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to byte type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to short type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to int type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to long type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to decimal type")
    .exclude("cast from invalid string to numeric should throw NumberFormatException")
    .exclude("Fast fail for cast string type to decimal type in ansi mode")
    .exclude("ANSI mode: cast string to boolean with parse error")
    .exclude("cast from timestamp II")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z II")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z")
    .exclude("cast from map II")
    .exclude("cast from struct II")
    .exclude("ANSI mode: cast string to timestamp with parse error")
    .exclude("ANSI mode: cast string to date with parse error")
    .exclude("SPARK-26218: Fix the corner case of codegen when casting float to Integer")
    .exclude("SPARK-35720: cast invalid string input to timestamp without time zone")
    .excludeGlutenTest("SPARK-35698: cast timestamp without time zone to string")
  enableSuite[GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSuite]
  enableSuite[GlutenDataSourceV2Suite]
    .exclude("partitioning reporting")
    .exclude("SPARK-33267: push down with condition 'in (..., null)' should not throw NPE")
  enableSuite[GlutenDeleteFromTableSuite]
  enableSuite[GlutenFileDataSourceV2FallBackSuite]
    // Rewritten
    .exclude("Fallback Parquet V2 to V1")
  enableSuite[GlutenKeyGroupedPartitioningSuite]
    .exclude("partitioned join: number of buckets mismatch should trigger shuffle")
    .exclude("partitioned join: only one side reports partitioning")
    .exclude("partitioned join: join with two partition keys and different # of partition keys")
  enableSuite[GlutenLocalScanSuite]
  enableSuite[GlutenSupportsCatalogOptionsSuite]
  enableSuite[GlutenTableCapabilityCheckSuite]
  enableSuite[GlutenWriteDistributionAndOrderingSuite]
  enableSuite[GlutenQueryCompilationErrorsSuite]
    .exclude("CANNOT_USE_MIXTURE: Using aggregate function with grouped aggregate pandas UDF")
    .exclude("UNSUPPORTED_FEATURE: Using pandas UDF aggregate expression with pivot")
  enableSuite[GlutenQueryExecutionErrorsSuite]
    .exclude(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION: compatibility with Spark 2.4/3.2 in reading/writing dates")
    .exclude("UNSUPPORTED_OPERATION - SPARK-38504: can't read TimestampNTZ as TimestampLTZ")
  enableSuite[GlutenQueryParsingErrorsSuite]
  enableSuite[FallbackStrategiesSuite]
  enableSuite[GlutenBroadcastExchangeSuite]
  enableSuite[GlutenCoalesceShufflePartitionsSuite]
    .exclude(
      "determining the number of reducers: aggregate operator(minNumPostShufflePartitions: 5)")
    .exclude("determining the number of reducers: join operator(minNumPostShufflePartitions: 5)")
    .exclude("determining the number of reducers: complex query 1(minNumPostShufflePartitions: 5)")
    .exclude("determining the number of reducers: complex query 2(minNumPostShufflePartitions: 5)")
    .exclude(
      "determining the number of reducers: plan already partitioned(minNumPostShufflePartitions: 5)")
    .exclude("determining the number of reducers: aggregate operator")
    .exclude("determining the number of reducers: join operator")
    .exclude("determining the number of reducers: complex query 1")
    .exclude("determining the number of reducers: complex query 2")
    .exclude("determining the number of reducers: plan already partitioned")
    .exclude("SPARK-24705 adaptive query execution works correctly when exchange reuse enabled")
    .exclude("Do not reduce the number of shuffle partition for repartition")
    .exclude("Union two datasets with different pre-shuffle partition number")
    .exclude("SPARK-34790: enable IO encryption in AQE partition coalescing")
    .excludeGlutenTest(
      "SPARK-24705 adaptive query execution works correctly when exchange reuse enabled")
    .excludeGlutenTest("SPARK-34790: enable IO encryption in AQE partition coalescing")
    .excludeGlutenTest(
      "determining the number of reducers: aggregate operator(minNumPostShufflePartitions: 5)")
    .excludeGlutenTest(
      "determining the number of reducers: join operator(minNumPostShufflePartitions: 5)")
    .excludeGlutenTest(
      "determining the number of reducers: complex query 1(minNumPostShufflePartitions: 5)")
    .excludeGlutenTest(
      "determining the number of reducers: complex query 2(minNumPostShufflePartitions: 5)")
    .excludeGlutenTest(
      "determining the number of reducers: plan already partitioned(minNumPostShufflePartitions: 5)")
    .excludeGlutenTest("determining the number of reducers: aggregate operator")
    .excludeGlutenTest("determining the number of reducers: join operator")
    .excludeGlutenTest("determining the number of reducers: complex query 1")
    .excludeGlutenTest("determining the number of reducers: complex query 2")
    .excludeGlutenTest("determining the number of reducers: plan already partitioned")
  enableSuite[GlutenExchangeSuite]
    .exclude("shuffling UnsafeRows in exchange")
    .exclude("SPARK-23207: Make repartition() generate consistent output")
    .exclude("Exchange reuse across the whole plan")
  enableSuite[GlutenReplaceHashWithSortAggSuite]
    .exclude("replace partial hash aggregate with sort aggregate")
    .exclude("replace partial and final hash aggregate together with sort aggregate")
    .exclude("do not replace hash aggregate if child does not have sort order")
    .exclude("do not replace hash aggregate if there is no group-by column")
    .excludeGlutenTest("replace partial hash aggregate with sort aggregate")
  enableSuite[GlutenReuseExchangeAndSubquerySuite]
  enableSuite[GlutenSQLAggregateFunctionSuite]
    .excludeGlutenTest("Return NaN or null when dividing by zero")
  enableSuite[GlutenSQLWindowFunctionSuite]
    .exclude("window function: partition and order expressions")
    .exclude("window function: expressions in arguments of a window functions")
    .exclude(
      "window function: multiple window expressions specified by range in a single expression")
    .exclude("SPARK-7595: Window will cause resolve failed with self join")
    .exclude(
      "SPARK-16633: lead/lag should return the default value if the offset row does not exist")
    .exclude("lead/lag should respect null values")
    .exclude("test with low buffer spill threshold")
  enableSuite[GlutenSameResultSuite]
  enableSuite[GlutenSortSuite]
    .exclude("basic sorting using ExternalSort")
    .exclude("sort followed by limit")
    .exclude("sorting does not crash for large inputs")
    .exclude("sorting updates peak execution memory")
    .exclude("SPARK-33260: sort order is a Stream")
    .exclude("sorting on StringType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on StringType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on StringType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on StringType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on StringType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on StringType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on StringType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on StringType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on LongType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on LongType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on LongType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on LongType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on LongType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on LongType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on LongType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on LongType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on IntegerType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on IntegerType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on IntegerType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on IntegerType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on IntegerType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on IntegerType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on IntegerType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on IntegerType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on DecimalType(20,5) with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DecimalType(20,5) with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on DecimalType(20,5) with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on DecimalType(20,5) with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on DecimalType(20,5) with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DecimalType(20,5) with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on DecimalType(20,5) with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude(
      "sorting on DecimalType(20,5) with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on DoubleType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DoubleType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on DoubleType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on DoubleType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on DoubleType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DoubleType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on DoubleType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on DoubleType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on DateType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DateType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on DateType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on DateType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on DateType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DateType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on DateType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on DateType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on BooleanType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on BooleanType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on BooleanType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on BooleanType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on BooleanType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on BooleanType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on BooleanType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on BooleanType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on DecimalType(38,18) with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DecimalType(38,18) with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on DecimalType(38,18) with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude(
      "sorting on DecimalType(38,18) with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude(
      "sorting on DecimalType(38,18) with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on DecimalType(38,18) with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude(
      "sorting on DecimalType(38,18) with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude(
      "sorting on DecimalType(38,18) with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on ByteType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on ByteType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on ByteType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on ByteType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on ByteType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on ByteType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on ByteType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on ByteType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on FloatType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on FloatType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on FloatType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on FloatType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on FloatType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on FloatType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on FloatType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on FloatType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on ShortType with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on ShortType with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on ShortType with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on ShortType with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on ShortType with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude("sorting on ShortType with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on ShortType with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on ShortType with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("SPARK-40089: decimal values sort correctly")
    .excludeByPrefix("sorting on YearMonthIntervalType(0,1) with")
  enableSuite[GlutenTakeOrderedAndProjectSuite]
    .exclude("TakeOrderedAndProject.doExecute without project")
    .exclude("TakeOrderedAndProject.doExecute with project")
  enableSuite[ClickHouseAdaptiveQueryExecSuite]
    .exclude("Change merge join to broadcast join")
    .exclude("Reuse the parallelism of coalesced shuffle in local shuffle read")
    .exclude("Reuse the default parallelism in local shuffle read")
    .exclude("Empty stage coalesced to 1-partition RDD")
    .exclude("Scalar subquery")
    .exclude("Scalar subquery in later stages")
    .exclude("multiple joins")
    .exclude("multiple joins with aggregate")
    .exclude("multiple joins with aggregate 2")
    .exclude("Exchange reuse")
    .exclude("Exchange reuse with subqueries")
    .exclude("Exchange reuse across subqueries")
    .exclude("Subquery reuse")
    .exclude("Broadcast exchange reuse across subqueries")
    .exclude("Change merge join to broadcast join without local shuffle read")
    .exclude(
      "Avoid changing merge join to broadcast join if too many empty partitions on build plan")
    .exclude("SPARK-32932: Do not use local shuffle read at final stage on write command")
    .exclude(
      "SPARK-30953: InsertAdaptiveSparkPlan should apply AQE on child plan of v2 write commands")
    .exclude("SPARK-37753: Allow changing outer join to broadcast join even if too many empty partitions on broadcast side")
    .exclude("SPARK-29544: adaptive skew join with different join types")
    .exclude("SPARK-34682: AQEShuffleReadExec operating on canonicalized plan")
    .exclude("SPARK-32717: AQEOptimizer should respect excludedRules configuration")
    .exclude("metrics of the shuffle read")
    .exclude("SPARK-31220, SPARK-32056: repartition by expression with AQE")
    .exclude("SPARK-31220, SPARK-32056: repartition by range with AQE")
    .exclude("SPARK-31220, SPARK-32056: repartition using sql and hint with AQE")
    .exclude("SPARK-32753: Only copy tags to node with no tags")
    .exclude("Logging plan changes for AQE")
    .exclude("SPARK-33551: Do not use AQE shuffle read for repartition")
    .exclude("SPARK-34091: Batch shuffle fetch in AQE partition coalescing")
    .exclude("SPARK-34899: Use origin plan if we can not coalesce shuffle partition")
    .exclude("SPARK-34980: Support coalesce partition through union")
    .exclude("SPARK-35239: Coalesce shuffle partition should handle empty input RDD")
    .exclude("SPARK-35264: Support AQE side broadcastJoin threshold")
    .exclude("SPARK-35264: Support AQE side shuffled hash join formula")
    .exclude("SPARK-35650: Coalesce number of partitions by AEQ")
    .exclude("SPARK-35650: Use local shuffle read if can not coalesce number of partitions")
    .exclude("SPARK-35725: Support optimize skewed partitions in RebalancePartitions")
    .exclude("SPARK-35888: join with a 0-partition table")
    .exclude("SPARK-33832: Support optimize skew join even if introduce extra shuffle")
    .exclude("SPARK-35968: AQE coalescing should not produce too small partitions by default")
    .exclude("SPARK-35794: Allow custom plugin for cost evaluator")
    .exclude("SPARK-36020: Check logical link in remove redundant projects")
    .exclude("SPARK-36032: Use inputPlan instead of currentPhysicalPlan to initialize logical link")
    .exclude("SPARK-37063: OptimizeSkewInRebalancePartitions support optimize non-root node")
    .exclude("SPARK-37357: Add small partition factor for rebalance partitions")
    .exclude("SPARK-37742: AQE reads invalid InMemoryRelation stats and mistakenly plans BHJ")
    .exclude("SPARK-37328: skew join with 3 tables")
    .exclude("SPARK-39915: Dataset.repartition(N) may not create N partitions")
    .exclude("Change broadcast join to merge join")
    .exclude("Avoid plan change if cost is greater")
    .exclude("SPARK-37652: optimize skewed join through union")
    .exclude("SPARK-35455: Unify empty relation optimization between normal and AQE optimizer " +
      "- single join")
    .exclude("SPARK-35455: Unify empty relation optimization between normal and AQE optimizer " +
      "- multi join")
    // Gluten columnar operator will have different number of shuffle
    .exclude("SPARK-29906: AQE should not introduce extra shuffle for outermost limit")
    .excludeGlutenTest("Empty stage coalesced to 1-partition RDD")
    .excludeGlutenTest(
      "Avoid changing merge join to broadcast join if too many empty partitions on build plan")
    .exclude("SPARK-30524: Do not optimize skew join if introduce additional shuffle")
    .excludeGlutenTest("SPARK-33551: Do not use AQE shuffle read for repartition")
    .excludeGlutenTest("SPARK-35264: Support AQE side broadcastJoin threshold")
    .excludeGlutenTest("SPARK-35264: Support AQE side shuffled hash join formula")
    .excludeGlutenTest("SPARK-35725: Support optimize skewed partitions in RebalancePartitions")
    .excludeGlutenTest(
      "SPARK-35968: AQE coalescing should not produce too small partitions by default")
    .excludeGlutenTest(
      "SPARK-37742: AQE reads invalid InMemoryRelation stats and mistakenly plans BHJ")
  enableSuite[GlutenBucketingUtilsSuite]
  enableSuite[GlutenCSVReadSchemaSuite]
  enableSuite[GlutenDataSourceStrategySuite]
  enableSuite[GlutenDataSourceSuite]
  enableSuite[GlutenFileFormatWriterSuite].excludeByPrefix(
    "empty file should be skipped while write to file")
  enableSuite[GlutenFileIndexSuite]
  enableSuite[GlutenFileMetadataStructSuite]
    .exclude("metadata struct (json): file metadata in streaming")
    .exclude("metadata struct (parquet): file metadata in streaming")
  enableSuite[GlutenFileSourceStrategySuite]
    .exclude("unpartitioned table, single partition")
    .exclude("partitioned table - after scan filters")
    .exclude("SPARK-32019: Add spark.sql.files.minPartitionNum config")
    .exclude(
      "SPARK-32352: Partially push down support data filter if it mixed in partition filters")
  enableSuite[GlutenHadoopFileLinesReaderSuite]
  enableSuite[GlutenHeaderCSVReadSchemaSuite]
    .exclude("append column at the end")
    .exclude("hide column at the end")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("read byte, int, short, long together")
    .exclude("change column type from float to double")
    .exclude("read float and double together")
    .exclude("change column type from float to decimal")
    .exclude("change column type from double to decimal")
    .exclude("read float, double, decimal together")
    .exclude("read as string")
  enableSuite[GlutenJsonReadSchemaSuite]
  enableSuite[GlutenMergedOrcReadSchemaSuite]
  enableSuite[GlutenMergedParquetReadSchemaSuite]
  enableSuite[GlutenOrcCodecSuite]
  enableSuite[GlutenOrcReadSchemaSuite]
  enableSuite[GlutenOrcV1AggregatePushDownSuite].exclude(
    "aggregate push down - different data types")
  enableSuite[GlutenOrcV2AggregatePushDownSuite].exclude(
    "aggregate push down - different data types")
  enableSuite[GlutenParquetCodecSuite]
  enableSuite[GlutenParquetReadSchemaSuite]
  enableSuite[GlutenParquetV1AggregatePushDownSuite]
  enableSuite[GlutenParquetV2AggregatePushDownSuite]
  enableSuite[GlutenPathFilterStrategySuite]
  enableSuite[GlutenPathFilterSuite]
  enableSuite[GlutenPruneFileSourcePartitionsSuite]
  enableSuite[GlutenVectorizedOrcReadSchemaSuite]
  enableSuite[GlutenVectorizedParquetReadSchemaSuite]
  enableSuite[GlutenBinaryFileFormatSuite]
    .exclude("column pruning - non-readable file")
  enableSuite[GlutenValidateRequirementsSuite]
  enableSuite[GlutenJsonLegacyTimeParserSuite]
    .exclude("Complex field and type inferring")
    .exclude("Loading a JSON dataset primitivesAsString returns complex fields as strings")
    .exclude("SPARK-4228 DataFrame to JSON")
    .exclude("SPARK-18352: Handle multi-line corrupt documents (PERMISSIVE)")
    .exclude("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .exclude("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenJsonSuite]
    .exclude("Complex field and type inferring")
    .exclude("Loading a JSON dataset primitivesAsString returns complex fields as strings")
    .exclude("SPARK-4228 DataFrame to JSON")
    .exclude("SPARK-18352: Handle multi-line corrupt documents (PERMISSIVE)")
    .exclude("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .exclude("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenJsonV1Suite]
    .exclude("Complex field and type inferring")
    .exclude("Loading a JSON dataset primitivesAsString returns complex fields as strings")
    .exclude("SPARK-4228 DataFrame to JSON")
    .exclude("SPARK-18352: Handle multi-line corrupt documents (PERMISSIVE)")
    .exclude("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .exclude("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenJsonV2Suite]
    .exclude("Complex field and type inferring")
    .exclude("Loading a JSON dataset primitivesAsString returns complex fields as strings")
    .exclude("SPARK-4228 DataFrame to JSON")
    .exclude("SPARK-18352: Handle multi-line corrupt documents (PERMISSIVE)")
    .exclude("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .exclude("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenOrcColumnarBatchReaderSuite]
  enableSuite[GlutenOrcFilterSuite].exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcPartitionDiscoverySuite]
  enableSuite[GlutenOrcQuerySuite]
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude(
      "SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
  enableSuite[GlutenOrcSourceSuite]
    .exclude("SPARK-24322 Fix incorrect workaround for bug in java.sql.Timestamp")
    .exclude("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .exclude("SPARK-31238, SPARK-31423: rebasing dates in write")
    .exclude("SPARK-31284: compatibility with Spark 2.4 in reading timestamps")
    .exclude("SPARK-31284, SPARK-31423: rebasing timestamps in write")
    .exclude("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle a column name which consists of only numbers")
    .exclude("SPARK-37812: Reuse result row when deserializing a struct")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    .excludeByPrefix(
      "SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false,")
    .excludeGlutenTest("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .excludeGlutenTest("SPARK-31238, SPARK-31423: rebasing dates in write")
    .excludeGlutenTest("SPARK-31284: compatibility with Spark 2.4 in reading timestamps")
    .excludeGlutenTest("SPARK-31284, SPARK-31423: rebasing timestamps in write")
    .excludeGlutenTest("SPARK-34862: Support ORC vectorized reader for nested column")
    .excludeGlutenTest(
      "SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
  enableSuite[GlutenOrcV1FilterSuite].exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcV1PartitionDiscoverySuite]
  enableSuite[GlutenOrcV1QuerySuite]
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude(
      "SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
  enableSuite[GlutenOrcV1SchemaPruningSuite]
    .exclude(
      "Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
  enableSuite[GlutenOrcV2QuerySuite]
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude(
      "SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
  enableSuite[GlutenOrcV2SchemaPruningSuite]
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation")
  enableSuite[GlutenParquetColumnIndexSuite]
    .exclude("test reading unaligned pages - test all types")
    .exclude("test reading unaligned pages - test all types (dict encode)")
  enableSuite[GlutenParquetCompressionCodecPrecedenceSuite]
  enableSuite[GlutenParquetDeltaByteArrayEncodingSuite]
  enableSuite[GlutenParquetDeltaEncodingInteger]
  enableSuite[GlutenParquetDeltaEncodingLong]
  enableSuite[GlutenParquetDeltaLengthByteArrayEncodingSuite]
  enableSuite[GlutenParquetEncodingSuite].exclude("All Types Dictionary").exclude("All Types Null")
  enableSuite[GlutenParquetFieldIdIOSuite]
  enableSuite[GlutenParquetFileFormatV1Suite]
    .exclude(
      "SPARK-36825, SPARK-36854: year-month/day-time intervals written and read as INT32/INT64")
  enableSuite[GlutenParquetFileFormatV2Suite]
    .exclude(
      "SPARK-36825, SPARK-36854: year-month/day-time intervals written and read as INT32/INT64")
  enableSuite[GlutenParquetIOSuite]
    .exclude("Standard mode - nested map with struct as key type")
    .exclude("Legacy mode - nested map with struct as key type")
    .exclude("vectorized reader: missing all struct fields")
    .exclude("SPARK-35640: read binary as timestamp should throw schema incompatible error")
    .exclude("SPARK-35640: int as long should throw schema incompatible error")
    .exclude("SPARK-36726: test incorrect Parquet row group file offset")
  enableSuite[GlutenParquetInteroperabilitySuite].exclude("parquet timestamp conversion")
  enableSuite[GlutenParquetProtobufCompatibilitySuite].exclude("struct with unannotated array")
  enableSuite[GlutenParquetRebaseDatetimeV1Suite]
    .exclude(
      "SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps")
    .exclude("SPARK-31159, SPARK-37705: rebasing timestamps in write")
    .exclude("SPARK-31159: rebasing dates in write")
    .exclude("SPARK-35427: datetime rebasing in the EXCEPTION mode")
    .excludeGlutenTest("SPARK-31159: rebasing dates in write")
  enableSuite[GlutenParquetRebaseDatetimeV2Suite]
    .exclude(
      "SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps")
    .exclude("SPARK-31159, SPARK-37705: rebasing timestamps in write")
    .exclude("SPARK-31159: rebasing dates in write")
    .exclude("SPARK-35427: datetime rebasing in the EXCEPTION mode")
  enableSuite[GlutenParquetSchemaInferenceSuite]
  enableSuite[GlutenParquetSchemaSuite]
    .exclude("schema mismatch failure error message for parquet reader")
    .exclude("schema mismatch failure error message for parquet vectorized reader")
  enableSuite[GlutenParquetThriftCompatibilitySuite]
    .exclude("Read Parquet file generated by parquet-thrift")
    .exclude("SPARK-10136 list of primitive list")
  enableSuite[GlutenParquetV1FilterSuite]
    .exclude("filter pushdown - date")
    .exclude("filter pushdown - timestamp")
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("filter pushdown - StringStartsWith")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-38825: in and notIn filters")
    .exclude("SPARK-36866: filter pushdown - year-month interval")
    .excludeGlutenTest("SPARK-25207: exception when duplicate fields in case-insensitive mode")
  enableSuite("org.apache.gluten.execution.parquet.GlutenParquetV1FilterSuite2")
    .exclude("filter pushdown - date")
    .exclude("filter pushdown - timestamp")
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("filter pushdown - StringStartsWith")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-38825: in and notIn filters")
    .exclude("SPARK-36866: filter pushdown - year-month interval")
    .excludeGlutenTest("SPARK-25207: exception when duplicate fields in case-insensitive mode")
  enableSuite[GlutenParquetV1PartitionDiscoverySuite]
    .exclude("SPARK-7847: Dynamic partition directory path escaping and unescaping")
    .exclude("Various partition value types")
    .exclude("Various inferred partition value types")
    .exclude(
      "SPARK-22109: Resolve type conflicts between strings and timestamps in partition column")
    .exclude("Resolve type conflicts - decimals, dates and timestamps in partition column")
  enableSuite[GlutenParquetV1QuerySuite]
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("SPARK-34212 Parquet should read decimals correctly")
  enableSuite[GlutenParquetV1SchemaPruningSuite]
    .exclude(
      "Spark vectorized reader - without partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - with partition data column - select only top-level fields")
    .exclude("Non-vectorized reader - without partition data column - select only top-level fields")
    .exclude("Non-vectorized reader - with partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude(
      "Spark vectorized reader - without partition data column - select only input_file_name()")
    .exclude("Spark vectorized reader - with partition data column - select only input_file_name()")
    .exclude(
      "Non-vectorized reader - without partition data column - select only input_file_name()")
    .exclude("Non-vectorized reader - with partition data column - select only input_file_name()")
    .exclude("Spark vectorized reader - without partition data column - select only expressions without references")
    .exclude("Spark vectorized reader - with partition data column - select only expressions without references")
    .exclude("Non-vectorized reader - without partition data column - select only expressions without references")
    .exclude("Non-vectorized reader - with partition data column - select only expressions without references")
    .exclude(
      "Spark vectorized reader - without partition data column - select a single complex field")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field")
    .exclude(
      "Non-vectorized reader - without partition data column - select a single complex field")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Spark vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude(
      "Spark vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude(
      "Non-vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Non-vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - without partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - with partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - without partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - with partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - without partition data column - select nullable complex field and having is not null predicate")
    .exclude("Spark vectorized reader - with partition data column - select nullable complex field and having is not null predicate")
    .exclude("Non-vectorized reader - without partition data column - select nullable complex field and having is not null predicate")
    .exclude("Non-vectorized reader - with partition data column - select nullable complex field and having is not null predicate")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and in clause")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and in clause")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and in clause")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and in clause")
    .exclude("Spark vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field and having is null predicate on another deep nested complex field")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field and having is null predicate on another deep nested complex field")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field and having is null predicate on another deep nested complex field")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field and having is null predicate on another deep nested complex field")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Spark vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function")
    .exclude(
      "Non-vectorized reader - with partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Sort")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Expand")
    .exclude(
      "Non-vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation")
  enableSuite[GlutenParquetV2FilterSuite]
    .exclude("filter pushdown - date")
    .exclude("filter pushdown - timestamp")
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("filter pushdown - StringStartsWith")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-38825: in and notIn filters")
    .exclude("SPARK-36866: filter pushdown - year-month interval")
    .excludeGlutenTest("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .excludeGlutenTest("filter pushdown - date")
  enableSuite[GlutenParquetV2PartitionDiscoverySuite]
    .exclude("SPARK-7847: Dynamic partition directory path escaping and unescaping")
    .exclude("Various partition value types")
    .exclude("Various inferred partition value types")
    .exclude(
      "SPARK-22109: Resolve type conflicts between strings and timestamps in partition column")
    .exclude("Resolve type conflicts - decimals, dates and timestamps in partition column")
  enableSuite[GlutenParquetV2QuerySuite]
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("SPARK-34212 Parquet should read decimals correctly")
  enableSuite[GlutenParquetV2SchemaPruningSuite]
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation")
  enableSuite[GlutenParquetVectorizedSuite]
  enableSuite[GlutenTextV1Suite]
  enableSuite[GlutenTextV2Suite]
  enableSuite[GlutenDataSourceV2StrategySuite]
  enableSuite[GlutenFileTableSuite]
  enableSuite[GlutenV2PredicateSuite]
  enableSuite[GlutenEnsureRequirementsSuite]
    .exclude("reorder should handle PartitioningCollection")
    .exclude("SPARK-35675: EnsureRequirements remove shuffle should respect PartitioningCollection")
  enableSuite[GlutenBroadcastJoinSuite]
    .exclude("unsafe broadcast hash join updates peak execution memory")
    .exclude("unsafe broadcast hash outer join updates peak execution memory")
    .exclude("unsafe broadcast left semi join updates peak execution memory")
    .exclude("SPARK-23192: broadcast hint should be retained after using the cached data")
    .exclude("SPARK-23214: cached data should not carry extra hint info")
    .exclude("broadcast hint in SQL")
    .exclude("Broadcast timeout")
    .exclude("broadcast join where streamed side's output partitioning is HashPartitioning")
    .exclude("broadcast join where streamed side's output partitioning is PartitioningCollection")
    .exclude("BroadcastHashJoinExec output partitioning size should be limited with a config")
    .exclude("SPARK-37742: join planning shouldn't read invalid InMemoryRelation stats")
  enableSuite[GlutenExistenceJoinSuite]
    .exclude("test single condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build left")
    .exclude("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build left")
    .exclude("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test composed unique condition (both non-equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test composed unique condition (both non-equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test composed unique condition (both non-equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test composed unique condition (both non-equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
  enableSuite[GlutenInnerJoinSuite]
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, one match per row using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, one match per row using SortMergeJoin (whole-stage-codegen on)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, multiple matches using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, multiple matches using SortMergeJoin (whole-stage-codegen on)")
    .exclude("inner join, no matches using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("inner join, no matches using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude(
      "inner join, no matches using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("inner join, no matches using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, no matches using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, no matches using SortMergeJoin (whole-stage-codegen on)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, null safe using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, null safe using SortMergeJoin (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using CartesianProduct")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
  enableSuite[GlutenOuterJoinSuite]
    .exclude("basic left outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic left outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic left outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic left outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("basic right outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic right outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic right outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic right outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("basic full outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic full outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic full outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic full outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("left outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("left outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("right outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("right outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("full outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("full outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
  enableSuite[GlutenCustomerExtensionSuite]
  enableSuite[GlutenSessionExtensionSuite]
  enableSuite[GlutenFallbackSuite]
  enableSuite[GlutenBucketedReadWithoutHiveSupportSuite]
    .exclude("avoid shuffle when join 2 bucketed tables")
    .exclude("only shuffle one side when join bucketed table and non-bucketed table")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket number")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket keys")
    .exclude("shuffle when join keys are not equal to bucket keys")
    .exclude("shuffle when join 2 bucketed tables with bucketing disabled")
    .exclude("check sort and shuffle when bucket and sort columns are join keys")
    .exclude("avoid shuffle and sort when sort columns are a super set of join keys")
    .exclude("only sort one side when sort columns are different")
    .exclude("only sort one side when sort columns are same but their ordering is different")
    .exclude("SPARK-17698 Join predicates should not contain filter clauses")
    .exclude(
      "SPARK-19122 Re-order join predicates if they match with the child's output partitioning")
    .exclude("SPARK-19122 No re-ordering should happen if set of join columns != set of child's partitioning columns")
    .exclude("SPARK-29655 Read bucketed tables obeys spark.sql.shuffle.partitions")
    .exclude("SPARK-32767 Bucket join should work if SHUFFLE_PARTITIONS larger than bucket number")
    .exclude("bucket coalescing eliminates shuffle")
    .exclude("bucket coalescing is not satisfied")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("disable bucketing when the output doesn't contain all bucketing columns")
    .exclude(
      "bucket coalescing is applied when join expressions match with partitioning expressions")
  enableSuite[GlutenBucketedWriteWithoutHiveSupportSuite]
  enableSuite[GlutenCreateTableAsSelectSuite]
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
  enableSuite[GlutenPartitionedWriteSuite]
    .exclude("SPARK-37231, SPARK-37240: Dynamic writes/reads of ANSI interval partitions")
  enableSuite[GlutenPathOptionSuite]
  enableSuite[GlutenPrunedScanSuite]
  enableSuite[GlutenResolvedDataSourceSuite]
  enableSuite[GlutenSaveLoadSuite]
  enableSuite[GlutenTableScanSuite]
    .exclude("Schema and all fields")
    .exclude("SELECT count(*) FROM tableWithSchema")
    .exclude("SELECT `string$%Field` FROM tableWithSchema")
    .exclude("SELECT int_Field FROM tableWithSchema WHERE int_Field < 5")
    .exclude("SELECT `longField_:,<>=+/~^` * 2 FROM tableWithSchema")
    .exclude(
      "SELECT structFieldSimple.key, arrayFieldSimple[1] FROM tableWithSchema a where int_Field=1")
    .exclude("SELECT structFieldComplex.Value.`value_(2)` FROM tableWithSchema")
  enableSuite[SparkFunctionStatistics]
  enableSuite[GlutenImplicitsTest]
    .excludeGlutenTest("fallbackSummary with shuffle")
    .excludeGlutenTest("fallbackSummary with cache")
    .excludeGlutenTest("fallbackSummary with cached data and shuffle")
  enableSuite[GlutenSparkSessionExtensionSuite]
  enableSuite[GlutenHiveSQLQueryCHSuite]
  enableSuite[GlutenPercentileSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = ClickHouseSQLQueryTestSettings
}
// scalastyle:on line.size.limit
