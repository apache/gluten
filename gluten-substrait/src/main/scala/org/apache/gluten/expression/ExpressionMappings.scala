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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.ExpressionNames._
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.ScalarSubquery

object ExpressionMappings {

  /** Mapping Spark scalar expression to Substrait function name */
  private lazy val SCALAR_SIGS: Seq[Sig] = Seq(
    Sig[Add](ADD),
    Sig[Asinh](ASINH),
    Sig[Acosh](ACOSH),
    Sig[Atanh](ATANH),
    Sig[Subtract](SUBTRACT),
    Sig[Multiply](MULTIPLY),
    Sig[Divide](DIVIDE),
    Sig[IntegralDivide](DIV),
    Sig[UnaryPositive](POSITIVE),
    Sig[UnaryMinus](NEGATIVE),
    Sig[And](AND),
    Sig[Or](OR),
    Sig[Cast](CAST),
    Sig[Coalesce](COALESCE),
    Sig[Like](LIKE),
    Sig[RLike](RLIKE),
    Sig[RegExpReplace](REGEXP_REPLACE),
    Sig[RegExpExtract](REGEXP_EXTRACT),
    Sig[RegExpExtractAll](REGEXP_EXTRACT_ALL),
    Sig[EqualTo](EQUAL),
    Sig[EqualNullSafe](EQUAL_NULL_SAFE),
    Sig[LessThan](LESS_THAN),
    Sig[LessThanOrEqual](LESS_THAN_OR_EQUAL),
    Sig[GreaterThan](GREATER_THAN),
    Sig[GreaterThanOrEqual](GREATER_THAN_OR_EQUAL),
    Sig[Alias](ALIAS),
    Sig[IsNotNull](IS_NOT_NULL),
    Sig[IsNull](IS_NULL),
    Sig[Not](NOT),
    Sig[IsNaN](IS_NAN),
    Sig[NaNvl](NANVL),
    Sig[TryEval](TRY_EVAL),

    // SparkSQL String functions
    Sig[Ascii](ASCII),
    Sig[Chr](CHR),
    Sig[Elt](ELT),
    Sig[Extract](EXTRACT),
    Sig[EndsWith](ENDS_WITH),
    Sig[StartsWith](STARTS_WITH),
    Sig[Concat](CONCAT),
    Sig[Contains](CONTAINS),
    Sig[StringInstr](INSTR),
    Sig[Length](LENGTH),
    Sig[Lower](LOWER),
    Sig[Upper](UPPER),
    Sig[SoundEx](SOUNDEX),
    Sig[StringLocate](LOCATE),
    Sig[StringTrimLeft](LTRIM),
    Sig[StringTrimRight](RTRIM),
    Sig[StringTrim](TRIM),
    Sig[StringTrimBoth](BTRIM),
    Sig[StringLPad](LPAD),
    Sig[StringRPad](RPAD),
    Sig[StringReplace](REPLACE),
    Sig[Reverse](REVERSE),
    Sig[StringSplit](SPLIT),
    Sig[Substring](SUBSTRING),
    Sig[SubstringIndex](SUBSTRING_INDEX),
    Sig[ConcatWs](CONCAT_WS),
    Sig[Left](LEFT),
    Sig[StringRepeat](REPEAT),
    Sig[StringTranslate](TRANSLATE),
    Sig[StringSpace](SPACE),
    Sig[InitCap](INITCAP),
    Sig[Overlay](OVERLAY),
    Sig[Conv](CONV),
    Sig[FindInSet](FIND_IN_SET),
    Sig[StringDecode](DECODE),
    Sig[Encode](ENCODE),
    Sig[Uuid](UUID),
    Sig[BitLength](BIT_LENGTH),
    Sig[OctetLength](OCTET_LENGTH),
    Sig[Levenshtein](LEVENSHTEIN),
    Sig[UnBase64](UNBASE64),
    Sig[Base64](BASE64),
    Sig[FormatString](FORMAT_STRING),

    // URL functions
    Sig[ParseUrl](PARSE_URL),

    // SparkSQL Math functions
    Sig[Abs](ABS),
    Sig[Bin](BIN),
    Sig[Ceil](CEIL),
    Sig[Floor](FLOOR),
    Sig[Exp](EXP),
    Sig[Expm1](EXPM1),
    Sig[Pow](POWER),
    Sig[Pmod](PMOD),
    Sig[Round](ROUND),
    Sig[BRound](BROUND),
    Sig[Sin](SIN),
    Sig[Sinh](SINH),
    Sig[Tan](TAN),
    Sig[Tanh](TANH),
    Sig[Cot](COT),
    Sig[BitwiseNot](BITWISE_NOT),
    Sig[BitwiseAnd](BITWISE_AND),
    Sig[BitwiseOr](BITWISE_OR),
    Sig[BitwiseXor](BITWISE_XOR),
    Sig[BitwiseGet](BITWISE_GET),
    Sig[BitwiseCount](BITWISE_COUNT),
    Sig[ShiftLeft](SHIFTLEFT),
    Sig[ShiftRight](SHIFTRIGHT),
    Sig[ShiftRightUnsigned](SHIFTRIGHTUNSIGNED),
    Sig[Sqrt](SQRT),
    Sig[Cbrt](CBRT),
    Sig[EulerNumber](E),
    Sig[Pi](PI),
    Sig[Hex](HEX),
    Sig[Unhex](UNHEX),
    Sig[Hypot](HYPOT),
    Sig[Signum](SIGN),
    Sig[Log10](LOG10),
    Sig[Log1p](LOG1P),
    Sig[Log2](LOG2),
    Sig[Log](LOG),
    Sig[Logarithm](LOGARITHM),
    Sig[ToRadians](RADIANS),
    Sig[Greatest](GREATEST),
    Sig[Least](LEAST),
    Sig[Remainder](REMAINDER),
    Sig[Factorial](FACTORIAL),
    Sig[Rand](RAND),
    Sig[Rint](RINT),
    // PrestoSQL Math functions
    Sig[Acos](ACOS),
    Sig[Asin](ASIN),
    Sig[Atan](ATAN),
    Sig[Atan2](ATAN2),
    Sig[Cos](COS),
    Sig[Cosh](COSH),
    Sig[ToDegrees](DEGREES),
    // SparkSQL DateTime functions
    Sig[Year](EXTRACT),
    Sig[YearOfWeek](EXTRACT),
    Sig[Quarter](EXTRACT),
    Sig[Month](EXTRACT),
    Sig[WeekOfYear](EXTRACT),
    Sig[WeekDay](EXTRACT),
    Sig[DayOfWeek](EXTRACT),
    Sig[DayOfMonth](EXTRACT),
    Sig[DayOfYear](EXTRACT),
    Sig[Hour](EXTRACT),
    Sig[Minute](EXTRACT),
    Sig[Second](EXTRACT),
    Sig[FromUnixTime](FROM_UNIXTIME),
    Sig[DateAdd](DATE_ADD),
    Sig[DateSub](DATE_SUB),
    Sig[DateDiff](DATE_DIFF),
    Sig[ToUnixTimestamp](TO_UNIX_TIMESTAMP),
    Sig[UnixTimestamp](UNIX_TIMESTAMP),
    Sig[AddMonths](ADD_MONTHS),
    Sig[DateFormatClass](DATE_FORMAT),
    Sig[TruncDate](TRUNC),
    Sig[TruncTimestamp](DATE_TRUNC),
    Sig[GetTimestamp](GET_TIMESTAMP),
    Sig[NextDay](NEXT_DAY),
    Sig[LastDay](LAST_DAY),
    Sig[MonthsBetween](MONTHS_BETWEEN),
    Sig[DateFromUnixDate](DATE_FROM_UNIX_DATE),
    Sig[UnixDate](UNIX_DATE),
    Sig[MakeDate](MAKE_DATE),
    Sig[MakeTimestamp](MAKE_TIMESTAMP),
    Sig[MakeYMInterval](MAKE_YM_INTERVAL),
    Sig[ToUTCTimestamp](TO_UTC_TIMESTAMP),
    Sig[FromUTCTimestamp](FROM_UTC_TIMESTAMP),
    Sig[UnixSeconds](UNIX_SECONDS),
    Sig[UnixMillis](UNIX_MILLIS),
    Sig[UnixMicros](UNIX_MICROS),
    Sig[SecondsToTimestamp](TIMESTAMP_SECONDS),
    Sig[MillisToTimestamp](TIMESTAMP_MILLIS),
    Sig[MicrosToTimestamp](TIMESTAMP_MICROS),
    Sig[PreciseTimestampConversion](PRECYSE_TIMESTAMP_CONVERSION),
    // JSON functions
    Sig[GetJsonObject](GET_JSON_OBJECT),
    Sig[LengthOfJsonArray](JSON_ARRAY_LENGTH),
    Sig[StructsToJson](TO_JSON),
    Sig[JsonToStructs](FROM_JSON),
    Sig[JsonTuple](JSON_TUPLE),
    Sig[JsonObjectKeys](JSON_OBJECT_KEYS),
    // Hash functions
    Sig[Murmur3Hash](MURMUR3HASH),
    Sig[XxHash64](XXHASH64),
    Sig[Md5](MD5),
    Sig[Sha1](SHA1),
    Sig[Sha2](SHA2),
    Sig[Crc32](CRC32),
    // Array functions
    Sig[ArrayTransform](TRANSFORM),
    Sig[Size](SIZE),
    Sig[Slice](SLICE),
    Sig[Sequence](SEQUENCE),
    Sig[CreateArray](CREATE_ARRAY),
    Sig[Explode](EXPLODE),
    // JsonTupleExplode' behavior are the same with Explode
    Sig[JsonTupleExplode](EXPLODE),
    Sig[Inline](INLINE),
    Sig[ArrayAggregate](AGGREGATE),
    Sig[LambdaFunction](LAMBDAFUNCTION),
    Sig[NamedLambdaVariable](NAMED_LAMBDA_VARIABLE),
    Sig[PosExplode](POSEXPLODE),
    Sig[GetArrayItem](GET_ARRAY_ITEM),
    Sig[ElementAt](ELEMENT_AT),
    Sig[ArrayContains](ARRAY_CONTAINS),
    Sig[ArrayMax](ARRAY_MAX),
    Sig[ArrayMin](ARRAY_MIN),
    Sig[ArrayJoin](ARRAY_JOIN),
    Sig[SortArray](SORT_ARRAY),
    Sig[ArraysOverlap](ARRAYS_OVERLAP),
    Sig[ArrayPosition](ARRAY_POSITION),
    Sig[ArrayDistinct](ARRAY_DISTINCT),
    Sig[ArrayUnion](ARRAY_UNION),
    Sig[ArrayIntersect](ARRAY_INTERSECT),
    Sig[GetArrayStructFields](GET_ARRAY_STRUCT_FIELDS),
    Sig[ArrayExcept](ARRAY_EXCEPT),
    Sig[ArrayRepeat](ARRAY_REPEAT),
    Sig[ArrayRemove](ARRAY_REMOVE),
    Sig[ArraysZip](ARRAYS_ZIP),
    Sig[ArrayFilter](FILTER),
    Sig[ArrayForAll](FORALL),
    Sig[ArrayExists](EXISTS),
    Sig[ArraySort](ARRAY_SORT),
    Sig[Shuffle](SHUFFLE),
    Sig[ZipWith](ZIP_WITH),
    Sig[Flatten](FLATTEN),
    // Map functions
    Sig[CreateMap](CREATE_MAP),
    Sig[GetMapValue](GET_MAP_VALUE),
    Sig[MapConcat](MAP_CONCAT),
    Sig[MapKeys](MAP_KEYS),
    Sig[MapValues](MAP_VALUES),
    Sig[MapFromArrays](MAP_FROM_ARRAYS),
    Sig[MapEntries](MAP_ENTRIES),
    Sig[MapZipWith](MAP_ZIP_WITH),
    Sig[MapFromEntries](MAP_FROM_ENTRIES),
    Sig[StringToMap](STR_TO_MAP),
    Sig[TransformKeys](TRANSFORM_KEYS),
    Sig[TransformValues](TRANSFORM_VALUES),
    // Struct functions
    Sig[GetStructField](GET_STRUCT_FIELD),
    Sig[CreateNamedStruct](NAMED_STRUCT),
    // Directly use child expression transformer
    Sig[KnownNotNull](KNOWN_NOT_NULL),
    Sig[KnownFloatingPointNormalized](KNOWN_FLOATING_POINT_NORMALIZED),
    Sig[NormalizeNaNAndZero](NORMALIZE_NANAND_ZERO),
    // Specific expression
    Sig[If](IF),
    Sig[AttributeReference](ATTRIBUTE_REFERENCE),
    Sig[BoundReference](BOUND_REFERENCE),
    Sig[Literal](LITERAL),
    Sig[CaseWhen](CASE_WHEN),
    Sig[In](IN),
    Sig[InSet](IN_SET),
    Sig[ScalarSubquery](SCALAR_SUBQUERY),
    Sig[DynamicPruningExpression](DYNAMIC_PRUNING_EXPRESSION),
    Sig[CheckOverflow](CHECK_OVERFLOW),
    Sig[MakeDecimal](MAKE_DECIMAL),
    Sig[PromotePrecision](PROMOTE_PRECISION),
    Sig[SparkPartitionID](SPARK_PARTITION_ID),
    Sig[AtLeastNNonNulls](AT_LEAST_N_NON_NULLS),
    Sig[WidthBucket](WIDTH_BUCKET),
    Sig[ReplicateRows](REPLICATE_ROWS),
    Sig[RaiseError](RAISE_ERROR),
    Sig[SparkVersion](VERSION),
    // Decimal
    Sig[UnscaledValue](UNSCALED_VALUE),
    // Generator function
    Sig[Stack](STACK)
  ) ++ SparkShimLoader.getSparkShims.scalarExpressionMappings

  /** Mapping Spark aggregate expression to Substrait function name */
  private val AGGREGATE_SIGS: Seq[Sig] = Seq(
    Sig[Sum](SUM),
    Sig[Average](AVG),
    Sig[Count](COUNT),
    Sig[CountDistinct](COUNT_DISTINCT),
    Sig[Min](MIN),
    Sig[Max](MAX),
    Sig[MaxBy](MAX_BY),
    Sig[MinBy](MIN_BY),
    Sig[StddevSamp](STDDEV_SAMP),
    Sig[StddevPop](STDDEV_POP),
    Sig[VarianceSamp](VAR_SAMP),
    Sig[VariancePop](VAR_POP),
    Sig[BitAndAgg](BIT_AND_AGG),
    Sig[BitOrAgg](BIT_OR_AGG),
    Sig[BitXorAgg](BIT_XOR_AGG),
    Sig[Corr](CORR),
    Sig[CovPopulation](COVAR_POP),
    Sig[CovSample](COVAR_SAMP),
    Sig[Last](LAST),
    Sig[First](FIRST),
    Sig[Skewness](SKEWNESS),
    Sig[Kurtosis](KURTOSIS),
    Sig[ApproximatePercentile](APPROX_PERCENTILE),
    Sig[HyperLogLogPlusPlus](APPROX_COUNT_DISTINCT),
    Sig[Percentile](PERCENTILE)
  ) ++ SparkShimLoader.getSparkShims.aggregateExpressionMappings

  /** Mapping Spark window expression to Substrait function name */
  private val WINDOW_SIGS: Seq[Sig] = Seq(
    Sig[Rank](RANK),
    Sig[DenseRank](DENSE_RANK),
    Sig[RowNumber](ROW_NUMBER),
    Sig[CumeDist](CUME_DIST),
    Sig[PercentRank](PERCENT_RANK),
    Sig[NTile](NTILE),
    Sig[Lead](LEAD),
    Sig[Lag](LAG),
    Sig[NthValue](NTH_VALUE)
  )

  private val RUNTIME_REPLACEABLE_SIGS: Seq[Sig] = Seq(
    Sig[AssertTrue](ASSERT_TRUE),
    Sig[NullIf](NULLIF),
    Sig[Nvl](NVL),
    Sig[Nvl2](NVL2),
    Sig[Right](RIGHT)
  ) ++ SparkShimLoader.getSparkShims.runtimeReplaceableExpressionMappings

  def blacklistExpressionMap: Map[Class[_], String] = {
    partitionExpressionMapByBlacklist._1
  }

  def expressionsMap: Map[Class[_], String] = {
    partitionExpressionMapByBlacklist._2
  }

  private def partitionExpressionMapByBlacklist: (Map[Class[_], String], Map[Class[_], String]) = {
    val blacklist = GlutenConfig.get.expressionBlacklist
    val (blacklistedExpr, filteredExpr) = (defaultExpressionsMap ++ toMap(
      BackendsApiManager.getSparkPlanExecApiInstance.extraExpressionMappings)).partition(
      kv => blacklist.contains(kv._2))
    (blacklistedExpr, filteredExpr)
  }

  // This is needed when generating function support status documentation for Spark built-in
  // functions.
  // Used by gluten/tools/scripts/gen-function-support-docs.py
  def listExpressionMappings(): Array[(String, String)] = {
    (expressionsMap ++ toMap(RUNTIME_REPLACEABLE_SIGS))
      .map(kv => (kv._1.getSimpleName, kv._2))
      .toArray
  }

  private lazy val defaultExpressionsMap: Map[Class[_], String] = {
    toMap(SCALAR_SIGS ++ AGGREGATE_SIGS ++ WINDOW_SIGS)
  }

  private def toMap(sigs: Seq[Sig]): Map[Class[_], String] = {
    sigs
      .map(s => (s.expClass, s.name))
      .toMap[Class[_], String]
  }
}
