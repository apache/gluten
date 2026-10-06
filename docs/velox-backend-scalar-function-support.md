# Scalar Functions Support Status

**Out of 433 scalar functions in Spark 4.1, Gluten currently fully supports 261 functions and partially supports 31 functions.**

The status applies to `spark.sql.ansi.enabled=false`. When ANSI mode is enabled, Gluten falls back to vanilla Spark
(see `spark.gluten.sql.ansiFallback.enabled`).

## Array Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| array             | CreateArray         | S        |                |
| array_append      | ArrayAppend         | S        |                |
| array_compact     | ArrayCompact        | S        |                |
| array_contains    | ArrayContains       | S        |                |
| array_distinct    | ArrayDistinct       | S        |                |
| array_except      | ArrayExcept         | S        |                |
| array_insert      | ArrayInsert         | S        |                |
| array_intersect   | ArrayIntersect      | S        |                |
| array_join        | ArrayJoin           | S        |                |
| array_max         | ArrayMax            | S        |                |
| array_min         | ArrayMin            | S        |                |
| array_position    | ArrayPosition       | S        |                |
| array_prepend     | ArrayPrepend        | S        |                |
| array_remove      | ArrayRemove         | S        |                |
| array_repeat      | ArrayRepeat         | S        |                |
| array_size        | ArraySize           | S        |                |
| array_union       | ArrayUnion          | S        |                |
| arrays_overlap    | ArraysOverlap       | S        |                |
| arrays_zip        | ArraysZip           | S        |                |
| flatten           | Flatten             | S        |                |
| get               | Get                 | S        |                |
| sequence          | Sequence            |          |                |
| shuffle           | Shuffle             | S        |                |
| slice             | Slice               | S        |                |
| sort_array        | SortArray           | S        |                |

## Bitwise Functions

| Spark Functions    | Spark Expressions   | Status   | Restrictions   |
|--------------------|---------------------|----------|----------------|
| &                  | BitwiseAnd          | S        |                |
| <<                 | ShiftLeft           | S        |                |
| >>                 | ShiftRight          | S        |                |
| >>>                | ShiftRightUnsigned  |          |                |
| ^                  | BitwiseXor          | S        |                |
| bit_count          | BitwiseCount        | S        |                |
| bit_get            | BitwiseGet          | S        |                |
| getbit             | BitwiseGet          | S        |                |
| shiftleft          | ShiftLeft           | S        |                |
| shiftright         | ShiftRight          | S        |                |
| shiftrightunsigned | ShiftRightUnsigned  |          |                |
| &#124;             | BitwiseOr           | S        |                |
| ~                  | BitwiseNot          | S        |                |

## Collection Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| cardinality       | Size                | S        |                |
| concat            | Concat              | S        |                |
| element_at        | ElementAt           | S        |                |
| reverse           | Reverse             | S        |                |
| size              | Size                | S        |                |
| try_element_at    | TryElementAt        |          |                |

## Conditional Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| between           | Between             | S        |                |
| coalesce          | Coalesce            | S        |                |
| if                | If                  | S        |                |
| ifnull            | Nvl                 | S        |                |
| nanvl             | NaNvl               | S        |                |
| nullif            | NullIf              | S        |                |
| nullifzero        | NullIfZero          | S        |                |
| nvl               | Nvl                 | S        |                |
| nvl2              | Nvl2                | S        |                |
| when              | CaseWhen            | S        |                |
| zeroifnull        | ZeroIfNull          | S        |                |

## Conversion Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| bigint            |                     | S        |                |
| binary            |                     | S        |                |
| boolean           |                     | S        |                |
| cast              | Cast                | S        |                |
| date              |                     | S        |                |
| decimal           |                     | S        |                |
| double            |                     | S        |                |
| float             |                     | S        |                |
| int               |                     | S        |                |
| smallint          |                     | S        |                |
| string            |                     | S        |                |
| time              |                     |          |                |
| timestamp         |                     | S        |                |
| tinyint           |                     | S        |                |

## Csv Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| from_csv          | CsvToStructs        |          |                |
| schema_of_csv     | SchemaOfCsv         |          |                |
| to_csv            | StructsToCsv        |          |                |

## Date and Timestamp Functions

| Spark Functions        | Spark Expressions                    | Status   | Restrictions                        |
|------------------------|--------------------------------------|----------|-------------------------------------|
| add_months             | AddMonths                            | S        |                                     |
| convert_timezone       | ConvertTimezone                      | S        |                                     |
| curdate                | CurDateExpressionBuilder             |          |                                     |
| current_date           | CurrentDate                          |          |                                     |
| current_time           | CurrentTime                          |          |                                     |
| current_timestamp      | CurrentTimestamp                     |          |                                     |
| current_timezone       | CurrentTimeZone                      |          |                                     |
| date_add               | DateAdd                              | S        |                                     |
| date_diff              | DateDiff                             | S        |                                     |
| date_format            | DateFormatClass                      | S        |                                     |
| date_from_unix_date    | DateFromUnixDate                     | S        |                                     |
| date_part              | DatePartExpressionBuilder            | S        |                                     |
| date_sub               | DateSub                              | S        |                                     |
| date_trunc             | TruncTimestamp                       | S        |                                     |
| dateadd                | DateAdd                              | S        |                                     |
| datediff               | DateDiff                             | S        |                                     |
| datepart               | DatePartExpressionBuilder            | S        |                                     |
| day                    | DayOfMonth                           | S        |                                     |
| dayname                | DayName                              | S        |                                     |
| dayofmonth             | DayOfMonth                           | S        |                                     |
| dayofweek              | DayOfWeek                            | S        |                                     |
| dayofyear              | DayOfYear                            | S        |                                     |
| extract                | Extract                              | S        |                                     |
| from_unixtime          | FromUnixTime                         | S        |                                     |
| from_utc_timestamp     | FromUTCTimestamp                     | S        |                                     |
| hour                   | HourExpressionBuilder                | S        |                                     |
| last_day               | LastDay                              | S        |                                     |
| localtimestamp         | LocalTimestamp                       |          |                                     |
| make_date              | MakeDate                             | S        |                                     |
| make_dt_interval       | MakeDTInterval                       |          |                                     |
| make_interval          | MakeInterval                         |          |                                     |
| make_time              | MakeTime                             |          |                                     |
| make_timestamp         | MakeTimestampExpressionBuilder       | PS       | DATE and TIME arguments unsupported |
| make_timestamp_ltz     | MakeTimestampLTZExpressionBuilder    | PS       | DATE and TIME arguments unsupported |
| make_timestamp_ntz     | MakeTimestampNTZExpressionBuilder    | PS       | DATE and TIME arguments unsupported |
| make_ym_interval       | MakeYMInterval                       | S        |                                     |
| minute                 | MinuteExpressionBuilder              | S        |                                     |
| month                  | Month                                | S        |                                     |
| monthname              | MonthName                            | S        |                                     |
| months_between         | MonthsBetween                        | S        |                                     |
| next_day               | NextDay                              | S        |                                     |
| now                    | Now                                  |          |                                     |
| quarter                | Quarter                              | S        |                                     |
| second                 | SecondExpressionBuilder              | S        |                                     |
| session_window         | SessionWindow                        |          |                                     |
| time_diff              | TimeDiff                             |          |                                     |
| time_trunc             | TimeTrunc                            |          |                                     |
| timestamp_micros       | MicrosToTimestamp                    | S        |                                     |
| timestamp_millis       | MillisToTimestamp                    | S        |                                     |
| timestamp_seconds      | SecondsToTimestamp                   | PS       |                                     |
| to_date                | ParseToDate                          |          |                                     |
| to_time                | ToTime                               |          |                                     |
| to_timestamp           | ParseToTimestamp                     |          |                                     |
| to_timestamp_ltz       | ParseToTimestampLTZExpressionBuilder |          |                                     |
| to_timestamp_ntz       | ParseToTimestampNTZExpressionBuilder |          |                                     |
| to_unix_timestamp      | ToUnixTimestamp                      | PS       |                                     |
| to_utc_timestamp       | ToUTCTimestamp                       | S        |                                     |
| trunc                  | TruncDate                            | S        |                                     |
| try_make_interval      | TryMakeInterval                      |          |                                     |
| try_make_timestamp     | TryMakeTimestampExpressionBuilder    |          |                                     |
| try_make_timestamp_ltz | TryMakeTimestampLTZExpressionBuilder |          |                                     |
| try_make_timestamp_ntz | TryMakeTimestampNTZExpressionBuilder |          |                                     |
| try_to_date            | TryToDateExpressionBuilder           |          |                                     |
| try_to_time            | TryToTimeExpressionBuilder           |          |                                     |
| try_to_timestamp       | TryToTimestampExpressionBuilder      |          |                                     |
| unix_date              | UnixDate                             | S        |                                     |
| unix_micros            | UnixMicros                           | S        |                                     |
| unix_millis            | UnixMillis                           | S        |                                     |
| unix_seconds           | UnixSeconds                          | S        |                                     |
| unix_timestamp         | UnixTimestamp                        | PS       |                                     |
| weekday                | WeekDay                              | S        |                                     |
| weekofyear             | WeekOfYear                           | S        |                                     |
| window                 | TimeWindow                           |          |                                     |
| window_time            | WindowTime                           |          |                                     |
| year                   | Year                                 | S        |                                     |

## Hash Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| crc32             | Crc32               | S        |                |
| hash              | Murmur3Hash         | S        |                |
| md5               | Md5                 | S        |                |
| sha               | Sha1                | S        |                |
| sha1              | Sha1                | S        |                |
| sha2              | Sha2                | S        |                |
| xxhash64          | XxHash64            | S        |                |

## JSON Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions                                                                                                                                                                                                                                                                                                                                         |
|-------------------|---------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| from_json         | JsonToStructs       | PS       | from_json with 'spark.sql.caseSensitive = true' is not supported in Velox<br>from_json with 'spark.sql.json.enablePartialResults = false' is not supported in Velox<br>from_json with column corrupt record is not supported in Velox<br>from_json with duplicate keys is not supported in Velox<br>from_json with options is not supported in Velox |
| get_json_object   | GetJsonObject       | S        |                                                                                                                                                                                                                                                                                                                                                      |
| json_array_length | LengthOfJsonArray   | S        |                                                                                                                                                                                                                                                                                                                                                      |
| json_object_keys  | JsonObjectKeys      | S        |                                                                                                                                                                                                                                                                                                                                                      |
| json_tuple        | JsonTuple           | S        |                                                                                                                                                                                                                                                                                                                                                      |
| schema_of_json    | SchemaOfJson        |          |                                                                                                                                                                                                                                                                                                                                                      |
| to_json           | StructsToJson       | PS       | When 'spark.sql.caseSensitive = false', to_json produces unexpected result for struct field with uppercase name<br>to_json with options is not supported in Velox                                                                                                                                                                                    |

## Lambda Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| aggregate         | ArrayAggregate      | S        |                |
| array_sort        | ArraySort           | S        |                |
| exists            | ArrayExists         | S        |                |
| filter            | ArrayFilter         | S        |                |
| forall            | ArrayForAll         | S        |                |
| map_filter        | MapFilter           | S        |                |
| map_zip_with      | MapZipWith          | S        |                |
| reduce            | ArrayAggregate      | S        |                |
| transform         | ArrayTransform      | S        |                |
| transform_keys    | TransformKeys       | S        |                |
| transform_values  | TransformValues     | S        |                |
| zip_with          | ZipWith             | S        |                |

## Map Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions                                                                |
|-------------------|---------------------|----------|-----------------------------------------------------------------------------|
| map               | CreateMap           | PS       |                                                                             |
| map_concat        | MapConcat           | PS       |                                                                             |
| map_contains_key  | MapContainsKey      | S        |                                                                             |
| map_entries       | MapEntries          | S        |                                                                             |
| map_from_arrays   | MapFromArrays       | S        |                                                                             |
| map_from_entries  | MapFromEntries      | S        |                                                                             |
| map_keys          | MapKeys             | S        |                                                                             |
| map_values        | MapValues           | S        |                                                                             |
| str_to_map        | StringToMap         | PS       | Only spark.sql.mapKeyDedupPolicy = EXCEPTION is supported for Velox backend |

## Mathematical Functions

| Spark Functions   | Spark Expressions      | Status   | Restrictions   |
|-------------------|------------------------|----------|----------------|
| %                 | Remainder              | S        |                |
| *                 | Multiply               | S        |                |
| +                 | Add                    | S        |                |
| -                 | Subtract               | S        |                |
| /                 | Divide                 | S        |                |
| abs               | Abs                    | S        |                |
| acos              | Acos                   | S        |                |
| acosh             | Acosh                  | S        |                |
| asin              | Asin                   | S        |                |
| asinh             | Asinh                  | S        |                |
| atan              | Atan                   | S        |                |
| atan2             | Atan2                  | S        |                |
| atanh             | Atanh                  | S        |                |
| bin               | Bin                    | S        |                |
| bround            | BRound                 |          |                |
| cbrt              | Cbrt                   | S        |                |
| ceil              | CeilExpressionBuilder  | PS       |                |
| ceiling           | CeilExpressionBuilder  | PS       |                |
| conv              | Conv                   | S        |                |
| cos               | Cos                    | S        |                |
| cosh              | Cosh                   | S        |                |
| cot               | Cot                    | S        |                |
| csc               | Csc                    | S        |                |
| degrees           | ToDegrees              | S        |                |
| div               | IntegralDivide         | S        |                |
| e                 | EulerNumber            | S        |                |
| exp               | Exp                    | S        |                |
| expm1             | Expm1                  | S        |                |
| factorial         | Factorial              | S        |                |
| floor             | FloorExpressionBuilder | PS       |                |
| greatest          | Greatest               | S        |                |
| hex               | Hex                    | S        |                |
| hypot             | Hypot                  | S        |                |
| least             | Least                  | S        |                |
| ln                | Log                    | S        |                |
| log               | Logarithm              | S        |                |
| log10             | Log10                  | S        |                |
| log1p             | Log1p                  | S        |                |
| log2              | Log2                   | S        |                |
| mod               | Remainder              | S        |                |
| negative          | UnaryMinus             | S        |                |
| pi                | Pi                     | S        |                |
| pmod              | Pmod                   | S        |                |
| positive          | UnaryPositive          | S        |                |
| pow               | Pow                    | S        |                |
| power             | Pow                    | S        |                |
| radians           | ToRadians              | S        |                |
| rand              | Rand                   | S        |                |
| randn             | Randn                  | S        |                |
| random            | Rand                   | S        |                |
| rint              | Rint                   | S        |                |
| round             | Round                  | S        |                |
| sec               | Sec                    | S        |                |
| sign              | Signum                 | S        |                |
| signum            | Signum                 | S        |                |
| sin               | Sin                    | S        |                |
| sinh              | Sinh                   | S        |                |
| sqrt              | Sqrt                   | S        |                |
| tan               | Tan                    | S        |                |
| tanh              | Tanh                   | S        |                |
| try_add           | TryAdd                 | PS       |                |
| try_divide        | TryDivide              |          |                |
| try_mod           | TryMod                 |          |                |
| try_multiply      | TryMultiply            |          |                |
| try_subtract      | TrySubtract            |          |                |
| unhex             | Unhex                  | S        |                |
| uniform           | Uniform                |          |                |
| width_bucket      | WidthBucket            | S        |                |

## Misc Functions

| Spark Functions                | Spark Expressions          | Status   | Restrictions   |
|--------------------------------|----------------------------|----------|----------------|
| aes_decrypt                    | AesDecrypt                 |          |                |
| aes_encrypt                    | AesEncrypt                 |          |                |
| approx_top_k_estimate          | ApproxTopKEstimate         |          |                |
| assert_true                    | AssertTrue                 | S        |                |
| bitmap_bit_position            | BitmapBitPosition          |          |                |
| bitmap_bucket_number           | BitmapBucketNumber         |          |                |
| bitmap_count                   | BitmapCount                |          |                |
| current_catalog                | CurrentCatalog             |          |                |
| current_database               | CurrentDatabase            |          |                |
| current_schema                 | CurrentDatabase            |          |                |
| current_user                   | CurrentUser                |          |                |
| from_avro                      | FromAvro                   |          |                |
| from_protobuf                  | FromProtobuf               |          |                |
| hll_sketch_estimate            | HllSketchEstimate          |          |                |
| hll_union                      | HllUnion                   |          |                |
| input_file_block_length        | InputFileBlockLength       |          |                |
| input_file_block_start         | InputFileBlockStart        |          |                |
| input_file_name                | InputFileName              |          |                |
| java_method                    | CallMethodViaReflection    |          |                |
| kll_sketch_get_n_bigint        | KllSketchGetNBigint        |          |                |
| kll_sketch_get_n_double        | KllSketchGetNDouble        |          |                |
| kll_sketch_get_n_float         | KllSketchGetNFloat         |          |                |
| kll_sketch_get_quantile_bigint | KllSketchGetQuantileBigint |          |                |
| kll_sketch_get_quantile_double | KllSketchGetQuantileDouble |          |                |
| kll_sketch_get_quantile_float  | KllSketchGetQuantileFloat  |          |                |
| kll_sketch_get_rank_bigint     | KllSketchGetRankBigint     |          |                |
| kll_sketch_get_rank_double     | KllSketchGetRankDouble     |          |                |
| kll_sketch_get_rank_float      | KllSketchGetRankFloat      |          |                |
| kll_sketch_merge_bigint        | KllSketchMergeBigint       |          |                |
| kll_sketch_merge_double        | KllSketchMergeDouble       |          |                |
| kll_sketch_merge_float         | KllSketchMergeFloat        |          |                |
| kll_sketch_to_string_bigint    | KllSketchToStringBigint    |          |                |
| kll_sketch_to_string_double    | KllSketchToStringDouble    |          |                |
| kll_sketch_to_string_float     | KllSketchToStringFloat     |          |                |
| monotonically_increasing_id    | MonotonicallyIncreasingID  |          |                |
| reflect                        | CallMethodViaReflection    |          |                |
| schema_of_avro                 | SchemaOfAvro               |          |                |
| session_user                   | CurrentUser                |          |                |
| spark_partition_id             | SparkPartitionID           | S        |                |
| theta_difference               | ThetaDifference            |          |                |
| theta_intersection             | ThetaIntersection          |          |                |
| theta_sketch_estimate          | ThetaSketchEstimate        |          |                |
| theta_union                    | ThetaUnion                 |          |                |
| to_avro                        | ToAvro                     |          |                |
| to_protobuf                    | ToProtobuf                 |          |                |
| try_aes_decrypt                | TryAesDecrypt              |          |                |
| try_reflect                    | TryReflect                 |          |                |
| typeof                         | TypeOf                     |          |                |
| user                           | CurrentUser                |          |                |
| uuid                           | Uuid                       | S        |                |
| version                        | SparkVersion               | S        |                |
| &#124;&#124;                   |                            | S        |                |

## Predicate Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions           |
|-------------------|---------------------|----------|------------------------|
| !                 | Not                 | S        |                        |
| !=                |                     | S        |                        |
| <                 | LessThan            | S        |                        |
| <=                | LessThanOrEqual     | S        |                        |
| <=>               | EqualNullSafe       | S        |                        |
| <>                |                     | S        |                        |
| =                 | EqualTo             | S        |                        |
| ==                | EqualTo             | S        |                        |
| >                 | GreaterThan         | S        |                        |
| >=                | GreaterThanOrEqual  | S        |                        |
| and               | And                 | S        |                        |
| between           | Between             | S        |                        |
| case              |                     | S        |                        |
| equal_null        | EqualNull           | S        |                        |
| ilike             | ILike               | S        |                        |
| in                | In                  | PS       |                        |
| isnan             | IsNaN               | S        |                        |
| isnotnull         | IsNotNull           | S        |                        |
| isnull            | IsNull              | S        |                        |
| like              | Like                | S        |                        |
| not               | Not                 | S        |                        |
| or                | Or                  | S        |                        |
| regexp            | RLike               | PS       | Lookaround unsupported |
| regexp_like       | RLike               | PS       | Lookaround unsupported |
| rlike             | RLike               | PS       | Lookaround unsupported |

## Geospatial Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| st_asbinary       | ST_AsBinary         |          |                |
| st_geogfromwkb    | ST_GeogFromWKB      |          |                |
| st_geomfromwkb    | ST_GeomFromWKB      |          |                |
| st_setsrid        | ST_SetSrid          |          |                |
| st_srid           | ST_Srid             |          |                |

## String Functions

| Spark Functions    | Spark Expressions           | Status   | Restrictions                                                                                                                                                                                                                                                                          |
|--------------------|-----------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ascii              | Ascii                       | S        |                                                                                                                                                                                                                                                                                       |
| base64             | Base64                      | PS       | base64 with chunkBase64String disabled is not supported                                                                                                                                                                                                                               |
| bit_length         | BitLength                   | S        |                                                                                                                                                                                                                                                                                       |
| btrim              | StringTrimBoth              | S        |                                                                                                                                                                                                                                                                                       |
| char               | Chr                         | S        |                                                                                                                                                                                                                                                                                       |
| char_length        | Length                      | S        |                                                                                                                                                                                                                                                                                       |
| character_length   | Length                      | S        |                                                                                                                                                                                                                                                                                       |
| chr                | Chr                         | S        |                                                                                                                                                                                                                                                                                       |
| collate            | CollateExpressionBuilder    |          |                                                                                                                                                                                                                                                                                       |
| collation          | Collation                   |          |                                                                                                                                                                                                                                                                                       |
| concat_ws          | ConcatWs                    | S        |                                                                                                                                                                                                                                                                                       |
| contains           | ContainsExpressionBuilder   | PS       | BinaryType unsupported                                                                                                                                                                                                                                                                |
| decode             | Decode                      |          |                                                                                                                                                                                                                                                                                       |
| elt                | Elt                         |          |                                                                                                                                                                                                                                                                                       |
| encode             | Encode                      |          |                                                                                                                                                                                                                                                                                       |
| endswith           | EndsWithExpressionBuilder   | PS       | BinaryType unsupported                                                                                                                                                                                                                                                                |
| find_in_set        | FindInSet                   | S        |                                                                                                                                                                                                                                                                                       |
| format_number      | FormatNumber                | PS       | format_number only supports tinyint, smallint, integer, bigint, float and double input; DecimalType input is not supported in Velox<br>format_number with a string format argument (e.g. '#,###.##') is not supported in Velox; only an integer number of decimal places is supported |
| format_string      | FormatString                |          |                                                                                                                                                                                                                                                                                       |
| initcap            | InitCap                     | S        |                                                                                                                                                                                                                                                                                       |
| instr              | StringInstr                 | S        |                                                                                                                                                                                                                                                                                       |
| is_valid_utf8      | IsValidUTF8                 |          |                                                                                                                                                                                                                                                                                       |
| lcase              | Lower                       | S        |                                                                                                                                                                                                                                                                                       |
| left               | Left                        | S        |                                                                                                                                                                                                                                                                                       |
| len                | Length                      | S        |                                                                                                                                                                                                                                                                                       |
| length             | Length                      | S        |                                                                                                                                                                                                                                                                                       |
| levenshtein        | Levenshtein                 | S        |                                                                                                                                                                                                                                                                                       |
| locate             | StringLocate                | S        |                                                                                                                                                                                                                                                                                       |
| lower              | Lower                       | S        |                                                                                                                                                                                                                                                                                       |
| lpad               | LPadExpressionBuilder       | PS       | BinaryType unsupported                                                                                                                                                                                                                                                                |
| ltrim              | StringTrimLeft              | S        |                                                                                                                                                                                                                                                                                       |
| luhn_check         | Luhncheck                   | S        |                                                                                                                                                                                                                                                                                       |
| make_valid_utf8    | MakeValidUTF8               |          |                                                                                                                                                                                                                                                                                       |
| mask               | MaskExpressionBuilder       | S        |                                                                                                                                                                                                                                                                                       |
| octet_length       | OctetLength                 |          |                                                                                                                                                                                                                                                                                       |
| overlay            | Overlay                     | S        |                                                                                                                                                                                                                                                                                       |
| position           | StringLocate                | S        |                                                                                                                                                                                                                                                                                       |
| printf             | FormatString                |          |                                                                                                                                                                                                                                                                                       |
| quote              | Quote                       |          |                                                                                                                                                                                                                                                                                       |
| randstr            | RandStr                     | S        |                                                                                                                                                                                                                                                                                       |
| regexp_count       | RegExpCount                 |          |                                                                                                                                                                                                                                                                                       |
| regexp_extract     | RegExpExtract               | PS       | Lookaround unsupported                                                                                                                                                                                                                                                                |
| regexp_extract_all | RegExpExtractAll            | PS       | Lookaround unsupported                                                                                                                                                                                                                                                                |
| regexp_instr       | RegExpInStr                 | PS       | Group index ignored<br>Lookaround unsupported                                                                                                                                                                                                                                         |
| regexp_replace     | RegExpReplace               | PS       | Lookaround unsupported                                                                                                                                                                                                                                                                |
| regexp_substr      | RegExpSubStr                |          |                                                                                                                                                                                                                                                                                       |
| repeat             | StringRepeat                | S        |                                                                                                                                                                                                                                                                                       |
| replace            | StringReplace               | S        |                                                                                                                                                                                                                                                                                       |
| right              | Right                       | S        |                                                                                                                                                                                                                                                                                       |
| rpad               | RPadExpressionBuilder       | PS       | BinaryType unsupported                                                                                                                                                                                                                                                                |
| rtrim              | StringTrimRight             | S        |                                                                                                                                                                                                                                                                                       |
| sentences          | Sentences                   |          |                                                                                                                                                                                                                                                                                       |
| soundex            | SoundEx                     | S        |                                                                                                                                                                                                                                                                                       |
| space              | StringSpace                 |          |                                                                                                                                                                                                                                                                                       |
| split              | StringSplit                 | S        |                                                                                                                                                                                                                                                                                       |
| split_part         | SplitPart                   | S        |                                                                                                                                                                                                                                                                                       |
| startswith         | StartsWithExpressionBuilder | PS       | BinaryType unsupported                                                                                                                                                                                                                                                                |
| substr             | Substring                   | PS       |                                                                                                                                                                                                                                                                                       |
| substring          | Substring                   | PS       |                                                                                                                                                                                                                                                                                       |
| substring_index    | SubstringIndex              | S        |                                                                                                                                                                                                                                                                                       |
| to_binary          | ToBinary                    |          |                                                                                                                                                                                                                                                                                       |
| to_char            | ToCharacterBuilder          |          |                                                                                                                                                                                                                                                                                       |
| to_number          | ToNumber                    |          |                                                                                                                                                                                                                                                                                       |
| to_varchar         | ToCharacterBuilder          |          |                                                                                                                                                                                                                                                                                       |
| translate          | StringTranslate             | S        |                                                                                                                                                                                                                                                                                       |
| trim               | StringTrim                  | S        |                                                                                                                                                                                                                                                                                       |
| try_to_binary      | TryToBinary                 |          |                                                                                                                                                                                                                                                                                       |
| try_to_number      | TryToNumber                 |          |                                                                                                                                                                                                                                                                                       |
| try_validate_utf8  | TryValidateUTF8             |          |                                                                                                                                                                                                                                                                                       |
| ucase              | Upper                       | S        |                                                                                                                                                                                                                                                                                       |
| unbase64           | UnBase64                    | PS       | unbase64 with failOnError is not supported                                                                                                                                                                                                                                            |
| upper              | Upper                       | S        |                                                                                                                                                                                                                                                                                       |
| validate_utf8      | ValidateUTF8                |          |                                                                                                                                                                                                                                                                                       |

## Struct Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| named_struct      | CreateNamedStruct   | S        |                |
| struct            | CreateStruct        | S        |                |

## URL Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| parse_url         | ParseUrl            |          |                |
| try_parse_url     | TryParseUrl         |          |                |
| try_url_decode    | TryUrlDecode        |          |                |
| url_decode        | UrlDecode           | S        |                |
| url_encode        | UrlEncode           | S        |                |

## Variant Functions

| Spark Functions       | Spark Expressions              | Status   | Restrictions   |
|-----------------------|--------------------------------|----------|----------------|
| is_variant_null       | IsVariantNull                  |          |                |
| parse_json            | ParseJsonExpressionBuilder     |          |                |
| schema_of_variant     | SchemaOfVariant                |          |                |
| schema_of_variant_agg | SchemaOfVariantAgg             |          |                |
| to_variant_object     | ToVariantObject                |          |                |
| try_parse_json        | TryParseJsonExpressionBuilder  |          |                |
| try_variant_get       | TryVariantGetExpressionBuilder |          |                |
| variant_explode       |                                |          |                |
| variant_explode_outer |                                |          |                |
| variant_get           | VariantGetExpressionBuilder    |          |                |

## XML Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| from_xml          | XmlToStructs        |          |                |
| schema_of_xml     | SchemaOfXml         |          |                |
| to_xml            | StructsToXml        |          |                |
| xpath             | XPathList           |          |                |
| xpath_boolean     | XPathBoolean        |          |                |
| xpath_double      | XPathDouble         |          |                |
| xpath_float       | XPathFloat          |          |                |
| xpath_int         | XPathInt            |          |                |
| xpath_long        | XPathLong           |          |                |
| xpath_number      | XPathDouble         |          |                |
| xpath_short       | XPathShort          |          |                |
| xpath_string      | XPathString         |          |                |

