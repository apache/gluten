# Scalar Functions Support Status

**Out of 357 scalar functions in Spark 3.5, Gluten currently fully supports 242 functions and partially supports 25 functions.**

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
| ^                  | BitwiseXor          | S        |                |
| bit_count          | BitwiseCount        | S        |                |
| bit_get            | BitwiseGet          | S        |                |
| getbit             | BitwiseGet          | S        |                |
| shiftright         | ShiftRight          | S        |                |
| shiftrightunsigned | ShiftRightUnsigned  |          |                |
| &#124;             | BitwiseOr           | S        |                |
| ~                  | BitwiseNot          | S        |                |

## Collection Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| array_size        | ArraySize           | S        |                |
| cardinality       | Size                | S        |                |
| concat            | Concat              | S        |                |
| reverse           | Reverse             | S        |                |
| size              | Size                | S        |                |

## Conditional Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| coalesce          | Coalesce            | S        |                |
| if                | If                  | S        |                |
| ifnull            | Nvl                 | S        |                |
| nanvl             | NaNvl               | S        |                |
| nullif            | NullIf              | S        |                |
| nvl               | Nvl                 | S        |                |
| nvl2              | Nvl2                | S        |                |
| when              | CaseWhen            | S        |                |

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
| timestamp         |                     | S        |                |
| tinyint           |                     | S        |                |

## Csv Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| from_csv          | CsvToStructs        |          |                |
| schema_of_csv     | SchemaOfCsv         |          |                |
| to_csv            | StructsToCsv        |          |                |

## Date and Timestamp Functions

| Spark Functions     | Spark Expressions                    | Status   | Restrictions   |
|---------------------|--------------------------------------|----------|----------------|
| add_months          | AddMonths                            | S        |                |
| convert_timezone    | ConvertTimezone                      |          |                |
| curdate             | CurDateExpressionBuilder             |          |                |
| current_date        | CurrentDate                          |          |                |
| current_timestamp   | CurrentTimestamp                     |          |                |
| current_timezone    | CurrentTimeZone                      |          |                |
| date_add            | DateAdd                              | S        |                |
| date_diff           | DateDiff                             | S        |                |
| date_format         | DateFormatClass                      | S        |                |
| date_from_unix_date | DateFromUnixDate                     | S        |                |
| date_part           | DatePartExpressionBuilder            |          |                |
| date_sub            | DateSub                              | S        |                |
| date_trunc          | TruncTimestamp                       | S        |                |
| dateadd             | DateAdd                              | S        |                |
| datediff            | DateDiff                             | S        |                |
| datepart            | DatePartExpressionBuilder            |          |                |
| day                 | DayOfMonth                           | S        |                |
| dayofmonth          | DayOfMonth                           | S        |                |
| dayofweek           | DayOfWeek                            | S        |                |
| dayofyear           | DayOfYear                            | S        |                |
| extract             | Extract                              | S        |                |
| from_unixtime       | FromUnixTime                         | S        |                |
| from_utc_timestamp  | FromUTCTimestamp                     | S        |                |
| hour                | Hour                                 | S        |                |
| last_day            | LastDay                              | S        |                |
| localtimestamp      | LocalTimestamp                       |          |                |
| make_date           | MakeDate                             | S        |                |
| make_dt_interval    | MakeDTInterval                       |          |                |
| make_interval       | MakeInterval                         |          |                |
| make_timestamp      | MakeTimestamp                        | S        |                |
| make_timestamp_ltz  | MakeTimestampLTZExpressionBuilder    |          |                |
| make_timestamp_ntz  | MakeTimestampNTZExpressionBuilder    |          |                |
| make_ym_interval    | MakeYMInterval                       | S        |                |
| minute              | Minute                               | S        |                |
| month               | Month                                | S        |                |
| months_between      | MonthsBetween                        | S        |                |
| next_day            | NextDay                              | S        |                |
| now                 | Now                                  |          |                |
| quarter             | Quarter                              | S        |                |
| second              | Second                               | S        |                |
| session_window      | SessionWindow                        |          |                |
| timestamp_micros    | MicrosToTimestamp                    | S        |                |
| timestamp_millis    | MillisToTimestamp                    | S        |                |
| timestamp_seconds   | SecondsToTimestamp                   | PS       |                |
| to_date             | ParseToDate                          |          |                |
| to_timestamp        | ParseToTimestamp                     |          |                |
| to_timestamp_ltz    | ParseToTimestampLTZExpressionBuilder |          |                |
| to_timestamp_ntz    | ParseToTimestampNTZExpressionBuilder |          |                |
| to_unix_timestamp   | ToUnixTimestamp                      | PS       |                |
| to_utc_timestamp    | ToUTCTimestamp                       | S        |                |
| trunc               | TruncDate                            | S        |                |
| try_to_timestamp    | TryToTimestampExpressionBuilder      |          |                |
| unix_date           | UnixDate                             | S        |                |
| unix_micros         | UnixMicros                           | S        |                |
| unix_millis         | UnixMillis                           | S        |                |
| unix_seconds        | UnixSeconds                          | S        |                |
| unix_timestamp      | UnixTimestamp                        | S        |                |
| weekday             | WeekDay                              | S        |                |
| weekofyear          | WeekOfYear                           | S        |                |
| window              | TimeWindow                           |          |                |
| window_time         | WindowTime                           |          |                |
| year                | Year                                 | S        |                |

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
| element_at        | ElementAt           | S        |                                                                             |
| map               | CreateMap           | PS       |                                                                             |
| map_concat        | MapConcat           | PS       |                                                                             |
| map_contains_key  | MapContainsKey      | S        |                                                                             |
| map_entries       | MapEntries          | S        |                                                                             |
| map_from_arrays   | MapFromArrays       |          |                                                                             |
| map_from_entries  | MapFromEntries      | S        |                                                                             |
| map_keys          | MapKeys             | S        |                                                                             |
| map_values        | MapValues           | S        |                                                                             |
| str_to_map        | StringToMap         | PS       | Only spark.sql.mapKeyDedupPolicy = EXCEPTION is supported for Velox backend |
| try_element_at    | TryElementAt        |          |                                                                             |

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
| ln                | Log                    |          |                |
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
| radians           | ToRadians              |          |                |
| rand              | Rand                   | S        |                |
| randn             | Randn                  |          |                |
| random            | Rand                   | S        |                |
| rint              | Rint                   | S        |                |
| round             | Round                  | S        |                |
| sec               | Sec                    | S        |                |
| shiftleft         | ShiftLeft              | S        |                |
| sign              | Signum                 | S        |                |
| signum            | Signum                 | S        |                |
| sin               | Sin                    |          |                |
| sinh              | Sinh                   | S        |                |
| sqrt              | Sqrt                   | S        |                |
| tan               | Tan                    |          |                |
| tanh              | Tanh                   |          |                |
| try_add           | TryAdd                 | PS       |                |
| try_divide        | TryDivide              |          |                |
| try_multiply      | TryMultiply            |          |                |
| try_subtract      | TrySubtract            |          |                |
| unhex             | Unhex                  | S        |                |
| width_bucket      | WidthBucket            | S        |                |

## Misc Functions

| Spark Functions             | Spark Expressions         | Status   | Restrictions   |
|-----------------------------|---------------------------|----------|----------------|
| aes_decrypt                 | AesDecrypt                |          |                |
| aes_encrypt                 | AesEncrypt                |          |                |
| assert_true                 | AssertTrue                | S        |                |
| bitmap_bit_position         | BitmapBitPosition         |          |                |
| bitmap_bucket_number        | BitmapBucketNumber        |          |                |
| bitmap_count                | BitmapCount               |          |                |
| current_catalog             | CurrentCatalog            |          |                |
| current_database            | CurrentDatabase           |          |                |
| current_schema              | CurrentDatabase           |          |                |
| current_user                | CurrentUser               |          |                |
| equal_null                  | EqualNull                 | S        |                |
| hll_sketch_estimate         | HllSketchEstimate         |          |                |
| hll_union                   | HllUnion                  |          |                |
| input_file_block_length     | InputFileBlockLength      |          |                |
| input_file_block_start      | InputFileBlockStart       |          |                |
| input_file_name             | InputFileName             |          |                |
| java_method                 | CallMethodViaReflection   |          |                |
| monotonically_increasing_id | MonotonicallyIncreasingID |          |                |
| reflect                     | CallMethodViaReflection   |          |                |
| spark_partition_id          | SparkPartitionID          | S        |                |
| try_aes_decrypt             | TryAesDecrypt             |          |                |
| typeof                      | TypeOf                    |          |                |
| user                        | CurrentUser               |          |                |
| uuid                        | Uuid                      | S        |                |
| version                     | SparkVersion              | S        |                |
| &#124;&#124;                |                           | S        |                |

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
| between           |                     | S        |                        |
| case              |                     | S        |                        |
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

## String Functions

| Spark Functions    | Spark Expressions           | Status   | Restrictions                                            |
|--------------------|-----------------------------|----------|---------------------------------------------------------|
| ascii              | Ascii                       | S        |                                                         |
| base64             | Base64                      | PS       | base64 with chunkBase64String disabled is not supported |
| bit_length         | BitLength                   | S        |                                                         |
| btrim              | StringTrimBoth              | S        |                                                         |
| char               | Chr                         | S        |                                                         |
| char_length        | Length                      | S        |                                                         |
| character_length   | Length                      | S        |                                                         |
| chr                | Chr                         | S        |                                                         |
| concat_ws          | ConcatWs                    | S        |                                                         |
| contains           | ContainsExpressionBuilder   | PS       | BinaryType unsupported                                  |
| decode             | Decode                      |          |                                                         |
| elt                | Elt                         |          |                                                         |
| encode             | Encode                      |          |                                                         |
| endswith           | EndsWithExpressionBuilder   | PS       | BinaryType unsupported                                  |
| find_in_set        | FindInSet                   | S        |                                                         |
| format_number      | FormatNumber                |          |                                                         |
| format_string      | FormatString                |          |                                                         |
| initcap            | InitCap                     | S        |                                                         |
| instr              | StringInstr                 | S        |                                                         |
| lcase              | Lower                       | S        |                                                         |
| left               | Left                        | S        |                                                         |
| len                | Length                      | S        |                                                         |
| length             | Length                      | S        |                                                         |
| levenshtein        | Levenshtein                 | S        |                                                         |
| locate             | StringLocate                | S        |                                                         |
| lower              | Lower                       | S        |                                                         |
| lpad               | LPadExpressionBuilder       | PS       | BinaryType unsupported                                  |
| ltrim              | StringTrimLeft              | S        |                                                         |
| luhn_check         | Luhncheck                   | S        |                                                         |
| mask               | MaskExpressionBuilder       | S        |                                                         |
| octet_length       | OctetLength                 |          |                                                         |
| overlay            | Overlay                     | S        |                                                         |
| position           | StringLocate                | S        |                                                         |
| printf             | FormatString                |          |                                                         |
| regexp_count       | RegExpCount                 |          |                                                         |
| regexp_extract     | RegExpExtract               | PS       | Lookaround unsupported                                  |
| regexp_extract_all | RegExpExtractAll            | PS       | Lookaround unsupported                                  |
| regexp_instr       | RegExpInStr                 |          |                                                         |
| regexp_replace     | RegExpReplace               | PS       | Lookaround unsupported                                  |
| regexp_substr      | RegExpSubStr                |          |                                                         |
| repeat             | StringRepeat                | S        |                                                         |
| replace            | StringReplace               | S        |                                                         |
| right              | Right                       | S        |                                                         |
| rpad               | RPadExpressionBuilder       | PS       | BinaryType unsupported                                  |
| rtrim              | StringTrimRight             | S        |                                                         |
| sentences          | Sentences                   |          |                                                         |
| soundex            | SoundEx                     | S        |                                                         |
| space              | StringSpace                 |          |                                                         |
| split              | StringSplit                 | S        |                                                         |
| split_part         | SplitPart                   | S        |                                                         |
| startswith         | StartsWithExpressionBuilder | PS       | BinaryType unsupported                                  |
| substr             | Substring                   | PS       |                                                         |
| substring          | Substring                   | PS       |                                                         |
| substring_index    | SubstringIndex              | S        |                                                         |
| to_binary          | ToBinary                    |          |                                                         |
| to_char            | ToCharacter                 |          |                                                         |
| to_number          | ToNumber                    |          |                                                         |
| to_varchar         | ToCharacter                 |          |                                                         |
| translate          | StringTranslate             | S        |                                                         |
| trim               | StringTrim                  | S        |                                                         |
| try_to_binary      | TryToBinary                 |          |                                                         |
| try_to_number      | TryToNumber                 |          |                                                         |
| ucase              | Upper                       | S        |                                                         |
| unbase64           | UnBase64                    | PS       | unbase64 with failOnError is not supported              |
| upper              | Upper                       | S        |                                                         |

## Struct Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| named_struct      | CreateNamedStruct   | S        |                |
| struct            |                     | S        |                |

## URL Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| parse_url         | ParseUrl            |          |                |
| url_decode        | UrlDecode           | S        |                |
| url_encode        | UrlEncode           | S        |                |

## XML Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| xpath             | XPathList           |          |                |
| xpath_boolean     | XPathBoolean        |          |                |
| xpath_double      | XPathDouble         |          |                |
| xpath_float       | XPathFloat          |          |                |
| xpath_int         | XPathInt            |          |                |
| xpath_long        | XPathLong           |          |                |
| xpath_number      | XPathDouble         |          |                |
| xpath_short       | XPathShort          |          |                |
| xpath_string      | XPathString         |          |                |

