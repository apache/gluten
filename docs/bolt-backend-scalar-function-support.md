---
layout: page
title: Bolt Backend Scalar Function Support
nav_order: 20
---

# Bolt Scalar Function Support Status

This document outlines the scalar functions that are **default-registered and available at runtime** in the current version of Bolt. 

## Overall Summary
**Status Column Legend:**
- `S`: Supported. The current implementation is fully available without significant functional gaps or special restrictions.
- `PS`: Partially Supported. The function is available but has important limitations or prerequisites for use (detailed in the "Restrictions/Notes" column).
- *blank*: Not used in this list.

### Count by Category

| Category | Function Count |
|---|---|
| Array | 24 |
| Bitwise | 4 |
| Collection | 2 |
| Conditional | 0 |
| Conversion | 6 |
| Date & Timestamp | 50 |
| Hash | 26 |
| JSON | 12 |
| Lambda | 3 |
| Map | 16 |
| Mathematical | 54 |
| Misc | 2 |
| Predicate | 11 |
| String | 79 |
| Struct | 1 |
| URL | 10 |

## Array Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| all_match | S | |
| any_match | S | |
| array_contains | S | |
| array_distinct | S | |
| array_duplicates | S | |
| array_except | S | |
| array_intersect | S | |
| array_position | S | |
| array_sort | S | |
| array_sort_desc | S | |
| array_sum | S | |
| arrays_overlap | S | |
| contains | S | |
| element | S | |
| find_first | S | |
| find_first_index | S | |
| none_match | S | |
| repeat | S | |
| sequence | S | |
| shuffle | S | |
| slice | S | |
| sort_array | S | |
| zip | S | |
| zip_with | S | |

## Bitwise Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| bit_count | S | |
| bitwise_arithmetic_shift_right | S | |
| bitwise_logical_shift_right | S | |
| bitwise_shift_left | S | |

## Collection Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| cardinality | S | |
| empty_approx_set | S | |

## Conditional Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| *None* |  | Conditional scalar functions are not enumerated individually in this audit. |

## Conversion Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| from_base | S | |
| from_utf8 | S | |
| to_base | S | |
| to_bigint | S | |
| to_integer | S | |
| to_utf8 | S | |

## Date & Timestamp Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| current_date | S | |
| date | S | |
| date2pdate | S | |
| date_add | S | |
| date_diff | S | |
| date_format | S | |
| date_parse | S | |
| date_trunc | S | |
| datediff | S | |
| day | S | |
| day_of_month | S | |
| day_of_week | S | |
| day_of_year | S | |
| dayofmonth | S | |
| dayofweek | S | |
| dayofyear | S | |
| dow | S | |
| doy | S | |
| format_datetime | S | |
| from_unixtime | S | |
| hour | S | |
| last_day | S | |
| last_day_of_month | S | |
| millisecond | S | |
| minus | S | |
| minute | S | |
| month | S | |
| months_between | S | |
| next_day | S | |
| parse_datetime | S | |
| pdate2date | S | |
| plus | S | |
| quarter | S | |
| second | S | |
| timestampadd | S | |
| timestampdiff | S | |
| timezone_hour | S | |
| timezone_minute | S | |
| to_date | S | |
| to_timestamp | S | |
| to_unix_timestamp | S | |
| to_unixtime | S | |
| trunc | S | |
| unix_timestamp | S | |
| week | S | |
| week_of_year | S | |
| weekofyear | S | |
| year | S | |
| year_of_week | S | |
| yow | S | |

## Hash Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| base64 | S | |
| crc32 | S | |
| from_base64 | S | |
| from_base64url | S | |
| from_big_endian_32 | S | |
| from_big_endian_64 | S | |
| from_hex | S | |
| from_ieee754_64 | S | |
| hmac_md5 | S | |
| hmac_sha1 | S | |
| hmac_sha256 | S | |
| hmac_sha512 | S | |
| md5 | S | |
| sha1 | S | |
| sha256 | S | |
| sha512 | S | |
| spooky_hash_v2_32 | S | |
| spooky_hash_v2_64 | S | |
| to_base64 | S | |
| to_base64url | S | |
| to_big_endian_32 | S | |
| to_big_endian_64 | S | |
| to_hex | S | |
| to_ieee754_32 | S | |
| to_ieee754_64 | S | |
| xxhash64 | S | |

## JSON Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| get_json_object | PS | JSON path functions support UTF-8 in JSON text but have limited support for UTF-8 in `json_path`. Behavior may depend on `QueryConfig` settings (e.g., whether to throw on invalid JSON). |
| get_json_object_detect_bad_json | PS | JSON path functions support UTF-8 in JSON text but have limited support for UTF-8 in `json_path`. Behavior may depend on `QueryConfig` settings (e.g., whether to throw on invalid JSON). |
| is_json_scalar | S | |
| json_array_contains | S | |
| json_array_length | S | |
| json_extract | PS | JSON path functions support UTF-8 in JSON text but have limited support for UTF-8 in `json_path`. Behavior may depend on `QueryConfig` settings (e.g., whether to throw on invalid JSON). |
| json_extract_scalar | PS | JSON path functions support UTF-8 in JSON text but have limited support for UTF-8 in `json_path`. Behavior may depend on `QueryConfig` settings (e.g., whether to throw on invalid JSON). |
| json_format | S | |
| json_parse | S | |
| json_size | PS | JSON path functions support UTF-8 in JSON text but have limited support for UTF-8 in `json_path`. Behavior may depend on `QueryConfig` settings (e.g., whether to throw on invalid JSON). |
| json_to_map | S | |
| to_json | S | |

## Lambda Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| filter | S | |
| reduce | S | |
| transform | S | |

## Map Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| all_keys_match | S | |
| any_keys_match | S | |
| any_values_match | S | |
| map | S | |
| map_entries | S | |
| map_filter | S | |
| map_filter_keys | S | |
| map_from_entries | S | |
| map_keys | S | |
| map_update | S | |
| map_values | S | |
| map_zip_with | S | |
| no_keys_match | S | |
| no_values_match | S | |
| str_to_map | S | |
| transform_keys | S | |
| transform_values | S | |

## Mathematical Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| abs | S | |
| acos | S | |
| asin | S | |
| atan | S | |
| atan2 | S | |
| beta_cdf | S | |
| binomial_cdf | S | |
| bytedance_ceil | S | |
| bytedance_floor | S | |
| bytedance_ln | S | |
| bytedance_rand | S | |
| cauchy_cdf | S | |
| cbrt | S | |
| chi_squared_cdf | S | |
| clamp | S | |
| cos | S | |
| cosh | S | |
| degrees | S | |
| divide | S | |
| e | S | |
| exp | S | |
| f_cdf | S | |
| find_in_set | S | |
| gamma_cdf | S | |
| infinity | S | |
| inverse_beta_cdf | S | |
| laplace_cdf | S | |
| ln | S | |
| log | S | |
| log10 | S | |
| log2 | S | |
| multiply | S | |
| nan | S | |
| negate | S | |
| normal_cdf | S | |
| not | S | |
| pi | S | |
| poisson_cdf | S | |
| pow | S | |
| power | S | |
| radians | S | |
| rand | S | |
| random | S | |
| round | S | |
| sin | S | |
| sqrt | S | |
| tan | S | |
| tanh | S | |
| translate | S | |
| truncate | S | |
| weibull_cdf | S | |
| width_bucket | S | |
| wilson_interval_lower | S | |
| wilson_interval_upper | S | |

## Misc Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| greatest | S | |
| least | S | |

## Predicate Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| between | S | |
| eq | S | |
| gt | S | |
| gte | S | |
| in | S | |
| is_finite | S | |
| is_infinite | S | |
| is_nan | S | |
| lt | S | |
| lte | S | |
| neq | S | |

## String Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| ascii | S | |
| base64 | S | |
| bit_length | S | |
| chr | S | |
| codepoint | S | |
| concat | S | |
| concat_ws_spark | S | |
| contains | S | |
| conv | S | |
| empty2null | S | |
| ends_with | S | |
| endswith | S | |
| find_in_set | S | |
| format_number | S | |
| instr | S | |
| length | S | |
| levenshtein | S | |
| levenshtein_distance | S | |
| like | PS | Uses RE2 regex engine, which is a subset of PCRE and does not support backtracking, backreferences, or lookarounds. Some implementations (e.g., `regexp_replace`, `regexp_split`) require the pattern to be a constant. |
| locate | S | |
| lower | S | |
| lpad | S | |
| ltrim | S | |
| mask | S | |
| overlay | S | |
| repeat | S | |
| regexp_extract | PS | Uses RE2 regex engine, which is a subset of PCRE and does not support backtracking, backreferences, or lookarounds. Some implementations (e.g., `regexp_replace`, `regexp_split`) require the pattern to be a constant. |
| regexp_extract_all | PS | Uses RE2 regex engine, which is a subset of PCRE and does not support backtracking, backreferences, or lookarounds. Some implementations (e.g., `regexp_replace`, `regexp_split`) require the pattern to be a constant. |
| regexp_like | PS | Uses RE2 regex engine, which is a subset of PCRE and does not support backtracking, backreferences, or lookarounds. Some implementations (e.g., `regexp_replace`, `regexp_split`) require the pattern to be a constant. |
| regexp_replace | PS | Uses RE2 regex engine, which is a subset of PCRE and does not support backtracking, backreferences, or lookarounds. Some implementations (e.g., `regexp_replace`, `regexp_split`) require the pattern to be a constant. |
| replace | S | |
| reverse | S | |
| right | S | |
| rlike | PS | Uses RE2 regex engine, which is a subset of PCRE and does not support backtracking, backreferences, or lookarounds. Some implementations (e.g., `regexp_replace`, `regexp_split`) require the pattern to be a constant. |
| rpad | S | |
| rtrim | S | |
| soundex | S | |
| space | S | |
| split | S | |
| split_part | S | |
| starts_with | S | |
| startswith | S | |
| strpos | S | |
| strrpos | S | |
| substr | S | |
| substring | S | |
| substring_index | S | |
| to_title | S | |
| translate | S | |
| trim | S | |
| unbase64 | S | |
| upper | S | |
| uuid | S | |

## Struct Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| row_constructor | S | |

## URL Functions

| Bolt Function(s) | Status | Restrictions/Notes |
|---|---|---|
| parse_url | S | |
| url_decode | S | |
| url_encode | S | |
| url_extract_fragment | S | |
| url_extract_host | S | |
| url_extract_parameter | S | |
| url_extract_path | S | |
| url_extract_port | S | |
| url_extract_protocol | S | |
| url_extract_query | S | |

## XML Functions

Bolt does not currently register any XML-related scalar functions.
