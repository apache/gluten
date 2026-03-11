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

import org.apache.gluten.utils.SQLQueryTestSettings

object VeloxSQLQueryTestSettings extends SQLQueryTestSettings {
  override def getResourceFilePath: String =
    getClass.getResource("/").getPath + "../../../src/test/resources/backends-velox/sql-tests"

  override def getSupportedSQLQueryTests: Set[String] = SUPPORTED_SQL_QUERY_LIST

  override def getOverwriteSQLQueryTests: Set[String] = OVERWRITE_SQL_QUERY_LIST

  // Put relative path to "/path/to/spark/sql/core/src/test/resources/sql-tests/inputs" in this list
  // Gluten currently only supports `SET spark.sql.legacy.timeParserPolicy=LEGACY`
  // Queries in `date.sql` and `timestamp.sql` are tested in `datetime-legacy.sql`.
  // There is a limitation in Spark-4.0 on ANSI gap and exception message gap.
  // we disabled these sql tests temporarily.
  // limit.sql, postgreSQL/limit.sql, subquery/in-subquery/in-limit.sql,
  // subquery/in-subquery/in-null-semantics.sql
  // try_arithmetic.sql, try_element_at.sql, typeCoercion/native/stringCastAndExpressions.sql,
  // window.sql
  val SUPPORTED_SQL_QUERY_LIST: Set[String] = Set(
    "ansi/conditional-functions.sql",
    "ansi/decimalArithmeticOperations.sql",
    "cast.sql",
    "change-column.sql",
    "ceil-floor-with-scale-param.sql",
    "column-resolution-aggregate.sql",
    "column-resolution-sort.sql",
    "columnresolution-negative.sql",
    "columnresolution-views.sql",
    "columnresolution.sql",
    "comments.sql",
    "comparator.sql",
    "cross-join.sql",
    "csv-functions.sql",
    "cte-legacy.sql",
    "cte-nested.sql",
    "cte-nonlegacy.sql",
    "current_database_catalog.sql",
    // "datetime-formatting-invalid.sql",
    "datetime-special.sql",
    "decimalArithmeticOperations.sql",
    "describe.sql",
    "describe-part-after-analyze.sql",
    "describe-table-after-alter-table.sql",
    "describe-query.sql",
    "double-quoted-identifiers.sql",
    "except.sql",
    "except-all.sql",
    "extract.sql",
    "group-by.sql",
    "group-by-all.sql",
    "group-by-all-duckdb.sql",
    "group-by-all-mosha.sql",
    "group-analytics.sql",
    "group-by-filter.sql",
    "group-by-ordinal.sql",
    "grouping_set.sql",
    "having.sql",
    "higher-order-functions.sql",
    "identifier-clause.sql",
    "ignored.sql",
    "ilike.sql",
    "ilike-all.sql",
    "ilike-any.sql",
    "inline-table.sql",
    "inner-join.sql",
    "intersect-all.sql",
    "join-empty-relation.sql",
    "join-lateral.sql",
    "json-functions.sql",
    "keywords.sql",
    "like-all.sql",
    "like-any.sql",
    // "limit.sql",
    "literals.sql",
    "map.sql",
    "mask-functions.sql",
    "math.sql",
    "named-function-arguments.sql",
    "natural-join.sql",
    "non-excludable-rule.sql",
    "null-handling.sql",
    "null-propagation.sql",
    "operators.sql",
    "order-by-all.sql",
    // "order-by-nulls-ordering.sql",
    "order-by-ordinal.sql",
    "outer-join.sql",
    "parse-schema-string.sql",
    "pivot.sql",
    "pred-pushdown.sql",
    "predicate-functions.sql",
    "query_regex_column.sql",
    "random.sql",
    "show-create-table.sql",
    "show-tables.sql",
    "show-tblproperties.sql",
    "show-views.sql",
    "show_columns.sql",
    "sql-compatibility-functions.sql",
    "struct.sql",
    "subexp-elimination.sql",
    "table-aliases.sql",
    "table-valued-functions.sql",
    "tablesample-negative.sql",
    "try-string-functions.sql",
    "subquery/exists-subquery/exists-aggregate.sql",
    "subquery/exists-subquery/exists-basic.sql",
    "subquery/exists-subquery/exists-cte.sql",
    "subquery/exists-subquery/exists-having.sql",
    "subquery/exists-subquery/exists-joins-and-set-ops.sql",
    "subquery/exists-subquery/exists-orderby-limit.sql",
    "subquery/exists-subquery/exists-outside-filter.sql",
    "subquery/exists-subquery/exists-within-and-or.sql",
    "subquery/in-subquery/in-basic.sql",
    "subquery/in-subquery/in-group-by.sql",
    "subquery/in-subquery/in-having.sql",
    "subquery/in-subquery/in-joins.sql",
    // "subquery/in-subquery/in-limit.sql",
    "subquery/in-subquery/in-multiple-columns.sql",
    "subquery/in-subquery/in-nullability.sql",
    "subquery/in-subquery/in-order-by.sql",
    "subquery/in-subquery/in-set-operations.sql",
    "subquery/in-subquery/in-with-cte.sql",
    "subquery/in-subquery/nested-not-in.sql",
    "subquery/in-subquery/not-in-group-by.sql",
    "subquery/in-subquery/not-in-joins.sql",
    "subquery/in-subquery/not-in-unit-tests-multi-column.sql",
    "subquery/in-subquery/not-in-unit-tests-multi-column-literal.sql",
    "subquery/in-subquery/not-in-unit-tests-single-column.sql",
    "subquery/in-subquery/not-in-unit-tests-single-column-literal.sql",
    "subquery/in-subquery/simple-in.sql",
    "subquery/negative-cases/invalid-correlation.sql",
    "subquery/negative-cases/subq-input-typecheck.sql",
    "subquery/scalar-subquery/scalar-subquery-count-bug.sql",
    "subquery/scalar-subquery/scalar-subquery-predicate.sql",
    "subquery/scalar-subquery/scalar-subquery-select.sql",
    "subquery/scalar-subquery/scalar-subquery-set-op.sql",
    "subquery/subquery-in-from.sql",
    "postgreSQL/aggregates_part1.sql",
    "postgreSQL/aggregates_part2.sql",
    "postgreSQL/aggregates_part3.sql",
    "postgreSQL/aggregates_part4.sql",
    "postgreSQL/boolean.sql",
    "postgreSQL/case.sql",
    "postgreSQL/comments.sql",
    "postgreSQL/create_view.sql",
    "postgreSQL/date.sql",
    "postgreSQL/float4.sql",
    "postgreSQL/float8.sql",
    "postgreSQL/groupingsets.sql",
    "postgreSQL/insert.sql",
    "postgreSQL/int2.sql",
    "postgreSQL/int4.sql",
    "postgreSQL/int8.sql",
    "postgreSQL/interval.sql",
    "postgreSQL/join.sql",
    // "postgreSQL/limit.sql",
    "postgreSQL/numeric.sql",
    "postgreSQL/select.sql",
    "postgreSQL/select_distinct.sql",
    "postgreSQL/select_having.sql",
    "postgreSQL/select_implicit.sql",
    "postgreSQL/strings.sql",
    "postgreSQL/text.sql",
    "postgreSQL/timestamp.sql",
    "postgreSQL/union.sql",
    "postgreSQL/window_part2.sql",
    "postgreSQL/with.sql",
    "datetime-special.sql",
    "timestamp-ltz.sql",
    "timestamp-ntz.sql",
    "timezone.sql",
    "transform.sql",
    "try-string-functions.sql",
    "try_aggregates.sql",
    "typeCoercion/native/arrayJoin.sql",
    "typeCoercion/native/binaryComparison.sql",
    "typeCoercion/native/booleanEquality.sql",
    "typeCoercion/native/caseWhenCoercion.sql",
    "typeCoercion/native/concat.sql",
    "typeCoercion/native/dateTimeOperations.sql",
    "typeCoercion/native/decimalPrecision.sql",
    "typeCoercion/native/division.sql",
    "typeCoercion/native/elt.sql",
    "typeCoercion/native/ifCoercion.sql",
    // "typeCoercion/native/implicitTypeCasts.sql",
    "typeCoercion/native/inConversion.sql",
    "typeCoercion/native/mapconcat.sql",
    "typeCoercion/native/mapZipWith.sql",
    "typeCoercion/native/promoteStrings.sql",
    "typeCoercion/native/widenSetOperationTypes.sql",
    "typeCoercion/native/windowFrameCoercion.sql",
    "udaf/udaf.sql - Grouped Aggregate Pandas UDF",
    "udf/udf-union.sql - Scala UDF",
    "udf/udf-intersect-all.sql - Scala UDF",
    "udf/udf-except-all.sql - Scala UDF",
    "udf/udf-udaf.sql - Scala UDF",
    "udf/udf-except.sql - Scala UDF",
    "udf/udf-pivot.sql - Scala UDF",
    "udf/udf-inline-table.sql - Scala UDF",
    "udf/postgreSQL/udf-select_having.sql - Scala UDF",
    "union.sql",
    "unpivot.sql",
    "using-join.sql",
    // newly added in Spark-4.0
    "variant/named-function-arguments.sql",
    "timestampNTZ/datetime-special-ansi.sql",
    "timestampNTZ/datetime-special.sql",
    // "timestampNTZ/timestamp-ansi.sql",
    // "timestampNTZ/timestamp.sql",
    // "nonansi/array.sql",
    "nonansi/cast.sql",
    "nonansi/conditional-functions.sql",
    // "nonansi/date.sql",
    // "nonansi/datetime-parsing-invalid.sql",
    "nonansi/datetime-special.sql",
    "nonansi/decimalArithmeticOperations.sql",
    "nonansi/double-quoted-identifiers.sql",
    "nonansi/higher-order-functions.sql",
    // "nonansi/interval.sql",
    "nonansi/keywords.sql",
    "nonansi/literals.sql",
    "nonansi/map.sql",
    "nonansi/math.sql",
    "nonansi/parse-schema-string.sql",
    // "nonansi/string-functions.sql",
    // "nonansi/timestamp.sql",
    "nonansi/try_aggregates.sql"
    // "nonansi/try_arithmetic.sql",
    // "nonansi/try_datetime_functions.sql",
    // "nonansi/try_element_at.sql"
  )

  val OVERWRITE_SQL_QUERY_LIST: Set[String] = Set(
    // The calculation formulas for corr, skewness, kurtosis, variance, and stddev in Velox differ
    // slightly from those in Spark, resulting in some differences in the final results.
    // Overwrite below test cases.
    // -- SPARK-24369 multiple distinct aggregations having the same argument set
    // -- Aggregate with nulls.
    "group-by.sql",
    "udf/udf-group-by.sql",
    // Overwrite some results of regr_intercept, regr_r2, corr.
    "linear-regression.sql",
    // Overwrite exception message.
    // TODO: Disable due to schema & ANSI gap
    // "array.sql",
    // Overwrite exception message.
    "bitwise.sql",
    // Enable NullPropagation rule for
    // "legacy behavior: allow calling function count without parameters".
    "count.sql",
    // Enable ConstantFolding rule for "typeof(...)".
    "charvarchar.sql",
    // Enable ConstantFolding rule for "typeof(...)".
    "cte.sql",
    // Removed some result mismatch cases.
    "datetime-legacy.sql",
    // Removed some result mismatch cases.
    "datetime-parsing.sql",
    // Removed some result mismatch cases.
    "datetime-parsing-legacy.sql",
    // Removed some result mismatch cases.
    "datetime-parsing-invalid.sql",
    // Overwrite exception message. See Spark-46550.
    "hll.sql",
    // Overwrite exception message.
    // TODO: Disable due to schema & ANSI gap
    // "interval.sql",
    // Enable ConstantFolding rule for "typeof(...)".
    "misc-functions.sql",
    // Removed some result mismatch cases.
    "regexp-functions.sql",
    // Removed some result mismatch cases.
    "string-functions.sql",
    // Removed some result mismatch cases.
    // TODO: Disable due to schema & ANSI gap
    // "try_arithmetic.sql",
    // Removed some result mismatch cases.
    "try_cast.sql",
    // Removed SQLs that can only pass with `set spark.sql.legacy.timeParserPolicy=LEGACY;`
    // TODO: Disable due to schema & ANSI gap
    // "typeCoercion/native/stringCastAndExpressions.sql",
    // Enable ConstantFolding rule for some queries.
    "percentiles.sql",
    // Enable ConstantFolding rule for some queries, otherwise Spark will throw an exception.
    "postgreSQL/window_part1.sql",
    // Enable ConstantFolding rule for some queries, otherwise Spark will throw an exception.
    "postgreSQL/window_part3.sql",
    // Enable ConstantFolding rule for some queries, otherwise Spark will throw an exception.
    "postgreSQL/window_part4.sql",
    // Enable NullPropagation rule for some queries that rely on the rule.
    // TODO: Disable due to schema & ANSI gap
    // "subquery/in-subquery/in-null-semantics.sql",
    // Removed some result mismatch cases.
    "try_datetime_functions.sql",
    // Overwrite exception message.
    // TODO: Disable due to schema & ANSI gap
    // "try_element_at.sql",
    // Overwrite exception message.
    "url-functions.sql"
    // Removed failed query. Adjust the output order for some queries.
    // TODO: Disable due to schema & ANSI gap
    // "window.sql"
  )
}
