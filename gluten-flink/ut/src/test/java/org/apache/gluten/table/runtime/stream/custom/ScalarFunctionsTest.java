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
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.gluten.table.runtime.stream.common.GlutenStreamingTestBase;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

class ScalarFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testAdd() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L), Row.of(2, 2L), Row.of(3, 3L));
    createSimpleBoundedValuesTable("tblAdd", "a int, b bigint", rows);

    String query1 = "select a + b as x from tblAdd where a > 0";
    runAndCheck(query1, Arrays.asList("+I[2]", "+I[4]", "+I[6]"));

    String query2 = "select a + 1 as x from tblAdd where a > 0";
    runAndCheck(query2, Arrays.asList("+I[2]", "+I[3]", "+I[4]"));
  }

  @Test
  void testSubtract() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L), Row.of(2, 2L), Row.of(3, 3L));
    createSimpleBoundedValuesTable("tblSub", "a int, b bigint", rows);
    String query1 = "select a - b as x from tblSub where a > 0";
    runAndCheck(query1, Arrays.asList("+I[0]", "+I[0]", "+I[0]"));

    String query2 = "select a - 1 as x from tblSub where a > 0";
    runAndCheck(query2, Arrays.asList("+I[0]", "+I[1]", "+I[2]"));
  }

  @Test
  void testMod() {
    List<Row> rows = Arrays.asList(Row.of(1, 100), Row.of(2, 3), Row.of(3, 5));
    createSimpleBoundedValuesTable("tblMod", "a int, d int", rows);
    String query1 = "select d % a as x from tblMod where a > 0";
    runAndCheck(query1, Arrays.asList("+I[0]", "+I[1]", "+I[2]"));

    String query2 = "select d % 3 as x from tblMod where a > 0";
    runAndCheck(query2, Arrays.asList("+I[1]", "+I[0]", "+I[2]"));
  }

  @Test
  void testLargerThen() {
    List<Row> rows =
        Arrays.asList(Row.of(1, 1L, "2", "1"), Row.of(2, 2L, "2", "2"), Row.of(3, 3L, "2", "1"));
    createSimpleBoundedValuesTable("tblLarger", "a int, b bigint, c string, d string", rows);
    String query1 = "select a > 1 as x from tblLarger where a > 0";
    runAndCheck(query1, Arrays.asList("+I[false]", "+I[true]", "+I[true]"));

    String query2 = "select b > 1 as x from tblLarger where a > 0";
    runAndCheck(query2, Arrays.asList("+I[false]", "+I[true]", "+I[true]"));

    String query3 = "select a > c as x from tblLarger where a > 0";
    runAndCheck(query3, Arrays.asList("+I[false]", "+I[false]", "+I[true]"));

    String query4 = "select c > d as x from tblLarger where a > 0";
    runAndCheck(query4, Arrays.asList("+I[true]", "+I[false]", "+I[true]"));
  }

  @Test
  void testLessThen() {
    List<Row> rows =
        Arrays.asList(Row.of(1, 1L, "2", "1"), Row.of(2, 2L, "2", "2"), Row.of(3, 3L, "2", "1"));
    createSimpleBoundedValuesTable("tblLess", "a int, b bigint, c string, d string", rows);

    String query1 = "select a < 2 as x from tblLess where a > 0";
    runAndCheck(query1, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query2 = "select b < 2 as x from tblLess where a > 0";
    runAndCheck(query2, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query3 = "select a < c as x from tblLess where a > 0";
    runAndCheck(query3, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query4 = "select c < d as x from tblLess where a > 0";
    runAndCheck(query4, Arrays.asList("+I[false]", "+I[false]", "+I[false]"));

    String query5 = "select c > '123' from tblLess where a > 0";
    runAndCheck(query5, Arrays.asList("+I[true]", "+I[true]", "+I[true]"));
  }

  @Test
  void testEqual() {
    List<Row> rows =
        Arrays.asList(Row.of(1, 1L, "2", "1"), Row.of(2, 2L, "2", "2"), Row.of(3, 3L, "2", "1"));
    createSimpleBoundedValuesTable("tblEqual", "a int, b bigint, c string, d string", rows);

    String query1 = "select a = 1 as x from tblEqual where a > 0";
    runAndCheck(query1, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query2 = "select b = 1 as x from tblEqual where a > 0";
    runAndCheck(query2, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query3 = "select a, c, a = c as x from tblEqual where a > 0";
    runAndCheck(query3, Arrays.asList("+I[1, 2, false]", "+I[2, 2, true]", "+I[3, 2, false]"));

    String query4 = "select c = d as x from tblEqual where a > 0";
    runAndCheck(query4, Arrays.asList("+I[false]", "+I[true]", "+I[false]"));
  }

  @Test
  void testSplitIndex() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 1L, "http://testflink/a/b/c"),
            Row.of(2, 2L, "http://testflink/a1/b1/c1"),
            Row.of(3, 3L, "http://testflink/a2/b2/c2"));
    createSimpleBoundedValuesTable("tblSplitIndex", "a int, b bigint, c string", rows);
    String query1 = "select split_index(c, '/', 2) from tblSplitIndex";
    runAndCheck(query1, Arrays.asList("+I[testflink]", "+I[testflink]", "+I[testflink]"));
    String query2 = "select split_index(c, '//', 1) from tblSplitIndex";
    runAndCheck(
        query2,
        Arrays.asList("+I[testflink/a/b/c]", "+I[testflink/a1/b1/c1]", "+I[testflink/a2/b2/c2]"));
    // Add some corner case tests from `ScalarFunctionsTest`#testSplitIndex in flink.
    rows = Arrays.asList(Row.of(1, 1L, "AQIDBA=="));
    createSimpleBoundedValuesTable("tblSplitIndexFlink", "a int, b bigint, c string", rows);
    String queryForInvalidIndex =
        "select split_index(c, 'I', 7), split_index(c, 'I', -1) from tblSplitIndexFlink";
    String queryForNumbericDelimiter =
        "select split_index(c, 73, 0), split_index(c, 12, 0) from tblSplitIndexFlink";
    runAndCheck(queryForInvalidIndex, Arrays.asList("+I[null, null]"));
    runAndCheck(queryForNumbericDelimiter, Arrays.asList("+I[AQ, AQIDBA==]"));
    rows = Arrays.asList(Row.of(2, 2L, null));
    createSimpleBoundedValuesTable("tblSplitIndexNullInput", "a int, b bigint, c string", rows);
    String queryForNullInput = "select split_index(c, 'I', 0) from tblSplitIndexNullInput";
    runAndCheck(queryForNullInput, Arrays.asList("+I[null]"));
    // TODO: The cases when index or delimeter parameters is null can not be supported currently.
    // String queryForIndexNull = "select split_index(c, 'I', cast(null as INT)) from
    // tblSplitIndexFlink";
    // runAndCheck(queryForIndexNull, Arrays.asList("+I[null]"));
    // String queryForDelimiterNull = "select split_index(c, cast(null as VARCHAR), 0) from
    // tblSplitIndexFlink";
    // runAndCheck(queryForDelimiterNull, Arrays.asList("+I[null]"));
  }

  @Disabled
  @Test
  void testReinterpret() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 1L, "2025-06-24 10:00:01", "1991-01-01 00:00:01"),
            Row.of(2, 2L, "2025-06-24 10:00:02", "1991-01-01 00:00:01"),
            Row.of(3, 3L, "2025-06-24 10:00:03", "1991-01-01 00:00:01"));
    createSimpleBoundedValuesTable(
        "tblReinterpret",
        "a int, b bigint, c string, d string, "
            + "e as case when a = 1 then cast(c as Timestamp(3)) else cast(d as Timestamp(3)) end, "
            + "WATERMARK FOR e AS e - INTERVAL '1' SECOND",
        rows);
    String query1 = "select e from tblReinterpret where a = 1";
    runAndCheck(query1, Arrays.asList("+I[2025-06-24T10:00:01]"));
    String query2 = "select e from tblReinterpret where a = 2";
    runAndCheck(query2, Arrays.asList("+I[1991-01-01T00:00:01]"));
  }

  @Test
  void testDecimal() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, new BigDecimal("1.0"), new BigDecimal("1.0"), 2L, 1.0),
            Row.of(2, new BigDecimal("2.0"), new BigDecimal("2.0"), 3L, 3.0),
            Row.of(3, new BigDecimal("3.0"), new BigDecimal("3.0"), 4L, 4.0));
    createSimpleBoundedValuesTable(
        "tblDecimal", "a int, b decimal(11, 2), c decimal(10, 3), d bigint, e double", rows);
    String query = "select b + c as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.000]", "+I[4.000]", "+I[6.000]"));

    query = "select b + a as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.00]", "+I[4.00]", "+I[6.00]"));

    query = "select b + d as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[3.00]", "+I[5.00]", "+I[7.00]"));

    query = "select b - c as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[0.000]", "+I[0.000]", "+I[0.000]"));

    query = "select b - a as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[0.00]", "+I[0.00]", "+I[0.00]"));

    query = "select b * c as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[1.00000]", "+I[4.00000]", "+I[9.00000]"));

    query = "select b * d as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.00]", "+I[6.00]", "+I[12.00]"));

    query = "select b / c as x from tblDecimal where a > 0";
    runAndCheck(
        query, Arrays.asList("+I[1.0000000000000]", "+I[1.0000000000000]", "+I[1.0000000000000]"));

    query = "select b / a as x from tblDecimal where a > 0";
    runAndCheck(
        query, Arrays.asList("+I[1.0000000000000]", "+I[1.0000000000000]", "+I[1.0000000000000]"));

    query = "select b + e as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.0]", "+I[5.0]", "+I[7.0]"));

    query = "select a from tblDecimal where b > 2";
    runAndCheck(query, Arrays.asList("+I[3]"));

    query = "select a from tblDecimal where b > 2.0";
    runAndCheck(query, Arrays.asList("+I[3]"));

    query = "select a from tblDecimal where b > cast(2.0 as decimal(5,1))";
    runAndCheck(query, Arrays.asList("+I[3]"));

    query = "select a from tblDecimal where b < 2";
    runAndCheck(query, Arrays.asList("+I[1]"));

    query = "select a from tblDecimal where b < 2.0";
    runAndCheck(query, Arrays.asList("+I[1]"));

    query = "select a from tblDecimal where b < cast(2.0 as decimal(5,1))";
    runAndCheck(query, Arrays.asList("+I[1]"));
  }

  @Test
  void testDateFormat() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    List<Row> rows =
        Arrays.asList(
            Row.of(1, LocalDateTime.parse("2024-12-31 12:12:12", formatter)),
            Row.of(2, LocalDateTime.parse("2025-02-28 12:12:12", formatter)));
    createSimpleBoundedValuesTable("timestampTable", "a int, b Timestamp(3)", rows);
    String query =
        "select a, DATE_FORMAT(b, 'yyyy-MM-dd'), DATE_FORMAT(b, 'yyyy-MM-dd HH:mm:ss') from timestampTable";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, 2024-12-31, 2024-12-31 12:12:12]", "+I[2, 2025-02-28, 2025-02-28 12:12:12]"));
    tEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, 2024-12-31, 2024-12-31 12:12:12]", "+I[2, 2025-02-28, 2025-02-28 12:12:12]"));

    rows =
        Arrays.asList(
            Row.of(
                1, LocalDateTime.parse("2024-12-31 12:12:12", formatter).toInstant(ZoneOffset.UTC)),
            Row.of(
                2,
                LocalDateTime.parse("2025-02-28 12:12:12", formatter).toInstant(ZoneOffset.UTC)));
    createSimpleBoundedValuesTable("timestampLtzTable", "a int, b Timestamp_LTZ(3)", rows);
    query =
        "select a, DATE_FORMAT(b, 'yyyy-MM-dd'), DATE_FORMAT(b, 'yyyy-MM-dd HH:mm:ss') from timestampLtzTable";
    tEnv().getConfig().setLocalTimeZone(ZoneId.of("America/Los_Angeles"));
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, 2024-12-31, 2024-12-31 04:12:12]", "+I[2, 2025-02-28, 2025-02-28 04:12:12]"));

    tEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, 2024-12-31, 2024-12-31 20:12:12]", "+I[2, 2025-02-28, 2025-02-28 20:12:12]"));

    rows =
        Arrays.asList(
            Row.of(
                1,
                LocalDateTime.parse("2024-12-31 12:12:12", formatter),
                LocalDateTime.parse("2024-12-31 12:12:12", formatter).toInstant(ZoneOffset.UTC)),
            Row.of(
                2,
                LocalDateTime.parse("2025-02-28 12:12:12", formatter),
                LocalDateTime.parse("2024-02-28 12:12:12", formatter).toInstant(ZoneOffset.UTC)));
    createSimpleBoundedValuesTable(
        "timestampTable0", "a int, b Timestamp(3), c Timestamp_LTZ(3)", rows);
    query =
        "select a, DATE_FORMAT(b, 'yyyy-MM-dd HH:mm:ss'), DATE_FORMAT(c, 'yyyy-MM-dd HH:mm:ss') from timestampTable0";
    tEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, 2024-12-31 12:12:12, 2024-12-31 20:12:12]",
            "+I[2, 2025-02-28 12:12:12, 2024-02-28 20:12:12]"));
  }


  @Test
  void testTimestampLtzShanghai() {
    // Verify TIMESTAMP_LTZ with different precisions under Asia/Shanghai (UTC+8).
    tEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

    // Precision 3 (millis)
    List<Row> rowsMillis =
        Arrays.asList(
            Row.of(
                1,
                LocalDateTime.of(2024, 12, 31, 12, 12, 12, 500000000)
                    .toInstant(ZoneOffset.UTC)),
            Row.of(
                2,
                LocalDateTime.of(2025, 6, 15, 0, 0, 0, 0)
                    .toInstant(ZoneOffset.UTC)));
    createSimpleBoundedValuesTable(
        "timestampLtzMillisSh", "a int, b Timestamp_LTZ(3)", rowsMillis);
    String queryMillis = "select a, cast(b as string) from timestampLtzMillisSh";
    runAndCheck(
        queryMillis,
        Arrays.asList(
            "+I[1, 2024-12-31 20:12:12.500]", "+I[2, 2025-06-15 08:00:00.000]"));

    // Precision 6 (micros)
    List<Row> rowsMicros =
        Arrays.asList(
            Row.of(
                1,
                LocalDateTime.of(2024, 12, 31, 12, 12, 12, 123456000)
                    .toInstant(ZoneOffset.UTC)),
            Row.of(
                2,
                LocalDateTime.of(2025, 6, 15, 0, 0, 0, 0)
                    .toInstant(ZoneOffset.UTC)));
    createSimpleBoundedValuesTable(
        "timestampLtzMicrosSh", "a int, b Timestamp_LTZ(6)", rowsMicros);
    String queryMicros = "select a, cast(b as string) from timestampLtzMicrosSh";
    runAndCheck(
        queryMicros,
        Arrays.asList(
            "+I[1, 2024-12-31 20:12:12.123456]", "+I[2, 2025-06-15 08:00:00.000000]"));

    // Precision 9 (nanos)
    List<Row> rowsNanos =
        Arrays.asList(
            Row.of(
                1,
                LocalDateTime.of(2024, 12, 31, 12, 12, 12, 123456789)
                    .toInstant(ZoneOffset.UTC)),
            Row.of(
                2,
                LocalDateTime.of(2025, 6, 15, 0, 0, 0, 0)
                    .toInstant(ZoneOffset.UTC)));
    createSimpleBoundedValuesTable(
        "timestampLtzNanosSh", "a int, b Timestamp_LTZ(9)", rowsNanos);
    String queryNanos = "select a, cast(b as string) from timestampLtzNanosSh";
    runAndCheck(
        queryNanos,
        Arrays.asList(
            "+I[1, 2024-12-31 20:12:12.123456789]", "+I[2, 2025-06-15 08:00:00.000000000]"));

    // Verify plain TIMESTAMP (not LTZ) is unaffected by timezone
    List<Row> rowsTs =
        Arrays.asList(
            Row.of(1, LocalDateTime.of(2024, 12, 31, 12, 12, 12, 500000000)),
            Row.of(2, LocalDateTime.of(2025, 6, 15, 0, 0, 0, 0)));
    createSimpleBoundedValuesTable(
        "timestampMillisSh", "a int, b Timestamp(3)", rowsTs);
    String queryTs = "select a, cast(b as string) from timestampMillisSh";
    runAndCheck(
        queryTs,
        Arrays.asList(
            "+I[1, 2024-12-31 12:12:12.500]", "+I[2, 2025-06-15 00:00:00.000]"));
  }

  @Test
  void testTimestampMicros() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, LocalDateTime.of(2024, 12, 31, 12, 12, 12, 123456000)),
            Row.of(2, LocalDateTime.of(2025, 2, 28, 12, 12, 12, 654321000)));
    createSimpleBoundedValuesTable("timestampMicrosTable", "a int, b Timestamp(6)", rows);

    String query = "select a, cast(b as string) from timestampMicrosTable";
    runAndCheck(
        query,
        Arrays.asList("+I[1, 2024-12-31 12:12:12.123456]", "+I[2, 2025-02-28 12:12:12.654321]"));

    rows =
        Arrays.asList(
            Row.of(
                1, LocalDateTime.of(2024, 12, 31, 12, 12, 12, 123456000).toInstant(ZoneOffset.UTC)),
            Row.of(
                2, LocalDateTime.of(2025, 2, 28, 12, 12, 12, 654321000).toInstant(ZoneOffset.UTC)));
    createSimpleBoundedValuesTable("timestampLtzMicrosTable", "a int, b Timestamp_LTZ(6)", rows);

    query = "select a, cast(b as string) from timestampLtzMicrosTable";
    tEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
    runAndCheck(
        query,
        Arrays.asList("+I[1, 2024-12-31 20:12:12.123456]", "+I[2, 2025-02-28 20:12:12.654321]"));
  }

  @Test
  void testNotEqual() {
    List<Row> rows =
        Arrays.asList(
            Row.of(new BigDecimal("1.2"), 1L, "2", "1"),
            Row.of(new BigDecimal("2.2"), 2L, "2", "2"),
            Row.of(new BigDecimal("3.2"), 3L, "2", "1"));
    createSimpleBoundedValuesTable("tblLess", "a decimal(4,2), b bigint, c string, d string", rows);
    String query = "select a <> 2.20 as x from tblLess where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]", "+I[true]"));
  }

  @Test
  void testIn() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 1L, "2025-06-24 10:00:01", "1991-01-01 00:00:01"),
            Row.of(2, 2L, "2025-06-24 10:00:02", "1991-01-01 00:00:02"),
            Row.of(3, 3L, "2025-06-24 10:00:03", "1991-01-01 00:00:03"));
    createSimpleBoundedValuesTable("tblIn", "a int, b bigint, c string, d string", rows);
    String query = "select d from tblIn where a in(1,2)";
    runAndCheck(query, Arrays.asList("+I[1991-01-01 00:00:01]", "+I[1991-01-01 00:00:02]"));
    query = "select b from tblIn where c in('2025-06-24 10:00:02', '2025-06-24 10:00:03')";
    runAndCheck(query, Arrays.asList("+I[2]", "+I[3]"));
  }

  @Test
  void testIsNotNull() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L, "abc"), Row.of(2, 2L, null));
    createSimpleBoundedValuesTable("tblIsNotNull", "a int, b bigint, c string", rows);
    String query = "select a from tblIsNotNull where c is not null";
    runAndCheck(query, Arrays.asList("+I[1]"));
  }

  @Test
  void testIsNull() {
    List<Row> rows = Arrays.asList(Row.of(1, "100"), Row.of(2, null), Row.of(3, "5"));
    createSimpleBoundedValuesTable("tblIsNull", "a int, b string NULL", rows);
    String query = "select a from tblIsNull where b is null";
    runAndCheck(query, Arrays.asList("+I[2]"));
  }
}
