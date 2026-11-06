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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

class DateTimeFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testToDate() {
    List<Row> rows =
        Arrays.asList(
            Row.of("2026-09-11"),
            Row.of("2026-9-1"),
            Row.of("2026-09-11 10:00:00"),
            Row.of("9999-12-31"),
            Row.of("2026-13-01"),
            Row.of("abc"),
            Row.of((Object) null));
    createSimpleBoundedValuesTable("tblToDate", "s string", rows);
    runAndCheck(
        "select to_date(s) as d from tblToDate",
        Arrays.asList(
            "+I[20707]",
            "+I[20697]",
            "+I[20707]",
            "+I[2932896]",
            "+I[null]",
            "+I[null]",
            "+I[null]"));
  }

  @Test
  void testToDateWithColumnFormat() {
    List<Row> rows =
        Arrays.asList(
            Row.of("2026/09/11", "yyyy/MM/dd"),
            Row.of("20260911", "yyyyMMdd"),
            Row.of("2026-09-11", "yyyy/MM/dd"),
            Row.of("26-09-11", "yy-MM-dd"),
            Row.of("2026-09-11", null),
            Row.of(null, "yyyy-MM-dd"));
    createSimpleBoundedValuesTable("tblToDateFmt", "s string, f string", rows);
    runAndCheck(
        "select to_date(s, f) as d from tblToDateFmt",
        Arrays.asList("+I[20707]", "+I[20707]", "+I[null]", "+I[20707]", "+I[null]", "+I[null]"));
  }

  @Test
  void testToDateWithConstantFormat() {
    List<Row> rows = Arrays.asList(Row.of("2026/09/11"), Row.of("2026/9/1"), Row.of("garbage"));
    createSimpleBoundedValuesTable("tblToDateConst", "s string", rows);
    runAndCheck(
        "select to_date(s, 'yyyy/MM/dd') as d from tblToDateConst",
        Arrays.asList("+I[20707]", "+I[20697]", "+I[null]"));
    // Constant folding: the planner folds this into a DATE literal, which exercises
    // the DATE literal channel end to end.
    runAndCheck(
        "select to_date('2026/09/11', 'yyyy/MM/dd') as d from tblToDateConst",
        Arrays.asList("+I[20707]", "+I[20707]", "+I[20707]"));
  }

  @Test
  void testToTimestamp() {
    List<Row> rows =
        Arrays.asList(
            Row.of("2026-09-11 10:00:00"),
            Row.of("2026-02-30 10:00:00"),
            Row.of("2026-09-11 10:00:00.123456789"),
            Row.of("2026-09-11 24:00:00"),
            Row.of("2026-13-01 10:00:00"),
            Row.of((Object) null));
    createSimpleBoundedValuesTable("tblToTimestamp", "s string", rows);
    runAndCheck(
        "select to_timestamp(s) as ts from tblToTimestamp",
        Arrays.asList(
            "+I[2026-09-11T10:00:00.000]",
            "+I[2026-02-28T10:00:00.000]",
            "+I[2026-09-11T10:00:00.123]",
            "+I[2026-09-12T00:00:00.000]",
            "+I[null]",
            "+I[null]"));
  }

  @Test
  void testToTimestampWithFormat() {
    List<Row> rows =
        Arrays.asList(
            Row.of("2026/09/11 10:00:00", "yyyy/MM/dd HH:mm:ss"),
            Row.of("2026-09-11 10:00:00", "yyyy-MM-dd HH:mm:ss"),
            Row.of("2026-09-11 10:00:00", "QQ"),
            Row.of(null, "yyyy-MM-dd HH:mm:ss"),
            Row.of("2026-09-11 10:00:00", null));
    createSimpleBoundedValuesTable("tblToTimestampFmt", "s string, f string", rows);
    runAndCheck(
        "select to_timestamp(s, f) as ts from tblToTimestampFmt",
        Arrays.asList(
            "+I[2026-09-11T10:00:00.000]",
            "+I[2026-09-11T10:00:00.000]",
            "+I[null]",
            "+I[null]",
            "+I[null]"));
  }

  @Test
  void testToTimestampLtz() {
    // Precision changes the unit of the same number: seconds vs milliseconds.
    List<Row> rows =
        Arrays.asList(
            Row.of(1789120800L, 0),
            Row.of(1789120800L, 3),
            Row.of(1789120800000L, 3),
            Row.of(253402300800000L, 3),
            Row.of((Object) null, (Object) null));
    createSimpleBoundedValuesTable("tblToTimestampLtz", "b bigint, p int", rows);
    runAndCheck(
        "select to_timestamp_ltz(b, p) as ts from tblToTimestampLtz",
        Arrays.asList(
            "+I[2026-09-11T10:00:00.000]",
            "+I[1970-01-21T16:58:40.800]",
            "+I[2026-09-11T10:00:00.000]",
            "+I[null]",
            "+I[null]"));
  }

  @Test
  void testToTimestampLtzDoubleAndDecimal() {
    List<Row> secRows =
        Arrays.asList(
            Row.of(1789120800.9d, new BigDecimal("1789120800.90")),
            Row.of(253402300800d, new BigDecimal("253402300800.00")));
    createSimpleBoundedValuesTable("tblLtzSec", "d double, p decimal(20,2)", secRows);
    // Seconds: the double keeps its fractional part through *1000, the decimal
    // truncates toward zero; the second row exceeds the epoch domain on both.
    runAndCheck(
        "select to_timestamp_ltz(d, 0) as ts1, to_timestamp_ltz(p, 0) as ts2 from tblLtzSec",
        Arrays.asList("+I[2026-09-11T10:00:00.900, 2026-09-11T10:00:00.000]", "+I[null, null]"));

    List<Row> milliRows =
        Arrays.asList(
            Row.of(1789120800900.9d, new BigDecimal("1789120800900.90")),
            Row.of(253402300800000.9d, new BigDecimal("253402300800000.90")));
    createSimpleBoundedValuesTable("tblLtzMilli", "d double, p decimal(20,2)", milliRows);
    runAndCheck(
        "select to_timestamp_ltz(d, 3) as ts1, to_timestamp_ltz(p, 3) as ts2 from tblLtzMilli",
        Arrays.asList("+I[2026-09-11T10:00:00.900, 2026-09-11T10:00:00.900]", "+I[null, null]"));
  }

  @Test
  void testTemporalFunctionsInCaseAndFilter() {
    List<Row> rows =
        Arrays.asList(
            Row.of("2026-09-11"),
            Row.of("2026-09-12"),
            Row.of("2026-13-01"),
            Row.of((Object) null));
    createSimpleBoundedValuesTable("tblTemporalCombo", "s string", rows);
    runAndCheck(
        "select case when cast(to_date(s) as string) = '2026-09-11' then 'hit' else 'miss' end"
            + " as r from tblTemporalCombo",
        Arrays.asList("+I[hit]", "+I[miss]", "+I[miss]", "+I[miss]"));
    runAndCheck(
        "select s from tblTemporalCombo where to_date(s) is not null",
        Arrays.asList("+I[2026-09-11]", "+I[2026-09-12]"));
  }
}
