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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** End-to-end tests for the Deduplicate stream operator. */
class DeduplicateFunctionsTest extends GlutenStreamingTestBase {

  @Test
  void testProcTimeKeepFirst() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 10L), Row.of(1, 20L), Row.of(2, 30L), Row.of(2, 40L), Row.of(3, 50L));
    createSimpleBoundedValuesTable("tblProcFirst", "a int, b bigint, proctime AS PROCTIME()", rows);
    String query =
        "select a, b from (select a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime ASC)"
            + " AS rn from tblProcFirst) where rn = 1";
    runAndCheck(query, Arrays.asList("+I[1, 10]", "+I[2, 30]", "+I[3, 50]"));
  }

  @Test
  void testProcTimeKeepLast() {
    List<Row> rows = Arrays.asList(Row.of(1, 10L), Row.of(1, 20L), Row.of(2, 30L), Row.of(2, 40L));
    createSimpleBoundedValuesTable("tblProcLast", "a int, b bigint, proctime AS PROCTIME()", rows);
    String query =
        "select a, b from (select a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC)"
            + " AS rn from tblProcLast) where rn = 1";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, 10]", "-U[1, 10]", "+U[1, 20]", "+I[2, 30]", "-U[2, 30]", "+U[2, 40]"));
  }

  @Test
  void testGlobalProcTimeKeepFirst() {
    List<Row> rows = Arrays.asList(Row.of(10L), Row.of(20L), Row.of(30L));
    createSimpleBoundedValuesTable("tblGlobal", "b bigint, proctime AS PROCTIME()", rows);
    String query =
        "select b from (select b, ROW_NUMBER() OVER (ORDER BY proctime ASC) AS rn from"
            + " tblGlobal) where rn = 1";
    runAndCheck(query, Arrays.asList("+I[10]"));
  }

  @Test
  void testRowtimeKeepFirst() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 100L, "2025-06-24 10:05:00"),
            Row.of(1, 90L, "2025-06-24 10:03:00"),
            Row.of(2, 200L, "2025-06-24 10:01:00"));
    createSimpleBoundedValuesTable(
        "tblRowFirst",
        "a int, b bigint, tstr string, ts AS CAST(tstr AS TIMESTAMP(3)),"
            + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
        rows);
    String query =
        "select a, b from (select a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY ts ASC) AS rn"
            + " from tblRowFirst) where rn = 1";
    runAndCheck(query, Arrays.asList("+I[1, 100]", "-U[1, 100]", "+U[1, 90]", "+I[2, 200]"));
  }

  @Test
  void testGlobalRowtimeKeepLast() {
    List<Row> rows =
        Arrays.asList(
            Row.of(100L, "2025-06-24 10:00:00"),
            Row.of(120L, "2025-06-24 10:05:00"),
            Row.of(90L, "2025-06-24 10:03:00"));
    createSimpleBoundedValuesTable(
        "tblRowGlobal",
        "b bigint, tstr string, ts AS CAST(tstr AS TIMESTAMP(3)),"
            + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
        rows);
    String query =
        "select b from (select b, ROW_NUMBER() OVER (ORDER BY ts DESC) AS rn from tblRowGlobal)"
            + " where rn = 1";
    runAndCheck(query, Arrays.asList("+I[100]", "-U[100]", "+U[120]"));
  }

  @Test
  void testRowtimeKeepLast() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 100L, "2025-06-24 10:01:00"),
            Row.of(1, 200L, "2025-06-24 10:03:00"),
            Row.of(2, 300L, "2025-06-24 10:01:00"),
            Row.of(1, 150L, "2025-06-24 10:02:00"),
            Row.of(1, 250L, "2025-06-24 10:03:00"));
    createSimpleBoundedValuesTable(
        "tblRowLast",
        "a int, b bigint, tstr string, ts AS CAST(tstr AS TIMESTAMP(3)),"
            + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
        rows);
    String query =
        "select a, b from (select a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY ts DESC) AS rn"
            + " from tblRowLast) where rn = 1";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, 100]", "-U[1, 100]", "+U[1, 200]", "+I[2, 300]", "-U[1, 200]", "+U[1, 250]"));
  }

  @Test
  void testProcTimeKeepLastSameValueSuppressed() {
    List<Row> rows = Arrays.asList(Row.of(1, 10L), Row.of(1, 10L), Row.of(1, 20L), Row.of(2, 30L));
    createSimpleBoundedValuesTable("tblProcSame", "a int, b bigint, proctime AS PROCTIME()", rows);
    String query =
        "select a, b from (select a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC)"
            + " AS rn from tblProcSame) where rn = 1";
    runAndCheck(query, Arrays.asList("+I[1, 10]", "-U[1, 10]", "+U[1, 20]", "+I[2, 30]"));
  }

  @Test
  void testProcTimeKeepLastStringPayload() {
    List<Row> rows = Arrays.asList(Row.of(1, "alpha"), Row.of(1, "alpha"), Row.of(1, "beta"));
    createSimpleBoundedValuesTable(
        "tblProcString", "a int, s string, proctime AS PROCTIME()", rows);
    String query =
        "select a, s from (select a, s, ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC)"
            + " AS rn from tblProcString) where rn = 1";
    runAndCheck(query, Arrays.asList("+I[1, alpha]", "-U[1, alpha]", "+U[1, beta]"));
  }

  @Test
  void testMiniBatchCompactChangesFails() {
    try {
      tEnv().getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "true");
      tEnv()
          .getConfig()
          .getConfiguration()
          .setString("table.exec.deduplicate.mini-batch.compact-changes-enabled", "true");
      createSimpleBoundedValuesTable(
          "tblMbCompact", "a int, b bigint, proctime AS PROCTIME()", Arrays.asList(Row.of(1, 10L)));
      String query =
          "select a, b from (select a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC)"
              + " AS rn from tblMbCompact) where rn = 1";
      createPrintSinkTable("printMb", tEnv().sqlQuery(query).getResolvedSchema());
      Throwable thrown =
          assertThrows(Exception.class, () -> tEnv().executeSql("insert into printMb " + query));
      assertThat(thrown)
          .hasStackTraceContaining(
              "Gluten deduplicate does not support mini-batch with compact-changes enabled");
    } finally {
      tEnv().getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "false");
      tEnv()
          .getConfig()
          .getConfiguration()
          .setString("table.exec.deduplicate.mini-batch.compact-changes-enabled", "false");
      tEnv().executeSql("drop table if exists printMb");
    }
  }
}
