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
package org.apache.gluten.table.runtime.stream;

import org.apache.gluten.table.runtime.stream.common.GlutenStreamingTestBase;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Translation-level tests of the gluten shadow StreamExecRank (rank2): the FirstN fast-path
 * routing, the removed offset/type guards, the SortLimit reuse through the class-loading binding.
 * Every query runs end to end through the velox operator at parallelism 1, so the changelog lines
 * below are exact.
 */
public class StreamExecRankTranslationTest extends GlutenStreamingTestBase {

  @BeforeEach
  public void before() throws Exception {
    super.before();
    // Exact changelog-order assertions need a single subtask.
    env().setParallelism(1);
    // The host's /etc/localtime is a legacy TZDB alias (Asia/Beijing); the
    // JVM reports it verbatim and velox's QueryConfig validation (new on the
    // rebased base) rejects non-IANA ids. Pin the canonical id like the other
    // upstream UT suites (ScalarFunctionsTest/TemporalFunctionsTest) do.
    tEnv().getConfig().setLocalTimeZone(java.time.ZoneId.of("Asia/Shanghai"));
  }

  private void createProcTimeTable() {
    createSimpleBoundedValuesTable(
        "rank_pt",
        "k int, v bigint, pt as PROCTIME()",
        Arrays.asList(Row.of(1, 30L), Row.of(2, 50L), Row.of(1, 20L), Row.of(2, 40L)));
  }

  private void createRowTimeTable() {
    createSimpleBoundedValuesTable(
        "rank_rt",
        "k int, v bigint, rt timestamp(3)",
        Arrays.asList(
            Row.of(1, 30L, LocalDateTime.of(2024, 1, 1, 10, 0, 3)),
            Row.of(1, 20L, LocalDateTime.of(2024, 1, 1, 10, 0, 1)),
            Row.of(2, 50L, LocalDateTime.of(2024, 1, 1, 10, 0, 4)),
            Row.of(2, 40L, LocalDateTime.of(2024, 1, 1, 10, 0, 2))));
  }

  private void createLimitTable() {
    createSimpleBoundedValuesTable(
        "rank_l",
        "k int, v bigint",
        Arrays.asList(
            Row.of(1, 30L), Row.of(2, 50L), Row.of(1, 20L), Row.of(2, 40L), Row.of(1, 10L)));
  }

  private void createTypedTable() {
    // The values connector cannot parse DATE strings (the FromElements
    // formatter reads ints only), so the date crosses as a string and the
    // sort key casts it: same DATE type at the rank's sort spec, real date
    // values. Two rows share the decimal value so the mixed-direction case
    // exercises the DATE tie-break.
    createSimpleBoundedValuesTable(
        "rank_typed",
        "k int, d decimal(10,2), ds string",
        Arrays.asList(
            Row.of(1, java.math.BigDecimal.valueOf(1050, 2), "2024-01-15"),
            Row.of(1, java.math.BigDecimal.valueOf(1050, 2), "2024-01-20"),
            Row.of(1, java.math.BigDecimal.valueOf(575, 2), "2024-01-10")));
  }

  private void createSingleRowProcTimeTable() {
    // One row per key: the descending-proctime Top1 counter-example keeps
    // its output deterministic (proctime values are wall-clock).
    createSimpleBoundedValuesTable(
        "rank_pt_single",
        "k int, v bigint, pt as PROCTIME()",
        Arrays.asList(Row.of(1, 30L), Row.of(2, 50L)));
  }

  private void createTimestampTable() {
    createSimpleBoundedValuesTable(
        "rank_ts",
        "k int, v bigint, ts timestamp(3)",
        Arrays.asList(Row.of(1, 30L, LocalDateTime.of(2024, 1, 1, 10, 0, 0))));
  }

  // rank2 B1: a single ascending proctime sort key routes to the FirstN
  // counter branch - the changelog is +I only, one line per admitted row in
  // arrival order (the counter never reads the time values).
  @Test
  public void testFirstNRouting() {
    createProcTimeTable();
    String query =
        "SELECT k, v, rn FROM ("
            + "  SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt) AS rn FROM rank_pt"
            + ") WHERE rn <= 2";
    runAndCheck(
        query, Arrays.asList("+I[1, 30, 1]", "+I[2, 50, 1]", "+I[1, 20, 2]", "+I[2, 40, 2]"));
  }

  // rank2 B1: the FirstN detection counter-examples stay on the generic
  // path - descending proctime, a second sort key and rowtime all translate
  // and run. The rowtime and multi-key shapes assert displacement chains
  // (deterministic sort values), which the counter branch would never emit.
  @Test
  public void testFirstNCounterExamples() {
    createProcTimeTable();
    createRowTimeTable();

    runAndCheck(
        "SELECT k, v, rn FROM ("
            + "  SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v, pt) AS rn FROM rank_pt"
            + ") WHERE rn <= 2",
        Arrays.asList(
            "+I[1, 30, 1]",
            "+I[2, 50, 1]",
            "-U[1, 30, 1]",
            "+U[1, 20, 1]",
            "+I[1, 30, 2]",
            "-U[2, 50, 1]",
            "+U[2, 40, 1]",
            "+I[2, 50, 2]"));

    runAndCheck(
        "SELECT k, v, rn FROM ("
            + "  SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt) AS rn FROM rank_rt"
            + ") WHERE rn <= 2",
        Arrays.asList(
            "+I[1, 30, 1]",
            "-U[1, 30, 1]",
            "+U[1, 20, 1]",
            "+I[1, 30, 2]",
            "+I[2, 50, 1]",
            "-U[2, 50, 1]",
            "+U[2, 40, 1]",
            "+I[2, 50, 2]"));

    // Descending proctime stays on the generic path - the (1,1) window of
    // a single time key converts to Deduplicate in the planner whatever the
    // projection does, so this counter-example uses a wider range with one
    // row per key: the output is deterministic whatever the wall-clock
    // proctime values are.
    createSingleRowProcTimeTable();
    runAndCheck(
        "SELECT k, v, rn FROM ("
            + "  SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt DESC) AS rn"
            + "    FROM rank_pt_single"
            + ") WHERE rn <= 2",
        Arrays.asList("+I[1, 30, 1]", "+I[2, 50, 1]"));
  }

  // rank2 D3: the bare ORDER BY ... LIMIT reaches the shadow rank through
  // the SortLimit subclass binding - behavior-level assertions pin the
  // "N rows, not 1" and the offset range: LIMIT 3 emits the best 3 rows,
  // LIMIT 3 OFFSET 2 emits the 3rd to 5th (rankStart/rankEnd truth).
  @Test
  public void testSortLimitBehavior() {
    createLimitTable();
    runAndCheck(
        "SELECT k, v FROM rank_l ORDER BY v LIMIT 3",
        Arrays.asList(
            "+I[1, 30]",
            "+I[2, 50]",
            "+I[1, 20]",
            "-D[2, 50]",
            "+I[2, 40]",
            "-D[2, 40]",
            "+I[1, 10]"));
    runAndCheck(
        "SELECT k, v FROM rank_l ORDER BY v LIMIT 3 OFFSET 2",
        Arrays.asList(
            "+I[2, 50]",
            "-U[2, 50]",
            "+U[2, 40]",
            "+I[2, 50]",
            "-U[2, 40]",
            "+U[1, 30]",
            "-U[2, 50]",
            "+U[2, 40]",
            "+I[2, 50]"));
  }

  // rank2 C3: the former L10 guard - an offset rank range now translates
  // and runs: emissions below rankStart are swallowed by the native
  // isInRankRange gate.
  @Test
  public void testOffsetWindow() {
    createLimitTable();
    runAndCheck(
        "SELECT k, v, rn FROM ("
            + "  SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM rank_l"
            + ") WHERE rn BETWEEN 2 AND 3",
        Arrays.asList(
            "+I[1, 30, 2]", "+I[2, 50, 2]", "-U[1, 30, 2]", "+U[1, 20, 2]", "+I[1, 30, 3]"));
  }

  // rank2 G2: DECIMAL and DATE sort keys (the former L11 guard) including
  // a mixed-direction multi-key shape whose DATE column breaks the decimal
  // tie in descending order.
  @Test
  public void testDecimalAndDateSortKeys() {
    createTypedTable();
    // The date rides only in the ordering: the gluten RowData output prints
    // a DATE column as its raw epoch-day int, so the assertions use the
    // changelog sequence (which date displaced which) instead of the value.
    runAndCheck(
        "SELECT k, d, rn FROM ("
            + "  SELECT k, d, CAST(ds AS DATE) AS dt, ROW_NUMBER() OVER (PARTITION BY k ORDER BY"
            + "    CAST(ds AS DATE)) AS rn FROM rank_typed"
            + ") WHERE rn <= 2",
        Arrays.asList(
            "+I[1, 10.50, 1]",
            "+I[1, 10.50, 2]",
            "-U[1, 10.50, 1]",
            "+U[1, 5.75, 1]",
            "-U[1, 10.50, 2]",
            "+U[1, 10.50, 2]"));
    // Mixed direction: the descending date tie-break on the equal decimals
    // displaces the first arrival instead of tail-inserting after it.
    runAndCheck(
        "SELECT k, d, rn FROM ("
            + "  SELECT k, d, CAST(ds AS DATE) AS dt, ROW_NUMBER() OVER (PARTITION BY k ORDER BY"
            + "    d ASC, CAST(ds AS DATE) DESC) AS rn FROM rank_typed"
            + ") WHERE rn <= 2",
        Arrays.asList(
            "+I[1, 10.50, 1]",
            "-U[1, 10.50, 1]",
            "+U[1, 10.50, 1]",
            "+I[1, 10.50, 2]",
            "-U[1, 10.50, 1]",
            "+U[1, 5.75, 1]",
            "-U[1, 10.50, 2]",
            "+U[1, 10.50, 2]"));
  }

  // rank2 G2 limit: TIME sort keys still fail loud - the type converter
  // has no TimeType entry (L14), so the translation of the rank input type
  // rejects the column.
  @Test
  public void testTimeSortKeyStillRejected() {
    createTimestampTable();
    String query =
        "SELECT k, v FROM ("
            + "  SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY CAST(ts AS TIME)) AS rn"
            + "    FROM rank_ts"
            + ") WHERE rn <= 2";
    createPrintSinkTable("printT", tEnv().sqlQuery(query).getResolvedSchema());
    try {
      // Upstream's TIME carrier semantics rejects this CAST at the converter
      // with a dedicated message; the rank guard never runs (the converter
      // rejects first) - the intent (TIME sort keys rejected at translation
      // time) is unchanged.
      assertThatThrownBy(() -> tEnv().executeSql("insert into printT " + query))
          .hasMessageContaining("casts a non-TIME operand to TIME");
    } finally {
      tEnv().executeSql("drop table if exists printT");
    }
  }
}
