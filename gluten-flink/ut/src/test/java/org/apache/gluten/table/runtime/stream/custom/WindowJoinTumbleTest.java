/*
 * Licensed under the Apache Software Foundation (ASF) under one or more
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


import org.junit.jupiter.api.Test;


import org.apache.flink.types.Row;
import java.util.Arrays;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL-level tests for the TUMBLE window TVF (velox projection) and the
 * TUMBLE window join (velox operator). Each test compares the rows the
 * gluten translation produces against the rows Flink semantics requires.
 */
class WindowJoinTumbleTest extends GlutenStreamingTestBase {

  // A bounded table with event-time values and a watermark. The test base
  // runs in bounded mode: the final watermark (MAX) fires every window.
  private void createTumbleTables() {
    createSimpleBoundedValuesTable(
        "tumbleSrc",
        "ts TIMESTAMP(3), k INT, v VARCHAR, "
            + "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND",
        Arrays.asList(
            Row.of(LocalDateTime.parse("2024-01-01T00:00:02"), 1, "a"),
            Row.of(LocalDateTime.parse("2024-01-01T00:00:05"), 1, "b"),
            Row.of(LocalDateTime.parse("2024-01-01T00:00:08"), 2, "c")));
  }

  @Test
  void testTumbleTVFWindowBounds() {
    createTumbleTables();
    String query =
        "SELECT k, window_start, window_end, COUNT(*) AS cnt "
            + "FROM TABLE("
            + "  TUMBLE(TABLE tumbleSrc, DESCRIPTOR(ts), INTERVAL '5' SECONDS)"
            + ") "
            + "GROUP BY k, window_start, window_end";
    List<String> rows = runQuery(query);
    // k=1: both rows (ts=2, ts=5) land in window [00:00:00, 00:00:05) =
    // 2 rows. k=2: ts=8 lands in [00:00:05, 00:00:10) = 1 row.
    assertThat(rows).hasSize(2);
    // k=1: both rows in [00:00:00, 00:00:05); k=2: one row in [00:00:05, 00:00:10).
    long k1Count =
        rows.stream().filter(r -> r.startsWith("+I[1,")).count();
    long k2Count =
        rows.stream().filter(r -> r.startsWith("+I[2,")).count();
    assertThat(k1Count).isEqualTo(1);
    assertThat(k2Count).isEqualTo(1);
    // The k=1 window holds 2 rows (cnt column).
    assertThat(rows.stream().filter(r -> r.startsWith("+I[1,")).findFirst().get())
        .contains(", 2]");
  }

  @Test
  void testTumbleTVFRawProjection() {
    createTumbleTables();
    String query =
        "SELECT k, v, window_start, window_end "
            + "FROM TABLE("
            + "  TUMBLE(TABLE tumbleSrc, DESCRIPTOR(ts), INTERVAL '5' SECONDS)"
            + ")";
    List<String> rows = runQuery(query);
    // All three input rows survive the TVF, tagged with their window
    // bounds; the pair (k=1, ts=2) and (k=1, ts=5) share one window.
    assertThat(rows).hasSize(3);
  }

  @Test
  void testTumbleWindowJoin() {
    createTumbleTables();
    // Two identical windows joined on the key: every row of one side pairs
    // with every row of the other in the same (key, window).
    tEnv()
        .executeSql(
            "CREATE VIEW leftSide AS "
                + "SELECT k AS lk, v AS lv, window_start, window_end "
                + "FROM TABLE(TUMBLE(TABLE tumbleSrc, DESCRIPTOR(ts), INTERVAL '5' SECONDS))");
    tEnv()
        .executeSql(
            "CREATE VIEW rightSide AS "
                + "SELECT k AS rk, v AS rv, window_start, window_end "
                + "FROM TABLE(TUMBLE(TABLE tumbleSrc, DESCRIPTOR(ts), INTERVAL '5' SECONDS))");
    String query =
        "SELECT lk, lv, rk, rv FROM leftSide AS L "
            + "JOIN rightSide AS R "
            + "ON L.lk = R.rk AND L.window_start = R.window_start AND L.window_end = R.window_end";
    List<String> rows = runQuery(query);
    // k=1: 2x2 = 4 pairs; k=2: 1x1 = 1 pair.
    assertThat(rows).hasSize(5);
    long lk1 = rows.stream().filter(r -> r.startsWith("+I[1,")).count();
    long lk2 = rows.stream().filter(r -> r.startsWith("+I[2,")).count();
    assertThat(lk1).isEqualTo(4);
    assertThat(lk2).isEqualTo(1);
  }

  @Test
  void testTumbleWindowJoinDifferentWindows() {
    // Left and right read disjoint time ranges: the windows never overlap
    // for the same key, so nothing pairs.
    createSimpleBoundedValuesTable(
        "leftSrc",
        "ts TIMESTAMP(3), k INT, v VARCHAR, "
            + "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND",
        Arrays.asList(Row.of(LocalDateTime.parse("2024-01-01T00:00:02"), 1, "a")));
    createSimpleBoundedValuesTable(
        "rightSrc",
        "ts TIMESTAMP(3), k INT, v VARCHAR, "
            + "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND",
        Arrays.asList(Row.of(LocalDateTime.parse("2024-01-01T00:01:02"), 1, "x")));
    String query =
        "SELECT L.k, L.v, R.k, R.v FROM "
            + "(SELECT k, v, window_start, window_end FROM "
            + "  TABLE(TUMBLE(TABLE leftSrc, DESCRIPTOR(ts), INTERVAL '5' SECONDS))) AS L "
            + "JOIN "
            + "(SELECT k, v, window_start, window_end FROM "
            + "  TABLE(TUMBLE(TABLE rightSrc, DESCRIPTOR(ts), INTERVAL '5' SECONDS))) AS R "
            + "ON L.k = R.k AND L.window_start = R.window_start AND L.window_end = R.window_end";
    List<String> rows = runQuery(query);
    assertThat(rows).isEmpty();
  }
}
