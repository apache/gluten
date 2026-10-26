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

import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CurrentTimeFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testCurrentTimeIsMillisOfDay() {
    List<Row> rows = Arrays.asList(Row.of(1), Row.of(2), Row.of(3));
    createSimpleBoundedValuesTable("tblCT", "x int", rows);
    String query = "select x, CAST(CURRENT_TIME AS TIME) from tblCT";
    // The velox print sink renders TIME as the underlying INT millis-of-day;
    // each row must be close to the local wall clock.
    List<String> result = runQuery(query);
    assertThat(result).hasSize(3);
    int nowMillis = (int) (LocalTime.now().toNanoOfDay() / 1_000_000);
    for (int i = 0; i < result.size(); i++) {
      String prefix = "+I[" + (i + 1) + ", ";
      assertThat(result.get(i)).startsWith(prefix).endsWith("]");
      int millis =
          Integer.parseInt(result.get(i).substring(prefix.length(), result.get(i).length() - 1));
      assertThat(millis).isBetween(0, 86_400_000 - 1);
      long delta = Math.abs(millis - nowMillis);
      delta = Math.min(delta, 86_400_000L - delta); // midnight wraparound
      assertThat(delta).isLessThanOrEqualTo(60_000L);
    }
  }

  @Test
  void testCurrentTimeComparisons() {
    List<Row> rows = Arrays.asList(Row.of(1), Row.of(2));
    createSimpleBoundedValuesTable("tblCT2", "x int", rows);
    // TIME literals become INT millis-of-day constants.
    String query =
        "select x, CURRENT_TIME > TIME '00:00:01', CURRENT_TIME < TIME '23:59:59' from tblCT2";
    runAndCheck(query, Arrays.asList("+I[1, true, true]", "+I[2, true, true]"));
  }
}
