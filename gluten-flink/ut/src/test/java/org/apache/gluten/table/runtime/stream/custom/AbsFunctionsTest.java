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

class AbsFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testAbsWithNegativeValues() {
    List<Row> rows =
        Arrays.asList(
            Row.of(-25L, new BigDecimal("-12.34"), -2.5d),
            Row.of(25L, new BigDecimal("12.34"), 2.5d),
            Row.of(null, null, null));
    createSimpleBoundedValuesTable("tblAbs", "i bigint, p decimal(10, 2), x double", rows);
    String query = "select ABS(i), ABS(p), ABS(x) from tblAbs";
    runAndCheck(
        query, Arrays.asList("+I[25, 12.34, 2.5]", "+I[25, 12.34, 2.5]", "+I[null, null, null]"));
  }

  @Test
  void testAbsWithArithmetic() {
    List<Row> rows = Arrays.asList(Row.of(3L), Row.of(19L), Row.of((Object) null));
    createSimpleBoundedValuesTable("tblAbsA", "i bigint", rows);
    String query = "select ABS(i - 30) from tblAbsA";
    runAndCheck(query, Arrays.asList("+I[27]", "+I[11]", "+I[null]"));
  }

  /** INT_MIN / int64-min wrap to themselves, matching Java Math.abs and Flink's -x. */
  @Test
  void testAbsIntegerBoundaries() {
    List<Row> rows =
        Arrays.asList(Row.of(Integer.MIN_VALUE, Long.MIN_VALUE), Row.of(-1, -9007199254740993L));
    createSimpleBoundedValuesTable("tblAbsEdge", "i int, l bigint", rows);
    String query = "select ABS(i), ABS(l) from tblAbsEdge";
    runAndCheck(
        query, Arrays.asList("+I[-2147483648, -9223372036854775808]", "+I[1, 9007199254740993]"));
  }
}
