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

class IfNullFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testIfNullWithNullableColumns() {
    List<Row> rows =
        Arrays.asList(
            Row.of("a", 1L, 1.5d, new BigDecimal("1.50")),
            Row.of(null, 7L, null, new BigDecimal("12.34")),
            Row.of(null, null, null, null));
    createSimpleBoundedValuesTable(
        "tblIfNull", "s string, b bigint, d double, p decimal(10, 2)", rows);
    String query =
        "select IFNULL(s, 'N/A'), IFNULL(b, -1), IFNULL(d, 0.5), IFNULL(p, 9.99) from tblIfNull";
    runAndCheck(
        query,
        Arrays.asList("+I[a, 1, 1.5, 1.50]", "+I[N/A, 7, 0.5, 12.34]", "+I[N/A, -1, 0.5, 9.99]"));
  }

  @Test
  void testIfNullWithNullLiteralFirstArg() {
    List<Row> rows = Arrays.asList(Row.of("x"), Row.of("y"));
    createSimpleBoundedValuesTable("tblIfNullLit", "s string", rows);
    String query = "select IFNULL(CAST(NULL AS STRING), s) from tblIfNullLit";
    runAndCheck(query, Arrays.asList("+I[x]", "+I[y]"));
  }

  @Test
  void testIfNullBothNull() {
    List<Row> rows = Arrays.asList(Row.of((Object) null), Row.of("y"));
    createSimpleBoundedValuesTable("tblIfNullBoth", "s string", rows);
    String query = "select IFNULL(s, CAST(NULL AS STRING)) from tblIfNullBoth";
    runAndCheck(query, Arrays.asList("+I[null]", "+I[y]"));
  }
}
