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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class ScanTest extends GlutenStreamingTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ScanTest.class);

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testFilter() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L, "1"), Row.of(2, 2L, "2"), Row.of(3, 3L, "3"));
    createSimpleBoundedValuesTable("MyTable", "a int, b bigint, c string", rows);
    String query = "select a, b as b,c, a > 2 from MyTable where a > 0";
    LOG.info("execution plan: {}", explainExecutionPlan(query));
    runAndCheck(
        query, Arrays.asList("+I[1, 1, 1, false]", "+I[2, 2, 2, false]", "+I[3, 3, 3, true]"));
  }

  @Test
  void testStructScan() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, Row.of(2L, "abc")),
            Row.of(2, Row.of(6L, "def")),
            Row.of(3, Row.of(8L, "ghi")));
    createSimpleBoundedValuesTable("strctTbl", "a int, b ROW<x bigint, y string>", rows);
    String query1 = "select a, b.x, b.y from strctTbl where a > 0";
    runAndCheck(query1, Arrays.asList("+I[1, 2, abc]", "+I[2, 6, def]", "+I[3, 8, ghi]"));

    /// TODO: The query2's output is not as expected.
    // String query2 = "select a, b from strctTbl where a > 1";
    // runAndCheck(query2, Arrays.asList("+I[2, +I[6, def]]", "+I[3, +I[8, ghi]]"));
  }

  @Test
  void testDoubleScan() {
    List<Row> rows = Arrays.asList(Row.of(1, 1.0), Row.of(2, 2.0));
    createSimpleBoundedValuesTable("floatTbl", "a int, b double", rows);
    String query = "select a, b from floatTbl where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, 1.0]", "+I[2, 2.0]"));
  }

  @Test
  void testArrayScan() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, new Integer[] {1, 2, 3}),
            Row.of(2, new Integer[] {4, 5, 6}),
            Row.of(3, new Integer[] {7, 8, 9}));
    createSimpleBoundedValuesTable("arrayTbl1", "a int, b array<int>", rows);
    String query = "select a, b from arrayTbl1 where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, [1, 2, 3]]", "+I[2, [4, 5, 6]]", "+I[3, [7, 8, 9]]"));

    rows =
        Arrays.asList(
            Row.of(1, new String[] {"a", "b", "c"}),
            Row.of(2, new String[] {"d", "e", "f"}),
            Row.of(3, new String[] {"g", "h", "i"}));
    createSimpleBoundedValuesTable("arrayTbl2", "a int, b array<string>", rows);
    query = "select a, b from arrayTbl2 where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, [a, b, c]]", "+I[2, [d, e, f]]", "+I[3, [g, h, i]]"));

    rows =
        Arrays.asList(
            Row.of(1, new Row[] {Row.of(1, 2), Row.of(3, 4)}), Row.of(3, new Row[] {Row.of(5, 6)}));
    createSimpleBoundedValuesTable("arrayTbl3", "a int, b array<ROW<x int, y int>>", rows);
    query = "select a, b from arrayTbl3 where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, [+I[1, 2], +I[3, 4]]]", "+I[3, [+I[5, 6]]]"));

    rows =
        Arrays.asList(
            Row.of(1, new Integer[][] {new Integer[] {1, 3}}),
            Row.of(3, new Integer[][] {new Integer[] {4, 5}}));
    createSimpleBoundedValuesTable("arrayTbl4", "a int, b array<array<int>>", rows);
    query = "select a, b from arrayTbl4 where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, [[1, 3]]]", "+I[3, [[4, 5]]]"));
  }

  private static <K, V> Map<K, V> orderedMap(K[] keys, V[] values) {
    Map<K, V> map = new LinkedHashMap<>();
    for (int i = 0; i < keys.length; ++i) {
      map.put(keys[i], values[i]);
    }
    return map;
  }

  @Test
  void testMapScan() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, orderedMap(new Integer[] {1}, new String[] {"a"})),
            Row.of(2, orderedMap(new Integer[] {2, 3}, new String[] {"b", "c"})),
            Row.of(3, orderedMap(new Integer[] {4, 5, 6}, new String[] {"d", "e", "f"})));
    createSimpleBoundedValuesTable("mapTbl1", "a int, b map<int, string>", rows);
    String query = "select a, b from mapTbl1 where a > 0";
    runAndCheck(
        query, Arrays.asList("+I[1, {1=a}]", "+I[2, {2=b, 3=c}]", "+I[3, {4=d, 5=e, 6=f}]"));

    rows =
        Arrays.asList(
            Row.of(
                1,
                new Map[] {
                  orderedMap(new String[] {"a"}, new Integer[] {1}),
                  orderedMap(new String[] {"b"}, new Integer[] {2})
                }),
            Row.of(2, new Map[] {orderedMap(new String[] {"b", "c"}, new Integer[] {2, 3})}),
            Row.of(
                3, new Map[] {orderedMap(new String[] {"d", "e", "f"}, new Integer[] {4, 5, 6})}));
    createSimpleBoundedValuesTable("mapTbl2", "a int, b array<map<string, int>>", rows);
    query = "select a, b from mapTbl2 where a > 0";
    runAndCheck(
        query,
        Arrays.asList("+I[1, [{a=1}, {b=2}]]", "+I[2, [{b=2, c=3}]]", "+I[3, [{d=4, e=5, f=6}]]"));
  }

  @Test
  void testNullScan() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L, "a"), Row.of(2, null, "b"), Row.of(3, 3L, null));
    createSimpleBoundedValuesTable("nullTbl", "a int, b bigint, c string", rows);
    String query = "select a, b, c from nullTbl where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, 1, a]", "+I[2, null, b]", "+I[3, 3, null]"));

    rows =
        Arrays.asList(Row.of(1, null), Row.of(2, Row.of(6L, null)), Row.of(3, Row.of(null, "ghi")));
    createSimpleBoundedValuesTable("nullStrctTbl", "a int, b ROW<x bigint, y string>", rows);
    query = "select a, b.x, b.y, b from nullStrctTbl where a > 0";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[1, null, null, null]",
            "+I[2, 6, null, +I[6, null]]",
            "+I[3, null, ghi, +I[null, ghi]]"));

    rows =
        Arrays.asList(
            Row.of(1, null),
            Row.of(2, new Integer[] {null, 5, 6}),
            Row.of(3, new Integer[] {7, 8, null}));
    createSimpleBoundedValuesTable("nullArrayTbl", "a int, b array<int>", rows);
    query = "select a, b from nullArrayTbl where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, null]", "+I[2, [null, 5, 6]]", "+I[3, [7, 8, null]]"));

    rows =
        Arrays.asList(
            Row.of(1, null),
            Row.of(2, orderedMap(new String[] {"a", "b", null}, new Integer[] {null, 2, 3})),
            Row.of(3, orderedMap(new String[] {"c", "d"}, new Integer[] {3, 4})));
    createSimpleBoundedValuesTable("nullMapTbl", "a int, b map<string, int>", rows);
    query = "select a, b from nullMapTbl where a > 0";
    runAndCheck(
        query, Arrays.asList("+I[1, null]", "+I[2, {a=null, b=2, null=3}]", "+I[3, {c=3, d=4}]"));
  }

  @Test
  void testCharScan() {
    List<Row> rows = Arrays.asList(Row.of(1, "a1"), Row.of(2, "b2"), Row.of(3, "c2"));
    createSimpleBoundedValuesTable("charTbl", "a int, b char(2)", rows);
    String query = "select a, b from charTbl where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, a1]", "+I[2, b2]", "+I[3, c2]"));
  }

  void testDateScan() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, LocalDate.parse("2023-01-01")), Row.of(2, LocalDate.parse("2023-01-02")));
    createSimpleBoundedValuesTable("dateTbl", "a int, b date", rows);
    String query = "select a, b from dateTbl where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, 2023-01-01]", "+I[2, 2023-01-02]"));
  }

  @Test
  void testDecimal() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, new BigDecimal("1.23")), Row.of(2, null), Row.of(3, new BigDecimal("7.89")));
    createSimpleBoundedValuesTable("decimalTbl", "a int, b decimal(5, 2)", rows);
    String query = "select a, b from decimalTbl where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, 1.23]", "+I[2, null]", "+I[3, 7.89]"));
  }
}
