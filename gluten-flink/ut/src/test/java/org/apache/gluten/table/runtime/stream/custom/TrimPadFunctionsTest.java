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

import java.util.Arrays;
import java.util.List;

// End-to-end tests for TRIM/LTRIM/RTRIM/LPAD/RPAD on the velox flinksql
// implementations. Expected values follow Flink semantics: 1-arg trim removes
// only 0x20 spaces (tabs preserved), the 2-arg trim seek is a character set,
// and LPAD/RPAD return NULL for a negative length or an empty pad.
class TrimPadFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testTrimSingleArg() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "  hello  "), Row.of(2, "\t ab \t"), Row.of(3, "   "), Row.of(4, "world"));
    createSimpleBoundedValuesTable("tblTrimSingle", "a int, s string", rows);

    String query = "select TRIM(s), LTRIM(s), RTRIM(s) from tblTrimSingle";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[hello, hello  ,   hello]",
            "+I[\t ab \t, \t ab \t, \t ab \t]",
            "+I[, , ]",
            "+I[world, world, world]"));
  }

  @Test
  void testTrimFlags() {
    List<Row> rows = Arrays.asList(Row.of(1, "xxhiyxx"));
    createSimpleBoundedValuesTable("tblTrimFlags", "a int, s string", rows);

    String query =
        "select TRIM(LEADING 'x' FROM s), TRIM(TRAILING 'x' FROM s), TRIM(BOTH 'x' FROM s)"
            + " from tblTrimFlags";
    runAndCheck(query, Arrays.asList("+I[hiyxx, xxhiy, hiy]"));
  }

  @Test
  void testTrimDefaultBothSpaces() {
    List<Row> rows = Arrays.asList(Row.of(1, "  data  "));
    createSimpleBoundedValuesTable("tblTrimBoth", "a int, s string", rows);

    String query = "select TRIM(s) from tblTrimBoth";
    runAndCheck(query, Arrays.asList("+I[data]"));
  }

  @Test
  void testTrimWithColumnSeek() {
    // The seek column data avoids strings whose first byte is a space and that
    // are multi-character (the Flink isSpaceString divergence).
    List<Row> rows =
        Arrays.asList(Row.of(1, "xxabx", "x"), Row.of(2, "abba", "ab"), Row.of(3, "abc", ""));
    createSimpleBoundedValuesTable("tblTrimSeek", "a int, c1 string, c2 string", rows);

    String query = "select TRIM(c2 FROM c1) from tblTrimSeek";
    runAndCheck(query, Arrays.asList("+I[ab]", "+I[]", "+I[abc]"));
  }

  @Test
  void testLtrimRtrimWithSeek() {
    List<Row> rows = Arrays.asList(Row.of(1, "xxabx"));
    createSimpleBoundedValuesTable("tblLtrimRtrimSeek", "a int, s string", rows);

    String query = "select LTRIM(s, 'x'), RTRIM(s, 'x') from tblLtrimRtrimSeek";
    runAndCheck(query, Arrays.asList("+I[abx, xxab]"));
  }

  @Test
  void testLpadRpad() {
    List<Row> rows = Arrays.asList(Row.of(1, "text"));
    createSimpleBoundedValuesTable("tblPad", "a int, s string", rows);

    String query =
        "select LPAD(s, 5, 'x'), RPAD(s, 5, 'x'), LPAD(s, 3, 'xy'), RPAD(s, 3, 'xy'),"
            + " LPAD(s, 0, 'e'), LPAD(s, 7, 'xy'), RPAD(s, 7, 'xy') from tblPad";
    runAndCheck(query, Arrays.asList("+I[xtext, textx, tex, tex, , xyxtext, textxyx]"));
  }

  @Test
  void testPadInvalidArgs() {
    List<Row> rows = Arrays.asList(Row.of(1, "abc"));
    createSimpleBoundedValuesTable("tblPadInvalid", "a int, s string", rows);

    String query =
        "select LPAD(s, -1, 'e'), RPAD(s, -1, 'e'), LPAD(s, 5, ''), RPAD(s, 5, '')"
            + " from tblPadInvalid";
    runAndCheck(query, Arrays.asList("+I[null, null, null, null]"));
  }

  @Test
  void testNullPropagation() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, null, 5, "x"), Row.of(2, "abc", null, "x"), Row.of(3, "abc", 5, null));
    createSimpleBoundedValuesTable("tblTrimPadNull", "a int, s string, b int, p string", rows);

    String query = "select TRIM(s), LTRIM(s), RTRIM(s), LPAD(s, b, p) from tblTrimPadNull";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[null, null, null, null]", "+I[abc, abc, abc, null]", "+I[abc, abc, abc, null]"));
  }

  @Test
  void testNested() {
    List<Row> rows = Arrays.asList(Row.of(1, "  data  "));
    createSimpleBoundedValuesTable("tblNested", "a int, s string", rows);

    String query = "select LPAD(TRIM(s), 9, '*'), RPAD(TRIM(s), 9, '*') from tblNested";
    runAndCheck(query, Arrays.asList("+I[*****data, data*****]"));
  }
}
