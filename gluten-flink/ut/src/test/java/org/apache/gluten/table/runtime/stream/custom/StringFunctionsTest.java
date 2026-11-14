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

import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SQL-level tests for the string functions REPLACE, RIGHT, FROM_BASE64 and LIKE. */
class StringFunctionsTest extends GlutenStreamingTestBase {

  @Test
  void testReplace() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "abc", "b", "X"),
            Row.of(2, "hello", "l", "L"),
            Row.of(3, "abc", "z", "Y"),
            Row.of(4, "abc", "b", null));
    createSimpleBoundedValuesTable(
        "tblReplace", "a int, s string, old_s string, new_s string", rows);
    String query = "select REPLACE(s, old_s, new_s) from tblReplace where a > 0";
    runAndCheck(query, Arrays.asList("+I[aXc]", "+I[heLLo]", "+I[abc]", "+I[null]"));
  }

  @Test
  void testReplaceEmptyOldStr() {
    // Empty `from` inserts the replacement at UTF-8 character boundaries
    // (the Flink-vs-Spark distinguishing semantic).
    List<Row> rows = Arrays.asList(Row.of(1, "abc"), Row.of(2, "你好"));
    createSimpleBoundedValuesTable("tblReplaceEmpty", "a int, s string", rows);
    String query = "select REPLACE(s, '', 'x') from tblReplaceEmpty where a > 0";
    runAndCheck(query, Arrays.asList("+I[xaxbxcx]", "+I[x你x好x]"));
  }

  @Test
  void testRight() {
    List<Row> rows = Arrays.asList(Row.of(1, "abc"), Row.of(2, "你好"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblRight", "a int, s string", rows);
    String query = "select RIGHT(s, 2) from tblRight where a > 0";
    runAndCheck(query, Arrays.asList("+I[bc]", "+I[你好]", "+I[null]"));
  }

  @Test
  void testRightBoundaries() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "abc", 0),
            Row.of(2, "abc", -1),
            Row.of(3, "abc", 5),
            Row.of(4, "你好", 1),
            Row.of(5, "你好", 10));
    createSimpleBoundedValuesTable("tblRightBound", "a int, s string, n int", rows);
    String query = "select RIGHT(s, n) from tblRightBound where a > 0";
    runAndCheck(query, Arrays.asList("+I[]", "+I[]", "+I[abc]", "+I[好]", "+I[你好]"));
  }

  @Test
  void testFromBase64() {
    List<Row> rows = Arrays.asList(Row.of(1, "dGVzdA=="), Row.of(2, "5L2g5aW9"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblB64", "a int, b64 string", rows);
    String query = "select FROM_BASE64(b64) from tblB64 where a > 0";
    runAndCheck(query, Arrays.asList("+I[test]", "+I[你好]", "+I[null]"));
  }

  @Test
  void testFromBase64UnpaddedAndEmpty() {
    List<Row> rows = Arrays.asList(Row.of("dGVzdA"), Row.of("YQ"));
    createSimpleBoundedValuesTable("tblB64Unpadded", "b64 string", rows);
    String query = "select FROM_BASE64(b64) from tblB64Unpadded";
    runAndCheck(query, Arrays.asList("+I[test]", "+I[a]"));

    List<Row> rows2 = Arrays.asList(Row.of(1));
    createSimpleBoundedValuesTable("tblB64Empty", "a int", rows2);
    String query2 = "select FROM_BASE64('') from tblB64Empty where a > 0";
    runAndCheck(query2, Arrays.asList("+I[]"));
  }

  @Test
  void testFromBase64InvalidInput() {
    // Invalid characters error out on both sides (velox raises a user error,
    // Flink's Base64.getDecoder() throws), so the job fails rather than emitting.
    List<Row> rows = Arrays.asList(Row.of("!@#$"));
    createSimpleBoundedValuesTable("tblB64Invalid", "b64 string", rows);
    tEnv().executeSql("CREATE TABLE printT (b64 VARCHAR) WITH ('connector' = 'print')");
    TableResult result =
        tEnv().executeSql("insert into printT select FROM_BASE64(b64) from tblB64Invalid");
    assertThatThrownBy(
            () -> result.getJobClient().get().getJobExecutionResult().get(60, TimeUnit.SECONDS))
        .isInstanceOf(Exception.class);
    tEnv().executeSql("drop table if exists printT");
  }

  @Test
  void testLike() {
    List<Row> rows = Arrays.asList(Row.of(1, "abc"), Row.of(2, "hello"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblLike", "a int, s string", rows);
    String query = "select s LIKE '%bc' from tblLike where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]", "+I[null]"));
  }

  @Test
  void testLikeEmptyPattern() {
    // A constant empty pattern vacuously matches every input: verified against
    // the Flink 1.19.2 native runtime by an end-to-end dual-run (the
    // master-only `mark > 0 && isEmptyPattern` gate of SqlLikeChainChecker is
    // absent in 1.19.x). Followed for Flink parity.
    List<Row> rows = Arrays.asList(Row.of(1, "abc"), Row.of(2, "hello"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblLikeEmpty", "a int, s string", rows);
    String query = "select s LIKE '' from tblLikeEmpty where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[true]", "+I[null]"));
  }

  @Test
  void testLikeUnderscore() {
    // '_' matches any single character.
    List<Row> rows = Arrays.asList(Row.of(1, "abc"), Row.of(2, "aXc"), Row.of(3, "ac"));
    createSimpleBoundedValuesTable("tblLikeUnder", "a int, s string", rows);
    String query = "select s LIKE 'a_c' from tblLikeUnder where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[true]", "+I[false]"));
  }

  @Test
  void testLikeEscape() {
    // '#' is the escape; escaped '%' becomes a literal percent.
    List<Row> rows = Arrays.asList(Row.of(1, "a%b"), Row.of(2, "a_b"), Row.of(3, "axb"));
    createSimpleBoundedValuesTable("tblLikeEsc", "a int, s string", rows);
    String query = "select s LIKE 'a#%b' ESCAPE '#' from tblLikeEsc where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));
  }

  @Test
  void testLikeEscapeUnderscore() {
    // Escaped '_' becomes a literal underscore.
    List<Row> rows = Arrays.asList(Row.of(1, "a_b"), Row.of(2, "axb"));
    createSimpleBoundedValuesTable("tblLikeEscUnder", "a int, s string", rows);
    String query = "select s LIKE 'a#_b' ESCAPE '#' from tblLikeEscUnder where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]"));
  }

  @Test
  void testLikeNonAsciiEscape() {
    // A non-ASCII escape (one Java char, multi-byte UTF-8) is valid.
    List<Row> rows = Arrays.asList(Row.of(1, "a%b"), Row.of(2, "axb"));
    createSimpleBoundedValuesTable("tblLikeNonAsciiEsc", "a int, s string", rows);
    String query = "select s LIKE 'aé%b' ESCAPE 'é' from tblLikeNonAsciiEsc where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]"));
  }
}
