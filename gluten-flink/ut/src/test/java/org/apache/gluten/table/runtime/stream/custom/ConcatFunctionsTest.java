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

class ConcatFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testConcat() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "hello", "world"), Row.of(2, "foo", "bar"), Row.of(3, "abc", "xyz"));
    createSimpleBoundedValuesTable("tblConcat", "a int, b string, c string", rows);

    String query = "select CONCAT(b, c) from tblConcat where a > 0";
    runAndCheck(query, Arrays.asList("+I[helloworld]", "+I[foobar]", "+I[abcxyz]"));
  }

  @Test
  void testConcatWithLiteral() {
    List<Row> rows = Arrays.asList(Row.of(1, "hello"), Row.of(2, "foo"), Row.of(3, "abc"));
    createSimpleBoundedValuesTable("tblConcatLit", "a int, b string", rows);

    String query = "select CONCAT(b, '_suffix') from tblConcatLit where a > 0";
    runAndCheck(query, Arrays.asList("+I[hello_suffix]", "+I[foo_suffix]", "+I[abc_suffix]"));
  }

  @Test
  void testConcatWs() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "hello", "world"), Row.of(2, "foo", "bar"), Row.of(3, "abc", "xyz"));
    createSimpleBoundedValuesTable("tblConcatWs", "a int, b string, c string", rows);

    String query = "select CONCAT_WS('-', b, c) from tblConcatWs where a > 0";
    runAndCheck(query, Arrays.asList("+I[hello-world]", "+I[foo-bar]", "+I[abc-xyz]"));
  }

  @Test
  void testConcatWsWithLiteral() {
    List<Row> rows = Arrays.asList(Row.of(1, "a", "b", "c"), Row.of(2, "d", "e", "f"));
    createSimpleBoundedValuesTable("tblConcatWsLit", "a int, b string, c string, d string", rows);

    String query = "select CONCAT_WS(',', b, c, d) from tblConcatWsLit where a > 0";
    runAndCheck(query, Arrays.asList("+I[a,b,c]", "+I[d,e,f]"));
  }

  @Test
  void testConcatWithNull() {
    List<Row> rows = Arrays.asList(Row.of(1, "hello", null), Row.of(2, null, "world"));
    createSimpleBoundedValuesTable("tblConcatNull", "a int, b string, c string", rows);

    // CONCAT returns null if any argument is null
    String query = "select CONCAT(b, c) from tblConcatNull where a > 0";
    runAndCheck(query, Arrays.asList("+I[null]", "+I[null]"));
  }
}
