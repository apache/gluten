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

class LikeFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
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
