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

/** SQL-level tests for the logical operators over a boolean column containing NULLs. */
class LogicalFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  // Three-valued truth-table input: TRUE, FALSE, UNKNOWN.
  private List<Row> truthTableRows() {
    return Arrays.asList(Row.of(true), Row.of(false), Row.of((Object) null));
  }

  @Test
  void testNot() {
    createSimpleBoundedValuesTable("tblLogicalNot", "c boolean", truthTableRows());
    String query = "select NOT(c) from tblLogicalNot";
    runAndCheck(query, Arrays.asList("+I[false]", "+I[true]", "+I[null]"));
  }

  @Test
  void testIsTrue() {
    createSimpleBoundedValuesTable("tblLogicalIsTrue", "c boolean", truthTableRows());
    String query = "select c IS TRUE from tblLogicalIsTrue";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));
  }

  @Test
  void testIsFalse() {
    createSimpleBoundedValuesTable("tblLogicalIsFalse", "c boolean", truthTableRows());
    String query = "select c IS FALSE from tblLogicalIsFalse";
    runAndCheck(query, Arrays.asList("+I[false]", "+I[true]", "+I[false]"));
  }

  @Test
  void testIsNotTrue() {
    createSimpleBoundedValuesTable("tblLogicalIsNotTrue", "c boolean", truthTableRows());
    String query = "select c IS NOT TRUE from tblLogicalIsNotTrue";
    runAndCheck(query, Arrays.asList("+I[false]", "+I[true]", "+I[true]"));
  }

  @Test
  void testIsNotFalse() {
    createSimpleBoundedValuesTable("tblLogicalIsNotFalse", "c boolean", truthTableRows());
    String query = "select c IS NOT FALSE from tblLogicalIsNotFalse";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]", "+I[true]"));
  }

  @Test
  void testNestedLogical() {
    createSimpleBoundedValuesTable("tblLogicalNested", "c boolean", truthTableRows());
    // NOT(c IS FALSE): IS FALSE never returns NULL, so NOT sees
    // {false,true,false} -> {true,false,true}.
    // (NOT(c)) IS NOT TRUE (parens required: IS binds tighter than NOT, so
    // NOT(c) IS NOT TRUE parses as NOT(c IS NOT TRUE)): NOT preserves NULL,
    // then IS NOT TRUE maps {false,true,NULL} -> {true,false,true}, never
    // NULL.
    // NOT(c) IS NOT TRUE parses as NOT(c IS NOT TRUE); c IS NOT TRUE is
    // never NULL, so NOT flips {false,true,true} -> {true,false,false}.
    String query =
        "select NOT(c IS FALSE), (NOT(c)) IS NOT TRUE, NOT(c) IS NOT TRUE from tblLogicalNested";
    runAndCheck(
        query,
        Arrays.asList("+I[true, true, true]", "+I[false, false, false]", "+I[true, true, false]"));
  }

  @Test
  void testConstantOperands() {
    createSimpleBoundedValuesTable("tblLogicalConst", "c boolean", truthTableRows());
    // Constant and constant-foldable operands: a NULL constant operand still
    // yields FALSE under IS TRUE, never NULL.
    String query =
        "select NOT(true), NOT(false), true IS TRUE, false IS FALSE, "
            + "CAST(NULL AS BOOLEAN) IS TRUE, NOT(1 = 1) from tblLogicalConst";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[false, true, true, true, false, false]",
            "+I[false, true, true, true, false, false]",
            "+I[false, true, true, true, false, false]"));
  }

  @Test
  void testLogicalFilter() {
    createSimpleBoundedValuesTable("tblLogicalFilter", "c boolean", truthTableRows());
    // IS NOT FALSE passes TRUE and UNKNOWN, rejects FALSE.
    String query = "select c from tblLogicalFilter where c IS NOT FALSE";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[null]"));
  }
}
