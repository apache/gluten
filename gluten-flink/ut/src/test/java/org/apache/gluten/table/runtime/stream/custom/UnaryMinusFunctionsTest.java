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

class UnaryMinusFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testUnaryMinus() {
    List<Row> rows =
        Arrays.asList(
            Row.of(3L, new BigDecimal("12.34"), 2.5d),
            Row.of(-3L, new BigDecimal("-12.34"), -2.5d),
            Row.of(null, null, null));
    createSimpleBoundedValuesTable("tblNeg", "i bigint, p decimal(10, 2), x double", rows);
    String query = "select -i, -p, -x from tblNeg";
    runAndCheck(
        query, Arrays.asList("+I[-3, -12.34, -2.5]", "+I[3, 12.34, 2.5]", "+I[null, null, null]"));
  }

  @Test
  void testUnaryMinusNestedWithAbs() {
    List<Row> rows = Arrays.asList(Row.of(7L), Row.of(-7L), Row.of((Object) null));
    createSimpleBoundedValuesTable("tblNegA", "i bigint", rows);
    String query = "select -ABS(i) from tblNegA";
    runAndCheck(query, Arrays.asList("+I[-7]", "+I[-7]", "+I[null]"));
  }
}
