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

class RoundFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testRoundDoubleHalfUp() {
    List<Row> rows =
        Arrays.asList(
            Row.of(28.05d, 0.15d),
            Row.of(99.99d, -2.5d),
            Row.of(2.5d, 12.5d),
            Row.of(-251.0d, 1234.789d),
            Row.of(null, 28.05d));
    createSimpleBoundedValuesTable("tblRoundD", "x double, y double", rows);
    String query = "select ROUND(x), ROUND(x, 1), ROUND(x, -1), ROUND(y, 1) from tblRoundD";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[28.0, 28.1, 30.0, 0.2]",
            "+I[100.0, 100.0, 100.0, -2.5]",
            "+I[3.0, 2.5, 0.0, 12.5]",
            "+I[-251.0, -251.0, -250.0, 1234.8]",
            "+I[null, null, null, 28.1]"));
  }

  @Test
  void testRoundDoubleNegativeScale() {
    List<Row> rows = Arrays.asList(Row.of(25.0d), Row.of(-25.0d), Row.of(249.0d), Row.of(-251.0d));
    createSimpleBoundedValuesTable("tblRoundN", "x double", rows);
    String query = "select ROUND(x, -1) from tblRoundN";
    runAndCheck(query, Arrays.asList("+I[30.0]", "+I[-30.0]", "+I[250.0]", "+I[-250.0]"));
  }

  @Test
  void testRoundDecimal() {
    List<Row> rows =
        Arrays.asList(
            Row.of(new BigDecimal("55.67")),
            Row.of(new BigDecimal("28.55")),
            Row.of(new BigDecimal("45.00")),
            Row.of((Object) null));
    createSimpleBoundedValuesTable("tblRoundP", "p decimal(10, 2)", rows);
    String query = "select ROUND(p), ROUND(p, 1), ROUND(p, -1) from tblRoundP";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[56, 55.7, 60]", "+I[29, 28.6, 30]", "+I[45, 45.0, 50]", "+I[null, null, null]"));
  }
}
