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

class FloorCeilFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testFloorCeilDouble() {
    List<Row> rows =
        Arrays.asList(
            Row.of(55.67d), Row.of(-55.01d), Row.of(45.00d), Row.of(-0.5d), Row.of((Object) null));
    createSimpleBoundedValuesTable("tblFC", "x double", rows);
    String query = "select FLOOR(x), CEIL(x) from tblFC";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[55.0, 56.0]",
            "+I[-56.0, -55.0]",
            "+I[45.0, 45.0]",
            "+I[-1.0, -0.0]",
            "+I[null, null]"));
  }

  @Test
  void testFloorCeilDecimal() {
    List<Row> rows =
        Arrays.asList(
            Row.of(new BigDecimal("55.67")),
            Row.of(new BigDecimal("-55.01")),
            Row.of(new BigDecimal("45.00")),
            Row.of((Object) null));
    createSimpleBoundedValuesTable("tblFCP", "p decimal(10, 2)", rows);
    String query = "select FLOOR(p), CEIL(p) from tblFCP";
    runAndCheck(query, Arrays.asList("+I[55, 56]", "+I[-56, -55]", "+I[45, 45]", "+I[null, null]"));
  }

  @Test
  void testFloorCeilIntegerIdentity() {
    List<Row> rows = Arrays.asList(Row.of(7L), Row.of(-7L), Row.of((Object) null));
    createSimpleBoundedValuesTable("tblFCI", "i bigint", rows);
    String query = "select FLOOR(i), CEIL(i) from tblFCI";
    runAndCheck(query, Arrays.asList("+I[7, 7]", "+I[-7, -7]", "+I[null, null]"));
  }

  @Test
  void testCeilingAlias() {
    List<Row> rows = Arrays.asList(Row.of(55.67d));
    createSimpleBoundedValuesTable("tblCA", "x double", rows);
    String query = "select CEILING(x) from tblCA";
    runAndCheck(query, Arrays.asList("+I[56.0]"));
  }
}
