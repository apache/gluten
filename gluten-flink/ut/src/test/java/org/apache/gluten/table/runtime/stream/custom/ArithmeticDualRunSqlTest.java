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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * The dual-run SQL cases for the arithmetic functions, embedded verbatim: each query is the exact
 * SELECT (including the VALUES source) that ran on Flink 1.19.2 in the dual-run verification
 * (native vs gluten-offloaded, identical inputs, exact changelog-multiset comparison), and the
 * expected rows are the native output the dual-run matched against. Kept here so the queries and
 * their verified results live with the code.
 */
class ArithmeticDualRunSqlTest extends GlutenStreamingTestBase {

  @Test
  void testRoundDoubleDualRunSql() {
    String query =
        "SELECT\n"
            + "  ROUND(x),\n"
            + "  ROUND(x, 1),\n"
            + "  ROUND(x, -1),\n"
            + "  ROUND(x, 2),\n"
            + "  ROUND(y, 1)\n"
            + "FROM (\n"
            + "  VALUES\n"
            + "    (28.05, 0.15),\n"
            + "    (99.99, -2.5),\n"
            + "    (2.5, 12.5),\n"
            + "    (-251.0, 1234.789),\n"
            + "    (CAST(NULL AS DOUBLE), 28.05)\n"
            + ") AS t (x, y)";
    List<String> expected =
        Arrays.asList(
            "+I[28.0, 28.1, 30.0, 28.05, 0.2]",
            "+I[100.0, 100.0, 100.0, 99.99, -2.5]",
            "+I[3.0, 2.5, 0.0, 2.5, 12.5]",
            "+I[-251.0, -251.0, -250.0, -251.0, 1234.8]",
            "+I[null, null, null, null, 28.1]");
    runAndCheck(query, expected);
  }

  @Test
  void testRoundDecimalDualRunSql() {
    String query =
        "SELECT\n"
            + "  ROUND(p),\n"
            + "  ROUND(p, 1),\n"
            + "  ROUND(p, -1),\n"
            + "  ROUND(p, 2)\n"
            + "FROM (\n"
            + "  VALUES\n"
            + "    (55.67),\n"
            + "    (28.55),\n"
            + "    (28.05),\n"
            + "    (45.00),\n"
            + "    (CAST(NULL AS DECIMAL(10, 2)))\n"
            + ") AS t (p)";
    List<String> expected =
        Arrays.asList(
            "+I[56, 55.7, 60, 55.67]",
            "+I[29, 28.6, 30, 28.55]",
            "+I[28, 28.1, 30, 28.05]",
            "+I[45, 45.0, 50, 45.00]",
            "+I[null, null, null, null]");
    runAndCheck(query, expected);
  }

  @Test
  void testAbsUminusDualRunSql() {
    String query =
        "SELECT\n"
            + "  ABS(i),\n"
            + "  ABS(p),\n"
            + "  -x,\n"
            + "  -p,\n"
            + "  -ABS(-i)\n"
            + "FROM (\n"
            + "  VALUES\n"
            + "    (-25, -12.34, 2.5),\n"
            + "    (25, 12.34, -2.5),\n"
            + "    (CAST(NULL AS BIGINT), CAST(NULL AS DECIMAL(10, 2)), CAST(NULL AS DOUBLE))\n"
            + ") AS t (i, p, x)";
    List<String> expected =
        Arrays.asList(
            "+I[25, 12.34, -2.5, 12.34, -25]",
            "+I[25, 12.34, 2.5, -12.34, -25]",
            "+I[null, null, null, null, null]");
    runAndCheck(query, expected);
  }

  @Test
  void testFloorCeilDualRunSql() {
    String query =
        "SELECT\n"
            + "  FLOOR(x),\n"
            + "  CEIL(x),\n"
            + "  FLOOR(p),\n"
            + "  CEIL(p),\n"
            + "  FLOOR(i),\n"
            + "  CEIL(i)\n"
            + "FROM (\n"
            + "  VALUES\n"
            + "    (55.67, 28.55, 7),\n"
            + "    (-55.01, -28.55, -7),\n"
            + "    (45.00, 45.00, 0),\n"
            + "    (-0.5, 0.5, CAST(NULL AS BIGINT)),\n"
            + "    (CAST(NULL AS DOUBLE), CAST(NULL AS DECIMAL(10, 2)), 9)\n"
            + ") AS t (x, p, i)";
    List<String> expected =
        Arrays.asList(
            "+I[55.0, 56.0, 28, 29, 7, 7]",
            "+I[-56.0, -55.0, -29, -28, -7, -7]",
            "+I[45.0, 45.0, 45, 45, 0, 0]",
            "+I[-1.0, -0.0, 0, 1, null, null]",
            "+I[null, null, null, null, 9, 9]");
    runAndCheck(query, expected);
  }
}
