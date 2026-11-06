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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testMd5Basic() {
    // Empty-string input is covered by testMd5Constant via MD5(''): the values
    // connector's from_elements drops an empty string row field.
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "test"),
            Row.of(2, "abc"),
            Row.of(3, "你好"),
            Row.of(4, null),
            Row.of(5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    createSimpleBoundedValuesTable("tblMd5Basic", "a int, s string", rows);
    String query = "select MD5(s) from tblMd5Basic where a > 0";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[098f6bcd4621d373cade4e832627b4f6]",
            "+I[900150983cd24fb0d6963f7d28e17f72]",
            "+I[7eca689f0d3389d9dea66ae112e5cfd7]",
            "+I[null]",
            "+I[5eca9bd3eb07c006cd43ae48dfde7fd3]"));
  }

  @Test
  void testSha1Basic() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "test"),
            Row.of(2, "abc"),
            Row.of(3, "你好"),
            Row.of(4, null),
            Row.of(5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    createSimpleBoundedValuesTable("tblSha1Basic", "a int, s string", rows);
    String query = "select SHA1(s) from tblSha1Basic where a > 0";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[a94a8fe5ccb19ba61c4c0873d391e987982fbbd3]",
            "+I[a9993e364706816aba3e25717850c26c9cd0d89d]",
            "+I[440ee0853ad1e99f962b63e459ef992d7c211722]",
            "+I[null]",
            "+I[68f84a59a3ca2d0e5cb1646fbb164da409b5d8f2]"));
  }

  @Test
  void testMd5Constant() {
    List<Row> rows = Arrays.asList(Row.of(1));
    createSimpleBoundedValuesTable("tblMd5Const", "a int", rows);
    // Literal inputs, including the empty string, hash without any row access.
    String query = "select MD5(''), MD5('abc') from tblMd5Const";
    runAndCheck(
        query,
        Arrays.asList("+I[d41d8cd98f00b204e9800998ecf8427e, 900150983cd24fb0d6963f7d28e17f72]"));
  }

  @Test
  void testSha1Constant() {
    List<Row> rows = Arrays.asList(Row.of(1));
    createSimpleBoundedValuesTable("tblSha1Const", "a int", rows);
    String query = "select SHA1(''), SHA1('abc') from tblSha1Const";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[da39a3ee5e6b4b0d3255bfef95601890afd80709, a9993e364706816aba3e25717850c26c9cd0d89d]"));
  }

  @Test
  void testMd5Nested() {
    List<Row> rows = Arrays.asList(Row.of(1, "Hello"));
    createSimpleBoundedValuesTable("tblMd5Nested", "a int, s string", rows);
    // MD5 over the output of another scalar function (LOWER is offloaded).
    String query = "select MD5(LOWER(s)) from tblMd5Nested";
    runAndCheck(query, Arrays.asList("+I[5d41402abc4b2a76b9719d911017c592]"));
  }

  @Test
  void testSha1Nested() {
    List<Row> rows = Arrays.asList(Row.of(1, "Hello"));
    createSimpleBoundedValuesTable("tblSha1Nested", "a int, s string", rows);
    String query = "select SHA1(LOWER(s)) from tblSha1Nested";
    runAndCheck(query, Arrays.asList("+I[aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d]"));
  }

  @Test
  void testMd5NonStringInput() {
    List<Row> rows = Arrays.asList(Row.of(1, 123), Row.of(2, 456));
    createSimpleBoundedValuesTable("tblMd5Int", "a int, n int", rows);
    // An explicit cast stringifies the int before hashing.
    String castQuery = "select MD5(CAST(n AS STRING)) from tblMd5Int";
    runAndCheck(
        castQuery,
        Arrays.asList(
            "+I[202cb962ac59075b964b07152d234b70]", "+I[250cf8b51c773f3f8dc8b4be867a9a02]"));
    // A bare int argument has no varchar signature and must be rejected.
    tEnv().executeSql("CREATE TABLE printT (s VARCHAR) WITH ('connector' = 'print')");
    assertThatThrownBy(() -> tEnv().executeSql("insert into printT select MD5(n) from tblMd5Int"))
        .hasStackTraceContaining("Cannot apply 'MD5'");
    tEnv().executeSql("drop table if exists printT");
  }

  @Test
  void testSha1NonStringInput() {
    List<Row> rows = Arrays.asList(Row.of(1, 123), Row.of(2, 456));
    createSimpleBoundedValuesTable("tblSha1Int", "a int, n int", rows);
    String castQuery = "select SHA1(CAST(n AS STRING)) from tblSha1Int";
    runAndCheck(
        castQuery,
        Arrays.asList(
            "+I[40bd001563085fc35165329ea1ff5c5ecbdbbeef]",
            "+I[51eac6b471a284d3341d8c0c63d0f1a286262a18]"));
    tEnv().executeSql("CREATE TABLE printT (s VARCHAR) WITH ('connector' = 'print')");
    assertThatThrownBy(() -> tEnv().executeSql("insert into printT select SHA1(n) from tblSha1Int"))
        .hasStackTraceContaining("Cannot apply 'SHA1'");
    tEnv().executeSql("drop table if exists printT");
  }
}
