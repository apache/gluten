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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashFunctionsTest extends GlutenStreamingTestBase {

  // Digests pinned from Flink's own test suite for "" and "test" (lowercase hex
  // over the string's UTF-8 bytes); the multi-byte row pins the encoding and the
  // long row pins inputs past one digest block.
  private static final String SHA224_EMPTY =
      "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f";
  private static final String SHA256_EMPTY =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  private static final String SHA384_EMPTY =
      "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc"
          + "7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b";
  private static final String SHA512_EMPTY =
      "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d"
          + "0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e";
  private static final String SHA224_TEST =
      "90a3ed9e32b2aaf4c61c410eb925426119e1a9dc53d4286ade99a809";
  private static final String SHA256_TEST =
      "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
  private static final String SHA384_TEST =
      "768412320f7b0aa5812fce428dc4706b3cae50e02a64caa16a782249bfe8efc"
          + "4b7ef1ccb126255d196047dfedf17a0a9";
  private static final String SHA512_TEST =
      "ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db"
          + "27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff";
  private static final String SHA224_UTF8 =
      "b3f5c93d7b531d7e29266412f2c842f8a4b7286871f87d259c6eaf07";
  private static final String SHA256_UTF8 =
      "beca6335b20ff57ccc47403ef4d9e0b8fccb4442b3151c2e7d50050673d43172";
  private static final String SHA384_UTF8 =
      "5621250177cc297c02d4c7c2a950d541a52b5c478e1fa16ca5022316de898d7be5c"
          + "66b16ad735295b48b84a47e986144";
  private static final String SHA512_UTF8 =
      "4b28a152c8e203ebb52e099301041e3cf704a56190d3097ec8b086a0f9bfb4b9d533"
          + "ce71fc3bcf374359e506dc5f17322ec3911eac8dd8f5b35308d938ba0c26";
  private static final String SHA224_LONG =
      "e6e4a6be069cc9bead8b6050856d2b26da6b3f7efa0951e5fb3a54dd";
  private static final String SHA256_LONG =
      "74e7e5bb9d22d6db26bf76946d40fff3ea9f0346b884fd0694920fccfad15e33";
  private static final String SHA384_LONG =
      "ce6d4ea5442bc6c830bea1942d4860db9f7b96f0e9d2c3073ffe47a0e1166d95612d"
          + "840ff15e5efdd23c1f273096da32";
  private static final String SHA512_LONG =
      "95cadc34aa46b9fdef432f62fe5bad8d9f475bfbecf797d5802bb5f2937a85d93ce48"
          + "57a6262b03834c01c610d74cd1215f9a466dc6ad3dd15078e3309a03a6d";
  private static final String LONG_INPUT = "0123456789abcdefghijklmnopqrstuvwxyz";

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testSha2ColumnLength() {
    List<Row> rows =
        Arrays.asList(
            Row.of("", 224), Row.of("test", 256), Row.of("你好世界", 384), Row.of(LONG_INPUT, 512));
    createSimpleBoundedValuesTable("tblSha2", "s varchar, n int", rows);

    String query = "select SHA2(s, n) from tblSha2";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[" + SHA224_EMPTY + "]",
            "+I[" + SHA256_TEST + "]",
            "+I[" + SHA384_UTF8 + "]",
            "+I[" + SHA512_LONG + "]"));
  }

  @Test
  void testSha2ZeroLengthFails() {
    // Flink 1.19.2 rejects hash length 0 outright (no Hive-style 0-means-256);
    // the offloaded path must fail the same way to keep both stacks consistent.
    List<Row> rows = Arrays.asList(Row.of("test", 0));
    createSimpleBoundedValuesTable("tblSha2Zero", "s varchar, n int", rows);
    tEnv().executeSql("CREATE TABLE printT (d VARCHAR) WITH ('connector' = 'print')");
    TableResult result = tEnv().executeSql("insert into printT select SHA2(s, n) from tblSha2Zero");
    assertThat(result.getJobClient()).isPresent();
    assertThatThrownBy(() -> result.getJobClient().get().getJobExecutionResult().get())
        .hasStackTraceContaining("Unsupported SHA-2 hash length: 0");
    tEnv().executeSql("drop table if exists printT");
  }

  @Test
  void testSha2ConstantLength() {
    // Planner-folded INT and BIGINT literals route through the int32 and int64
    // sha2 signatures.
    List<Row> rows = Arrays.asList(Row.of("test"), Row.of("你好世界"));
    createSimpleBoundedValuesTable("tblSha2Const", "s varchar", rows);

    String query = "select SHA2(s, 256), SHA2(s, CAST(512 AS BIGINT)) from tblSha2Const";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[" + SHA256_TEST + ", " + SHA512_TEST + "]",
            "+I[" + SHA256_UTF8 + ", " + SHA512_UTF8 + "]"));
  }

  @Test
  void testSha2ConstantInvalidLengthRejectedAtConversion() {
    // Flink fails a job with a bad constant hashLength in the generated
    // operator's constructor — even on empty input, since the check is
    // input-independent; the conversion rejects it the same way, while the
    // column form keeps the per-row runtime rejection.
    List<Row> rows = Arrays.asList(Row.of("test"));
    createSimpleBoundedValuesTable("tblSha2ConstBad", "s varchar", rows);
    tEnv().executeSql("CREATE TABLE printT (d VARCHAR) WITH ('connector' = 'print')");
    assertThatThrownBy(
            () -> tEnv().executeSql("insert into printT select SHA2(s, 0) from tblSha2ConstBad"))
        .hasStackTraceContaining("SHA2 with literal hashLength 0 is not supported");
    assertThatThrownBy(
            () -> tEnv().executeSql("insert into printT select SHA2(s, 385) from tblSha2ConstBad"))
        .hasStackTraceContaining("SHA2 with literal hashLength 385 is not supported");
    tEnv().executeSql("drop table if exists printT");
  }

  @Test
  void testSha224() {
    List<Row> rows = Arrays.asList(Row.of(""), Row.of("test"), Row.of("你好世界"), Row.of(LONG_INPUT));
    createSimpleBoundedValuesTable("tblSha224", "s varchar", rows);

    String query = "select SHA224(s) from tblSha224";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[" + SHA224_EMPTY + "]",
            "+I[" + SHA224_TEST + "]",
            "+I[" + SHA224_UTF8 + "]",
            "+I[" + SHA224_LONG + "]"));
  }

  @Test
  void testSha256() {
    List<Row> rows = Arrays.asList(Row.of(""), Row.of("test"), Row.of("你好世界"), Row.of(LONG_INPUT));
    createSimpleBoundedValuesTable("tblSha256", "s varchar", rows);

    String query = "select SHA256(s) from tblSha256";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[" + SHA256_EMPTY + "]",
            "+I[" + SHA256_TEST + "]",
            "+I[" + SHA256_UTF8 + "]",
            "+I[" + SHA256_LONG + "]"));
  }

  @Test
  void testSha384() {
    List<Row> rows = Arrays.asList(Row.of(""), Row.of("test"), Row.of("你好世界"), Row.of(LONG_INPUT));
    createSimpleBoundedValuesTable("tblSha384", "s varchar", rows);

    String query = "select SHA384(s) from tblSha384";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[" + SHA384_EMPTY + "]",
            "+I[" + SHA384_TEST + "]",
            "+I[" + SHA384_UTF8 + "]",
            "+I[" + SHA384_LONG + "]"));
  }

  @Test
  void testSha512() {
    List<Row> rows = Arrays.asList(Row.of(""), Row.of("test"), Row.of("你好世界"), Row.of(LONG_INPUT));
    createSimpleBoundedValuesTable("tblSha512", "s varchar", rows);

    String query = "select SHA512(s) from tblSha512";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[" + SHA512_EMPTY + "]",
            "+I[" + SHA512_TEST + "]",
            "+I[" + SHA512_UTF8 + "]",
            "+I[" + SHA512_LONG + "]"));
  }

  @Test
  void testNullPropagation() {
    List<Row> rows = Arrays.asList(Row.of("test", null), Row.of(null, 256), Row.of(null, null));
    createSimpleBoundedValuesTable("tblSha2Null", "s varchar, n int", rows);

    String query1 = "select SHA2(s, n) from tblSha2Null";
    runAndCheck(query1, Arrays.asList("+I[null]", "+I[null]", "+I[null]"));

    String query2 = "select SHA256(s) from tblSha2Null";
    runAndCheck(query2, Arrays.asList("+I[" + SHA256_TEST + "]", "+I[null]", "+I[null]"));
  }

  @Test
  void testSha2UnsupportedLengthFails() {
    // Flink rejects unsupported hash lengths; the velox flinksql function set
    // fails the job with the same channel.
    List<Row> rows = Arrays.asList(Row.of("test", 385));
    createSimpleBoundedValuesTable("tblSha2Bad", "s varchar, n int", rows);
    tEnv().executeSql("CREATE TABLE printT (d VARCHAR) WITH ('connector' = 'print')");
    TableResult result = tEnv().executeSql("insert into printT select SHA2(s, n) from tblSha2Bad");
    assertThat(result.getJobClient()).isPresent();
    assertThatThrownBy(() -> result.getJobClient().get().getJobExecutionResult().get())
        .hasStackTraceContaining("Unsupported SHA-2 hash length: 385");
    tEnv().executeSql("drop table if exists printT");
  }
}
