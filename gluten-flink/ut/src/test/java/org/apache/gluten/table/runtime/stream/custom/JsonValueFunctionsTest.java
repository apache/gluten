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

class JsonValueFunctionsTest extends GlutenStreamingTestBase {

  @Test
  void testJsonValue() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, "{\"name\":\"alice\",\"n\":42}"), Row.of(2, "not-json"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblJson", "a int, j string", rows);
    String query = "select JSON_VALUE(j, '$.name'), JSON_VALUE(j, '$.n') from tblJson where a > 0";
    runAndCheck(query, Arrays.asList("+I[alice, 42]", "+I[null, null]", "+I[null, null]"));
  }

  @Test
  void testJsonValueNullOnErrorHit() {
    // NULL ON ERROR: a missing/non-scalar result yields SQL NULL; a hit returns
    // the value. (Flink's Calcite rejects DEFAULT NULL, so NULL is the equivalent form.)
    List<Row> rows = Arrays.asList(Row.of(1, "not-json"), Row.of(2, "{\"name\":\"alice\"}"));
    createSimpleBoundedValuesTable("tblJsonDn", "a int, j string", rows);
    String query = "select JSON_VALUE(j, '$.name' NULL ON ERROR) from tblJsonDn where a > 0";
    runAndCheck(query, Arrays.asList("+I[null]", "+I[alice]"));
  }

  @Test
  void testJsonValueHandlers() {
    List<Row> rows = Arrays.asList(Row.of(1, "{\"name\":\"alice\"}"));
    createSimpleBoundedValuesTable("tblJsonH", "a int, j string", rows);
    String query =
        "select "
            + "JSON_VALUE(j, 'lax $.missing' DEFAULT 'empty' ON EMPTY), "
            + "JSON_VALUE(j, 'lax $.missing' NULL ON EMPTY), "
            + "JSON_VALUE(j, '$.name') "
            + "from tblJsonH where a > 0";
    runAndCheck(query, Arrays.asList("+I[empty, null, alice]"));
  }

  @Test
  void testJsonValueReturning() {
    List<Row> rows = Arrays.asList(Row.of(1, "{\"n\":42,\"b\":true,\"d\":1.5}"));
    createSimpleBoundedValuesTable("tblJsonR", "a int, j string", rows);
    String query =
        "select "
            + "JSON_VALUE(j, '$.n' RETURNING INT), "
            + "JSON_VALUE(j, '$.b' RETURNING BOOLEAN), "
            + "JSON_VALUE(j, '$.d' RETURNING DOUBLE) "
            + "from tblJsonR where a > 0";
    runAndCheck(query, Arrays.asList("+I[42, true, 1.5]"));
  }

  @Test
  void testJsonValueLaxVsStrict() {
    List<Row> rows = Arrays.asList(Row.of(1, "{\"name\":\"alice\"}"));
    createSimpleBoundedValuesTable("tblJsonLS", "a int, j string", rows);
    String query =
        "select "
            + "JSON_VALUE(j, 'lax $.missing' DEFAULT 'e' ON EMPTY), "
            + "JSON_VALUE(j, 'strict $.missing' DEFAULT 's' ON ERROR) "
            + "from tblJsonLS where a > 0";
    runAndCheck(query, Arrays.asList("+I[e, s]"));
  }

  @Test
  void testJsonValueNonScalar() {
    List<Row> rows = Arrays.asList(Row.of(1, "{\"obj\":{},\"arr\":[1,2],\"x\":null}"));
    createSimpleBoundedValuesTable("tblJsonNS", "a int, j string", rows);
    String query =
        "select "
            + "JSON_VALUE(j, 'lax $.obj' DEFAULT 'e' ON EMPTY), "
            + "JSON_VALUE(j, 'strict $.obj' DEFAULT 's' ON ERROR), "
            + "JSON_VALUE(j, 'lax $.arr' DEFAULT 'e' ON EMPTY), "
            + "JSON_VALUE(j, 'lax $.x' DEFAULT 'e' ON EMPTY), "
            + "JSON_VALUE(j, 'strict $.x' DEFAULT 's' ON ERROR) "
            + "from tblJsonNS where a > 0";
    runAndCheck(query, Arrays.asList("+I[e, s, e, e, s]"));
  }

  @Test
  void testJsonValueConversionFailure() {
    // A RETURNING-type mismatch fails the query (Flink raises an uncaught
    // ClassCastException outside the extraction try/catch), so the job fails
    // instead of emitting a NULL row.
    List<Row> rows = Arrays.asList(Row.of(1, "{\"s\":\"not-a-number\"}"));
    createSimpleBoundedValuesTable("tblJsonCF", "a int, j string", rows);
    tEnv().executeSql("CREATE TABLE printT (c INT) WITH ('connector' = 'print')");
    TableResult result =
        tEnv()
            .executeSql(
                "insert into printT select JSON_VALUE(j, '$.s' RETURNING INT NULL ON ERROR) "
                    + "from tblJsonCF");
    assertThatThrownBy(
            () -> result.getJobClient().get().getJobExecutionResult().get(60, TimeUnit.SECONDS))
        .isInstanceOf(Exception.class);
    tEnv().executeSql("drop table if exists printT");
  }

  @Test
  void testJsonValueNullOnError() {
    List<Row> rows = Arrays.asList(Row.of(1, "{\"name\":\"alice\"}"));
    createSimpleBoundedValuesTable("tblJsonNOE", "a int, j string", rows);
    String query =
        "select JSON_VALUE(j, 'strict $.missing' NULL ON ERROR) from tblJsonNOE where a > 0";
    runAndCheck(query, Arrays.asList("+I[null]"));
  }
}
