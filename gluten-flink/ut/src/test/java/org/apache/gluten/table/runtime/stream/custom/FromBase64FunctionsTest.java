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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FromBase64FunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testFromBase64() {
    List<Row> rows = Arrays.asList(Row.of(1, "dGVzdA=="), Row.of(2, "5L2g5aW9"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblB64", "a int, b64 string", rows);
    String query = "select FROM_BASE64(b64) from tblB64 where a > 0";
    runAndCheck(query, Arrays.asList("+I[test]", "+I[你好]", "+I[null]"));
  }

  @Test
  void testFromBase64UnpaddedAndEmpty() {
    List<Row> rows = Arrays.asList(Row.of("dGVzdA"), Row.of("YQ"));
    createSimpleBoundedValuesTable("tblB64Unpadded", "b64 string", rows);
    String query = "select FROM_BASE64(b64) from tblB64Unpadded";
    runAndCheck(query, Arrays.asList("+I[test]", "+I[a]"));

    List<Row> rows2 = Arrays.asList(Row.of(1));
    createSimpleBoundedValuesTable("tblB64Empty", "a int", rows2);
    String query2 = "select FROM_BASE64('') from tblB64Empty where a > 0";
    runAndCheck(query2, Arrays.asList("+I[]"));
  }

  @Test
  void testFromBase64InvalidInput() {
    // Invalid characters error out on both sides (velox raises a user error,
    // Flink's Base64.getDecoder() throws), so the job fails rather than emitting.
    List<Row> rows = Arrays.asList(Row.of("!@#$"));
    createSimpleBoundedValuesTable("tblB64Invalid", "b64 string", rows);
    tEnv().executeSql("CREATE TABLE printT (b64 VARCHAR) WITH ('connector' = 'print')");
    TableResult result =
        tEnv().executeSql("insert into printT select FROM_BASE64(b64) from tblB64Invalid");
    assertThatThrownBy(
            () -> result.getJobClient().get().getJobExecutionResult().get(60, TimeUnit.SECONDS))
        .isInstanceOf(Exception.class);
    tEnv().executeSql("drop table if exists printT");
  }
}
