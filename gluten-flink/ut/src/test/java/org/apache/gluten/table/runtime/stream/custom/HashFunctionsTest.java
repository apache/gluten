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

class HashFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testMd5() {
    List<Row> rows =
        Arrays.asList(Row.of(1, "test"), Row.of(2, "abc"), Row.of(3, "你好"), Row.of(4, null));
    createSimpleBoundedValuesTable("tblMd5", "a int, s string", rows);
    String query = "select MD5(s) from tblMd5 where a > 0";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[098f6bcd4621d373cade4e832627b4f6]",
            "+I[900150983cd24fb0d6963f7d28e17f72]",
            "+I[7eca689f0d3389d9dea66ae112e5cfd7]",
            "+I[null]"));
  }

  @Test
  void testSha1() {
    List<Row> rows =
        Arrays.asList(Row.of(1, "test"), Row.of(2, "abc"), Row.of(3, "你好"), Row.of(4, null));
    createSimpleBoundedValuesTable("tblSha1", "a int, s string", rows);
    String query = "select SHA1(s) from tblSha1 where a > 0";
    runAndCheck(
        query,
        Arrays.asList(
            "+I[a94a8fe5ccb19ba61c4c0873d391e987982fbbd3]",
            "+I[a9993e364706816aba3e25717850c26c9cd0d89d]",
            "+I[440ee0853ad1e99f962b63e459ef992d7c211722]",
            "+I[null]"));
  }
}
