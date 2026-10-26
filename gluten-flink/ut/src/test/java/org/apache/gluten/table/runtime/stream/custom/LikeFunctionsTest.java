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

class LikeFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testLike() {
    List<Row> rows = Arrays.asList(Row.of(1, "abc"), Row.of(2, "hello"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblLike", "a int, s string", rows);
    String query = "select s LIKE '%bc' from tblLike where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[false]", "+I[null]"));
  }

  @Test
  void testLikeEmptyPattern() {
    // A literal empty pattern matches every input (vacuous truth), aligned
    // with Flink's LikeCallGen -> SqlLikeChainChecker path.
    List<Row> rows = Arrays.asList(Row.of(1, "abc"), Row.of(2, "hello"), Row.of(3, null));
    createSimpleBoundedValuesTable("tblLikeEmpty", "a int, s string", rows);
    String query = "select s LIKE '' from tblLikeEmpty where a > 0";
    runAndCheck(query, Arrays.asList("+I[true]", "+I[true]", "+I[null]"));
  }
}
