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
package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkFunSuite

class GlutenExecutionDataSourceSuite extends SparkFunSuite {
  test("sort executions by description instead of fallback plan") {
    val executions = Seq(
      new GlutenSQLExecutionUIData(1L, "z-query", 0, 0, "a-plan", Seq.empty),
      new GlutenSQLExecutionUIData(2L, "a-query", 0, 0, "z-plan", Seq.empty))

    val ascending = new GlutenExecutionDataSource(executions, 10, "Description", desc = false)
    assert(ascending.sliceData(0, 10).map(_.executionUIData.description) ===
      Seq("a-query", "z-query"))

    val descending = new GlutenExecutionDataSource(executions, 10, "Description", desc = true)
    assert(descending.sliceData(0, 10).map(_.executionUIData.description) ===
      Seq("z-query", "a-query"))
  }
}
