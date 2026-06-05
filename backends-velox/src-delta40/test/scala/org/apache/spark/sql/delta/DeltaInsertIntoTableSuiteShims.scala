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
package org.apache.spark.sql.delta

object DeltaInsertIntoTableSuiteShims {
  private val isSpark41 = org.apache.spark.SPARK_VERSION.startsWith("4.1")

  val INSERT_INTO_TMP_VIEW_ERROR_MSG =
    if (isSpark41) {
      "[TABLE_OR_VIEW_NOT_FOUND]"
    } else {
      "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE]"
    }

  val INVALID_COLUMN_DEFAULT_VALUE_ERROR_MSG =
    if (isSpark41) {
      "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION"
    } else {
      "INVALID_DEFAULT_VALUE.NOT_CONSTANT"
    }
}
