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
package org.apache.spark.sql

import org.apache.spark.SparkException

class GlutenResolveDefaultColumnsSuite
  extends ResolveDefaultColumnsSuite
  with GlutenSQLTestsTrait {

  // The explicit DEFAULT value is checked by the native project, so the INSERT fails with Velox's
  // error wrapped in a SparkException instead of Spark's EXCEED_LIMIT_LENGTH SparkRuntimeException.
  testGluten(
    "SPARK-57187: non-foldable default exceeding CHAR/VARCHAR length fails at INSERT time " +
      "(explicit DEFAULT keyword)") {
    Seq("CHAR", "VARCHAR").foreach {
      typeName =>
        withTable("t") {
          sql(s"CREATE TABLE t(i INT, s $typeName(1) DEFAULT current_user()) USING parquet")
          val e = intercept[SparkException](sql("INSERT INTO t VALUES (1, DEFAULT)"))
          assert(e.getMessage.contains("Exceeds allowed length limitation: 1"))
        }
    }
  }
}
