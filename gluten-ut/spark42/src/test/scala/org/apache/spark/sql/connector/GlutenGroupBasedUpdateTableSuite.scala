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
package org.apache.spark.sql.connector

import org.apache.spark.SparkException
import org.apache.spark.sql.GlutenSQLTestsTrait

class GlutenGroupBasedUpdateTableSuite
  extends GroupBasedUpdateTableSuite
  with GlutenSQLTestsTrait {

  private def assertNotNullViolation(f: => Unit): Unit = {
    val exception = intercept[SparkException](f)
    val messages = Iterator
      .iterate[Throwable](exception)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage))

    assert(messages.exists(_.contains("Null value appeared in non-nullable field")))
  }

  testGluten("update with NOT NULL checks") {
    createAndInitTable(
      "pk INT NOT NULL, s STRUCT<n_i: INT NOT NULL, n_l: LONG>, dep STRING",
      """{ "pk": 1, "s": { "n_i": 1, "n_l": 11 }, "dep": "hr" }
        |{ "pk": 2, "s": { "n_i": 2, "n_l": 22 }, "dep": "software" }
        |{ "pk": 3, "s": { "n_i": 3, "n_l": 33 }, "dep": "hr" }
        |""".stripMargin
    )

    assertNotNullViolation {
      sql(s"UPDATE $tableNameAsString SET s = named_struct('n_i', null, 'n_l', -1L) WHERE pk = 1")
    }
  }
}
