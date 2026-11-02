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
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.connector.catalog.Aborted

/**
 * Gluten versions of [[UpdateTableSuiteBase]] tests that expect Spark's
 * `SparkRuntimeException(NOT_NULL_ASSERT_VIOLATION)`. Velox `assert_not_null` throws a
 * `VeloxUserError` instead, which fails the query with a `SparkException`.
 */
trait GlutenUpdateTableSuiteBase extends UpdateTableSuiteBase with GlutenSQLTestsBaseTrait {

  protected def assertNotNullViolation(f: => Unit): Unit = {
    val exception = intercept[SparkException](f)
    val messages = Iterator
      .iterate[Throwable](exception)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage))

    assert(messages.exists(_.contains("Null value appeared in non-nullable field")))
  }

  testGluten("update with constraint violation and transactional checks") {
    createAndInitTable(
      "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin
    )

    assertNotNullViolation {
      executeTransaction {
        sql(s"""UPDATE $tableNameAsString
               |SET pk = NULL
               |WHERE dep = 'hr'
               |""".stripMargin) // NULL violates NOT NULL constraint
      }
    }

    assert(catalog.lastTransaction.currentState == Aborted)
    assert(catalog.lastTransaction.isClosed)
  }
}
