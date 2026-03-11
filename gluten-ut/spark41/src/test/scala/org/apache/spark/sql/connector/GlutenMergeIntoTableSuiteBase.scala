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

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.internal.SQLConf

/**
 * A trait that provides Gluten-compatible cardinality error assertion for merge operations.
 *
 * In Gluten, SparkRuntimeException is wrapped inside GlutenException, so we need to check the
 * exception chain for the expected error message instead of matching the exact exception type.
 */
trait GlutenMergeIntoTableSuiteBase extends MergeIntoTableSuiteBase with GlutenSQLTestsTrait {

  import testImplicits._

  /** Helper method to find if any exception in the chain contains the expected message. */
  private def findInExceptionChain(e: Throwable, expectedMessage: String): Boolean = {
    var current: Throwable = e
    while (current != null) {
      if (current.getMessage != null && current.getMessage.contains(expectedMessage)) {
        return true
      }
      current = current.getCause
    }
    false
  }

  /**
   * Gluten-compatible version of assertCardinalityError. The original method expects
   * SparkRuntimeException directly, but Gluten wraps it in GlutenException.
   */
  protected def assertGlutenCardinalityError(query: String): Unit = {
    val e = intercept[Exception] {
      sql(query)
    }
    assert(
      findInExceptionChain(e, "ON search condition of the MERGE statement"),
      s"Expected cardinality violation error but got: ${e.getMessage}")
  }

  testGluten("merge cardinality check with conditional MATCHED clause (delete)") {
    withTempView("source") {
      createAndInitTable(
        "pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin
      )

      val sourceRows = Seq((1, 101, "support"), (1, 102, "support"), (2, 201, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      assertGlutenCardinalityError(
        s"""MERGE INTO $tableNameAsString AS t
           |USING source AS s
           |ON t.pk = s.pk
           |WHEN MATCHED AND s.salary = 101 THEN
           | DELETE
           |""".stripMargin
      )
    }
  }

  testGluten("merge cardinality check with small target and large source (broadcast enabled)") {
    withTempView("source") {
      createAndInitTable(
        "pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin
      )

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString) {
        assertGlutenCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin
        )

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  testGluten("merge cardinality check with small target and large source (broadcast disabled)") {
    withTempView("source") {
      createAndInitTable(
        "pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin
      )

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        assertGlutenCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin
        )

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  testGluten("merge cardinality check with small target and large source (shuffle hash enabled)") {
    withTempView("source") {
      createAndInitTable(
        "pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin
      )

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "false") {
        assertGlutenCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin
        )

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  testGluten("merge cardinality check without equality condition and only MATCHED clauses") {
    withTempView("source") {
      createAndInitTable(
        "pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin
      )

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        assertGlutenCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk > s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin
        )

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  testGluten("merge cardinality check without equality condition") {
    withTempView("source") {
      createAndInitTable(
        "pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin
      )

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        assertGlutenCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk > s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin
        )

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }
}
