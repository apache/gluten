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
package org.apache.spark.sql.execution

import org.apache.spark.SparkException
import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.internal.SQLConf.STORE_ANALYZED_PLAN_FOR_VIEW

class GlutenSimpleSQLViewSuite extends SimpleSQLViewSuite with GlutenSQLTestsTrait {
  import testImplicits._

  private def assertMissingFileError(f: => Unit): Unit = {
    val exception = intercept[SparkException](f)
    val messages = Iterator
      .iterate[Throwable](exception)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage))

    assert(messages.exists(_.contains("FILE_NOT_FOUND")))
  }

  testGluten("alter temporary view should follow current storeAnalyzedPlanForView config") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      withView("v1") {
        withSQLConf(STORE_ANALYZED_PLAN_FOR_VIEW.key -> "true") {
          sql("CREATE TEMPORARY VIEW v1 AS SELECT * FROM t")
          Seq(4, 6, 5).toDF("c1").write.mode("overwrite").format("parquet").saveAsTable("t")
          assertMissingFileError(sql("SELECT * FROM v1").collect())
        }

        withSQLConf(STORE_ANALYZED_PLAN_FOR_VIEW.key -> "false") {
          sql("ALTER VIEW v1 AS SELECT * FROM t")
          Seq(1, 3, 5).toDF("c1").write.mode("overwrite").format("parquet").saveAsTable("t")
          checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1), Row(3), Row(5)))
        }

        withSQLConf(STORE_ANALYZED_PLAN_FOR_VIEW.key -> "true") {
          sql("ALTER VIEW v1 AS SELECT * FROM t")
          Seq(2, 4, 6).toDF("c1").write.mode("overwrite").format("parquet").saveAsTable("t")
          assertMissingFileError(sql("SELECT * FROM v1").collect())
        }
      }
    }
  }
}
