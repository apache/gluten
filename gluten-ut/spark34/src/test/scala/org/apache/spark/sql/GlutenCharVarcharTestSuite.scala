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
import org.apache.spark.sql.internal.SQLConf

class GlutenFileSourceCharVarcharTestSuite
  extends FileSourceCharVarcharTestSuite
  with GlutenSQLTestsTrait {
  private def testTableWrite(f: String => Unit): Unit = {
    withTable("t")(f("char"))
    withTable("t")(f("varchar"))
  }

  private val ERROR_MESSAGE =
    "Exceeds char/varchar type length limitation: 5"

  testGluten("length check for input string values: partitioned columns") {
    // DS V2 doesn't support partitioned table.
    if (!conf.contains(SQLConf.DEFAULT_CATALOG.key)) {
      val tableName = "t"
      testTableWrite {
        typeName =>
          sql(s"CREATE TABLE $tableName(i INT, c $typeName(5)) USING $format PARTITIONED BY (c)")
          sql(s"INSERT INTO $tableName VALUES (1, null)")
          checkAnswer(spark.table(tableName), Row(1, null))
          val e = intercept[SparkException](sql(s"INSERT INTO $tableName VALUES (1, '123456')"))
          assert(e.getCause.getMessage.contains(ERROR_MESSAGE))
      }
    }
  }

  testGluten("char/varchar type values length check: partitioned columns of other types") {
    val tableName = "t"
    Seq("CHAR(5)", "VARCHAR(5)").foreach {
      typ =>
        withTable(tableName) {
          sql(s"CREATE TABLE $tableName(i STRING, c $typ) USING $format PARTITIONED BY (c)")
          Seq(1, 10, 100, 1000, 10000).foreach {
            v =>
              sql(s"INSERT OVERWRITE $tableName VALUES ('1', $v)")
              checkPlainResult(spark.table(tableName), typ, v.toString)
              sql(s"ALTER TABLE $tableName DROP PARTITION(c=$v)")
              checkAnswer(spark.table(tableName), Nil)
          }

          val e1 =
            intercept[SparkException](sql(s"INSERT OVERWRITE $tableName VALUES ('1', 100000)"))
          assert(e1.getCause.getMessage.contains(ERROR_MESSAGE))

          val e2 = intercept[RuntimeException](sql("ALTER TABLE t DROP PARTITION(c=100000)"))
          assert(e2.getMessage.contains(ERROR_MESSAGE))
        }
    }
  }
}

class GlutenDSV2CharVarcharTestSuite extends DSV2CharVarcharTestSuite with GlutenSQLTestsTrait {}
