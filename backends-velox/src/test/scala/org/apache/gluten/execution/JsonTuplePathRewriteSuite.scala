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
package org.apache.gluten.execution

import org.apache.spark.sql.Row

class JsonTuplePathRewriteSuite extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  test("Test json_tuple with get_json_object fallback") {
    withTempView("t") {
      Seq[(String)](
        "{\"k\":\"v\",\"a.b\":\"dot_value\",\"x\":\"1\",\"y\":\"2\"}",
        "{\"k\":\"v2\",\"a.b\":\"dot_value2\",\"x\":\"3\",\"y\":\"4\"}",
        null
      ).toDF("json_field")
        .createOrReplaceTempView("t")
      withSQLConf("spark.gluten.expression.blacklist" -> "get_json_object") {
        // Basic single key extraction
        checkAnswer(
          sql("SELECT fk from t lateral view json_tuple(json_field, 'k') as fk"),
          Seq(Row("v"), Row("v2"), Row(null))
        )

        // Key containing dot (core scenario for bracket notation)
        checkAnswer(
          sql("SELECT fk from t lateral view json_tuple(json_field, 'a.b') as fk"),
          Seq(Row("dot_value"), Row("dot_value2"), Row(null))
        )

        // Multiple keys extraction
        checkAnswer(
          sql(
            "SELECT fx, fy from t lateral view json_tuple(json_field, 'x', 'y') as fx, fy"),
          Seq(Row("1", "2"), Row("3", "4"), Row(null, null))
        )

        // Non-existent key returns null
        checkAnswer(
          sql(
            "SELECT fk from t lateral view json_tuple(json_field, 'nonexistent') as fk"),
          Seq(Row(null), Row(null), Row(null))
        )

        // Mix of existing and non-existing keys
        checkAnswer(
          sql(
            """SELECT fk, fm from t
              |lateral view json_tuple(json_field, 'k', 'missing') as fk, fm""".stripMargin),
          Seq(Row("v", null), Row("v2", null), Row(null, null))
        )
      }
    }
  }
}
