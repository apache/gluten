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

import org.apache.gluten.execution.GlutenPlan

class GlutenJsonFunctionsSuite extends JsonFunctionsSuite with GlutenSQLTestsTrait {
  import testImplicits._

  testGluten("SPARK-42782: Hive compatibility check for get_json_object ") {
    val book0 = "{\"author\":\"Nigel Rees\",\"title\":\"Sayings of the Century\"" +
      ",\"category\":\"reference\",\"price\":8.95}"
    val backet0 = "[1,2,{\"b\":\"y\",\"a\":\"x\"}]"
    val backet = "[" + backet0 + ",[3,4],[5,6]]"
    val backetFlat = backet0.substring(0, backet0.length() - 1) + ",3,4,5,6]"

    val book = "[" + book0 + ",{\"author\":\"Herman Melville\",\"title\":\"Moby Dick\"," +
      "\"category\":\"fiction\",\"price\":8.99" +
      ",\"isbn\":\"0-553-21311-3\"},{\"author\":\"J. R. R. Tolkien\"" +
      ",\"title\":\"The Lord of the Rings\",\"category\":\"fiction\"" +
      ",\"reader\":[{\"age\":25,\"name\":\"bob\"},{\"age\":26,\"name\":\"jack\"}]" +
      ",\"price\":22.99,\"isbn\":\"0-395-19395-8\"}]"

    val json = "{\"store\":{\"fruit\":[{\"weight\":8,\"type\":\"apple\"}," +
      "{\"weight\":9,\"type\":\"pear\"}],\"basket\":" + backet + ",\"book\":" + book +
      ",\"bicycle\":{\"price\":19.95,\"color\":\"red\"}}" +
      ",\"email\":\"amy@only_for_json_udf_test.net\"" +
      ",\"owner\":\"amy\",\"zip code\":\"94025\",\"fb:testid\":\"1234\"}"

    // Basic test
    runTest(json, "$.owner", "amy")
    runTest(json, "$.store.bicycle", "{\"price\":19.95,\"color\":\"red\"}")
    runTest(json, "$.store.book", book)
    runTest(json, "$.store.book[0]", book0)
    // runTest(json, "$.store.book[*]", book) - not supported in velox
    runTest(json, "$.store.book[0].category", "reference")
    // runTest(json, "$.store.book[*].category",
    // "[\"reference\",\"fiction\",\"fiction\"]") - not supported in velox
    // runTest(json, "$.store.book[*].reader[0].age", "25") - not supported in velox
    // runTest(json, "$.store.book[*].reader[*].age", "[25,26]") - not supported in velox
    runTest(json, "$.store.basket[0][1]", "2")
    // runTest(json, "$.store.basket[*]", backet) - not supported in velox
    // runTest(json, "$.store.basket[*][0]", "[1,3,5]") - not supported in velox
    // runTest(json, "$.store.basket[0][*]", backet0) - not supported in velox
    // runTest(json, "$.store.basket[*][*]", backetFlat) - not supported in velox
    runTest(json, "$.store.basket[0][2].b", "y")
    // runTest(json, "$.store.basket[0][*].b", "[\"y\"]") - not supported in velox
    runTest(json, "$.non_exist_key", null)
    runTest(json, "$.store.book[10]", null)
    runTest(json, "$.store.book[0].non_exist_key", null)
    // runTest(json, "$.store.basket[*].non_exist_key", null) - not supported in velox
    // runTest(json, "$.store.basket[0][*].non_exist_key", null) - not supported in velox
    // runTest(json, "$.store.basket[*][*].non_exist_key", null) - not supported in velox
    runTest(json, "$.zip code", "94025")
    runTest(json, "$.fb:testid", "1234")
    // runTest("{\"a\":\"b\nc\"}", "$.a", "b\nc") - not supported in velox

    // Test root array
    runTest("[1,2,3]", "$[0]", "1")
    runTest("[1,2,3]", "$.[0]", null) // Not supported in spark and velox
    runTest("[1,2,3]", "$.[1]", null) // Not supported in spark and velox
    runTest("[1,2,3]", "$[1]", "2")

    runTest("[1,2,3]", "$[3]", null)
    runTest("[1,2,3]", "$.[*]", null) // Not supported in spark and velox
    // runTest("[1,2,3]", "$[*]", "[1,2,3]") - not supported in velox
    // runTest("[1,2,3]", "$", "[1,2,3]") - not supported in velox
    runTest("[{\"k1\":\"v1\"},{\"k2\":\"v2\"},{\"k3\":\"v3\"}]", "$[2]", "{\"k3\":\"v3\"}")
    runTest("[{\"k1\":\"v1\"},{\"k2\":\"v2\"},{\"k3\":\"v3\"}]", "$[2].k3", "v3")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0].k11[1]", "2")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0].k11", "[1,2,3]")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0]", "{\"k11\":[1,2,3]}")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1", "[{\"k11\":[1,2,3]}]")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0]", "{\"k1\":[{\"k11\":[1,2,3]}]}")
    runTest("[[1,2,3],[4,5,6],[7,8,9]]", "$[1]", "[4,5,6]")
    runTest("[[1,2,3],[4,5,6],[7,8,9]]", "$[1][0]", "4")
    runTest("[\"a\",\"b\"]", "$[1]", "b")
    runTest("[[\"a\",\"b\"]]", "$[0][1]", "b")

    runTest("[1,2,3]", "[0]", null)
    runTest("[1,2,3]", "$0", null)
    runTest("[1,2,3]", "0", null)
    runTest("[1,2,3]", "$.", null)

    runTest("[1,2,3]", "$", "[1,2,3]")
    runTest("{\"a\":4}", "$", "{\"a\":4}")

    def runTest(json: String, path: String, exp: String): Unit = {
      checkAnswer(Seq(json).toDF().selectExpr(s"get_json_object(value, '$path')"), Row(exp))
    }
  }

  testGluten("function get_json_object - Codegen Support") {
    withTempView("GetJsonObjectTable") {
      val data = Seq(("1", """{"f1": "value1", "f5": 5.23}""")).toDF("key", "jstring")
      data.createOrReplaceTempView("GetJsonObjectTable")
      val df = sql("SELECT key, get_json_object(jstring, '$.f1') FROM GetJsonObjectTable")
      val plan = df.queryExecution.executedPlan
      assert(plan.isInstanceOf[GlutenPlan])
      checkAnswer(df, Seq(Row("1", "value1")))
    }
  }
  testGluten("function get_json_object - path is null") {
    val data = Seq(("""{"name": "alice", "age": 5}""", "")).toDF("a", "b")
    val df = data.selectExpr("get_json_object(a, null)")
    val plan = df.queryExecution.executedPlan
    assert(plan.isInstanceOf[GlutenPlan])
    checkAnswer(df, Row(null))
  }

  testGluten("function get_json_object - json is null") {
    val data = Seq(("""{"name": "alice", "age": 5}""", "")).toDF("a", "b")
    val df = data.selectExpr("get_json_object(null, '$.name')")
    val plan = df.queryExecution.executedPlan
    assert(plan.isInstanceOf[GlutenPlan])
    checkAnswer(df, Row(null))
  }
}
