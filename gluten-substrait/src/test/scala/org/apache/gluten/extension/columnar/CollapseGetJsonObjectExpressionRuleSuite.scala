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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class CollapseGetJsonObjectExpressionRuleSuite extends SharedSparkSession {

  private def checkExpression(expr: Expression, path: String): Boolean = {
    expr match {
      case g: GetJsonObject
          if g.path.isInstanceOf[Literal] && g.path.dataType.isInstanceOf[StringType] =>
        g.path.asInstanceOf[Literal].value.toString.equals(path) || g.children.exists(
          c => checkExpression(c, path))
      case _ =>
        if (expr.children.isEmpty) {
          false
        } else {
          expr.children.exists(c => checkExpression(c, path))
        }
    }
  }

  private def checkPlan(plan: LogicalPlan, path: String): Boolean = plan match {
    case p: Project =>
      p.projectList.exists(x => checkExpression(x, path)) || checkPlan(p.child, path)
    case f: Filter =>
      checkExpression(f.condition, path) || checkPlan(f.child, path)
    case _ =>
      if (plan.children.isEmpty) {
        false
      } else {
        plan.children.exists(c => checkPlan(c, path))
      }
  }

  /** Apply the rule and check if the given path exists in the resulting plan. */
  private def checkPathAfterRule(sql: String, path: String): Boolean = {
    val df = spark.sql(sql)
    val rule = CollapseGetJsonObjectExpressionRule(spark)
    val optimized = rule.apply(df.queryExecution.analyzed)
    checkPlan(optimized, path)
  }

  private def withJsonTestTable(f: => Unit): Unit = {
    val schema = StructType(
      Array(
        StructField("double_field1", DoubleType, true),
        StructField("int_field1", IntegerType, true),
        StructField("string_field1", StringType, true)
      ))
    val data = Seq(
      Row(1.025, 1, """{"a":"b"}"""),
      Row(1.035, 2, null),
      Row(1.045, 3, """{"1a":"b"}"""),
      Row(1.011, 4, """{"a 2":"b"}"""),
      Row(1.011, 5, """{"a_2":"b"}"""),
      Row(1.011, 5, """{"a":"b", "x":{"i":1}}"""),
      Row(1.011, 5, """{"a":"b", "x":{"i":2}}"""),
      Row(1.011, 5, """{"a":1, "x":{"i":2}}"""),
      Row(1.0, 5, """{"a":"{\"x\":5}"}"""),
      Row(1.0, 6, """{"a":{"y": 5, "z": {"m":1, "n": {"p": "k"}}}}"""),
      Row(1.0, 7, """{"a":[{"y": 5}, {"z":[{"m":1, "n":{"p":"k"}}]}]}""")
    )
    spark.createDataFrame(sparkContext.parallelize(data), schema)
      .createOrReplaceTempView("json_test")
    try {
      f
    } finally {
      spark.catalog.dropTempView("json_test")
    }
  }

  test("GLUTEN-8304: Optimize nested get_json_object") {
    withJsonTestTable {
      withSQLConf(GlutenConfig.ENABLE_COLLAPSE_GET_JSON_OBJECT.key -> "true") {
        // Basic two-level nesting: GJO(GJO(col, '$.a'), '$.y') => GJO(col, '$.a.y')
        assert(
          checkPathAfterRule(
            "select get_json_object(get_json_object(string_field1, '$.a'), '$.y') " +
              " from json_test where int_field1 = 6",
            "$.a.y"))

        // Bracket notation with quoted keys: $['a'] and $['y']
        // Both Spark and Velox/CH support quoted bracket notation $['key'].
        assert(
          checkPathAfterRule(
            "select get_json_object(get_json_object(string_field1, " +
              "\"$['a']\"), \"$['y']\") from json_test where int_field1 = 6",
            "$['a']['y']"))

        // Three-level nesting
        assert(
          checkPathAfterRule(
            "select get_json_object(get_json_object(get_json_object(string_field1, " +
              "'$.a'), '$.y'), '$.z') from json_test where int_field1 = 6",
            "$.a.y.z"))

        // Non-literal path in middle GJO: GJO(GJO(GJO(col, '$.a'), col), '$.z')
        // The inner GJO(col, '$.a') can't merge with the non-literal path,
        // so '$.a' and '$.z' should remain separate.
        val sql4 =
          "select get_json_object(get_json_object(get_json_object(string_field1, '$.a')," +
            " string_field1), '$.z') from json_test where int_field1 = 6"
        assert(checkPathAfterRule(sql4, "$.a"))
        assert(checkPathAfterRule(sql4, "$.z"))

        // Non-literal path in innermost GJO
        assert(
          checkPathAfterRule(
            "select get_json_object(get_json_object(get_json_object(string_field1, " +
              " string_field1), '$.a'), '$.z') from json_test where int_field1 = 6",
            "$.a.z"
          ))

        // Non-GJO expression (substring) as input to innermost GJO
        assert(
          checkPathAfterRule(
            "select get_json_object(get_json_object(get_json_object(" +
              " substring(string_field1, 10), '$.a'), '$.z'), string_field1) " +
              " from json_test where int_field1 = 6",
            "$.a.z"
          ))

        // Array index in path
        assert(
          checkPathAfterRule(
            "select get_json_object(get_json_object(string_field1, '$.a[0]'), '$.y') " +
              " from json_test where int_field1 = 7",
            "$.a[0].y"))

        // Multi-level array index
        assert(
          checkPathAfterRule(
            "select get_json_object(get_json_object(get_json_object(string_field1, " +
              " '$.a[1]'), '$.z[1]'), '$.n') from json_test where int_field1 = 7",
            "$.a[1].z[1].n"
          ))

        // GJO in Filter condition
        assert(
          checkPathAfterRule(
            "select * from json_test where " +
              " get_json_object(get_json_object(get_json_object(string_field1, '$.a'), " +
              "'$.y'), '$.z') != null",
            "$.a.y.z"))
      }
    }
  }

  test("CollapseGetJsonObject should not merge paths across non-GJO expressions") {
    withJsonTestTable {
      withSQLConf(GlutenConfig.ENABLE_COLLAPSE_GET_JSON_OBJECT.key -> "true") {
        // GJO(substring(GJO(col, '$.a'), 1), '$.y') should NOT merge paths
        assert(
          !checkPathAfterRule(
            "select get_json_object(" +
              "substring(get_json_object(string_field1, '$.a'), 1)," +
              " '$.y') from json_test where int_field1 = 6",
            "$.a.y"),
          "Path $.a should NOT merge with $.y across substring"
        )

        // GJO(concat(GJO(col, '$.a'), ''), '$.y') should NOT merge paths
        assert(
          !checkPathAfterRule(
            "select get_json_object(" +
              "concat(get_json_object(string_field1, '$.a'), '')," +
              " '$.y') from json_test where int_field1 = 6",
            "$.a.y"),
          "Path $.a should NOT merge with $.y across concat"
        )

        // GJO(substring(GJO(GJO(col, '$.a'), '$.z'), 1), '$.m')
        // Inner GJOs should still be collapsed to '$.a.z',
        // but outer '$.m' must NOT merge across substring.
        val sql3 =
          "select get_json_object(" +
            "substring(get_json_object(get_json_object(string_field1, '$.a'), '$.z'), 1)," +
            " '$.m') from json_test where int_field1 = 6"
        assert(
          checkPathAfterRule(sql3, "$.a.z"),
          "Inner nested GJOs should be collapsed to $.a.z")
        assert(
          !checkPathAfterRule(sql3, "$.a.z.m"),
          "Outer path $.m should NOT merge across substring to form $.a.z.m")
      }
    }
  }
}
