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
package org.apache.gluten.functions

import org.apache.gluten.execution.{FilterExecTransformer, ProjectExecTransformer, TransformSupport}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.execution.SparkPlan

/**
 * Regression tests for apache/gluten#13251. Spark's `from_json` matches JSON object keys against
 * schema field names case-sensitively, whatever `spark.sql.caseSensitive` is. Every query is
 * compared with vanilla Spark, and `from_json` must be evaluated natively.
 */
class FromJsonCaseSensitivitySuite extends FunctionsValidateSuite {

  import testImplicits._

  private val caseSensitiveModes = Seq("false", "true")

  /** Plan nodes, including those inside whole-stage transformers. */
  private def allNodes(df: DataFrame): Seq[SparkPlan] = getExecutedPlan(df)

  private def hasFromJson(plan: SparkPlan): Boolean =
    plan.expressions.exists(_.find(_.isInstanceOf[JsonToStructs]).isDefined)

  /**
   * `from_json` must be evaluated by a native Project or Filter transformer and never by a Spark
   * operator. (A scan transformer only lists it among its pushed-down data filters.)
   */
  private def assertFromJsonNative(df: DataFrame): Unit = {
    val nodes = allNodes(df)
    val sparkSide = nodes.filter(n => hasFromJson(n) && !n.isInstanceOf[TransformSupport])
    assert(
      nodes.exists(
        n =>
          (n.isInstanceOf[ProjectExecTransformer] || n.isInstanceOf[FilterExecTransformer]) &&
            hasFromJson(n)),
      s"from_json is not evaluated by a native Project/Filter:\n${df.queryExecution.executedPlan}"
    )
    assert(
      sparkSide.isEmpty,
      s"from_json is evaluated by non-native operators ${sparkSide.map(_.nodeName)}:\n" +
        df.queryExecution.executedPlan
    )
  }

  private def withJsonTable(rows: Seq[String], view: String = "tbl")(f: => Unit): Unit = {
    withTempPath {
      path =>
        rows.zipWithIndex
          .map { case (txt, i) => (i, i % 2, txt) }
          .toDF("id", "grp", "txt")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView(view)
        f
    }
  }

  /**
   * Compares all columns of every row with vanilla Spark, then checks that `from_json` is native.
   * `runQueryAndCompare` is not enough on its own: for a query with a partial `ORDER BY` it only
   * compares the sorted columns, which would skip the `from_json` result column.
   */
  private def check(sql: String, expected: Option[Seq[Row]] = None): Unit = {
    var vanilla: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) { vanilla = spark.sql(sql).collect().toSeq }
    val df = runQueryAndCompare(sql)(assertFromJsonNative)
    val actual = df.collect().toSeq
    assert(
      actual.map(_.toString).sorted === vanilla.map(_.toString).sorted,
      s"Gluten and vanilla Spark rows differ for: $sql"
    )
    expected.foreach(e => assert(actual === e, sql))
  }

  // (name, schema, json, expected value of the struct column).
  private val singleRowCases: Seq[(String, String, String, Row)] = Seq(
    ("exact case", "Id STRING", """{"Id":"10"}""", Row("10")),
    ("lowercase key", "Id STRING", """{"id":"10"}""", Row(null)),
    ("uppercase key", "Id STRING", """{"ID":"10"}""", Row(null)),
    ("mixed-case key", "Id STRING", """{"iD":"10"}""", Row(null)),
    ("lowercase key, lowercase-only mismatch", "Value STRING", """{"value":"11"}""", Row(null)),
    ("uppercase key, lowercase schema", "id STRING", """{"ID":"10"}""", Row(null)),
    ("exact lowercase schema", "id STRING", """{"id":"10"}""", Row("10")),
    (
      "mixed-case schema, exact",
      "userId INT, UserName STRING",
      """{"userId":1,"UserName":"a"}""",
      Row(1, "a")),
    (
      "mixed-case schema, wrong case",
      "userId INT, UserName STRING",
      """{"userid":1,"username":"a"}""",
      Row(null, null)),
    (
      "one of two keys matches",
      "Id STRING, Value STRING",
      """{"Id":"10","value":"11"}""",
      Row("10", null)),
    (
      "keys differing only by case",
      "ID STRING, id STRING",
      """{"ID":"10","id":"20"}""",
      Row("10", "20")),
    (
      "keys differing only by case, one present",
      "ID STRING, id STRING",
      """{"id":"20"}""",
      Row(null, "20")),
    ("missing field", "Id STRING, Value STRING", """{"Id":"10"}""", Row("10", null)),
    ("null field", "Id STRING, Value STRING", """{"Id":null,"Value":"11"}""", Row(null, "11")),
    (
      "null field, wrong case",
      "Id STRING, Value STRING",
      """{"id":null,"Value":"11"}""",
      Row(null, "11")),
    (
      "nested exact",
      "Outer STRUCT<Inner: STRING>",
      """{"Outer":{"Inner":"10"}}""",
      Row(Row("10"))),
    (
      "nested wrong case, outer",
      "Outer STRUCT<Inner: STRING>",
      """{"outer":{"Inner":"10"}}""",
      Row(null)),
    (
      "nested wrong case, inner",
      "Outer STRUCT<Inner: STRING>",
      """{"Outer":{"inner":"10"}}""",
      Row(Row(null))),
    (
      "nested wrong case, both",
      "Outer STRUCT<Inner: STRING>",
      """{"outer":{"inner":"10"}}""",
      Row(null)),
    ("deeply nested", "A STRUCT<B: STRUCT<C: INT>>", """{"A":{"B":{"C":1}}}""", Row(Row(Row(1)))),
    (
      "deeply nested wrong case",
      "A STRUCT<B: STRUCT<C: INT>>",
      """{"A":{"B":{"c":1}}}""",
      Row(Row(Row(null)))),
    (
      "array of structs",
      "Arr ARRAY<STRUCT<K: INT>>",
      """{"Arr":[{"K":1},{"k":2},{"K":3}]}""",
      Row(Seq(Row(1), Row(null), Row(3)))),
    (
      "array of structs, wrong case",
      "Arr ARRAY<STRUCT<K: INT>>",
      """{"arr":[{"K":1}]}""",
      Row(null)),
    (
      "map values containing structs",
      "M MAP<STRING, STRUCT<K: INT>>",
      """{"M":{"a":{"K":1},"b":{"k":2}}}""",
      Row(Map("a" -> Row(1), "b" -> Row(null)))),
    (
      "map values containing structs, wrong case",
      "M MAP<STRING, STRUCT<K: INT>>",
      """{"m":{"a":{"K":1}}}""",
      Row(null))
  )

  caseSensitiveModes.foreach {
    caseSensitive =>
      singleRowCases.foreach {
        case (name, schema, json, expected) =>
          test(s"from_json key case: $name, caseSensitive=$caseSensitive") {
            withSQLConf("spark.sql.caseSensitive" -> caseSensitive) {
              withJsonTable(Seq(json)) {
                check(
                  s"select from_json(txt, '$schema') as r from tbl",
                  Some(Seq(Row(expected))))
              }
            }
          }
      }
  }

  test("from_json with a map root and struct values keeps the value field names") {
    caseSensitiveModes.foreach {
      caseSensitive =>
        withSQLConf("spark.sql.caseSensitive" -> caseSensitive) {
          withJsonTable(Seq("""{"a":{"K":1},"b":{"k":2}}""")) {
            check(
              "select from_json(txt, 'MAP<STRING, STRUCT<K: INT>>') as r from tbl",
              Some(Seq(Row(Map("a" -> Row(1), "b" -> Row(null))))))
          }
        }
    }
  }

  test("from_json with an array root and struct elements keeps the field names") {
    caseSensitiveModes.foreach {
      caseSensitive =>
        withSQLConf("spark.sql.caseSensitive" -> caseSensitive) {
          withJsonTable(Seq("""[{"K":1},{"k":2}]""")) {
            check(
              "select from_json(txt, 'ARRAY<STRUCT<K: INT>>') as r from tbl",
              Some(Seq(Row(Seq(Row(1), Row(null))))))
          }
        }
    }
  }

  // Rows exercising exact, lowercase, uppercase, mixed-case, missing, null and invalid input.
  private val mixedRows = Seq(
    """{"Id":"10","Value":"11"}""",
    """{"id":"10","value":"11"}""",
    """{"ID":"10","VALUE":"11"}""",
    """{"ID":"10","Value":"11"}""",
    """{"Id":"12"}""",
    """{"Id":null,"Value":"13"}""",
    """{"Id":"10","Value":"14"}""",
    null
  )

  private val parsed =
    "with parsed as (select id, grp, from_json(txt, 'Id STRING, Value STRING') as r from tbl) "

  caseSensitiveModes.foreach {
    caseSensitive =>
      def inMode(f: => Unit): Unit =
        withSQLConf("spark.sql.caseSensitive" -> caseSensitive) {
          withJsonTable(mixedRows)(f)
        }

      test(s"from_json projection, caseSensitive=$caseSensitive") {
        inMode {
          check("select id, from_json(txt, 'Id STRING, Value STRING') as r from tbl order by id")
          check(parsed + "select id, r.Id, r.Value from parsed order by id")
        }
      }

      test(s"from_json filter, caseSensitive=$caseSensitive") {
        inMode {
          check(parsed + "select id from parsed where r.Id = '10' order by id")
          check(parsed + "select id from parsed where r.Id is null order by id")
          check(parsed + "select id from parsed where r.Value = '11' and r.Id = '10' order by id")
          check(
            "select id from tbl where from_json(txt, 'Id STRING, Value STRING').Id = '10' " +
              "order by id")
        }
      }

      test(s"from_json join, caseSensitive=$caseSensitive") {
        inMode {
          withTempView("dim") {
            Seq(("10", "ten"), ("12", "twelve"), ("13", "thirteen"))
              .toDF("k", "name")
              .createOrReplaceTempView("dim")
            check(
              parsed + "select p.id, d.name from parsed p join dim d on p.r.Id = d.k " +
                "order by p.id")
            check(
              parsed + "select p.id, d.name from parsed p left join dim d on p.r.Id = d.k " +
                "order by p.id")
          }
        }
      }

      test(s"from_json group by and aggregation, caseSensitive=$caseSensitive") {
        inMode {
          check(parsed + "select r.Id, count(*) c from parsed group by r.Id order by r.Id")
          check(
            parsed + "select grp, count(r.Id) c, max(r.Value) m, min(r.Value) n " +
              "from parsed group by grp order by grp")
          check(parsed + "select count(*), count(r.Id), count(r.Value) from parsed")
          check(
            parsed + "select r, count(*) c from parsed group by r order by c, cast(r as string)")
        }
      }

      test(s"from_json order by, caseSensitive=$caseSensitive") {
        inMode {
          check(parsed + "select id, r from parsed order by r.Id, r.Value, id")
          check(parsed + "select id from parsed order by r.Value desc nulls last, id")
        }
      }

      test(s"from_json window, caseSensitive=$caseSensitive") {
        inMode {
          check(
            parsed + "select id, row_number() over (partition by r.Id order by id) rn, " +
              "count(r.Value) over (partition by grp order by id) c from parsed order by id")
        }
      }
  }
}
