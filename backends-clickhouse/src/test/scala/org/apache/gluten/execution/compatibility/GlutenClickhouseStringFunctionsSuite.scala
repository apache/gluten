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
package org.apache.gluten.execution.compatibility

import org.apache.gluten.execution.GlutenClickHouseWholeStageTransformerSuite

import org.apache.spark.SparkConf

class GlutenClickhouseStringFunctionsSuite extends GlutenClickHouseWholeStageTransformerSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "SNAPPY")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  test("GLUTEN-5821: trim_character support value from column.") {
    withTable("trim") {
      sql("create table trim(a String, b String) using parquet")
      sql("""
            |insert into trim values
            |  ('aba', 'a'),('bba', 'b'),('abcdef', 'abcd'), (null, '123'),('123', null)
            |""".stripMargin)

      val sql_str =
        s"""select
           |    trim(both b from a)
           |  from trim
          """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("GLUTEN-6989: rtrim support source column const") {
    withTable("trim") {
      sql("create table trim(trim_col String, src_col String) using parquet")
      sql("""
            |insert into trim values
            |  ('bAa', 'a'),('bba', 'b'),('abcdef', 'abcd'),
            |  (null, '123'),('123', null), ('', 'aaa'), ('bbb', '')
            |""".stripMargin)

      val sql0 = "select rtrim('aba', 'a') from trim order by src_col"
      val sql1 = "select rtrim(trim_col, src_col) from trim order by src_col"
      val sql2 = "select rtrim(trim_col, 'cCBbAa') from trim order by src_col"
      val sql3 = "select rtrim(trim_col, '') from trim order by src_col"
      val sql4 = "select rtrim('', 'AAA') from trim order by src_col"
      val sql5 = "select rtrim('', src_col) from trim order by src_col"
      val sql6 = "select rtrim('ab', src_col) from trim order by src_col"

      runQueryAndCompare(sql0) { _ => }
      runQueryAndCompare(sql1) { _ => }
      runQueryAndCompare(sql2) { _ => }
      runQueryAndCompare(sql3) { _ => }
      runQueryAndCompare(sql4) { _ => }
      runQueryAndCompare(sql5) { _ => }
      runQueryAndCompare(sql6) { _ => }

      // test other trim functions
      val sql7 = "SELECT trim(LEADING trim_col FROM src_col) from trim"
      val sql8 = "SELECT trim(LEADING trim_col FROM 'NSB') from trim"
      val sql9 = "SELECT trim(TRAILING trim_col FROM src_col) from trim"
      val sql10 = "SELECT trim(TRAILING trim_col FROM '') from trim"
      runQueryAndCompare(sql7) { _ => }
      runQueryAndCompare(sql8) { _ => }
      runQueryAndCompare(sql9) { _ => }
      runQueryAndCompare(sql10) { _ => }

    }
  }

  test("GLUTEN-5897: fix regexp_extract with bracket") {
    withTable("regexp_extract_bracket") {
      sql("create table regexp_extract_bracket(a String) using parquet")
      sql("""
            |insert into regexp_extract_bracket
            | values ('123.123abc-abc'),('123-LOW'),('123]abc-abc')
            |""".stripMargin)

      val sql_str =
        s"""select
           |    regexp_extract(a, '([0-9][[\\.][0-9]]*)', 1)
           |  , regexp_extract(a, '([0-9][[\\.][0-9]]*)', 1)
           |  , regexp_extract(a, '([0-9][[]]]*)', 1)
           |  from regexp_extract_bracket
          """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("GLUTEN-8325 $. missmatched") {
    withTable("regexp_string_end") {
      sql("create table regexp_string_end(a String) using parquet")
      sql("""
            |insert into regexp_string_end
            | values ('@abc'),('@abc\n'),('@abc\nsdd'), ('sfsd\n@abc'), ('sdfsdf\n@abc\n'),
            | ('sdfsdf\n@abc\nsdf'), ('sdfsdf@abc\nsdf\n'), ('sdfsdf@abc'), ('sdfsdf@abc\n')
            |""".stripMargin)
      runQueryAndCompare("""
                           |select
                           |regexp_extract(a, '@(.*?)($)', 1),
                           |regexp_extract(a, '@(.*?)(f|$)', 1),
                           |regexp_extract(a, '^@(.*?)(f|$)', 1)
                           |from regexp_string_end""".stripMargin) { _ => }

      runQueryAndCompare("""
                           |select
                           |regexp_extract(a, '@(.*)($)', 1),
                           |regexp_extract(a, '@(.*)(f|$)', 1),
                           |regexp_extract(a, '^@(.*?)(f|$)', 1)
                           |from regexp_string_end""".stripMargin) { _ => }
    }
  }

  test("replace") {
    val tableName = "replace_table"
    withTable(tableName) {
      sql(s"create table $tableName(src String, idx String, dest String) using parquet")
      sql(s"""
             |insert into $tableName values
             |  (null, null, null),
             |  ('1', '1', null),
             |  ('1', '1', '2'),
             |  ('1', null, '2'),
             |  ('1', '1', '3'),
             |  (null, '1', '2'),
             |  ('1', '', '3')
          """.stripMargin)

      val sql_str =
        s"""
           |select
           |    REPLACE(src, idx, dest),
           |    REPLACE(src, null, dest),
           |    REPLACE(null, null, dest),
           |    REPLACE(null, null, null),
           |    REPLACE(src, '1', null)
           |  from $tableName
      """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  ignoreSpark33OnlyCase("base64") {
    // fallback on Spark-352, see https://github.com/apache/spark/pull/47303
    val tableName = "base64_table"
    withTable(tableName) {
      sql(s"create table $tableName(data String) using parquet")
      sql(s"""
             |insert into $tableName values
             |  ("hello")
          """.stripMargin)

      val sql_str =
        s"""
           |select
           |    base64(data)
           |  from $tableName
      """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("unbase64") {
    val tableName = "unbase64_table"
    withTable(tableName) {
      sql(s"create table $tableName(data String) using parquet")
      sql(s"""
             |insert into $tableName values
             |  ("U3BhcmsgU1FM")
          """.stripMargin)

      val sql_str =
        s"""
           |select
           |    unbase64(data)
           |  from $tableName
      """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("GLUTEN-7621: fix repeat function reports an error when times is a negative number") {
    val tableName = "repeat_issue_7621"
    withTable(tableName) {
      sql(s"create table $tableName(data string ) using parquet")
      sql(s"""
             |insert into $tableName values
             |  ('-1'),('0'),('2')
            """.stripMargin)

      val sql_str =
        s"""
           |select
           |    repeat('1', data), data
           |  from $tableName
      """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

}
