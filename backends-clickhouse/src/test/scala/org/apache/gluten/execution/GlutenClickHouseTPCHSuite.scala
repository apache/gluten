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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenTestUtils, Row}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{DecimalType, StructType}

class GlutenClickHouseTPCHSuite extends MergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  final override val testCases: Seq[Int] = Seq(
    4, 6, 9, 10, 11, 12, 13, 15, 16, 19, 20, 21, 22
  )
  final override val testCasesWithConfig: Map[Int, Seq[(String, String)]] =
    Map(
      7 -> Seq(
        ("spark.sql.shuffle.partitions", "1"),
        ("spark.sql.autoBroadcastJoinThreshold", "-1")),
      8 -> Seq(
        ("spark.sql.shuffle.partitions", "1"),
        ("spark.sql.autoBroadcastJoinThreshold", "-1")),
      14 -> Seq(
        ("spark.sql.shuffle.partitions", "1"),
        ("spark.sql.autoBroadcastJoinThreshold", "-1")),
      17 -> Seq(("spark.shuffle.sort.bypassMergeThreshold", "2")),
      18 -> Seq(("spark.shuffle.sort.bypassMergeThreshold", "2"))
    )
  setupTestCase()

  test("TPCH Q1") {
    customCheck(1) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 1)

        assert(scanExec.head.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))

        val sortExec = df.queryExecution.executedPlan.collect {
          case sortExec: SortExecTransformer => sortExec
        }
        assert(sortExec.size == 1)
    }
  }

  test("TPCH Q2") {
    customCheck(2) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 8)
    }
  }

  test("TPCH Q3") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      customCheck(3) {
        df =>
          val shjBuildLeft = df.queryExecution.executedPlan.collect {
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildLeft => shj
          }
          assert(shjBuildLeft.size == 1)
          val shjBuildRight = df.queryExecution.executedPlan.collect {
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildRight => shj
          }
          assert(shjBuildRight.size == 1)
      }
    }
  }

  test("TPCH Q5") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      customCheck(5) {
        df =>
          val bhjRes = df.queryExecution.executedPlan.collect {
            case bhj: BroadcastHashJoinExecTransformerBase => bhj
          }
          assert(bhjRes.isEmpty)
      }
    }
  }

  test("test 'select count(*) from table'") {
    // currently, it can't support 'select count(*)' for non-partitioned tables.
    val result = runSql("""
                          |select count(*) from lineitem
                          |""".stripMargin) { _ => }
  }

  test("test 'select count(*)'") {
    val result = runSql("""
                          |select count(*) from lineitem
                          |where l_quantity < 24
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 275436L)
  }

  test("test 'select global/local limit'") {
    val result = runSql("""
                          |select * from (
                          | select * from lineitem limit 10
                          |) where l_suppkey != 0 limit 100;
                          |""".stripMargin) { _ => }
    assert(result.size == 10)
  }

  test("test 'function explode(array)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, explode(array(l_returnflag, l_linestatus)),
                          |  l_suppkey from lineitem);
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 1201144L)
  }

  test("test 'function posexplode(array)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, posexplode(array(l_returnflag, l_linestatus)),
                          |  l_suppkey from lineitem);
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 1201144L)
  }

  test("test 'lateral view explode(array)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, l_suppkey, col1, col2 from lineitem
                          |  lateral view explode(array(l_returnflag, l_linestatus)) as col1
                          |  lateral view explode(array(l_shipmode, l_comment)) as col2)
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 2402288L)
  }

  test("test 'lateral view posexplode(array)'") {
    val result =
      runSql("""
               |select count(*) from (
               |  select l_orderkey, l_suppkey, pos1, col1, pos2, col2 from lineitem
               |  lateral view posexplode(array(l_returnflag, l_linestatus)) as pos1, col1
               |  lateral view posexplode(array(l_shipmode, l_comment)) as pos2, col2)
               |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 2402288L)
  }

  test("test 'function explode(map)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey,
                          |    explode(map('returnflag', l_returnflag, 'linestatus', l_linestatus)),
                          |    l_suppkey from lineitem);
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 1201144L)
  }

  test("test 'function posexplode(map)'") {
    val result =
      runSql("""
               |select count(*) from (
               |  select l_orderkey,
               |    posexplode(map('returnflag', l_returnflag, 'linestatus', l_linestatus)),
               |    l_suppkey from lineitem);
               |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 1201144L)
  }

  test("test 'lateral view explode(map)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, l_suppkey, k1, v1, k2, v2 from lineitem
                          |  lateral view
                          |    explode(map('returnflag', l_returnflag, 'linestatus', l_linestatus))
                          |    as k1, v1
                          |  lateral view
                          |    explode(map('orderkey', l_orderkey, 'partkey', l_partkey))
                          |    as k2, v2
                          |)
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 2402288L)
  }

  test("test 'lateral view posexplode(map)'") {
    val result =
      runSql("""
               |select count(*) from (
               |  select l_orderkey, l_suppkey, p1, k1, v1, p2, k2, v2 from lineitem
               |  lateral view
               |    posexplode(map('returnflag', l_returnflag, 'linestatus', l_linestatus))
               |    as p1, k1, v1
               |  lateral view
               |    posexplode(map('orderkey', l_orderkey, 'partkey', l_partkey))
               |    as p2, k2, v2
               |)
               |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 2402288L)
  }

  test("test 'select count(1)'") {
    val result = runSql("""
                          |select count(1) from lineitem
                          |where l_quantity < 20
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 227302L)
  }

  test("test 'select count(1)' with empty columns to read") {
    val result = runSql("""
                          |select count(1) from lineitem
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 600572L)
  }

  test("test 'select count(*)' with empty columns to read") {
    val result = runSql("""
                          |select count(*) from lineitem
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 600572L)
  }

  test("test 'select sum(2)' with empty columns to read") {
    val result = runSql("""
                          |select sum(2) from lineitem
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 1201144L)
  }

  test("test 'select 1' with empty columns to read") {
    val result = runSql("""
                          |select 1 from lineitem limit 2
                          |""".stripMargin) { _ => }
    assert(result.size == 2)
    assert(result.head.getInt(0) == 1 && result(1).getInt(0) == 1)
  }

  test("test 'order by'") {
    val result = runSql("""
                          |select l_suppkey from lineitem
                          |where l_orderkey < 3 order by l_partkey / 2
                          |""".stripMargin) { _ => }
    assert(result.size == 7)
    val expected =
      Seq(Row(465.0), Row(67.0), Row(160.0), Row(371.0), Row(732.0), Row(138.0), Row(785.0))
    GlutenTestUtils.compareAnswers(result, expected)
  }

  test("test 'order by' two keys") {
    runSql("""
             |select n_nationkey, n_name, n_regionkey from nation
             |order by n_name, n_regionkey + 1
             |""".stripMargin) {
      df =>
        val sortExec = df.queryExecution.executedPlan.collect {
          case sortExec: SortExecTransformer => sortExec
        }
        assert(sortExec.size == 1)
        val result = df.take(3)
        val expected =
          Seq(Row(0, "ALGERIA", 0), Row(1, "ARGENTINA", 1), Row(2, "BRAZIL", 1))
        GlutenTestUtils.compareAnswers(result, expected)
    }
  }

  test("test 'order by limit'") {
    runSql("""
             |select n_nationkey from nation order by n_nationkey limit 5
             |""".stripMargin) {
      df =>
        val sortExec = df.queryExecution.executedPlan.collect {
          case sortExec: TakeOrderedAndProjectExecTransformer => sortExec
        }
        assert(sortExec.size == 1)
        val result = df.collect()
        val expectedResult = Seq(Row(0), Row(1), Row(2), Row(3), Row(4))
        GlutenTestUtils.compareAnswers(result, expectedResult)
    }
  }

  test("test 'function space'") {
    val result = runSql("""
                          | select
                          | space(3),
                          | space(0),
                          | space(NULL),
                          | space(3/3.00f)
                          | from lineitem limit 1
                          |""".stripMargin) { _ => }
    assert(result.head.getString(0).equals("   "))
    assert(result.head.getString(1).equals(""))
    assert(result.head.getString(2) == null)
    assert(result.head.getString(3).equals(" "))
  }

  test("test 'ISSUE https://github.com/Kyligence/ClickHouse/issues/225'") {
    val result = runSql("""
                          |SELECT
                          |cast(1.11 as decimal(20, 3)),
                          |cast(1.123456789 as decimal(20,9)),
                          |cast(123456789.123456789 as decimal(30,9)),
                          |cast(1.12345678901234567890123456789 as decimal(38,29)),
                          |cast(123456789.123456789012345678901234567 as decimal(38,27)),
                          |cast(123456789.123456789012345678901234567 as decimal(38,28)) + 0.1,
                          |array(cast(123456789.123456789012345678901234567 as decimal(38,27)))
                          |FROM lineitem
                          |WHERE l_shipdate <= date'1998-09-02' - interval 1 day limit 1
                          |""".stripMargin) { _ => }
    assert(result.length == 1)
    val expectedResult = Seq(
      Row(
        new java.math.BigDecimal("1.110"),
        new java.math.BigDecimal("1.123456789"),
        new java.math.BigDecimal("123456789.123456789"),
        new java.math.BigDecimal("1.12345678901234567890123456789"),
        new java.math.BigDecimal("123456789.123456789012345678901234567"),
        new java.math.BigDecimal("123456789.223456789012345678901234567"),
        Seq(new java.math.BigDecimal("123456789.123456789012345678901234567"))
      ))
    GlutenTestUtils.compareAnswers(result, expectedResult)
  }

  test("test decimal128") {
    val struct = Row(new java.math.BigDecimal("123456789.123456789012345678901234567"))
    val data = sparkContext.parallelize(
      Seq(
        Row(new java.math.BigDecimal("123456789.123456789012345678901234566"), struct)
      ))

    val schema = new StructType()
      .add("a", DecimalType(38, 27))
      .add(
        "b",
        new StructType()
          .add("b1", DecimalType(38, 27)))

    val df2 = spark.createDataFrame(data, schema)
    GlutenTestUtils.compareAnswers(df2.select("b").collect(), Seq(Row(struct)))
    GlutenTestUtils.compareAnswers(
      df2.select("a").collect(),
      Seq(Row(new java.math.BigDecimal("123456789.123456789012345678901234566"))))
  }

  test("test 'sum/count/max/min from empty table'") {
    spark.sql(
      """
        | create table test_tbl(id bigint, name string) using parquet;
        |""".stripMargin
    )
    val sql1 = "select count(1), sum(id), max(id), min(id), 'abc' as x from test_tbl"
    val sql2 =
      "select count(1) as cnt, sum(id) as sum, max(id) as max, min(id) as min from test_tbl"
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })
    spark.sql("drop table test_tbl")
  }

  test("test 'function json_tuple'") {
    val sql = """
                | select
                | json_tuple('{"hello":"world", "hello1":"world1", "hello2":["a","b"]}',
                |            'hello', 'hello1','hello2', 'hello3')
                | from lineitem where l_linenumber = 3 and l_orderkey < 3 limit 1
                |""".stripMargin
    val result = runSql(sql) { _ => }
    assert(result.head.getString(0).equals("world"))
    assert(result.head.getString(1).equals("world1"))
    assert(result.head.getString(2).equals("[\"a\",\"b\"]"))
    assert(result.head.isNullAt(3))
  }

  test("GLUTEN-3271: Bug fix arrayElement from split") {
    val table_create_sql =
      """
        | create table test_tbl_3271(id bigint, data string) using parquet;
        |""".stripMargin
    val table_drop_sql = "drop table test_tbl_3271"
    val data_insert_sql = "insert into test_tbl_3271 values(1, 'ab')"
    val select_sql_1 = "select id, split(data, ',')[1] from test_tbl_3271 where id = 1"
    val select_sql_2 = "select id, element_at(split(data, ','), 2) from test_tbl_3271 where id = 1"
    val select_sql_3 = "select id, element_at(map(id, data), 1) from test_tbl_3271 where id = 1"
    spark.sql(table_create_sql)
    spark.sql(data_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql_1, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_2, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_3, true, { _ => })

    spark.sql(table_drop_sql)
  }

  test("GLUTEN-5904 NaN values from stddev") {
    val sql1 =
      """
        |select a, stddev(b/c) from (select * from values (1,2, 1), (1,3,0) as data(a,b,c))
        |group by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })
    val sql2 =
      """
        |select a, stddev(b) from (select * from values (1,2, 1) as data(a,b,c)) group by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })

  }

  test("var_samp returns null instead of NaN") {
    val sql1 = "select var_samp(id) from range(1)"
    val sql2 = "select covar_samp(id, id) from range(1)"

    compareResultsAgainstVanillaSpark(sql1, true, { _ => })
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })
  }

  test("existence join") {
    spark.sql("create table t1(a int, b int) using parquet")
    spark.sql("create table t2(a int, b int) using parquet")
    spark.sql("insert into t1 values(0, 0), (1, 2), (2, 3), (3, 4), (null, 5), (6, null)")
    spark.sql("insert into t2 values(0, 0), (1, 2), (2, 3), (2,4), (null, 5), (6, null)")

    val sql1 = """
                 |select * from t1 where exists (select 1 from t2 where t1.a = t2.a) or t1.a > 1
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 = """
                 |select * from t1 where exists (select 1 from t2 where t1.a = t2.a) or t1.a > 3
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })

    val sql3 = """
                 |select * from t1 where exists (select 1 from t2 where t1.a = t2.a) or t1.b > 0
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => })

    val sql4 = """
                 |select * from t1 where exists (select 1 from t2
                 |where t1.a = t2.a and t1.b = t2.b) or t1.a > 0
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql4, true, { _ => })

    spark.sql("drop table t1")
    spark.sql("drop table t2")
  }

  test("gluten-7077 bug in cross broad cast join") {
    spark.sql("create table cross_join_t(a bigint, b string, c string) using parquet")
    var sql = """
                | insert into cross_join_t
                | select id as a, cast(id as string) as b,
                |   concat('1231231232323232322', cast(id as string)) as c
                | from range(0, 10000)
                |""".stripMargin
    spark.sql(sql)
    sql = """
            | insert into cross_join_t
            | select id as a, cast(id as string) as b,
            |   concat('1231231232323232322', cast(id as string)) as c
            | from range(10000, 20000)
            |""".stripMargin
    spark.sql(sql)
    sql = """
            |select * from (
            | select a as a1, b as b1, c as c1 from cross_join_t
            |) as t1 full join (
            | select a as a2, b as b2, c as c2 from cross_join_t
            |) as t2
            |order by a1, b1, c1, a2, b2, c2
            |limit 10
            |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
    spark.sql("drop table cross_join_t")
  }

  test("Pushdown aggregation pre-projection ahead expand") {
    spark.sql("create table t1(a bigint, b bigint, c bigint, d bigint) using parquet")
    spark.sql("insert into t1 values(1,2,3,4), (1,2,4,5), (1,3,4,5), (2,3,4,5)")
    var sql = """
                | select a, b , sum(d+c) from t1 group by a,b with cube
                | order by a,b
                |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
    sql = """
            | select a, b , sum(a+c), sum(b+d) from t1 group by a,b with cube
            | order by a,b
            |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
    spark.sql("drop table t1")
  }

  test("GLUTEN-7780 fix split diff") {
    val sql = "select split(concat('a|b|c', cast(id as string)), '\\|')" +
      ", split(concat('a|b|c', cast(id as string)), '\\\\|')" +
      ", split(concat('a|b|c', cast(id as string)), '|') from range(10)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }
  test("GLUTEN-8142 duplicated columns in group by") {
    sql("create table test_8142 (day string, rtime int, uid string, owner string) using parquet")
    sql("insert into test_8142 values ('2024-09-01', 123, 'user1', 'owner1')")
    sql("insert into test_8142 values ('2024-09-01', 123, 'user1', 'owner1')")
    sql("insert into test_8142 values ('2024-09-02', 567, 'user2', 'owner2')")
    compareResultsAgainstVanillaSpark(
      """
        |select days, rtime, uid, owner, day1
        |from (
        | select day1 as days, rtime, uid, owner, day1
        | from (
        |   select distinct coalesce(day, "today") as day1, rtime, uid, owner
        |   from test_8142 where day = '2024-09-01'
        | )) group by days, rtime, uid, owner, day1
        |""".stripMargin,
      true,
      { _ => }
    )
    sql("drop table test_8142")
  }

  test("GLUTEN-9317 duplicated column names") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {

      sql("create table test_9317 (a int, b int, c int) using parquet")
      sql("insert into test_9317 values (1, 2, 3), (4, 5, 6), (7, 8, 9)")
      sql("insert into test_9317 values (1, 2, 3), (4, 5, 6), (7, 8, 9)")
      val sqlStr =
        """
          | select * from (
          |select * from (
          | select a, b, b, row_number() over(partition by a order by c) as r
          | from test_9317
          |) where r = 1
          |) order by a, b
          |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sqlStr,
        true,
        {
          df =>
            {
              val shuffles = df.queryExecution.executedPlan.collect {
                case shuffle: ColumnarShuffleExchangeExec => shuffle
              }
              assert(shuffles.size == 2)
            }
        }
      )
      sql("drop table test_9317")
    }
  }
}
