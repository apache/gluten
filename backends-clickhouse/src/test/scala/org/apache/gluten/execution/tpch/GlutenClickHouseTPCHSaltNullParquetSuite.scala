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
package org.apache.gluten.execution.tpch

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.execution.GlutenPlan

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, ConstantFolding, NullPropagation}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ReusedSubqueryExec, SubqueryExec}
import org.apache.spark.sql.functions.{col, rand, when}
import org.apache.spark.sql.internal.SQLConf

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

trait TPCHSaltedTable extends TPCHDatabase {
  override protected def createTestTables(): Unit = {

    // first process the parquet data to:
    // 1. make every column nullable in schema (optional rather than required)
    // 2. salt some null values randomly
    val saltedTablesPath = s"$dataHome/tpch-salted"
    withSQLConf((GlutenConfig.GLUTEN_ENABLED.key, "false")) {
      tpchTables
        .map(
          tableName => {
            val originTablePath = s"$testParquetAbsolutePath/$tableName"
            val df = spark.read.parquet(originTablePath)
            var salted_df: Option[DataFrame] = None
            for (c <- df.schema) {
              salted_df = Some((salted_df match {
                case Some(x) => x
                case None => df
              }).withColumn(c.name, when(rand() < 0.01, null).otherwise(col(c.name))))
            }

            val currentSaltedTablePath = saltedTablesPath + "/" + tableName
            val file = new File(currentSaltedTablePath)
            if (file.exists()) {
              file.delete()
            }
            salted_df.get.write.parquet(currentSaltedTablePath)
          })
    }

    createTPCHTables(saltedTablesPath)
    val result = spark.sql("show tables").collect()
    assertResult(8)(result.length)
  }
}
class GlutenClickHouseTPCHSaltNullParquetSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with withTPCHQuery
  with TPCHSaltedTable {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set(GlutenConfig.GLUTEN_SUPPORTED_SCALA_UDFS.key, "my_add")
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
          case scanExec: BasicScanExecTransformer => true
        }
        assert(scanExec.size == 1)
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

  test("GLUTEN-2115: Fix wrong number of records shuffle written") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.adaptive.enabled", "true")
    ) {
      compareResultsAgainstVanillaSpark(
        """
          |select
          |    l_shipdate_grp l_shipdate,
          |    (lead(count(distinct l_suppkey), -1) over (order by l_shipdate_grp)) cc
          |from
          |    (select l_suppkey, EXTRACT(year from `l_shipdate`)  l_shipdate_grp from lineitem) t
          |group by l_shipdate_grp
          |order by l_shipdate_grp desc
          |limit 20
          |""".stripMargin,
        compareResult = true,
        _ => {}
      )
    }
  }

  test("test 'function pmod'") {
    val df = runQueryAndCompare(
      "select pmod(-10, id+10) from range(10)"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test 'function ascii'") {
    val df = runQueryAndCompare(
      "select ascii(cast(id as String)) from range(10)"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test 'function rand'") {
    runSql("select rand(), rand(1), rand(null) from range(10)")(
      checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'function date_add/date_sub/datediff'") {
    runQueryAndCompare(
      "select l_shipdate, l_commitdate, " +
        "date_add(l_shipdate, 1), date_add(l_shipdate, -1), " +
        "date_sub(l_shipdate, 1), date_sub(l_shipdate, -1), " +
        "datediff(l_shipdate, l_commitdate), datediff(l_commitdate, l_shipdate) " +
        "from lineitem order by l_shipdate, l_commitdate limit 1"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'function remainder'") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, l_orderkey % l_partkey, l_partkey % l_orderkey " +
        "from lineitem order by l_orderkey desc, l_partkey desc limit 1"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test positive/negative") {
    runQueryAndCompare(
      "select +n_nationkey, positive(n_nationkey), -n_nationkey, negative(n_nationkey) from nation"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  private val withoutConstantFoldingAndNullPropagation = SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
    (ConstantFolding.ruleName + "," + NullPropagation.ruleName)
  // TODO: enable when supports interval type
  ignore("test positive/negative with interval type") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select +interval 1 day, positive(interval 1 day), -interval 1 day, negative(interval 1 day)",
        noFallBack = false
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test array_intersect") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select a from (select array_intersect(split(n_comment, ' '), split(n_comment, ' ')) as arr " +
          "from nation) lateral view explode(arr) as a order by a"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select a from (select array_intersect(array(null,1,2,3,null), array(3,5,1,null,null)) as arr) " +
          "lateral view explode(arr) as a order by a"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_intersect(array(null,1,2,3,null), cast(null as array<int>))"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select a from (select array_intersect(array(array(1,2),array(3,4)), array(array(1,2),array(3,4))) as arr) " +
          "lateral view explode(arr) as a order by a",
        noFallBack = false
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test array_position") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select array_position(split(n_comment, ' '), 'final') from nation"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_position(array(1,2,3,null), 1), array_position(array(1,2,3,null), null)," +
          "array_position(array(1,2,3,null), 5), array_position(array(1,2,3), 5), " +
          "array_position(array(1,2,3), 2), array_position(cast(null as array<int>), 1)"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test array_contains") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select array_contains(split(n_comment, ' '), 'final') from nation"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_contains(array(1,2,3,null), 1), array_contains(array(1,2,3,null), " +
          "cast(null as int)), array_contains(array(1,2,3,null), 5), array_contains(array(1,2,3), 5)," +
          "array_contains(array(1,2,3), 2), array_contains(cast(null as array<int>), 1)"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test sort_array") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select sort_array(split(n_comment, ' ')) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select sort_array(split(n_comment, ' '), false) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select sort_array(array(1,3,2,null)), sort_array(array(1,2,3,null),false)"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test coalesce") {
    var df = runQueryAndCompare(
      "select l_orderkey, coalesce(l_comment, 'default_val') " +
        "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(cast(null as string), l_comment, 'default_val') " +
        "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(cast(null as string), cast(null as string), l_comment) " +
        "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(cast(null as string), cast(null as string), 1, 2) " +
        "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, " +
        "coalesce(cast(null as string), cast(null as string), cast(null as string)) "
        + "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
  }

  test("test 'function from_unixtime'") {
    val df = runQueryAndCompare(
      "select l_orderkey, from_unixtime(l_orderkey, 'yyyy-MM-dd HH:mm:ss'), " +
        "from_unixtime(l_orderkey, 'yyyy-MM-dd') " +
        "from lineitem order by l_orderkey desc limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test 'aggregate function collect_list'") {
    val df = runQueryAndCompare(
      "select l_orderkey,from_unixtime(l_orderkey, 'yyyy-MM-dd HH:mm:ss') " +
        "from lineitem order by l_orderkey desc limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test find_in_set") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select find_in_set(null, 'a'), find_in_set('a', null), " +
          "find_in_set('a', 'a,b'), find_in_set('a', 'ab,ab')"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test 'function regexp_replace'") {
    runQueryAndCompare(
      "select l_orderkey, regexp_replace(l_comment, '([a-z])', '1') " +
        "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_orderkey, regexp_replace(l_comment, '([a-z])', '1', 1) " +
        "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
  }

  test("regexp_extract") {
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract(l_comment, '([a-z])', 1) " +
        s"from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract(l_comment, '([a-z])') " +
        s"from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract(l_comment, '([a-z])', 0) " +
        s"from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
  }

  test("lpad") {
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, 80) " +
        s"from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, 80, '??') " +
        s"from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
  }

  test("rpad") {
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, 80) " +
        s"from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, 80, '??') " +
        s"from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test elt") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select elt(2, n_comment, n_regionkey) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
      runQueryAndCompare(
        "select elt(null, 'a', 'b'), elt(0, 'a', 'b'), elt(1, 'a', 'b'), elt(3, 'a', 'b')"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test array_max") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select array_max(split(n_comment, ' ')) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
      runQueryAndCompare(
        "select array_max(null), array_max(array(null)), array_max(array(1, 2, 3, null)), " +
          "array_max(array(1.0, 2.0, 3.0, null)), array_max(array('z', 't', 'abc'))"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test array_min") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select array_min(split(n_comment, ' ')) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
      runQueryAndCompare(
        "select array_min(null), array_min(array(null)), array_min(array(1, 2, 3, null)), " +
          "array_min(array(1.0, 2.0, 3.0, null)), array_min(array('z', 't', 'abc'))"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test slice function") {
    val sql =
      """
        |select slice(arr, 1, 5), slice(arr, 1, 100), slice(arr, -2, 5), slice(arr, 1, n_nationkey),
        |slice(null, 1, 2), slice(arr, null, 2), slice(arr, 1, null)
        |from (select split(n_comment, ' ') as arr, n_nationkey from nation) t
        |""".stripMargin
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test slice function with unexpected arguments") {
    def checkException(sql: String, expectedErrMsg: String): Unit = {
      val errMsg = intercept[SparkException] {
        spark.sql(sql).collect()
      }.getMessage

      if (errMsg == null) {
        fail(s"Expected null error message, but `$errMsg` found")
      } else if (!errMsg.contains(expectedErrMsg)) {
        fail(s"Expected error message is `$expectedErrMsg`, but `$errMsg` found")
      }
    }

    checkException(
      "select slice(split(n_comment, ' '), n_regionkey, 5) from nation",
      "Unexpected value for start")
    checkException(
      "select slice(split(n_comment, ' '), 1, -5) from nation",
      "Unexpected value for length")
  }

  test("test array_distinct") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select array_distinct(split(n_comment, ' ')) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_distinct(array(1,2,1,2,3)), array_distinct(array(null,1,null,1,2,null,3)), " +
          "array_distinct(array(array(1,null,2), array(1,null,2))), array_distinct(null), array_distinct(array(null))"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test array_union") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select array_union(split(n_comment, ' '), reverse(split(n_comment, ' '))) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_union(array(1,2,1,2,3), array(2,4,2,3,5)), " +
          "array_union(array(null,1,null,1,2,null,3), array(1,null,2,null,3,null,4)), " +
          "array_union(array(array(1,null,2), array(2,null,3)), array(array(2,null,3), array(1,null,2))), " +
          "array_union(array(null), array(null)), " +
          "array_union(cast(null as array<int>), cast(null as array<int>))"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test shuffle function") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select shuffle(split(n_comment, ' ')) from nation",
        compareResult = false
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select shuffle(array(1,2,3,4,5)), shuffle(array(1,3,null,3,4)), shuffle(null)",
        compareResult = false
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test 'function regexp_extract_all'") {
    runQueryAndCompare(
      "select l_orderkey, regexp_extract_all(l_comment, '([a-z])', 1) " +
        "from lineitem limit 5")(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'function to_unix_timestamp/unix_timestamp'") {
    runQueryAndCompare(
      "select to_unix_timestamp(concat(cast(l_shipdate as String), ' 00:00:00')) " +
        "from lineitem order by l_shipdate limit 10;")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      "select unix_timestamp(concat(cast(l_shipdate as String), ' 00:00:00')) " +
        "from lineitem order by l_shipdate limit 10;")(checkGlutenPlan[ProjectExecTransformer])
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      runQueryAndCompare(
        "select to_unix_timestamp(concat(cast(l_shipdate as String), ' 00:00:00')) " +
          "from lineitem order by l_shipdate limit 10")(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test literals") {
    val query =
      """
      SELECT
        CAST(NULL AS BOOLEAN) AS boolean_literal,
        CAST(1 AS TINYINT) AS tinyint_literal,
        CAST(2 AS SMALLINT) AS smallint_literal,
        CAST(3 AS INTEGER) AS integer_literal,
        CAST(4 AS BIGINT) AS bigint_literal,
        CAST(5.5 AS FLOAT) AS float_literal,
        CAST(6.6 AS DOUBLE) AS double_literal,
        CAST('7' AS STRING) AS string_literal,
        DATE '2022-01-01' AS date_literal,
        TIMESTAMP '2022-01-01 10:00:00' AS timestamp_literal,
        CAST(X'48656C6C6F' AS BINARY) AS binary_literal,
        ARRAY(1, 2, 3, 4) AS array_literal,
        MAP("a", 1, "b", 2) AS map_literal,
        STRUCT("hello", 123) AS struct_literal,
        ARRAY() as empty_array_literal,
        MAP() as empty_map_literal,
        ARRAY(1, NULL, 3) as array_with_null_literal,
        MAP(1, 2, CAST(3 as SHORT), null) as map_with_null_literal
      from range(10)"""
    runQueryAndCompare(query)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("window row_number") {
    val sql =
      """
        |select row_number() over (partition by n_regionkey order by n_nationkey) as num from nation
        |order by n_regionkey, n_nationkey, num
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("issue-3195 window row_number") {
    val sql =
      """
        |select row_number() over (order by 1) as num, n_nationkey from nation
        |order by num, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window sum 1") {
    val sql =
      """
        |select sum(n_nationkey + 1) over (partition by n_regionkey order by n_nationkey)
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window sum 2") {
    val sql =
      """
        |select sum(n_nationkey + 1) over (partition by n_regionkey order by n_name)
        |from nation
        |order by n_regionkey, n_name
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window sum const") {
    val sql =
      """
        |select n_regionkey, sum(2) over (partition by n_regionkey)
        |from nation
        |order by n_regionkey
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[WindowExecTransformer])
  }

  test("window max") {
    val sql =
      """
        |select max(n_nationkey) over (partition by n_regionkey order by n_nationkey) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window min") {
    val sql =
      """
        |select min(n_nationkey) over (partition by n_regionkey order by n_nationkey) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window avg") {
    val sql =
      """
        |select avg(n_nationkey) over (partition by n_regionkey order by n_nationkey) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window offset preceding") {
    val sql =
      """
        |select avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 3
        |preceding and current row) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window offset following") {
    val sql =
      """
        |select avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between
        |current row and 3 following) as x from nation
        |order by n_regionkey, n_nationkey, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window range") {
    val sql =
      """
        |select n_nationkey, n_name, n_regionkey,
        |  sum(n_nationkey) over (partition by n_regionkey order by n_nationkey range
        |  between unbounded preceding and current row) as n_sum
        |from nation
        |order by n_regionkey,n_nationkey,n_name,n_sum
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("windows") {
    val sql =
      """
        |select n_nationkey, n_name, n_regionkey,
        | rank() over (partition by n_regionkey order by n_nationkey) as n_rank,
        | sum(n_nationkey) over (partition by n_regionkey order by n_nationkey) as n_sum
        |from nation
        |order by n_regionkey,n_nationkey,n_name,n_rank
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window rank") {
    val sql =
      """
        |select n_nationkey, n_name, n_regionkey,
        | rank() over (partition by n_regionkey order by n_nationkey) as n_rank
        |from nation
        |order by n_regionkey,n_nationkey,n_name,n_rank
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lead") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lead(n_nationkey, 1) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead
        |from nation
        |order by n_regionkey, n_nationkey, n_lead
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lead / lag with negative offset") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lead(n_nationkey, -3, 2) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead1,
        | lead(n_nationkey, 1) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead2,
        | lag(n_nationkey, -1, 3) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag1,
        | lag(n_nationkey, 2) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag2
        |from nation
        |order by n_regionkey, n_nationkey, n_lead1, n_lead2, n_lag1, n_lag2
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lead with default value") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lead(n_nationkey, 1, 3) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead
        |from nation
        |order by n_regionkey, n_nationkey, n_lead
        |""".stripMargin

    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lag") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lag(n_nationkey, 1) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lag with default value") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lag(n_nationkey, 1, 3) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag
        |from nation
        |order by n_regionkey, n_nationkey, n_lag
        |""".stripMargin
    val sql1 =
      """
        | select n_regionkey, n_nationkey,
        | lag(n_nationkey, 1, n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag
        |from nation
        |order by n_regionkey, n_nationkey, n_lag
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
    compareResultsAgainstVanillaSpark(sql1, true, { _ => }, false)
  }

  test("window lag with null value") {
    val sql =
      """
        |select n_regionkey,
        | lag(count(distinct n_nationkey), -1) OVER (ORDER BY n_regionkey) as n_lag
        |from nation
        |group by n_regionkey
        |order by n_regionkey, n_lag
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window dense_rank") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | dense_rank(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_rank
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window percent_rank") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | percent_rank(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_rank
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window ntile") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   first_value(n_nationkey) over (partition by n_regionkey order by n_nationkey) as
        |   first_v,
        |   ntile(4) over (partition by n_regionkey order by n_nationkey) as ntile_v
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window first value with nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   first_value(n_nationkey) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window first value ignore nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   first_value(n_nationkey, true) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window last value with nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   last_value(n_nationkey) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window last value ignore nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   last_value(n_nationkey, true) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window bug #2586") {
    val sql =
      """
        | select row_number() over (partition by n_regionkey, id  order by n_nationkey) as num from (
        |   select n_regionkey, 'x' as id , n_nationkey from nation
        | ) order by n_regionkey, id, n_nationkey, num
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("group with rollup") {
    val sql =
      """
        |select l_shipdate, l_shipmode, count(l_shipmode) as n from lineitem
        |group by l_shipdate, l_shipmode with rollup
        |order by l_shipdate, l_shipmode, n
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("group with cube") {
    val sql =
      """
        |select l_shipdate, l_shipmode, count(l_tax) as n from lineitem
        |group by l_shipdate, l_shipmode with cube
        |order by l_shipdate, l_shipmode, n
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("group with sets") {
    val sql =
      """
        |select l_shipdate, l_shipmode, count(1) as cnt from lineitem
        |group by grouping sets (l_shipdate, l_shipmode, (l_shipdate, l_shipmode))
        |order by l_shipdate, l_shipmode, cnt
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("expand with nullable type not match") {
    val sql =
      """
        |select a, n_regionkey, n_nationkey from
        |(select nvl(n_name, "aaaa") as a, n_regionkey, n_nationkey from nation)
        |group by n_regionkey, n_nationkey
        |grouping sets((a, n_regionkey, n_nationkey),(a, n_regionkey), (a))
        |order by a, n_regionkey, n_nationkey
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ExpandExecTransformer])
  }

  test("expand col result") {
    val sql =
      """
        |select n_regionkey, n_nationkey, count(1) as cnt from nation
        |group by n_regionkey, n_nationkey with rollup
        |order by n_regionkey, n_nationkey, cnt
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ExpandExecTransformer])
  }

  test("expand with not nullable") {
    val sql =
      """
        |select a,b, sum(c) from
        |(select nvl(n_nationkey, 0) as c, nvl(n_name, '') as b, nvl(n_nationkey, 0) as a from nation)
        |group by a,b with rollup
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ExpandExecTransformer])
  }

  test("expand with function expr") {
    val sql =
      """
        |select
        | n_name,
        | count(distinct n_regionkey) as col1,
        | count(distinct concat(n_regionkey, n_nationkey)) as col2
        |from nation
        |group by n_name
        |order by n_name, col1, col2
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ExpandExecTransformer])
  }

  test("test 'position/locate'") {
    runQueryAndCompare(
      """
        |select position('D', l_shipinstruct, 0), position('', l_shipinstruct, 0),
        |position('I', l_shipinstruct, 5), position('IN', l_shipinstruct),
        |position('', l_shipinstruct), locate(l_returnflag, l_shipinstruct),
        |position(l_returnflag in l_shipinstruct), position('bar', 'foobarbar'),
        |position(l_returnflag, 'TENSTNTEST', 4), position('bar', 'foobarbar', 5),
        |position(l_returnflag, l_shipinstruct, l_linenumber + 11),
        |position(null, l_shipinstruct),
        |position(l_returnflag, null),
        |position(l_returnflag, l_shipinstruct, null),
        |position(l_returnflag, l_shipinstruct, 0),
        |position(l_returnflag, null, 0),
        |position(null, l_shipinstruct, 0),
        |position(null, null, 0),
        |position(l_returnflag, null, null),
        |position(null, l_shipinstruct, null),
        |position(null, null, null)
        |from lineitem
        |""".stripMargin
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test stddev_samp 1") {
    val sql =
      """
        |select stddev_samp(n_nationkey), stddev_samp(n_regionkey) from nation
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test stddev_samp 2") {
    val sql =
      """
        |select stddev_samp(l_orderkey), stddev_samp(l_quantity) from lineitem
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test stddev") {
    val sql =
      """
        |select stddev(l_orderkey), stddev(l_quantity) from lineitem
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("isNaN") {
    val sql =
      """
        |select isNaN(l_shipinstruct), isNaN(l_partkey), isNaN(l_discount)
        |from lineitem
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nanvl") {
    val sql =
      """
        |SELECT nanvl(cast('nan' as float), 1f),
        | nanvl(n_nationkey, cast('null' as double)),
        | nanvl(cast('null' as double), n_nationkey),
        | nanvl(n_nationkey, n_nationkey / 0.0d),
        | nanvl(cast('nan' as float), n_nationkey)
        | from nation
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test bin function") {
    runQueryAndCompare("select bin(id - 50) from range (100)")(
      checkGlutenPlan[ProjectExecTransformer])

    runQueryAndCompare("select bin(n_nationkey) from nation")(
      checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'sequence'") {
    runQueryAndCompare(
      "select sequence(id, id+10), sequence(id+10, id), sequence(id, id+10, 3), " +
        "sequence(id+10, id, -3) from range(1)")(checkGlutenPlan[ProjectExecTransformer])
  }

  test("GLUTEN-2491: sequence with null value as argument") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select sequence(null, 1), sequence(1, null), sequence(1, 3, null), sequence(1, 5)," +
          "sequence(5, 1), sequence(1, 5, 2), sequence(5, 1, -2)"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select sequence(n_nationkey, n_nationkey+10), sequence(n_nationkey, n_nationkey+10, 2) " +
          "from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("Bug-398 collect_list failure") {
    val sql =
      """
        |select n_regionkey, collect_list(if(n_regionkey=0, n_name, null)) as t from nation group by n_regionkey
        |order by n_regionkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, df => {})
  }

  test("collect_set") {
    val sql =
      """
        |select a, b from (
        |select n_regionkey as a, collect_set(if(n_regionkey=0, n_name, null)) as set from nation group by n_regionkey)
        |lateral view explode(set) as b
        |order by a, b
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[CHHashAggregateExecTransformer])
  }

  test("collect_set should return empty set") {
    runQueryAndCompare(
      "select collect_set(if(n_regionkey != -1, null, n_regionkey)) from nation"
    )(checkGlutenPlan[CHHashAggregateExecTransformer])
  }

  test("Test 'spark.gluten.enabled' false") {
    withSQLConf((GlutenConfig.GLUTEN_ENABLED.key, "false")) {
      customCheck(2, native = false) {
        df =>
          val glutenPlans = df.queryExecution.executedPlan.collect {
            case glutenPlan: GlutenPlan => glutenPlan
          }
          assert(glutenPlans.isEmpty)
      }
    }
  }

  test("test 'cast null value'") {
    val sql = "select cast(x as double), cast(x as float), cast(x as string), cast(x as binary)," +
      "cast(x as long), cast(x as int), cast(x as short), cast(x as byte), cast(x as boolean)," +
      "cast(x as date), cast(x as timestamp), cast(x as decimal(10, 2)) from " +
      "(select cast(null as string) as x from range(10) union all " +
      "select cast(id as string) as x from range(2))"
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'max(NULL)/min(NULL) from table'") {
    val sql =
      """
        |select
        | l_linenumber, max(NULL), min(NULL)
        | from lineitem where l_linenumber = 3 and l_orderkey < 3
        | group by l_linenumber limit 1
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test 'dayofweek/weekday'") {
    val sql = "select l_orderkey, l_shipdate, weekday(l_shipdate), dayofweek(l_shipdate) " +
      "from lineitem limit 10"
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'to_date/to_timestamp'") {
    val sql = "select to_date(concat('2022-01-0', cast(id+1 as String)), 'yyyy-MM-dd') as a1," +
      "to_timestamp(concat('2022-01-01 10:30:0', cast(id+1 as String)), 'yyyy-MM-dd HH:mm:ss') as a2," +
      "to_date(date_add(date'2024-05-07', cast(id as int)), 'yyyy-MM-dd') as a3, " +
      "to_date(date_add(date'2024-05-07', cast(id as int)), 'yyyyMMdd') as a4, " +
      "to_date(date_add(date'2024-05-07', cast(id as int)), 'yyyy-MM') as a5, " +
      "to_date(date_add(date'2024-05-07', cast(id as int)), 'yyyy') as a6, " +
      "to_date(to_timestamp(concat('2022-01-01 10:30:0', cast(id+1 as String))), 'yyyy-MM-dd HH:mm:ss') as a7, " +
      "to_timestamp(date_add(date'2024-05-07', cast(id as int)), 'yyyy-MM') as a8, " +
      "to_timestamp(to_timestamp(concat('2022-01-01 10:30:0', cast(id+1 as String))), 'yyyy-MM-dd HH:mm:ss') as a9," +
      "to_timestamp('2024-10-09 11:22:33.123', 'yyyy-MM-dd HH:mm:ss.SSS') " +
      "from range(9)"
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'btrim/ltrim/rtrim/trim'") {
    runQueryAndCompare(
      "select l_comment, btrim(l_comment), btrim(l_comment, 'abcd') " +
        "from lineitem limit 10")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_comment, ltrim(l_comment), ltrim('abcd', l_comment) " +
        "from lineitem limit 10")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_comment, rtrim(l_comment), rtrim('abcd', l_comment) " +
        "from lineitem limit 10")(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_comment, trim(l_comment), trim('abcd' from l_comment), " +
        "trim(BOTH 'abcd' from l_comment), trim(LEADING 'abcd' from l_comment), " +
        "trim(TRAILING 'abcd' from l_comment) from lineitem limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("bit_and/bit_or/bit_xor") {
    runQueryAndCompare(
      "select bit_and(n_regionkey), bit_or(n_regionkey), bit_xor(n_regionkey) from nation") {
      checkGlutenPlan[CHHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select bit_and(l_partkey), bit_or(l_suppkey), bit_xor(l_orderkey) from lineitem") {
      checkGlutenPlan[CHHashAggregateExecTransformer]
    }
  }

  test("bit_get/bit_count") {
    runQueryAndCompare(
      "select bit_count(id), bit_get(id, 0), bit_get(id, 1), bit_get(id, 2), bit_get(id, 3) from range(100)") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("test 'EqualNullSafe'") {
    runQueryAndCompare("select l_linenumber <=> l_orderkey, l_linenumber <=> null from lineitem") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  testSparkVersionLE33("test posexplode issue: https://github.com/oap-project/gluten/issues/1767") {
    spark.sql("create table test_1767 (id bigint, data map<string, string>) using parquet")
    spark.sql("INSERT INTO test_1767 values(1, map('k', 'v'))")

    val sql =
      """
        | select id from test_1767 lateral view
        | posexplode(split(data['k'], ',')) tx as a, b""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[CHGenerateExecTransformer])

    spark.sql("drop table test_1767")
  }

  test("test posexplode issue: https://github.com/oap-project/gluten/issues/2492") {
    val sql = "select posexplode(split(n_comment, ' ')) from nation where n_comment is null"
    runQueryAndCompare(sql)(checkGlutenPlan[CHGenerateExecTransformer])
  }

  test("test posexplode issue: https://github.com/oap-project/gluten/issues/2454") {
    val sqls = Seq(
      "select id, explode(array(id, id+1)) from range(10)",
      "select id, explode(map(id, id+1, id+2, id+3)) from range(10)",
      "select id, posexplode(array(id, id+1)) from range(10)",
      "select id, posexplode(map(id, id+1, id+2, id+3)) from range(10)"
    )

    for (sql <- sqls) {
      runQueryAndCompare(sql)(checkGlutenPlan[CHGenerateExecTransformer])
    }
  }

  test("test explode issue: https://github.com/oap-project/gluten/issues/3124") {
    spark.sql("create table test_3124 (id bigint, name string, sex string) using parquet")
    spark.sql("insert into test_3124  values (31, null, 'm'), (32, 'a,b,c', 'f')")

    val sql = "select id, flag from test_3124 lateral view explode(split(name, ',')) as flag"
    runQueryAndCompare(sql)(checkGlutenPlan[CHGenerateExecTransformer])

    spark.sql("drop table test_3124")
  }

  test("test 'scala udf'") {
    spark.udf.register("my_add", (x: Long, y: Long) => x + y)
    runQueryAndCompare("select my_add(id, id+1) from range(10)")(
      checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'ColumnarToRowExec should not be used'") {
    withSQLConf(
      GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "false"
    ) {
      runQueryAndCompare(
        "select l_shipdate from lineitem where l_shipdate = '1996-05-07'",
        noFallBack = false) {
        df => getExecutedPlan(df).count(plan => plan.isInstanceOf[ColumnarToRowExec]) == 0
      }
    }
  }

  test("GLUTEN-2104: test size function") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConstantFolding.ruleName) {
      runQueryAndCompare(
        "select size(null), size(split(l_shipinstruct, ' ')) from lineitem"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-1822: test reverse/concat") {
    val sql =
      """
        |select reverse(split(n_comment, ' ')), reverse(n_comment),
        |concat(split(n_comment, ' ')), concat(n_comment), concat(n_comment, n_name),
        |concat(split(n_comment, ' '), split(n_name, ' ')), concat(array()), concat(array(n_name))
        |from nation
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("GLUTEN-1620: fix 'attribute binding failed.' when executing hash agg without aqe") {
    val sql =
      """
        |SELECT *
        |    FROM (
        |      SELECT t1.O_ORDERSTATUS, t4.ACTIVECUSTOMERS / t1.ACTIVECUSTOMERS AS REPEATPURCHASERATE
        |      FROM (
        |         SELECT o_orderstatus AS O_ORDERSTATUS, COUNT(1) AS ACTIVECUSTOMERS
        |         FROM orders
        |         GROUP BY o_orderstatus
        |      ) t1
        |         INNER JOIN (
        |            SELECT o_orderstatus AS O_ORDERSTATUS, MAX(o_totalprice) AS ACTIVECUSTOMERS
        |                FROM orders
        |                GROUP BY o_orderstatus
        |         ) t4
        |         ON t1.O_ORDERSTATUS = t4.O_ORDERSTATUS
        |    ) t5
        |      INNER JOIN (
        |         SELECT t8.O_ORDERSTATUS, t9.ACTIVECUSTOMERS / t8.ACTIVECUSTOMERS AS REPEATPURCHASERATE
        |            FROM (
        |                SELECT o_orderstatus AS O_ORDERSTATUS, COUNT(1) AS ACTIVECUSTOMERS
        |                FROM orders
        |                GROUP BY o_orderstatus
        |            ) t8
        |                INNER JOIN (
        |                    SELECT o_orderstatus AS O_ORDERSTATUS, MAX(o_totalprice) AS ACTIVECUSTOMERS
        |                    FROM orders
        |                    GROUP BY o_orderstatus
        |                ) t9
        |                ON t8.O_ORDERSTATUS = t9.O_ORDERSTATUS
        |            ) t12
        |      ON t5.O_ORDERSTATUS = t12.O_ORDERSTATUS
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, NOOP)
  }

  test("GLUTEN-1848: Fix execute subquery repeatedly issue with ReusedSubquery") {
    val sql =
      """
        |SELECT
        |    s_suppkey,
        |    s_name,
        |    s_address,
        |    s_phone,
        |    total_revenue,
        |    total_revenue_1
        |FROM
        |    supplier,
        |    (
        |        SELECT
        |            l_suppkey AS supplier_no,
        |            sum(l_extendedprice * (1 - l_discount)) AS total_revenue,
        |            sum(l_extendedprice * l_discount) AS total_revenue_1
        |        FROM
        |            lineitem
        |        WHERE
        |            l_shipdate >= date'1996-01-01' AND l_shipdate < date'1996-01-01' + interval 3 month
        |        GROUP BY
        |            supplier_no) revenue0
        |WHERE
        |    s_suppkey = supplier_no
        |    AND total_revenue = (
        |        SELECT
        |            max(total_revenue)
        |        FROM (
        |            SELECT
        |                l_suppkey AS supplier_no,
        |                sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        |            FROM
        |                lineitem
        |            WHERE
        |                l_shipdate >= date'1996-01-01' AND l_shipdate < date'1996-01-01' + interval 3 month
        |            GROUP BY
        |                supplier_no) revenue1)
        |    AND total_revenue_1 < (
        |        SELECT
        |            max(total_revenue)
        |        FROM (
        |            SELECT
        |                l_suppkey AS supplier_no,
        |                sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        |            FROM
        |                lineitem
        |            WHERE
        |                l_shipdate >= date'1996-01-01' AND l_shipdate < date'1996-01-01' + interval 3 month
        |            GROUP BY
        |                supplier_no) revenue1)
        |ORDER BY
        |    s_suppkey;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val subqueriesId = df.queryExecution.executedPlan.collectWithSubqueries {
            case s: SubqueryExec => s.id
            case rs: ReusedSubqueryExec => rs.child.id
          }
          assert(subqueriesId.distinct.size == 1)
      }
    )
  }

  test("GLUTEN-1875: UnionExecTransformer for BroadcastRelation") {
    val sql =
      """
        |select /*+ BROADCAST(t2) */ t1.l_orderkey, t1.l_partkey, t2.o_custkey
        |from lineitem t1
        |join (
        |  select o_orderkey, o_custkey from orders
        |  union all
        |  select  o_orderkey, o_custkey from orders) t2
        |on t1.l_orderkey = cast(t2.o_orderkey as int)
        |order by t1.l_orderkey, t1.l_partkey, t2.o_custkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2198: Fix wrong schema when there is no aggregate function") {
    val sql =
      """
        |select b, a
        |from
        |  (
        |    select l_shipdate as a, l_returnflag as b from lineitem
        |    union
        |    select o_orderdate as a, o_orderstatus as b from orders
        |  )
        |group by b, a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })

    val sql1 =
      """
        |select b, a, sum(c), avg(c)
        |from
        |  (
        |    select l_shipdate as a, l_returnflag as b, l_quantity as c from lineitem
        |    union
        |    select o_orderdate as a, o_orderstatus as b, o_totalprice as c from orders
        |  )
        |group by b, a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 =
      """
        |select t1.o_orderkey, o_shippriority, sss from (
        |(
        |  select o_orderkey,o_shippriority from orders
        |) t1
        |left join
        |(
        | select c_custkey custkey, c_nationkey, sum(c_acctbal) as sss
        | from customer
        | group by 1,2
        |) t2
        |on t1.o_orderkey=t2.custkey and t1.o_shippriority=t2.c_nationkey
        |)
        |order by t1.o_orderkey desc, o_shippriority
        |limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })

    val sql3 =
      """
        |select t1.o_orderkey, o_shippriority from (
        |(
        |  select o_orderkey,o_shippriority from orders
        |) t1
        |left join
        |(
        | select c_custkey custkey, c_nationkey
        | from customer
        | group by 1,2
        |) t2
        |on t1.o_orderkey=t2.custkey and t1.o_shippriority=t2.c_nationkey
        |)
        |order by t1.o_orderkey desc, o_shippriority
        |limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => })
  }

  test("GLUTEN-2079: aggregate function with filter") {
    val sql =
      """
        | select
        |  count(distinct(a)), count(distinct(b)), count(distinct(c))
        | from
        |  values (1, null,2), (2,2,4), (3,2,4) as data(a,b,c)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-1956: fix error conversion of Float32 in CHColumnToSparkRow") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConstantFolding.ruleName) {
      runQueryAndCompare(
        "select struct(1.0f), array(2.0f), map('a', 3.0f) from range(1)"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-1790 count multi cols") {
    val sql1 =
      """
        | select count(n_regionkey, n_nationkey) from nation
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 =
      """
        | select count(a, b) from values(1,null),(2, 2) as data(a,b)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })

    val sql3 =
      """
        | select count(a, b) from values(null,1),(2, 2) as data(a,b)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => })

    val sql4 =
      """
        | select count(n_regionkey, n_name) from nation
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql4, true, { _ => })
  }

  test("GLUTEN-2028: struct as join key") {
    val tables = Seq("struct_1", "struct_2")
    tables.foreach {
      table =>
        spark.sql(s"create table $table (info struct<a:int, b:int>) using parquet")
        spark.sql(s"insert overwrite $table values (named_struct('a', 1, 'b', 2))")
    }
    val hints = Seq("BROADCAST(t2)", "SHUFFLE_MERGE(t2), SHUFFLE_HASH(t2)")
    hints.foreach(
      hint =>
        compareResultsAgainstVanillaSpark(
          s"select /*+ $hint */ t1.info from struct_1 t1 join struct_2 t2 on t1.info = t2.info",
          true,
          { _ => }))
  }

  test("GLUTEN-2005: Json_tuple return cause data loss") {
    spark.sql(
      """
        | create table test_2005(tuple_data struct<a:string, b:string>, id bigint, json_data string, name string) using parquet;
        |""".stripMargin
    )
    spark.sql(
      "insert into test_2005 values(struct('a', 'b'), 1, '{\"a\":\"b\", \"c\":\"d\"}', 'gluten')")
    val sql =
      """
        | select tuple_data, json_tuple(json_data, 'a', 'c'), name from test_2005
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2060 null count") {
    val sql =
      """
        |select
        | count(a),count(b), count(1), count(distinct(a)), count(distinct(b))
        |from
        | values (1, null), (2,2) as data(a,b)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2221 empty hash aggregate exec") {
    val sql1 =
      """
        | select count(1) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 =
      """
        | select count(1) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 left join (
        |       select * from values(6,5),(7,5),(8,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })

    val sql3 =
      """
        | select count(1) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(6,5),(7,5),(8,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => })

    val sql4 =
      """
        | select count(*) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql4, true, { _ => })

    val sql5 =
      """
        | select count(*) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(6,5),(7,5),(8,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql5, true, { _ => })
  }

  test("GLUTEN-1874 not null in one stream") {
    val sql =
      """
        |select n_regionkey from (
        | select *, row_number() over (partition by n_regionkey order by is_new) as rank from(
        |   select n_regionkey, 0 as is_new from nation where n_regionkey is not null
        |   union all
        |   select n_regionkey, 1 as is_new from (
        |     select n_regionkey,
        |       row_number() over (partition by n_regionkey order by n_nationkey) as rn from nation
        |   ) t0 where rn = 1
        | ) t1
        |) t2 where rank = 1
    """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-1874 not null in both streams") {
    val sql =
      """
        |select n_regionkey from (
        | select *, row_number() over (partition by n_regionkey order by is_new) as rank from(
        |   select n_regionkey, 0 as is_new from nation where n_regionkey is not null
        |   union all
        |   select n_regionkey, 1 as is_new from (
        |     select n_regionkey,
        |       row_number() over (partition by n_regionkey order by n_nationkey) as rn
        |     from nation where n_regionkey is not null
        |   ) t0 where rn = 1
        | ) t1
        |) t2 where rank = 1
    """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2095: test cast(string as binary)") {
    runQueryAndCompare(
      "select cast(n_nationkey as binary), cast(n_comment as binary) from nation"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("var_samp") {
    runQueryAndCompare("""
                         |select var_samp(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[CHHashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey % 5, var_samp(l_quantity) from lineitem
                         |group by l_orderkey % 5;
                         |""".stripMargin) {
      checkGlutenPlan[CHHashAggregateExecTransformer]
    }
  }

  test("var_pop") {
    runQueryAndCompare("""
                         |select var_pop(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[CHHashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey % 5, var_pop(l_quantity) from lineitem
                         |group by l_orderkey % 5;
                         |""".stripMargin) {
      checkGlutenPlan[CHHashAggregateExecTransformer]
    }
  }

  test("corr") {
    runQueryAndCompare("""
                         |select corr(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[CHHashAggregateExecTransformer]
    }

    runQueryAndCompare(
      "select corr(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[CHHashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("test concat_ws") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select concat_ws(null), concat_ws('-'), concat_ws('-', null), concat_ws('-', null, null), " +
          "concat_ws(null, 'a'), concat_ws('-', 'a'), concat_ws('-', 'a', null), " +
          "concat_ws('-', 'a', null, 'b', 'c', null, array(null), array('d', null), array('f', 'g'))"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select concat_ws('-', n_comment, " +
          "array(if(n_regionkey=0, null, cast(n_regionkey as string)))) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-2422 range bound with nan/inf") {
    val sql =
      """
        |select a from values (1.0), (2.1), (null), (cast('NaN' as double)), (cast('inf' as double)),
        | (cast('-inf' as double)) as data(a) order by a asc nulls last
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[SortExecTransformer])
  }

  test("GLUTEN-2639: log1p") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select log1p(n_regionkey), log1p(-1.0), log1p(-2.0) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-2243 empty projection") {
    val sql =
      """
        | select count(1) from(
        |   select b,c from values(1,2),(1,2) as data(b,c) group by b,c
        |   union all
        |   select a, b from values (1,2),(1,2),(2,3) as data(a,b) group by a, b
        | )
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("Gluten-2430 hash partition column not found") {
    val sql =
      """
        |
        | select a.l_shipdate,
        |    a.l_partkey,
        |    b.l_shipmode,
        |    if(c.l_suppkey is not null, 'new', 'old') as usertype,
        |    a.uid
        |from (
        |        select l_shipdate,
        |            l_partkey,
        |            l_suppkey as uid
        |        from lineitem
        |        where l_shipdate = '2023-03-07'
        |        group by l_shipdate,
        |            l_partkey,
        |            l_suppkey
        |    ) a
        |    join (
        |        select l_shipdate,
        |            l_suppkey as uid,
        |            l_shipmode
        |        from lineitem
        |        where l_shipdate = '2023-03-07'
        |    ) b on a.l_shipdate = b.l_shipdate
        |    and a.uid = b.uid
        |    left join (
        |        select l_shipdate,
        |            l_suppkey
        |        from lineitem
        |        where l_shipdate = '2023-03-07'
        |        group by l_shipdate,
        |            l_suppkey
        |    ) c on a.l_shipdate = c.l_shipdate
        |    and a.uid = c.l_suppkey
        |limit 100;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("cast date issue-2474") {
    spark.sql(
      """
        | create table test_date (pid BIGINT) using parquet;
        |""".stripMargin
    )
    spark.sql(
      """
        | insert into test_date values (6927737632337729200), (6927744564414944949)
        |""".stripMargin
    )

    val sql1 =
      """
        | select
        |   pid,
        |   from_unixtime(bigint(pid / 4294967296),'yyyy-mm-dd') < date_sub('2023-01-01', 1) as c
        | from test_date
        | order by pid
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 =
      """
        | select pid
        | from test_date
        | where from_unixtime(bigint(pid / 4294967296),'yyyy-mm-dd') < date_sub('2023-01-01', 1)
        | order by pid
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })
  }

  test("test-conv-function") {
    {
      val sql =
        """
          | select conv(a, 2, 10) from(
          |   select a from values('100'),('1010') as data(a))
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
    {
      val sql =
        """
          | select conv(a, 200, 10) from(
          |   select a from values('100'),('1010') as data(a))
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
    {
      val sql =
        """
          | select conv(a, 16, 10) from(
          |   select a from values(10),(20) as data(a))
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
  }

  test("GLUTEN-3105: test json output format") {
    val sql =
      """
        |select to_json(struct(cast(id as string), id, 1.1, 1.1f, 1.1d)) from range(3)
        |""".stripMargin
    val sql1 =
      """
        | select to_json(named_struct('name', concat('/val/', id))) from range(3)
        |""".stripMargin
    // cast('nan' as double) output 'NaN' in Spark, 'nan' in CH
    // cast('inf' as double) output 'Infinity' in Spark, 'inf' in CH
    // ignore them temporarily
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
    runQueryAndCompare(sql1)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("GLUTEN-3501: test json output format with struct contains null value") {
    val sql =
      """
        |select to_json(struct(cast(id as string), null, id, 1.1, 1.1f, 1.1d)) from range(3)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("GLUTEN-3216: invalid read rel schema in aggregation") {
    val sql =
      """
        |select count(distinct(n_regionkey,n_nationkey)) from nation
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("Test plan json non-empty") {
    val df1 = spark
      .sql("""
             | select * from lineitem limit 1
             | """.stripMargin)
    val executedPlan1 = df1.queryExecution.executedPlan
    val lastStageTransformer1 = executedPlan1.find(_.isInstanceOf[WholeStageTransformer])
    executedPlan1.execute()
    assert(lastStageTransformer1.get.asInstanceOf[WholeStageTransformer].substraitPlanJson.nonEmpty)
  }

  test("GLUTEN-3140: Bug fix array_contains return null") {
    val create_table_sql =
      """
        | create table test_tbl_3140(id bigint, name string) using parquet;
        |""".stripMargin
    val insert_data_sql =
      """
        | insert into test_tbl_3140 values(1, "");
        |""".stripMargin
    spark.sql(create_table_sql)
    spark.sql(insert_data_sql)
    val select_sql =
      "select id, array_contains(split(name, ','), '2899') from test_tbl_3140 where id = 1"
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
  }

  test("GLUTEN-3149 convert Nan to int") {
    val sql =
      """
        | select cast(a as Int) as n from(
        |   select cast(s as Float) as a from(
        |     select if(n_name='ALGERIA', 'nan', '1.0') as s from nation
        |   ))""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-3149 convert Inf to int") {
    val sql =
      """
        | select n_regionkey, n is null, isnan(n),  cast(n as int) from (
        |   select n_regionkey, x, n_regionkey/(x) as n from (
        |     select n_regionkey, cast(n_nationkey as float) as x from  nation
        |   )t1
        | )t2""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-3149/GLUTEN-5580: Fix convert float to int") {
    val tbl_create_sql = "create table test_tbl_3149(a int, b bigint) using parquet"
    val tbl_insert_sql = "insert into test_tbl_3149 values(1, 0), (2, 171396196666200)"
    val select_sql_1 = "select cast(a * 1.0f/b as int) as x from test_tbl_3149 where a = 1"
    val select_sql_2 = "select cast(b/100 as int) from test_tbl_3149 where a = 2"
    spark.sql(tbl_create_sql)
    spark.sql(tbl_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql_1, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_2, true, { _ => })
    spark.sql("drop table test_tbl_3149")
  }

  test("GLUTEN-3289: Fix convert float to string") {
    val tbl_create_sql = "create table test_tbl_3289(id bigint, a float) using parquet"
    val tbl_insert_sql = "insert into test_tbl_3289 values(1, 2.0), (2, 2.1), (3, 2.2)"
    val select_sql_1 = "select cast(a as string), cast(a * 1.0f as string) from test_tbl_3289"
    val select_sql_2 =
      "select cast(cast(a as double) as string), cast(cast(a * 1.0f as double) as string) from test_tbl_3289"
    spark.sql(tbl_create_sql)
    spark.sql(tbl_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql_1, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_2, true, { _ => })
    spark.sql("drop table test_tbl_3289")
  }

  test("test in-filter contains null value (bigint)") {
    val sql = "select s_nationkey from supplier where s_nationkey in (null, 1, 2)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test in-filter contains null value (string)") {
    val sql = "select n_name from nation where n_name in ('CANADA', null, 'BRAZIL')"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-3287: diff when divide zero") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select 1/0f, 1/0.0d"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select n_nationkey / n_regionkey from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-3135/GLUTEN-7896: Bug fix to_date") {
    val create_table_sql =
      """
        | create table test_tbl_3135(id bigint, data string) using parquet
        |""".stripMargin
    val insert_data_sql =
      """
        |insert into test_tbl_3135 values
        |(1, '2023-09-02 23:59:59.299+11'),
        |(2, '2023-09-02 23:59:59.299-11'),
        |(3, '2023-09-02 00:00:01.333+11'),
        |(4, '2023-09-02 00:00:01.333-11'),
        |(5, '  2023-09-02 agdfegfew'),
        |(6, 'afe2023-09-02 11:22:33'),
        |(7, '1970-01-01 00:00:00'),
        |(8, '2024-3-2'),
        |(9, '2024-03-2'),
        |(10, '2024-03'),
        |(11, '2024-03-02 11:22:33')
        |""".stripMargin
    spark.sql(create_table_sql)
    spark.sql(insert_data_sql)

    val select_sql = "select id, to_date(data) from test_tbl_3135"
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })

    withSQLConf("spark.sql.legacy.timeParserPolicy" -> "corrected") {
      compareResultsAgainstVanillaSpark(
        "select id, to_date('2024-03-2 11:22:33', 'yyyy-MM-dd') from test_tbl_3135 where id = 11",
        true,
        { _ => })
    }
    withSQLConf("spark.sql.legacy.timeParserPolicy" -> "legacy") {
      compareResultsAgainstVanillaSpark(
        "select id, to_date(data, 'yyyy-MM-dd') from test_tbl_3135 where id = 11",
        true,
        { _ => })
    }
    spark.sql("drop table test_tbl_3135")
  }

  test("GLUTEN-3134: Bug fix left join not match") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "1B")) {
      val left_tbl_create_sql =
        "create table test_tbl_left_3134(id bigint, name string) using parquet"
      val right_tbl_create_sql =
        "create table test_tbl_right_3134(id string, name string) using parquet"
      val left_data_insert_sql =
        "insert into test_tbl_left_3134 values(2, 'a'), (3, 'b'), (673, 'c')"
      val right_data_insert_sql = "insert into test_tbl_right_3134 values('673', 'c')"
      val join_select_sql_1 = "select a.id, b.cnt from " +
        "(select id from test_tbl_left_3134) as a " +
        "left join (select id, 12 as cnt from test_tbl_right_3134 group by id) as b on a.id = b.id"
      val join_select_sql_2 = "select a.id, b.cnt from" +
        "(select id from test_tbl_left_3134) as a " +
        "left join (select id, count(1) as cnt from test_tbl_right_3134 group by id) as b on a.id = b.id"
      val join_select_sql_3 = "select a.id, b.cnt1, b.cnt2 from" +
        "(select id as id from test_tbl_left_3134) as a " +
        "left join (select id as id, 12 as cnt1, count(1) as cnt2 from test_tbl_right_3134 group by id) as b on a.id = b.id"
      val agg_select_sql_4 =
        "select id, 12 as cnt1, count(1) as cnt2 from test_tbl_left_3134 group by id"

      spark.sql(left_tbl_create_sql)
      spark.sql(right_tbl_create_sql)
      spark.sql(left_data_insert_sql)
      spark.sql(right_data_insert_sql)
      compareResultsAgainstVanillaSpark(join_select_sql_1, true, { _ => })
      compareResultsAgainstVanillaSpark(join_select_sql_2, true, { _ => })
      compareResultsAgainstVanillaSpark(join_select_sql_3, true, { _ => })
      compareResultsAgainstVanillaSpark(agg_select_sql_4, true, { _ => })
      spark.sql("drop table test_tbl_left_3134")
      spark.sql("drop table test_tbl_right_3134")
    }
  }

  test("GLUTEN-3534: Fix incorrect logic of judging whether supports pre-project for the shuffle") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runQueryAndCompare(
        s"""
           |select t1.l_orderkey, t2.o_orderkey, extract(year from t1.l_shipdate), t2.o_year,
           |t1.l_cnt, t2.o_cnt
           |from (
           |  select l_orderkey, l_shipdate, count(1) as l_cnt
           |  from lineitem
           |  group by l_orderkey, l_shipdate) t1
           |join (
           |  select o_orderkey, extract(year from o_orderdate) as o_year, count(1) as o_cnt
           |  from orders
           |  group by o_orderkey, o_orderdate) t2
           |on t1.l_orderkey = t2.o_orderkey
           | and extract(year from t1.l_shipdate) = o_year
           |order by t1.l_orderkey, t2.o_orderkey, t2.o_year, t1.l_cnt, t2.o_cnt
           |limit 100
           |
           |""".stripMargin
      )(df => {})

      runQueryAndCompare(
        s"""
           |select t1.l_orderkey, t2.o_orderkey, extract(year from t1.l_shipdate), t2.o_year
           |from (
           |  select l_orderkey, l_shipdate, count(1) as l_cnt
           |  from lineitem
           |  group by l_orderkey, l_shipdate) t1
           |join (
           |  select o_orderkey, extract(year from o_orderdate) as o_year, count(1) as o_cnt
           |  from orders
           |  group by o_orderkey, o_orderdate) t2
           |on t1.l_orderkey = t2.o_orderkey
           | and extract(year from t1.l_shipdate) = o_year
           |order by t1.l_orderkey, t2.o_orderkey, t2.o_year
           |limit 100
           |
           |""".stripMargin
      )(df => {})
    }
  }

  test("GLUTEN-3861: Fix parse exception when join postJoinFilter contains singularOrList") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      val sql =
        """
          |select t1.l_orderkey, t1.l_year, t2.o_orderkey, t2.o_year
          |from (
          |  select l_orderkey, extract(year from l_shipdate) as l_year, count(1) as l_cnt
          |  from lineitem
          |  group by l_orderkey, l_shipdate) t1
          |left join (
          |  select o_orderkey, extract(year from o_orderdate) as o_year, count(1) as o_cnt
          |  from orders
          |  group by o_orderkey, o_orderdate) t2
          |on t1.l_orderkey = t2.o_orderkey
          | and l_year in (1997, 1995, 1993)
          |order by t1.l_orderkey, t1.l_year, t2.o_orderkey, t2.o_year
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
  }

  test("GLUTEN-4376: Fix parse exception when parsing post_join_filter in JoinRelParser") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      val sql =
        """
          |SELECT
          |  n_nationkey,
          |  u_type
          |FROM
          |  (
          |    SELECT
          |      t1.n_nationkey,
          |      CASE
          |        WHEN t3.n_regionkey = 0 AND t2.n_name IS NULL THEN '0'
          |        WHEN t3.n_regionkey = 1 AND t2.n_name IS NULL THEN '1'
          |        ELSE 'other'
          |      END u_type
          |    FROM
          |      nation t1
          |      LEFT JOIN (
          |        SELECT
          |          n_nationkey,
          |          n_regionkey,
          |          n_name
          |        FROM
          |          nation
          |        WHERE
          |          n_regionkey IS NOT NULL
          |      ) t2 ON t1.n_nationkey = t2.n_nationkey
          |      JOIN (
          |        SELECT
          |          n_nationkey,
          |          MAX(IF(n_regionkey > 0, 1, 0)) AS n_regionkey
          |        FROM
          |          (
          |            SELECT
          |              n_nationkey,
          |              n_name,
          |              SUM(n_regionkey) AS n_regionkey
          |            FROM
          |              nation
          |            GROUP BY
          |              n_nationkey,
          |              n_name
          |          ) t
          |        GROUP BY
          |          n_nationkey
          |      ) t3 ON t1.n_nationkey = t3.n_nationkey
          |  )
          |WHERE
          |  u_type IN ('0', '1')
          |ORDER BY
          |  n_nationkey,
          |  u_type
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
  }

  test("GLUTEN-4914: Fix exceptions in ASTParser") {
    val sql =
      """
        |select t1.l_orderkey, t1.l_partkey, t1.l_shipdate, t2.o_custkey from (
        |select l_orderkey, l_partkey, l_shipdate from lineitem where l_orderkey < 1000 ) t1
        |join (
        |  select o_orderkey, o_custkey, o_orderdate from orders where o_orderkey < 1000
        |) t2 on t1.l_orderkey = t2.o_orderkey
        |and unix_timestamp(t1.l_shipdate, 'yyyy-MM-dd') < unix_timestamp(t2.o_orderdate, 'yyyy-MM-dd')
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-3467: Fix 'Names of tuple elements must be unique' error for ch backend") {
    val sql =
      """
        |select named_struct('a', r_regionkey, 'b', r_name, 'a', r_comment) as mergedValue
        |from region
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-3521: Bug fix substring index start from 1") {
    val tbl_create_sql = "create table test_tbl_3521(id bigint, name string) using parquet"
    val data_insert_sql = "insert into test_tbl_3521 values(1, 'abcdefghijk'), (2, '2023-10-32')"
    val select_sql =
      "select id, substring(name, 0), substring(name, 0, 3), substring(name from 0), substring(name from 0 for 100) from test_tbl_3521"
    spark.sql(tbl_create_sql)
    spark.sql(data_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_3521")
  }

  test("GLUTEN-3948: trunc function") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select trunc('2023-12-06', 'MM'), trunc('2023-12-06', 'YEAR'), trunc('2023-12-06', 'WEEK'), trunc('2023-12-06', 'QUARTER')"
      )(checkGlutenPlan[ProjectExecTransformer])

      runQueryAndCompare(
        "select trunc(l_shipdate, 'MM'), trunc(l_shipdate, 'YEAR'), trunc(l_shipdate, 'WEEK'), " +
          "trunc(l_shipdate, 'QUARTER') from lineitem"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-3934: log10/log2/ln") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select log10(n_regionkey), log10(-1.0), log10(0), log10(n_regionkey - 100000), " +
          "log2(n_regionkey), log2(-1.0), log2(0), log2(n_regionkey - 100000), " +
          "ln(n_regionkey), ln(-1.0), ln(0), ln(n_regionkey - 100000) from nation"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-6669: test cast string to boolean") {
    withSQLConf(withoutConstantFoldingAndNullPropagation) {
      runQueryAndCompare(
        "select cast('1' as boolean), cast('t' as boolean), cast('all' as boolean), cast('f' as boolean)"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-4032: fix shuffle read coredump after union") {
    val sql =
      """
        |select p_partkey from (
        |    select *, row_number() over (partition by p_partkey order by is_new) as rank from(
        |    select p_partkey, 0 as is_new from part where p_partkey is not null
        |    union all
        |    select p_partkey, p_partkey%2 as is_new from part where p_partkey is not null
        |  ) t1
        |) t2 where rank = 1 order by p_partkey limit 100
        |""".stripMargin
    runQueryAndCompare(sql) { _ => }
  }

  test("GLUTEN-7979: fix different output schema array<void> and array<string> before union") {
    val sql =
      """
        |select
        |    a.multi_peer_user_id,
        |    max(a.user_id) as max_user_id,
        |    max(a.peer_user_id) as max_peer_user_id,
        |    max(a.is_match_line) as max_is_match_line
        |from
        |(
        |    select
        |        t1.user_id,
        |        t1.peer_user_id,
        |        t1.is_match_line,
        |        t1.pk_type,
        |        t1.pk_start_time,
        |        t1.pk_end_time,
        |        t1.multi_peer_user_id
        |    from
        |    (
        |        select
        |            id as user_id,
        |            id as peer_user_id,
        |            id % 2 as is_match_line,
        |            id % 3 as pk_type,
        |            id as pk_start_time,
        |            id as pk_end_time,
        |            array() as multi_peer_user_id
        |        from range(10)
        |
        |        union all
        |
        |        select
        |            id as user_id,
        |            id as peer_user_id,
        |            id % 2 as is_match_line,
        |            id % 3 as pk_type,
        |            id as pk_start_time,
        |            id as pk_end_time,
        |            array('a', 'b', 'c') as multi_peer_user_id
        |        from range(10)
        |    ) t1
        |    where t1.user_id > 0 and t1.peer_user_id > 0
        |) a
        |group by
        |    a.multi_peer_user_id
        |""".stripMargin
    runQueryAndCompare(sql) { _ => }
  }

  test("GLUTEN-4190: crush on flattening a const null column") {
    val sql =
      """
        | select n_nationkey, rank() over (partition by n_regionkey, null order by n_nationkey)
        |from nation
        |""".stripMargin
    runQueryAndCompare(sql) { _ => }
  }

  test("GLUTEN-4115 aggregate without any function") {
    val sql =
      """
        | select n_regionkey, n_nationkey from nation group by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-4202: fixed hash partition on null rows") {
    val sql =
      """
        |select a,b,c,d, rank() over (partition by a+d, if (a=3, b, null) sort by c ) as r
        |from(
        |select a,b,c,d from
        |values(0,'d', 4.0,1), (1, 'a', 1.0, 0), (0, 'b', 2.0, 1), (1, 'c', 3.0, 0) as data(a,b,c,d)
        |)
        |""".stripMargin
    runQueryAndCompare(sql) { _ => }
  }

  test("GLUTEN-4085: Fix unix_timestamp/to_unix_timestamp") {
    val tbl_create_sql = "create table test_tbl_4085(id bigint, data string) using parquet"
    val data_insert_sql =
      "insert into test_tbl_4085 values(1, '2023-12-18'),(2, '2023-12-19'), (3, '2023-12-20'),  (4, '2024-10-15 07:35:26.486')"
    val select_sql =
      "select id, unix_timestamp(to_date(data), 'yyyy-MM-dd') from test_tbl_4085"
    val select_sql_1 = "select id, to_unix_timestamp(to_date(data)) from test_tbl_4085"
    val select_sql_2 = "select id, to_unix_timestamp(to_timestamp(data)) from test_tbl_4085"
    val select_sql_3 =
      "select id, unix_timestamp(data, 'yyyy-MM-dd HH:mm:ss') from test_tbl_4085 where id = 4"
    val select_sql_4 = "select id, unix_timestamp(data, 'yyyy-M-d') from test_tbl_4085 where id = 1"
    val select_sql_5 =
      "select id, unix_timestamp(data, 'yyyy-MM-dd HH') from test_tbl_4085 where id = 4"
    val select_sql_6 =
      "select id, unix_timestamp(data, 'yyyy-MM-dd HH:mm') from test_tbl_4085 where id = 4"
    val select_sql_7 =
      "select id, unix_timestamp(data, 'yyyy-MM-dd') from test_tbl_4085 where id = 4"
    val select_sql_8 =
      "select id, unix_timestamp(data, 'yyyy-MM-dd HH:mm:ss.SSS') from test_tbl_4085 where id = 4"
    val select_sql_9 =
      "select id, unix_timestamp(data, 'yyyy-MM-dd HH:mm:ss.S') from test_tbl_4085 where id = 4"
    val select_sql_10 =
      "select id, unix_timestamp(data, 'yyyy-MM-dd HH:mm:ss.') from test_tbl_4085 where id = 4"
    spark.sql(tbl_create_sql)
    spark.sql(data_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_1, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_2, true, { _ => })
    withSQLConf("spark.sql.legacy.timeParserPolicy" -> "LEGACY") {
      compareResultsAgainstVanillaSpark(select_sql_3, true, { _ => })
      compareResultsAgainstVanillaSpark(select_sql_4, true, { _ => })
      compareResultsAgainstVanillaSpark(select_sql_5, true, { _ => })
      compareResultsAgainstVanillaSpark(select_sql_6, true, { _ => })
      compareResultsAgainstVanillaSpark(select_sql_7, true, { _ => })
      compareResultsAgainstVanillaSpark(select_sql_8, true, { _ => })
      compareResultsAgainstVanillaSpark(select_sql_9, true, { _ => })
      compareResultsAgainstVanillaSpark(select_sql_10, true, { _ => })
    }
    spark.sql("drop table test_tbl_4085")
  }

  test("GLUTEN-3951: Bug fix floor") {
    val tbl_create_sql = "create table test_tbl_3951(d double) using parquet"
    val data_insert_sql = "insert into test_tbl_3951 values(1.0), (2.0), (2.5)"
    val select_sql =
      "select floor(d), floor(log10(d-1)), floor(log10(d-2)) from test_tbl_3951"
    spark.sql(tbl_create_sql)
    spark.sql(data_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_3951")
  }

  // will test on local NativeWriter and NativeReader on different types
  test("GLUTEN-4603: NativeWriter and NativeReader on different types") {
    val create_sql =
      """
        |create table if not exists test_shuffle_type(
        | id int,
        | str string,
        | f32 float,
        | f64 double,
        | dec decimal(10, 2),
        | a_str array<string>,
        | m_str map<string, string>,
        | st struct<x: int, y: string>
        |) using parquet
        |""".stripMargin
    val fill_sql =
      """
        |insert into test_shuffle_type select
        | l_orderkey as id, '123213', 1.2, 3.4, 5.6,
        | array('123', '22'), map('1', '2'),
        | named_struct('x', 1, 'y', 'sdfsd')
        |from lineitem limit 100000;
        |""".stripMargin
    val query_sql =
      """
        |select t1.l_orderkey, t2.* from
        | (select l_orderkey from lineitem limit 100000) as t1
        |left join test_shuffle_type as t2
        |on t1.l_orderkey = t2.id order by t1.l_orderkey limit 10;
        |""".stripMargin
    spark.sql(create_sql)
    spark.sql(fill_sql)
    compareResultsAgainstVanillaSpark(query_sql, true, { _ => })
    spark.sql("drop table test_shuffle_type")
  }

  test("GLUTEN-4521: Invalid result from grace mergeing aggregation with spill") {
    withSQLConf(
      (CHConfig.runtimeConfig("max_allowed_memory_usage_ratio_for_aggregate_merging"), "0.0001")) {
      val sql =
        """
          |select count(l_orderkey, l_partkey) from (
          |  select l_orderkey, l_partkey from lineitem group by l_orderkey, l_partkey
          |)
          |""".stripMargin
      runQueryAndCompare(sql) { _ => }
    }
  }

  test("GLUTEN-4279: Bug fix hour diff") {
    val tbl_create_sql = "create table test_tbl_4279(id bigint, data string) using parquet"
    val tbl_insert_sql = "insert into test_tbl_4279 values(1, '2024-01-04 11:22:33'), " +
      "(2, '2024-01-04 11:22:33.456+08'), (3, '2024'), (4, '2024-01'), (5, '2024-01-04'), " +
      "(6, '2024-01-04 12'), (7, '2024-01-04 12:12'), (8, '11:22:33'), (9, '22:33')," +
      "(10, '2024-01-04 '), (11, '2024-01-04 11.22.33.')"
    val select_sql = "select id, hour(data) from test_tbl_4279 order by id"
    spark.sql(tbl_create_sql)
    spark.sql(tbl_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_4279")
  }

  test("GLUTEN-4997/GLUTEN-5352: Bug fix year diff") {
    val tbl_create_sql = "create table test_tbl_4997(id bigint, data string) using parquet"
    val tbl_insert_sql =
      "insert into test_tbl_4997 values(1, '2024-01-03'), (2, '2024'), (3, '2024-'), (4, '2024-1')," +
        "(5, '2024-1-'), (6, '2024-1-3'), (7, '2024-1-3T'), (8, '21-0'), (9, '12-9'), (10, '-1')," +
        "(11, '999'), (12, '1000'), (13, '9999'), (15, '2024-04-19 00:00:00-12'), (16, '2024-04-19 00:00:00+12'), " +
        "(17, '2024-04-19 23:59:59-12'), (18, '2024-04-19 23:59:59+12'), (19, '1899-12-01')," +
        "(20, '2024:12'), (21, '2024ABC'), (22, NULL), (23, '0'), (24, '')"
    val select_sql = "select id, year(data) from test_tbl_4997 order by id"
    spark.sql(tbl_create_sql)
    spark.sql(tbl_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_4997")
  }

  test("aggregate function approx_percentile") {
    // single percentage
    val sql1 = "select l_linenumber % 10, approx_percentile(l_extendedprice, 0.5) " +
      "from lineitem group by l_linenumber % 10"
    runQueryAndCompare(sql1) { _ => }

    // multiple percentages
    val sql2 =
      "select l_linenumber % 10, approx_percentile(l_extendedprice, array(0.1, 0.2, 0.3)) " +
        "from lineitem group by l_linenumber % 10"
    runQueryAndCompare(sql2) { _ => }
  }

  test("aggregate function percentile") {
    // single percentage
    val sql1 = "select l_linenumber % 10, percentile(l_extendedprice, 0.5) " +
      "from lineitem group by l_linenumber % 10"
    runQueryAndCompare(sql1) { _ => }

    // multiple percentages
    val sql2 =
      "select l_linenumber % 10, percentile(l_extendedprice, array(0.1, 0.2, 0.3)) " +
        "from lineitem group by l_linenumber % 10"
    runQueryAndCompare(sql2) { _ => }
  }

  test("GLUTEN-5096: Bug fix regexp_extract diff") {
    val tbl_create_sql = "create table test_tbl_5096(id bigint, data string) using parquet"
    val tbl_insert_sql = "insert into test_tbl_5096 values(1, 'abc'), (2, 'abc\n')"
    val select_sql = "select id, regexp_extract(data, '(abc)$', 1) from test_tbl_5096"
    spark.sql(tbl_create_sql)
    spark.sql(tbl_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_5096")
  }

  test("GLUTEN-5896: Bug fix greatest/least diff") {
    val tbl_create_sql =
      "create table test_tbl_5896(id bigint, x1 int, x2 int, x3 int) using parquet"
    val tbl_insert_sql =
      "insert into test_tbl_5896 values(1, 12, NULL, 13), (2, NULL, NULL, NULL), (3, 11, NULL, NULL), (4, 10, 9, 8)"
    val select_sql = "select id, greatest(x1, x2, x3), least(x1, x2, x3) from test_tbl_5896"
    spark.sql(tbl_create_sql)
    spark.sql(tbl_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_5896")
  }

  test("test left with len -1") {
    val tbl_create_sql =
      "create table test_left(col string) using parquet"
    val tbl_insert_sql =
      "insert into test_left values('test1'), ('test2')"
    spark.sql(tbl_create_sql)
    spark.sql(tbl_insert_sql)
    compareResultsAgainstVanillaSpark("select left(col, -1) from test_left", true, { _ => })
    compareResultsAgainstVanillaSpark("select left(col, -2) from test_left", true, { _ => })
    compareResultsAgainstVanillaSpark("select substring(col, 0, -1) from test_left", true, { _ => })
    spark.sql("drop table test_left")
  }

  test("Inequal join support") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      spark.sql("create table ineq_join_t1 (key bigint, value bigint) using parquet")
      spark.sql("create table ineq_join_t2 (key bigint, value bigint) using parquet")
      spark.sql("insert into ineq_join_t1 values(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      spark.sql("insert into ineq_join_t2 values(2, 2), (2, 1), (3, 3), (4, 6), (5, 3)")
      val sql1 =
        """
          | select t1.key, t1.value, t2.key, t2.value from ineq_join_t1 as t1
          | left join ineq_join_t2 as t2
          | on t1.key = t2.key and t1.value > t2.value
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql1, true, { _ => })

      val sql2 =
        """
          | select t1.key, t1.value from ineq_join_t1 as t1
          | left join ineq_join_t2 as t2
          | on t1.key = t2.key and t1.value > t2.value and t1.value > t2.key
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql2, true, { _ => })
      spark.sql("drop table ineq_join_t1")
      spark.sql("drop table ineq_join_t2")
    }
  }

  test("GLUTEN-5910: Fix ASTLiteral type is lost in CH") {
    spark.sql("create table test_tbl_5910_0(c_time bigint, type int) using parquet")
    spark.sql("create table test_tbl_5910_1(type int) using parquet")
    spark.sql("insert into test_tbl_5910_0 values(1717209159, 12)")
    spark.sql("insert into test_tbl_5910_1 values(12)")
    val select_sql =
      """
        | select t1.cday, t2.type from (
        | select type, to_date(from_unixtime(c_time)) as cday from test_tbl_5910_0 ) t1
        | left join (
        | select type, "2024-06-01" as cday from test_tbl_5910_1 ) t2
        | on t1.cday = t2.cday and t1.type = t2.type
        |""".stripMargin
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_5910_0")
    spark.sql("drop table test_tbl_5910_1")
  }

  test("GLUTEN-4451: Fix schema may be changed by filter") {
    val create_sql =
      """
        |create table if not exists test_tbl_4451(
        |  month_day string,
        |  month_dif int,
        |  is_month_new string,
        |  country string,
        |  os string,
        |  mr bigint
        |) using parquet
        |PARTITIONED BY (
        |  day string,
        |  app_name string)
        |""".stripMargin
    val insert_sql1 =
      "INSERT into test_tbl_4451 partition (day='2024-06-01', app_name='abc') " +
        "values('2024-06-01', 0, '1', 'CN', 'iOS', 100)"
    val insert_sql2 =
      "INSERT into test_tbl_4451 partition (day='2024-06-01', app_name='abc') " +
        "values('2024-06-01', 0, '1', 'CN', 'iOS', 50)"
    val insert_sql3 =
      "INSERT into test_tbl_4451 partition (day='2024-06-01', app_name='abc') " +
        "values('2024-06-01', 1, '1', 'CN', 'iOS', 80)"
    spark.sql(create_sql)
    spark.sql(insert_sql1)
    spark.sql(insert_sql2)
    spark.sql(insert_sql3)
    val select_sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    month_day,
        |    country,
        |    if(os = 'ALite','Android',os) AS os,
        |    is_month_new,
        |    nvl(sum(if(month_dif = 0, mr, 0)),0) AS `month0_n`,
        |    nvl(sum(if(month_dif = 1, mr, 0)) / sum(if(month_dif = 0, mr, 0)),0) AS `month1_rate`,
        |    '2024-06-18' as day,
        |    app_name
        |  FROM test_tbl_4451
        |  GROUP BY month_day,country,if(os = 'ALite','Android',os),is_month_new,app_name
        |) tt
        |WHERE month0_n > 0 AND month1_rate <= 1  AND os IN ('all','Android','iOS')
        |  AND app_name IS NOT NULL
        |""".stripMargin
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_tbl_4451")
  }

  test("array functions date add") {
    spark.sql("create table tb_date(day Date) using parquet")
    spark.sql("""
                |insert into tb_date values
                |(cast('2024-06-01' as Date)),
                |(cast('2024-06-02' as Date)),
                |(cast('2024-06-03' as Date)),
                |(cast('2024-06-04' as Date)),
                |(cast('2024-06-05' as Date))
                |""".stripMargin)
    val sql1 = """
                 |select * from tb_date where day between
                 |'2024-06-01' and
                 |cast('2024-06-01' as Date) + interval 2 day
                 |order by day
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })
    val sql2 = """
                 |select * from tb_date where day between
                 |'2024-06-01' and
                 |cast('2024-06-01' as Date) + interval 48 hour
                 |order by day
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })
    val sql3 = """
                 |select * from tb_date where day between
                 |'2024-06-01' and
                 |cast('2024-06-05' as Date) - interval 2 day
                 |order by day
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => })
    val sql4 = """
                 |select * from tb_date where day between
                 |'2024-06-01' and
                 |cast('2024-06-05' as Date) - interval 48 hour
                 |order by day
                 |""".stripMargin
    compareResultsAgainstVanillaSpark(sql4, true, { _ => })

    spark.sql("drop table tb_date")
  }

  test("test CartesianProductExec") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      val sql = """
                  |select t1.n_regionkey, t2.n_regionkey from
                  |(select n_regionkey from nation) t1
                  |cross join
                  |(select n_regionkey from nation) t2
                  |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
  }

  test("GLUTEN-6583 serializing bug in aggregating with nullable compilated type keys") {
    val sql = """
                |select n_regionkey, x, count(1) from (
                |  select n_regionkey, if(n_regionkey = 'xx', null, x) as x from (
                |    select n_regionkey, array(n_name, if(n_name != 'KENYA', n_name, null)) as x
                |    from nation
                |  )
                |) group by n_regionkey, x;
                |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-341: Support BHJ + isNullAwareAntiJoin for the CH backend") {
    def checkBHJWithIsNullAwareAntiJoin(df: DataFrame): Unit = {
      val bhjs = df.queryExecution.executedPlan.collect {
        case bhj: CHBroadcastHashJoinExecTransformer if bhj.isNullAwareAntiJoin => true
      }
      assert(bhjs.size == 1)
    }

    val sql =
      s"""
         |SELECT
         |    p_brand,
         |    p_type,
         |    p_size,
         |    count(DISTINCT ps_suppkey) AS supplier_cnt
         |FROM
         |    partsupp,
         |    part
         |WHERE
         |    p_partkey = ps_partkey
         |    AND p_brand <> 'Brand#45'
         |    AND p_type NOT LIKE 'MEDIUM POLISHED%'
         |    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
         |    AND ps_suppkey NOT IN (
         |        SELECT
         |            s_suppkey
         |        FROM
         |            supplier
         |        WHERE
         |            s_comment is null)
         |GROUP BY
         |    p_brand,
         |    p_type,
         |    p_size
         |ORDER BY
         |    supplier_cnt DESC,
         |    p_brand,
         |    p_type,
         |    p_size;
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        checkBHJWithIsNullAwareAntiJoin(df)
      })

    val sql1 =
      s"""
         |SELECT
         |    p_brand,
         |    p_type,
         |    p_size,
         |    count(DISTINCT ps_suppkey) AS supplier_cnt
         |FROM
         |    partsupp,
         |    part
         |WHERE
         |    p_partkey = ps_partkey
         |    AND p_brand <> 'Brand#45'
         |    AND p_type NOT LIKE 'MEDIUM POLISHED%'
         |    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
         |    AND ps_suppkey NOT IN (
         |        SELECT
         |            s_suppkey
         |        FROM
         |            supplier
         |        WHERE
         |            s_comment LIKE '%Customer%Complaints11%')
         |GROUP BY
         |    p_brand,
         |    p_type,
         |    p_size
         |ORDER BY
         |    supplier_cnt DESC,
         |    p_brand,
         |    p_type,
         |    p_size;
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql1,
      compareResult = true,
      df => {
        checkBHJWithIsNullAwareAntiJoin(df)
      })

    val sql2 =
      s"""
         |select * from partsupp
         |where
         |ps_suppkey NOT IN (SELECT suppkey FROM VALUES (50), (null) sub(suppkey))
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql2,
      compareResult = true,
      df => {
        checkBHJWithIsNullAwareAntiJoin(df)
      })

    val sql3 =
      s"""
         |select * from partsupp
         |where
         |ps_suppkey NOT IN (SELECT suppkey FROM VALUES (50) sub(suppkey) WHERE suppkey > 100)
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql3,
      compareResult = true,
      df => {
        checkBHJWithIsNullAwareAntiJoin(df)
      })

    val sql4 =
      s"""
         |select * from partsupp
         |where
         |ps_suppkey NOT IN (SELECT suppkey FROM VALUES (50), (60) sub(suppkey))
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql4,
      compareResult = true,
      df => {
        checkBHJWithIsNullAwareAntiJoin(df)
      })

    val sql5 =
      s"""
         |select * from partsupp
         |where
         |ps_suppkey NOT IN (SELECT suppkey FROM VALUES (null) sub(suppkey))
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql5,
      compareResult = true,
      df => {
        checkBHJWithIsNullAwareAntiJoin(df)
      })

    withSQLConf(("spark.sql.adaptive.enabled", "true")) {
      def checkAQEBHJWithIsNullAwareAntiJoin(df: DataFrame, isNullAwareBhjCnt: Int = 1): Unit = {
        val bhjs = collect(df.queryExecution.executedPlan) {
          case bhj: CHBroadcastHashJoinExecTransformer if bhj.isNullAwareAntiJoin => true
        }
        assert(bhjs.size == isNullAwareBhjCnt)
      }

      val sql6 =
        s"""
           |select * from partsupp
           |where
           |ps_suppkey NOT IN (SELECT suppkey FROM VALUES (null), (6) sub(suppkey))
           |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql6,
        compareResult = true,
        df => {
          checkAQEBHJWithIsNullAwareAntiJoin(df, 0)
        })

      val sql7 =
        s"""
           |select * from partsupp
           |where
           |cast(ps_suppkey AS INT) NOT IN (SELECT suppkey FROM VALUES (null), (6) sub(suppkey))
           |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql7,
        compareResult = true,
        df => {
          checkAQEBHJWithIsNullAwareAntiJoin(df, 0)
        })

      val sql8 =
        s"""
           |select * from partsupp
           |where
           |ps_suppkey NOT IN (SELECT suppkey FROM VALUES (5), (6) sub(suppkey))
           |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql8,
        compareResult = true,
        df => {
          checkAQEBHJWithIsNullAwareAntiJoin(df)
        })
    }

  }

  test("soundex") {
    runQueryAndCompare("select soundex(c_mktsegment) from customer limit 50") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("GLUTEN-7220: Fix bug of grouping sets") {
    val table_create_sql = "create table test_tbl_7220(id bigint, name string) using parquet"
    val insert_data_sql = "insert into test_tbl_7220 values(1, 'a123'), (2, 'a124'), (3, 'a125')"
    val query_sql = "select '2024-08-26' as day, id,name from" +
      " (select id, name from test_tbl_7220 group by id, name grouping sets((id),(id,name))) " +
      " where name  = 'a124'"
    spark.sql(table_create_sql)
    spark.sql(insert_data_sql)
    compareResultsAgainstVanillaSpark(query_sql, true, { _ => })
    spark.sql("drop table test_tbl_7220")
  }

  test("GLLUTEN-7647 lazy expand") {
    def checkLazyExpand(df: DataFrame): Unit = {
      val expands = collectWithSubqueries(df.queryExecution.executedPlan) {
        case e: ExpandExecTransformer if e.child.isInstanceOf[HashAggregateExecBaseTransformer] =>
          e
      }
      assert(expands.size == 1)
    }
    var sql =
      """
        |select n_regionkey, n_nationkey,
        |sum(n_regionkey), count(n_name), max(n_regionkey), min(n_regionkey)
        |from nation group by n_regionkey, n_nationkey with cube
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)

    sql = """
            |select n_regionkey, n_nationkey, sum(n_regionkey), count(distinct n_name)
            |from nation group by n_regionkey, n_nationkey with cube
            |order by n_regionkey, n_nationkey
            |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)

    sql = """
            |select * from(
            |select n_regionkey, n_nationkey,
            |sum(n_regionkey), count(n_name), max(n_regionkey), min(n_regionkey)
            |from nation group by n_regionkey, n_nationkey with cube
            |) where n_regionkey != 0
            |order by n_regionkey, n_nationkey
            |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)

    sql = """
            |select * from(
            |select n_regionkey, n_nationkey,
            |sum(n_regionkey), count(distinct n_name), max(n_regionkey), min(n_regionkey)
            |from nation group by n_regionkey, n_nationkey with cube
            |) where n_regionkey != 0
            |order by n_regionkey, n_nationkey
            |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)

    sql = """
            |select x, n_regionkey, n_nationkey,
            |sum(n_regionkey), count(n_name), max(n_regionkey), min(n_regionkey)
            |from (select '123' as x, * from nation) group by x, n_regionkey, n_nationkey with cube
            |order by x, n_regionkey, n_nationkey
            |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)
  }

  test("GLUTEN-7647 lazy expand for avg and sum") {
    val create_table_sql =
      """
        |create table test_7647(x bigint, y bigint, z bigint, v decimal(10, 2)) using parquet
        |""".stripMargin
    spark.sql(create_table_sql)
    val insert_data_sql =
      """
        |insert into test_7647 values
        |(1, 1, 1, 1.0),
        |(2, 2, 2, 2.0),
        |(3, 3, 3, 3.0),
        |(2,2,1, 4.0)
        |""".stripMargin
    spark.sql(insert_data_sql)

    def checkLazyExpand(df: DataFrame): Unit = {
      val expands = collectWithSubqueries(df.queryExecution.executedPlan) {
        case e: ExpandExecTransformer if e.child.isInstanceOf[HashAggregateExecBaseTransformer] =>
          e
      }
      assert(expands.size == 1)
    }

    var sql = "select x, y, avg(z), sum(v) from test_7647 group by x, y with cube order by x, y"
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)
    sql =
      "select x, y, count(distinct z), avg(v) from test_7647 group by x, y with cube order by x, y"
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)
    sql =
      "select x, y, count(distinct z), sum(v) from test_7647 group by x, y with cube order by x, y"
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkLazyExpand)
    spark.sql("drop table if exists test_7647")
  }

  test("GLUTEN-7905 get topk of window by aggregate") {
    withSQLConf(
      (CHConfig.runtimeSettings("enable_window_group_limit_to_aggregate"), "true"),
      (CHConfig.runtimeSettings("window.aggregate_topk_high_cardinality_threshold"), "2.0")
    ) {
      def checkWindowGroupLimit(df: DataFrame): Unit = {
        val expands = collectWithSubqueries(df.queryExecution.executedPlan) {
          case e: CHAggregateGroupLimitExecTransformer => e
          case wgl: CHWindowGroupLimitExecTransformer => wgl
        }
        assert(expands.nonEmpty)
      }
      spark.sql("create table test_win_top (a string, b int, c int) using parquet")
      spark.sql("""
                  |insert into test_win_top values
                  |('a', 3, 3), ('a', 1, 5), ('a', 2, 2), ('a', null, null), ('a', null, 1),
                  |('b', 1, 1), ('b', 2, 1),
                  |('c', 2, 3)
                  |""".stripMargin)
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c,
          |row_number() over (partition by a order by b desc nulls first, c nulls last) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b desc, c nulls last) as r
          |from test_win_top
          |)where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b asc nulls first, c) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b asc nulls last) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b , c) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )

      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b, c, a) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      spark.sql("drop table if exists test_win_top")
    }

  }

  test("GLUTEN-7905 get topk of window by window") {
    withSQLConf(
      (CHConfig.runtimeSettings("enable_window_group_limit_to_aggregate"), "true"),
      (CHConfig.runtimeSettings("window.aggregate_topk_high_cardinality_threshold"), "0.0")
    ) {
      def checkWindowGroupLimit(df: DataFrame): Unit = {
        // for spark 3.5, CHWindowGroupLimitExecTransformer is in used
        val expands = collectWithSubqueries(df.queryExecution.executedPlan) {
          case e: CHAggregateGroupLimitExecTransformer => e
          case wgl: CHWindowGroupLimitExecTransformer => wgl
        }
        assert(expands.nonEmpty)
      }
      spark.sql("drop table if exists test_win_top")
      spark.sql("create table test_win_top (a string, b int, c int) using parquet")
      spark.sql("""
                  |insert into test_win_top values
                  |('a', 3, 3), ('a', 1, 5), ('a', 2, 2), ('a', null, null), ('a', null, 1),
                  |('b', 1, 1), ('b', 2, 1),
                  |('c', 2, 3)
                  |""".stripMargin)
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c,
          |row_number() over (partition by a order by b desc nulls first, c nulls last) as r
          |from test_win_top
          |)where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b desc, c nulls last) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          | select * from(
          |select a, b, c, row_number() over (partition by a order by b asc nulls first, c) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b asc nulls last) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      compareResultsAgainstVanillaSpark(
        """
          |select * from(
          |select a, b, c, row_number() over (partition by a order by b , c) as r
          |from test_win_top)
          |where r <= 1
          |""".stripMargin,
        compareResult = true,
        checkWindowGroupLimit
      )
      spark.sql("drop table if exists test_win_top")
    }

  }

  test("GLUTEN-7759: Fix bug of agg pre-project push down") {
    val table_create_sql =
      "create table test_tbl_7759(id bigint, name string, day string) using parquet"
    val insert_data_sql =
      "insert into test_tbl_7759 values(1, 'a123', '2024-11-01'),(2, 'a124', '2024-11-01')"
    val query_sql =
      """
        |select distinct day, name from(
        |select '2024-11-01' as day
        |,coalesce(name,'all') name
        |,cnt
        |from
        |(
        |select count(distinct id) as cnt, name
        |from test_tbl_7759
        |group by name
        |with cube
        |)) limit 10
        |""".stripMargin
    spark.sql(table_create_sql)
    spark.sql(insert_data_sql)
    compareResultsAgainstVanillaSpark(query_sql, true, { _ => })
    spark.sql("drop table test_tbl_7759")
  }

  test("GLUTEN-8253: Fix cast failed when in-filter with tuple values") {
    spark.sql("drop table if exists test_filter")
    spark.sql("create table test_filter(c1 string, c2 string) using parquet")
    spark.sql(s"""
                 |insert into test_filter values
                 |('a1', 'b1'), ('a2', 'b2'), ('a3', 'b3'), ('a4', 'b4'), ('a5', 'b5'),
                 |('a6', 'b6'), ('a7', 'b7'), ('a8', 'b8'), ('a9', 'b9'), ('a10', 'b10'),
                 |('a11', 'b11'), ('a12', null), (null, 'b13'), (null, null)
                 |""".stripMargin)
    val sql = "select * from test_filter where (c1, c2) in (('a1', 'b1'), ('a2', 'b2'))"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-8343: Cast number to decimal") {
    val create_table_sql = "create table test_tbl_8343(id bigint, d bigint, f double) using parquet"
    val insert_data_sql =
      "insert into test_tbl_8343 values(1, 55, 55.12345), (2, 137438953483, 137438953483.12345), (3, -12, -12.123), (4, 0, 0.0001), (5, NULL, NULL), (6, %d, NULL), (7, %d, NULL)"
        .format(Double.MaxValue.longValue(), Double.MinValue.longValue())
    val query_sql =
      "select cast(d as decimal(1, 0)), cast(d as decimal(9, 1)), cast((f-55.12345) as decimal(9,1)), cast(f as decimal(4,2)), " +
        "cast(f as decimal(32, 3)), cast(f as decimal(2, 1)), cast(d as decimal(38,3)) from test_tbl_8343"
    spark.sql(create_table_sql)
    spark.sql(insert_data_sql)
    compareResultsAgainstVanillaSpark(query_sql, true, { _ => })
    spark.sql("drop table test_tbl_8343")
  }

  test("GLUTEN-8995: Fix column not found in row_number") {
    val select_sql =
      "select  id from (select id ,row_number() over (partition by id order by id desc) as rank from range(1)) c1 where  rank =1"
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
  }

  test("GLUTEN-8974 accelerate join + aggregate by any join") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      // check EliminateDeduplicateAggregateWithAnyJoin is effective
      def checkOnlyOneAggregate(df: DataFrame): Unit = {
        val aggregates = collectWithSubqueries(df.queryExecution.executedPlan) {
          case e: HashAggregateExecBaseTransformer => e
        }
        assert(aggregates.size == 1)
      }

      val sql1 =
        """
          |select t1.*, t2.* from nation as t1
          |left join (select n_regionkey, n_nationkey from nation group by n_regionkey, n_nationkey) t2
          |on t1.n_regionkey = t2.n_regionkey and t1.n_nationkey = t2.n_nationkey
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql1, compareResult = true, checkOnlyOneAggregate)

      val sql2 =
        """
          |select t1.*, t2.* from nation as t1
          |left join (select n_nationkey, n_regionkey from nation group by n_regionkey, n_nationkey) t2
          |on t1.n_regionkey = t2.n_regionkey and t1.n_nationkey = t2.n_nationkey
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql2, compareResult = true, checkOnlyOneAggregate)

      val sql3 =
        """
          |select t1.*, t2.* from nation as t1
          |left join (select n_regionkey from nation group by n_regionkey) t2
          |on t1.n_regionkey = t2.n_regionkey
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql3, compareResult = true, checkOnlyOneAggregate)
    }
  }

  test("GLUTEN-9177: Fix diff of parse_url") {
    val create_tbl_sql = "create table test_9177(id bigint, s string) using parquet"
    val insert_data_sql = "insert into test_9177 values(1, 'http://user:pass@locahost')," +
      "(2, 'http://user:pass@localhost/a/b/c'), (3, 'http://user:pass@localhost:10010/a/b/c')"
    val select_sql = "select id, parse_url(s, 'HOST') from test_9177"
    spark.sql(create_tbl_sql)
    spark.sql(insert_data_sql)
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    spark.sql("drop table test_9177")
  }
}
// scalastyle:on line.size.limit
