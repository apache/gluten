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

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}

import org.scalatest.DoNotDiscover

@DoNotDiscover
class GlutenClickHouseTPCHNullableSuite extends NullableMergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "SNAPPY")
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

  test("TPCH Q9 without BHJ") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      check(9)
    }
  }

  test("test 'select count(*) from table'") {
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

  test("test 'select count(1)'") {
    val result = runSql("""
                          |select count(1) from lineitem
                          |where l_quantity < 20
                          |""".stripMargin) { _ => }
    assert(result.head.getLong(0) == 227302L)
  }

  test("test 'GLUTEN-5016'") {
    val sql =
      """
        |SELECT
        |   sum(l_quantity) AS sum_qty
        |FROM
        |   lineitem
        |WHERE
        |   l_shipdate <= date'1998-09-02'
        |""".stripMargin
    runSql(sql) { _ => }
  }

  test("test rewrite date conversion") {
    val sqlStr =
      """
        |SELECT
        |to_date(
        |  from_unixtime(
        |    unix_timestamp(date_format(l_shipdate, 'yyyyMMdd'), 'yyyyMMdd')
        |  )
        |)
        |FROM lineitem
        |limit 10
        |""".stripMargin

    Seq(("true", false), ("false", true)).foreach(
      conf => {
        withSQLConf((CHConfig.ENABLE_CH_REWRITE_DATE_CONVERSION.key, conf._1)) {
          runSql(sqlStr)(
            df => {
              val project = df.queryExecution.executedPlan.collect {
                case project: ProjectExecTransformer => project
              }
              assert(project.size == 1)
              assert(
                project.head.projectList.head
                  .asInstanceOf[Alias]
                  .child
                  .toString()
                  .contains("from_unixtime") == conf._2)
            })
        }
      })
  }

  test("Fix array_sort issue-9038") {
    val sql = "select array_sort(split(l_comment, ' ')) from lineitem limit 10"
    runSql(sql) { _ => }
  }
}
