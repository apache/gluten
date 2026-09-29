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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.test.AllDataTypesWithComplexType
import org.apache.gluten.test.AllDataTypesWithComplexType.genTestData

import org.apache.spark.SparkConf
class GlutenClickhouseCountDistinctSuite extends GlutenClickHouseWholeStageTransformerSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.sql.countDistinctWithoutExpand", "true")
      .set("spark.sql.adaptive.enabled", "false")
  }

  test("check count distinct correctness") {
    // simple case
    var sql = "select count(distinct(a))  from values (1,1,1), (2,2,2) as data(a,b,c)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })

    // with null
    sql = "select count(distinct(a))  from " +
      "values (1,1,1), (2,2,2), (1,3,3), (null,4,4), (null,5,5) as data(a,b,c)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })

    // three CD
    sql = "select count(distinct(b)), count(distinct(a)),count(distinct c)  from " +
      "values (0, null,1), (0,null,1), (1, 1,1), (2, 2, 1) ,(2,2,2) as data(a,b,c)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })

    // count distinct with multiple args
    sql = "select count(distinct(a,b)), count(distinct(a,b,c))  from " +
      "values (0, null,1), (0,null,1), (1, 1,1), (2, 2, 1) ,(2,2,2) as data(a,b,c)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("check count distinct execution plan") {
    val sql =
      "select count(distinct(b)), count(distinct a, b)  from " +
        "values (0, null,1), (1, 1,1), (2, 2,1), (1, 2,1) ,(2,2,2) as data(a,b,c) group by c"

    val df = spark.sql(sql)
    GlutenQueryComparisonTest.checkFallBack(df)

    val planExecs = df.queryExecution.executedPlan.collect {
      case aggTransformer: HashAggregateExecBaseTransformer => aggTransformer
    }

    planExecs.head.aggregateExpressions.foreach {
      expr => assert(expr.toString().startsWith("countdistinct"))
    }
    planExecs(1).aggregateExpressions.foreach {
      expr => assert(expr.toString().startsWith("partial_countdistinct"))
    }
  }

  test("test distinct with not-distinct") {
    val sql = "select a,  count(distinct(b)), sum(c)  from " +
      "values (0, null,1), (0,null,1), (1, 1,1), (2, 2, 1) ,(2,2,2) as data(a,b,c) group by a"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("check all data types") {
    spark.createDataFrame(genTestData()).createOrReplaceTempView("all_data_types")

    // Vanilla does not support map
    for (
      field <- AllDataTypesWithComplexType().getClass.getDeclaredFields.filterNot(
        p => p.getName.startsWith("map"))
    ) {
      val sql = s"select count(distinct(${field.getName})) from all_data_types"
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
      spark.sql(sql).show
    }

    // just test success run
    for (
      field <- AllDataTypesWithComplexType().getClass.getDeclaredFields.filter(
        p => p.getName.startsWith("map"))
    ) {
      val sql = s"select count(distinct(${field.getName})) from all_data_types"
      spark.sql(sql).show
    }
  }

  test("check count distinct with agg fallback") {
    val sql = "select count(distinct(a,b)) , skewness(b) from " +
      "values (0, null,1), (0,null,1), (1, 1,1), (2, 2, 1) ,(2,2,2),(3,3,3) as data(a,b,c)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })

    val sqlWithKeys = "select a, count(distinct(b)) , skewness(b) from " +
      "values (0, null,1), (0,null,1), (1, 1,1), (2, 2, 1) ,(2,2,2),(3,3,3) as data(a,b,c) " +
      "group by a"
    compareResultsAgainstVanillaSpark(sqlWithKeys, true, { _ => })
  }

  test("check count distinct with expr fallback") {
    // try_add is not supported, will cause fallback after a project operator
    val sql = s"""
      select count(distinct(a,b)) , try_add(c,b) from
      values (0, null,1), (0,null,2), (1, 1,4) as data(a,b,c) group by try_add(c,b)
      """
    val df = spark.sql(sql)
    GlutenQueryComparisonTest.checkFallBack(df, noFallback = isSparkVersionGE("3.5"))
  }

  test("check count distinct with filter") {
    val sql = "select count(distinct(a,b)) FILTER (where c <3) from " +
      "values (0, null,1), (0,null,1), (1, 1,1), (2, 2, 1) ,(2,2,2),(3,3,3) as data(a,b,c)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test(
    "Gluten-5618: [CH] Fix 'Position x is out of bound in Block' error " +
      "when executing count distinct") {

    withSQLConf((GlutenConfig.ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND.key, "false")) {
      val sql =
        """
          |select count(distinct a, b, c)  from
          |values (0, null, 1), (1, 1, 1), (2, 2, 1), (1, 2, 1) ,(2, 2, 2) as data(a,b,c) group by c
          |""".stripMargin

      compareResultsAgainstVanillaSpark(
        sql,
        true,
        {
          df =>
            {

              val planExecs = df.queryExecution.executedPlan.collect {
                case aggTransformer: HashAggregateExecBaseTransformer => aggTransformer
              }

              planExecs.head.aggregateExpressions.foreach {
                expr => assert(expr.toString().startsWith("count("))
              }
              planExecs(1).aggregateExpressions.foreach {
                expr => assert(expr.toString().startsWith("partial_count("))
              }
            }
        }
      )
    }

    val sql =
      """
        |select count(distinct a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
        |from values
        |(0, null, 1, 0, null, 1, 0, 5, 1, 0),
        |(null, 1, 1, null, 1, 1, null, 1, 1, 3),
        |(2, 2, 1, 2, 2, 1, 2, 2, 1, 2),
        |(1, 2, null, 1, 2, null, 1, 2, 3, 1),
        |(2, 2, 2, 2, 2, 2, 2, 2, 2, 2)
        |as data(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
        |group by a10
        |""".stripMargin

    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          {

            val planExecs = df.queryExecution.executedPlan.collect {
              case aggTransformer: HashAggregateExecBaseTransformer => aggTransformer
            }

            planExecs.head.aggregateExpressions.foreach {
              expr => assert(expr.toString().startsWith("count("))
            }
            planExecs(1).aggregateExpressions.foreach {
              expr => assert(expr.toString().startsWith("partial_count("))
            }
          }
      }
    )

    val sql1 =
      """
        |select count(distinct a, b, c)
        |from
        |values (0, null, 1), (1, 1, 1), (null, 2, 1), (1, 2, 1) ,(2, 2, null)
        |as data(a,b,c)
        |group by c
        |""".stripMargin

    compareResultsAgainstVanillaSpark(
      sql1,
      true,
      {
        df =>
          {

            val planExecs = df.queryExecution.executedPlan.collect {
              case aggTransformer: HashAggregateExecBaseTransformer => aggTransformer
            }

            planExecs.head.aggregateExpressions.foreach {
              expr => assert(expr.toString().startsWith("countdistinct("))
            }
            planExecs(1).aggregateExpressions.foreach {
              expr => assert(expr.toString().startsWith("partial_countdistinct("))
            }
          }
      }
    )
  }

  test("GLUTEN-5852: Fix mismatch result columns size exception related to 5618") {
    val sql =
      "select distinct * from (select 2 as r3, count(distinct a, b, c), 2 as r1, 2 as r2  from " +
        "values (0, null, 1), (1, 1, 1), (2, 2, 1), (1, 2, 1) ,(2, 2, 2) as data(a,b,c) group by c)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }
}
