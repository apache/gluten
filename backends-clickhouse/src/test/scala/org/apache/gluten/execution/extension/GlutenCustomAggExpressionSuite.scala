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
package org.apache.gluten.execution.extension

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.execution._
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.utils.SubstraitPlanPrinterUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.aggregate.CustomSum
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

class GlutenCustomAggExpressionSuite extends ParquetSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
      .set(
        CHConfig.EXTENDED_EXPRESSION_TRAN_CONF.key,
        "org.apache.gluten.execution.extension.CustomAggExpressionTransformer")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (expressionInfo, builder) =
      FunctionRegistryBase.build[CustomSum]("custom_sum", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("custom_sum"),
      expressionInfo,
      builder
    )
  }

  test("test custom aggregate function") {
    val sql =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    custom_sum(l_quantity) AS sum_qty,
         |    custom_sum(l_linenumber) AS sum_linenumber,
         |    sum(l_extendedprice) AS sum_base_price
         |FROM
         |    lineitem
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |""".stripMargin
    val df = spark.sql(sql)
    // Final stage is not supported, it will be fallback
    GlutenQueryComparisonTest.checkFallBack(df, noFallback = false)

    val planExecs = df.queryExecution.executedPlan.collect {
      case agg: HashAggregateExec => agg
      case aggTransformer: HashAggregateExecBaseTransformer => aggTransformer
      case wholeStage: WholeStageTransformer => wholeStage
    }

    // First stage fallback
    assert(planExecs(3).isInstanceOf[HashAggregateExec])

    val substraitContext = new SubstraitContext
    planExecs(2).asInstanceOf[CHHashAggregateExecTransformer].transform(substraitContext)

    // Check the functions
    assert(substraitContext.registeredFunction.containsKey("custom_sum_double:req_fp64"))
    assert(substraitContext.registeredFunction.containsKey("custom_sum:req_i64"))
    assert(substraitContext.registeredFunction.containsKey("sum:req_fp64"))

    val wx = planExecs(1).asInstanceOf[WholeStageTransformer].doWholeStageTransform()
    val planJson = SubstraitPlanPrinterUtil.substraitPlanToJson(wx.root.toProtobuf)
    assert(planJson.contains("#Partial#custom_sum_double"))
    assert(planJson.contains("#Partial#custom_sum"))
    assert(planJson.contains("#Partial#sum"))
  }
}
