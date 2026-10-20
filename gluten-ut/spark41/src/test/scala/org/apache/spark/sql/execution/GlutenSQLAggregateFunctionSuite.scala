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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.HashAggregateExecBaseTransformer

import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.internal.SQLConf

class GlutenSQLAggregateFunctionSuite extends GlutenSQLTestsTrait {

  testGluten("GLUTEN-4853: The result order is reversed for count and count distinct") {
    val query =
      """
        |select count(distinct if(sex = 'x', id, null)) as uv, count(if(sex = 'x', id, null)) as pv
        |from values (1, 'x'), (1, 'x'), (2, 'y'), (3, 'x'), (3, 'x'), (4, 'y'), (5, 'x')
        |AS tab(id, sex)
        |""".stripMargin
    val df = sql(query)
    checkAnswer(df, Seq(Row(3, 5)))
    assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecBaseTransformer]) == 4)
  }

  testGluten("Return NaN or null when dividing by zero") {
    val query =
      """
        |select skewness(value), kurtosis(value)
        |from values (1), (1)
        |AS tab(value)
        |""".stripMargin
    val df = sql(query)

    withSQLConf(
      SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "true"
    ) {
      checkAnswer(df, Seq(Row(Double.NaN, Double.NaN)))
      assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecBaseTransformer]) == 2)
    }

    withSQLConf(
      SQLConf.LEGACY_STATISTICAL_AGGREGATE.key ->
        SQLConf.LEGACY_STATISTICAL_AGGREGATE.defaultValueString
    ) {
      checkAnswer(df, Seq(Row(null, null)))
      assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecBaseTransformer]) == 2)
    }
  }
}
