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

import org.apache.gluten.execution.{SortExecTransformer, WindowExecTransformer, WindowGroupLimitExecTransformer}

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GlutenSQLWindowFunctionSuite extends SQLWindowFunctionSuite with GlutenSQLTestsTrait {

  private def decimal(v: BigDecimal): Decimal = Decimal(v, 7, 2)

  val customerSchema = StructType(
    List(
      StructField("c_custkey", IntegerType),
      StructField("c_nationkey", IntegerType),
      StructField("c_acctbal", DecimalType(7, 2))
    )
  )

  val customerData = Seq(
    Row(4553, 11, decimal(6388.41)),
    Row(4953, 10, decimal(6037.28)),
    Row(35403, 5, decimal(6034.70)),
    Row(35803, 12, decimal(5284.87)),
    Row(60865, 5, decimal(-227.82)),
    Row(61065, 13, decimal(7284.77)),
    Row(127412, 13, decimal(4621.41)),
    Row(148303, 10, decimal(4302.30)),
    Row(9954, 5, decimal(7587.25)),
    Row(95337, 12, decimal(915.61))
  )

  // TODO: fix in Spark-4.0
  ignore("Literal in window partition by and sort") {
    withTable("customer") {
      val rdd = spark.sparkContext.parallelize(customerData)
      val customerDF = spark.createDataFrame(rdd, customerSchema)
      customerDF.createOrReplaceTempView("customer")
      val query =
        """
          |SELECT
          |  c_custkey,
          |  c_acctbal,
          |  row_number() OVER (
          |    PARTITION BY c_nationkey,
          |    "a"
          |    ORDER BY
          |      c_custkey,
          |      "a"
          |  ) AS row_num
          |FROM
          |   customer
          |ORDER BY 1, 2;
          |""".stripMargin
      val df = sql(query)
      checkAnswer(
        df,
        Seq(
          Row(4553, BigDecimal(638841L, 2), 1),
          Row(4953, BigDecimal(603728L, 2), 1),
          Row(9954, BigDecimal(758725L, 2), 1),
          Row(35403, BigDecimal(603470L, 2), 2),
          Row(35803, BigDecimal(528487L, 2), 1),
          Row(60865, BigDecimal(-22782L, 2), 3),
          Row(61065, BigDecimal(728477L, 2), 1),
          Row(95337, BigDecimal(91561L, 2), 2),
          Row(127412, BigDecimal(462141L, 2), 2),
          Row(148303, BigDecimal(430230L, 2), 2)
        )
      )
      assert(
        getExecutedPlan(df).exists {
          case _: WindowExecTransformer => true
          case _ => false
        }
      )
    }
  }

  // TODO: fix in Spark-4.0
  ignore("Filter on row number") {
    withTable("customer") {
      val rdd = spark.sparkContext.parallelize(customerData)
      val customerDF = spark.createDataFrame(rdd, customerSchema)
      customerDF.createOrReplaceTempView("customer")
      val query =
        """
          |SELECT * from (SELECT
          |  c_custkey,
          |  c_acctbal,
          |  row_number() OVER (
          |    PARTITION BY c_nationkey,
          |    "a"
          |    ORDER BY
          |      c_custkey,
          |      "a"
          |  ) AS row_num
          |FROM
          |   customer ORDER BY 1, 2) where row_num <=2
          |""".stripMargin
      val df = sql(query)
      checkAnswer(
        df,
        Seq(
          Row(4553, BigDecimal(638841L, 2), 1),
          Row(4953, BigDecimal(603728L, 2), 1),
          Row(9954, BigDecimal(758725L, 2), 1),
          Row(35403, BigDecimal(603470L, 2), 2),
          Row(35803, BigDecimal(528487L, 2), 1),
          Row(61065, BigDecimal(728477L, 2), 1),
          Row(95337, BigDecimal(91561L, 2), 2),
          Row(127412, BigDecimal(462141L, 2), 2),
          Row(148303, BigDecimal(430230L, 2), 2)
        )
      )
      assert(
        getExecutedPlan(df).exists {
          case _: WindowGroupLimitExecTransformer => true
          case _ => false
        }
      )
      assert(
        getExecutedPlan(df).collect { case s: SortExecTransformer if !s.global => s }.size == 1
      )
    }
  }

  testGluten("Filter on rank") {
    withTable("customer") {
      val rdd = spark.sparkContext.parallelize(customerData)
      val customerDF = spark.createDataFrame(rdd, customerSchema)
      customerDF.createOrReplaceTempView("customer")
      val query =
        """
          |SELECT * from (SELECT
          |  c_custkey,
          |  c_acctbal,
          |  rank() OVER (
          |    PARTITION BY c_nationkey,
          |    "a"
          |    ORDER BY
          |      c_custkey,
          |      "a"
          |  ) AS rank
          |FROM
          |   customer ORDER BY 1, 2) where rank <=2
          |""".stripMargin
      val df = sql(query)
      checkAnswer(
        df,
        Seq(
          Row(4553, BigDecimal(638841L, 2), 1),
          Row(4953, BigDecimal(603728L, 2), 1),
          Row(9954, BigDecimal(758725L, 2), 1),
          Row(35403, BigDecimal(603470L, 2), 2),
          Row(35803, BigDecimal(528487L, 2), 1),
          Row(61065, BigDecimal(728477L, 2), 1),
          Row(95337, BigDecimal(91561L, 2), 2),
          Row(127412, BigDecimal(462141L, 2), 2),
          Row(148303, BigDecimal(430230L, 2), 2)
        )
      )
      assert(
        getExecutedPlan(df).exists {
          case _: WindowGroupLimitExecTransformer => true
          case _ => false
        }
      )
    }
  }

  testGluten("Filter on dense_rank") {
    withTable("customer") {
      val rdd = spark.sparkContext.parallelize(customerData)
      val customerDF = spark.createDataFrame(rdd, customerSchema)
      customerDF.createOrReplaceTempView("customer")
      val query =
        """
          |SELECT * from (SELECT
          |  c_custkey,
          |  c_acctbal,
          |  dense_rank() OVER (
          |    PARTITION BY c_nationkey,
          |    "a"
          |    ORDER BY
          |      c_custkey,
          |      "a"
          |  ) AS rank
          |FROM
          |   customer ORDER BY 1, 2) where rank <=2
          |""".stripMargin
      val df = sql(query)
      checkAnswer(
        df,
        Seq(
          Row(4553, BigDecimal(638841L, 2), 1),
          Row(4953, BigDecimal(603728L, 2), 1),
          Row(9954, BigDecimal(758725L, 2), 1),
          Row(35403, BigDecimal(603470L, 2), 2),
          Row(35803, BigDecimal(528487L, 2), 1),
          Row(61065, BigDecimal(728477L, 2), 1),
          Row(95337, BigDecimal(91561L, 2), 2),
          Row(127412, BigDecimal(462141L, 2), 2),
          Row(148303, BigDecimal(430230L, 2), 2)
        )
      )
      assert(
        getExecutedPlan(df).exists {
          case _: WindowGroupLimitExecTransformer => true
          case _ => false
        }
      )
    }
  }

  // TODO: fix in Spark-4.0
  ignore("Expression in WindowExpression") {
    withTable("customer") {
      val rdd = spark.sparkContext.parallelize(customerData)
      val customerDF = spark.createDataFrame(rdd, customerSchema)
      customerDF.createOrReplaceTempView("customer")
      val query =
        """
          |SELECT
          |  c_custkey,
          |  avg(c_acctbal) OVER (
          |    PARTITION BY c_nationkey
          |    ORDER BY c_custkey
          |  )
          |FROM
          |   customer
          |ORDER BY 1, 2;
          |""".stripMargin
      val df = sql(query)
      checkAnswer(
        df,
        Seq(
          Row(4553, BigDecimal(6388410000L, 6)),
          Row(4953, BigDecimal(6037280000L, 6)),
          Row(9954, BigDecimal(7587250000L, 6)),
          Row(35403, BigDecimal(6810975000L, 6)),
          Row(35803, BigDecimal(5284870000L, 6)),
          Row(60865, BigDecimal(4464710000L, 6)),
          Row(61065, BigDecimal(7284770000L, 6)),
          Row(95337, BigDecimal(3100240000L, 6)),
          Row(127412, BigDecimal(5953090000L, 6)),
          Row(148303, BigDecimal(5169790000L, 6))
        )
      )
      assert(
        getExecutedPlan(df).exists {
          case _: WindowExecTransformer => true
          case _ => false
        }
      )
    }
  }
}
