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
package org.apache.gluten.execution.parquet

import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseWholeStageTransformerSuite}
import org.apache.gluten.test.{GlutenSQLTestUtils, GlutenTPCHBase}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal

import java.time.LocalDate

class GlutenParquetFilterSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with GlutenSQLTestUtils
  with GlutenTPCHBase
  with Logging {

  private val tpchQueriesResourceFolder: String = resPath + "queries/tpch-queries-ch"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1") // disable broadcast

  private val result: Array[Map[String, Seq[Predicate]]] = Array(
    Map( // q1
      "lineitem0" -> Seq(
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_shipdate").date <= LocalDate.of(1998, 9, 1)
      )),
    Map( // q2
      "part0" -> Seq(
        Symbol("p_size").int.isNotNull,
        Symbol("p_type").string.isNotNull,
        Symbol("p_size").int === 15,
        Symbol("p_partkey").long.isNotNull
      ),
      "partsupp1" -> Seq(
        Symbol("ps_partkey").long.isNotNull,
        Symbol("ps_suppkey").long.isNotNull,
        Symbol("ps_supplycost").decimal(10, 0).isNotNull
      ),
      "partsupp2" -> Seq(
        Symbol("ps_partkey").long.isNotNull,
        Symbol("ps_suppkey").long.isNotNull
      ),
      "supplier3" -> Seq(
        Symbol("s_suppkey").long.isNotNull,
        Symbol("s_nationkey").long.isNotNull
      ),
      "nation4" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_regionkey").long.isNotNull
      ),
      "region5" -> Seq(
        Symbol("r_name").string.isNotNull,
        Symbol("r_name").string === "EUROPE",
        Symbol("r_regionkey").long.isNotNull
      ),
      "supplier6" -> Seq(
        Symbol("s_suppkey").long.isNotNull,
        Symbol("s_nationkey").long.isNotNull
      ),
      "nation7" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_regionkey").long.isNotNull
      )
    ),
    Map( // q3
      "customer0" -> Seq(
        Symbol("c_mktsegment").string.isNotNull,
        Symbol("c_mktsegment").string === "BUILDING",
        Symbol("c_custkey").long.isNotNull
      ),
      "orders1" -> Seq(
        Symbol("o_orderkey").long.isNotNull,
        Symbol("o_custkey").long.isNotNull,
        Symbol("o_orderdate").date.isNotNull,
        Symbol("o_orderdate").date < LocalDate.of(1995, 3, 15)
      ),
      "lineitem2" -> Seq(
        Symbol("l_orderkey").long.isNotNull,
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_shipdate").date > LocalDate.of(1995, 3, 15)
      )
    ),
    Map( // q4
      "orders0" -> Seq(
        Symbol("o_orderdate").date.isNotNull,
        Symbol("o_orderdate").date >= LocalDate.of(1993, 7, 1),
        Symbol("o_orderdate").date < LocalDate.of(1993, 10, 1)
      ),
      "lineitem1" -> Seq(
        Symbol("l_commitdate").date.isNotNull,
        Symbol("l_receiptdate").date.isNotNull
      )
    ),
    Map( // q5
      "customer0" -> Seq(
        Symbol("c_custkey").long.isNotNull,
        Symbol("c_nationkey").long.isNotNull
      ),
      "orders1" -> Seq(
        Symbol("o_orderkey").long.isNotNull,
        Symbol("o_custkey").long.isNotNull,
        Symbol("o_orderdate").date.isNotNull,
        Symbol("o_orderdate").date >= LocalDate.of(1994, 1, 1),
        Symbol("o_orderdate").date < LocalDate.of(1995, 1, 1)
      ),
      "lineitem2" -> Seq(
        Symbol("l_orderkey").long.isNotNull,
        Symbol("l_suppkey").long.isNotNull
      ),
      "supplier3" -> Seq(
        Symbol("s_suppkey").long.isNotNull,
        Symbol("s_nationkey").long.isNotNull
      ),
      "nation4" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_regionkey").long.isNotNull
      ),
      "region5" -> Seq(
        Symbol("r_name").string.isNotNull,
        Symbol("r_name").string === "ASIA",
        Symbol("r_regionkey").long.isNotNull
      )
    ),
    Map( // q6
      "lineitem0" -> Seq(
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_discount").decimal(10, 2).isNotNull,
        Symbol("l_quantity").decimal(10, 0).isNotNull,
        Symbol("l_shipdate").date >= LocalDate.of(1994, 1, 1),
        Symbol("l_shipdate").date < LocalDate.of(1995, 1, 1),
        Symbol("l_discount").decimal(10, 2) >= Decimal(BigDecimal(0.05), 10, 2),
        Symbol("l_discount").decimal(10, 2) <= Decimal(BigDecimal(0.07), 10, 2),
        Symbol("l_quantity").decimal(10, 0) < Decimal(24)
      )),
    Map( // q7
      "supplier0" -> Seq(
        Symbol("s_suppkey").long.isNotNull,
        Symbol("s_nationkey").long.isNotNull
      ),
      "lineitem1" -> Seq(
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_shipdate").date >= LocalDate.of(1995, 1, 1),
        Symbol("l_shipdate").date <= LocalDate.of(1996, 12, 31),
        Symbol("l_suppkey").long.isNotNull,
        Symbol("l_orderkey").long.isNotNull
      ),
      "orders2" -> Seq(
        Symbol("o_orderkey").long.isNotNull,
        Symbol("o_custkey").long.isNotNull
      ),
      "customer3" -> Seq(
        Symbol("c_nationkey").long.isNotNull,
        Symbol("c_custkey").long.isNotNull
      ),
      "nation4" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_name").string === "FRANCE" || Symbol("n_name").string === "GERMANY"
      )
    ),
    Map( // q8
      "part0" -> Seq(
        Symbol("p_partkey").long.isNotNull,
        Symbol("p_type").string.isNotNull,
        Symbol("p_type").string === "ECONOMY ANODIZED STEEL"
      ),
      "lineitem1" -> Seq(
        Symbol("l_partkey").long.isNotNull,
        Symbol("l_suppkey").long.isNotNull,
        Symbol("l_orderkey").long.isNotNull
      ),
      "supplier2" -> Seq(
        Symbol("s_suppkey").long.isNotNull,
        Symbol("s_nationkey").long.isNotNull
      ),
      "orders3" -> Seq(
        Symbol("o_orderkey").long.isNotNull,
        Symbol("o_custkey").long.isNotNull,
        Symbol("o_orderdate").date.isNotNull,
        Symbol("o_orderdate").date >= LocalDate.of(1995, 1, 1),
        Symbol("o_orderdate").date <= LocalDate.of(1996, 12, 31)
      ),
      "customer4" -> Seq(
        Symbol("c_custkey").long.isNotNull,
        Symbol("c_nationkey").long.isNotNull
      ),
      "nation5" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_regionkey").long.isNotNull
      ),
      "nation6" -> Seq(
        Symbol("n_nationkey").long.isNotNull
      ),
      "region7" -> Seq(
        Symbol("r_regionkey").long.isNotNull,
        Symbol("r_name").string.isNotNull,
        Symbol("r_name").string === "AMERICA"
      )
    ),
    Map( // q9
      "part0" -> Seq(
        Symbol("p_partkey").long.isNotNull,
        Symbol("p_name").string.isNotNull
      ),
      "lineitem1" -> Seq(
        Symbol("l_partkey").long.isNotNull,
        Symbol("l_suppkey").long.isNotNull,
        Symbol("l_orderkey").long.isNotNull
      ),
      "supplier2" -> Seq(
        Symbol("s_suppkey").long.isNotNull,
        Symbol("s_nationkey").long.isNotNull
      ),
      "partsupp3" -> Seq(
        Symbol("ps_partkey").long.isNotNull,
        Symbol("ps_suppkey").long.isNotNull
      ),
      "orders4" -> Seq(
        Symbol("o_orderkey").long.isNotNull
      ),
      "nation5" -> Seq(
        Symbol("n_nationkey").long.isNotNull
      )
    ),
    Map( // q10
      "customer0" -> Seq(
        Symbol("c_custkey").long.isNotNull,
        Symbol("c_nationkey").long.isNotNull
      ),
      "orders1" -> Seq(
        Symbol("o_orderkey").long.isNotNull,
        Symbol("o_custkey").long.isNotNull,
        Symbol("o_orderdate").date.isNotNull,
        Symbol("o_orderdate").date >= LocalDate.of(1993, 10, 1),
        Symbol("o_orderdate").date < LocalDate.of(1994, 1, 1)
      ),
      "lineitem2" -> Seq(
        Symbol("l_orderkey").long.isNotNull,
        Symbol("l_returnflag").string.isNotNull,
        Symbol("l_returnflag").string === "R"
      ),
      "nation3" -> Seq(
        Symbol("n_nationkey").long.isNotNull
      )
    ),
    Map( // q11
      "partsupp0" -> Seq(
        Symbol("ps_suppkey").long.isNotNull
      ),
      "supplier1" -> Seq(
        Symbol("s_suppkey").long.isNotNull,
        Symbol("s_nationkey").long.isNotNull
      ),
      "nation2" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_name").string.isNotNull,
        Symbol("n_name").string === "GERMANY"
      )
    ),
    Map( // q12
      "orders0" -> Seq(
        Symbol("o_orderkey").long.isNotNull
      ),
      "lineitem1" -> Seq(
        Symbol("l_orderkey").long.isNotNull,
        Symbol("l_receiptdate").date.isNotNull,
        Symbol("l_receiptdate").date >= LocalDate.of(1994, 1, 1),
        Symbol("l_receiptdate").date < LocalDate.of(1995, 1, 1),
        Symbol("l_commitdate").date.isNotNull,
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_shipmode").string.in("MAIL", "SHIP").asInstanceOf[Predicate]
      )
    ),
    Map( // q13
      "customer0" -> Nil,
      "orders1" -> Seq(
        Symbol("o_custkey").long.isNotNull,
        Symbol("o_comment").string.isNotNull
      )),
    Map( // q14
      "lineitem0" -> Seq(
        Symbol("l_partkey").long.isNotNull,
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_shipdate").date >= LocalDate.of(1995, 9, 1),
        Symbol("l_shipdate").date < LocalDate.of(1995, 10, 1)
      ),
      "part1" -> Seq(
        Symbol("p_partkey").long.isNotNull
      )
    ),
    Map( // q15
      "supplier0" -> Seq(
        Symbol("s_suppkey").long.isNotNull
      ),
      "lineitem1" -> Seq(
        Symbol("l_suppkey").long.isNotNull,
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_shipdate").date >= LocalDate.of(1996, 1, 1),
        Symbol("l_shipdate").date < LocalDate.of(1996, 4, 1)
      )
    ),
    Map( // q16
      "partsupp0" -> Seq(
        Symbol("ps_partkey").long.isNotNull
      ),
      "supplier1" -> Seq(
        Symbol("s_comment").string.isNotNull
      ),
      "part2" -> Seq(
        Symbol("p_partkey").long.isNotNull,
        Symbol("p_brand").string.isNotNull,
        Symbol("p_brand").string =!= "Brand#45",
        Symbol("p_type").string.isNotNull,
        Symbol("p_size").int.in(49, 14, 23, 45, 19, 3, 36, 9).asInstanceOf[Predicate]
      )
    ),
    Map( // q17
      "lineitem0" -> Seq(
        Symbol("l_partkey").long.isNotNull,
        Symbol("l_quantity").decimal(10, 0).isNotNull
      ),
      "part1" -> Seq(
        Symbol("p_partkey").long.isNotNull,
        Symbol("p_brand").string.isNotNull,
        Symbol("p_brand").string === "Brand#23",
        Symbol("p_container").string.isNotNull,
        Symbol("p_container").string === "MED BOX"
      ),
      "lineitem2" -> Seq(
        Symbol("l_partkey").long.isNotNull
      )
    ),
    Map( // q18
      "customer0" -> Seq(
        Symbol("c_custkey").long.isNotNull
      ),
      "orders1" -> Seq(
        Symbol("o_orderkey").long.isNotNull,
        Symbol("o_custkey").long.isNotNull
      ),
      "lineitem2" -> Nil,
      "lineitem3" -> Seq(
        Symbol("l_orderkey").long.isNotNull
      )
    ),
    Map( // q19
      "lineitem0" -> Seq(
        Symbol("l_shipinstruct").string.isNotNull,
        Symbol("l_shipmode").string.in("AIR", "AIR REG").asInstanceOf[Predicate],
        Symbol("l_shipinstruct").string === "DELIVER IN PERSON",
        Symbol("l_partkey").long.isNotNull,
        (Symbol("l_quantity").decimal(10, 0) >= Decimal(1) &&
          Symbol("l_quantity").decimal(10, 0) <= Decimal(11)) ||
          (Symbol("l_quantity").decimal(10, 0) >= Decimal(10) &&
            Symbol("l_quantity").decimal(10, 0) <= Decimal(20)) ||
          (Symbol("l_quantity").decimal(10, 0) >= Decimal(20) &&
            Symbol("l_quantity").decimal(10, 0) <= Decimal(30))
      ),
      "part1" -> Seq(
        Symbol("p_size").int.isNotNull,
        Symbol("p_size").int >= 1,
        Symbol("p_partkey").long.isNotNull,
        (Symbol("p_brand").string === "Brand#12" &&
          Symbol("p_container").string.in("SM CASE", "SM BOX", "SM PACK", "SM PKG") &&
          Symbol("p_size").int <= 5) ||
          (Symbol("p_brand").string === "Brand#23" &&
            Symbol("p_container").string.in("MED BAG", "MED BOX", "MED PKG", "MED PACK") &&
            Symbol("p_size").int <= 10) ||
          (Symbol("p_brand").string === "Brand#34" &&
            Symbol("p_container").string.in("LG CASE", "LG BOX", "LG PACK", "LG PKG") &&
            Symbol("p_size").int <= 15)
      )
    ),
    Map( // q20
      "supplier0" -> Seq(
        Symbol("s_nationkey").long.isNotNull
      ),
      "partsupp1" -> Seq(
        Symbol("ps_suppkey").long.isNotNull,
        Symbol("ps_partkey").long.isNotNull,
        Symbol("ps_availqty").int.isNotNull
      ),
      "part2" -> Seq(
        Symbol("p_name").string.isNotNull
      ),
      "lineitem3" -> Seq(
        Symbol("l_partkey").long.isNotNull,
        Symbol("l_suppkey").long.isNotNull,
        Symbol("l_shipdate").date.isNotNull,
        Symbol("l_shipdate").date >= LocalDate.of(1994, 1, 1),
        Symbol("l_shipdate").date < LocalDate.of(1995, 1, 1)
      ),
      "nation4" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_name").string.isNotNull,
        Symbol("n_name").string === "CANADA"
      )
    ),
    Map( // q21
      "supplier0" -> Seq(
        Symbol("s_nationkey").long.isNotNull,
        Symbol("s_suppkey").long.isNotNull
      ),
      "lineitem1" -> Seq(
        Symbol("l_orderkey").long.isNotNull,
        Symbol("l_suppkey").long.isNotNull,
        Symbol("l_commitdate").date.isNotNull,
        Symbol("l_receiptdate").date.isNotNull
      ),
      "lineitem2" -> Nil,
      "lineitem3" -> Seq(
        Symbol("l_receiptdate").date.isNotNull,
        Symbol("l_commitdate").date.isNotNull
      ),
      "orders4" -> Seq(
        Symbol("o_orderstatus").string.isNotNull,
        Symbol("o_orderstatus").string === "F",
        Symbol("o_orderkey").long.isNotNull
      ),
      "nation5" -> Seq(
        Symbol("n_nationkey").long.isNotNull,
        Symbol("n_name").string.isNotNull,
        Symbol("n_name").string === "SAUDI ARABIA"
      )
    ),
    Map( // q22
      "customer0" -> Seq(
        Symbol("c_acctbal").decimal(10, 0).isNotNull
      ),
      "orders1" -> Nil)
  )

  def runTest(i: Int): Unit = withDataFrame(tpchSQL(i + 1, tpchQueriesResourceFolder)) {
    df =>
      val scans = df.queryExecution.executedPlan
        .collect { case scan: FileSourceScanExecTransformer => scan }
      assertResult(result(i).size)(scans.size)
      scans.zipWithIndex
        .foreach {
          case (scan, fileIndex) =>
            val tableName = scan.tableIdentifier
              .map(_.table)
              .getOrElse(scan.relation.options("path").split("/").last)
            val predicates = scan.filterExprs()
            val expected = result(i)(s"$tableName$fileIndex")
            assertResult(expected.size)(predicates.size)
            if (expected.isEmpty) assert(predicates.isEmpty)
            else compareExpressions(expected.reduceLeft(And), predicates.reduceLeft(And))
        }
  }

  tpchQueries.zipWithIndex.foreach {
    case (q, i) =>
      if (q == "q2" || q == "q9") {
        ignoreSpark33OnlyCase(q) {
          runTest(i)
        }
      } else {
        test(q) {
          runTest(i)
        }
      }
  }
}
