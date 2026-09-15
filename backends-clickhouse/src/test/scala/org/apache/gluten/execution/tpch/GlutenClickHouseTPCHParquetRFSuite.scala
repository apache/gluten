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

import org.apache.spark.SparkConf

class GlutenClickHouseTPCHParquetRFSuite extends GlutenClickHouseTPCHSaltNullParquetSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // radically small threshold to force runtime bloom filter
      .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB")
      .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
  }

  test("GLUTEN-3779: Fix core dump when executing sql with runtime filter") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.files.maxPartitionBytes", "204800"),
      ("spark.sql.files.openCostInBytes", "102400")
    ) {
      compareResultsAgainstVanillaSpark(
        """
          |SELECT
          |    sum(l_extendedprice) / 7.0 AS avg_yearly
          |FROM
          |    lineitem,
          |    part
          |WHERE
          |    p_partkey = l_partkey
          |    AND p_size > 5
          |    AND l_quantity < (
          |        SELECT
          |            0.2 * avg(l_quantity)
          |        FROM
          |            lineitem
          |        WHERE
          |            l_partkey = p_partkey);
          |
          |""".stripMargin,
        compareResult = true,
        NOOP
      )
    }
  }

  test("GLUTEN-7596: Empty list of columns passed") {
    val sql_str =
      s"""
         |SELECT
         |    l_orderkey,
         |    count(*) cnt
         |FROM lineitem inner join
         |    (
         |    select * from (
         |    select count(*) cct,o_orderkey from orders group by o_orderkey)
         |    where cct > 10000
         |    )
         |GROUP BY
         |    l_orderkey
         |ORDER BY
         |    l_orderkey
         |LIMIT 10;
         |""".stripMargin

    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      compareResultsAgainstVanillaSpark(sql_str, compareResult = true, _ => {})
    }
  }
}
