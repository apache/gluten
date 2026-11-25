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

import org.apache.spark.SparkConf

/**
 * Native union tests that don't need the functions MiscOperatorSuite registers, so they also run on
 * Spark 4.2+, where MiscOperatorSuite is ignored (GLUTEN-13179).
 */
class VeloxUnionSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("union_all two tables with known partitioning") {
    withSQLConf(GlutenConfig.NATIVE_UNION_ENABLED.key -> "true") {
      compareDfResultsAgainstVanillaSpark(
        () => {
          val df1 = spark.sql("select l_orderkey as orderkey from lineitem")
          val df2 = spark.sql("select o_orderkey as orderkey from orders")
          df1.repartition(5).union(df2.repartition(5))
        },
        compareResult = true,
        checkGlutenPlan[UnionExecTransformer]
      )

      compareDfResultsAgainstVanillaSpark(
        () => {
          val df1 = spark.sql("select l_orderkey as orderkey from lineitem")
          val df2 = spark.sql("select o_orderkey as orderkey from orders")
          df1.repartition(5).union(df2.repartition(6))
        },
        compareResult = true,
        checkGlutenPlan[ColumnarUnionExec]
      )
    }
  }
}
