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

import org.scalatest.DoNotDiscover

@DoNotDiscover
class GlutenClickHouseDSV2Suite extends MergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  final override val testCases: Seq[Int] = Seq(
    3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
  )
  setupTestCase()

  test("TPCH Q1") {
    customCheck(1) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
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
}
