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
import org.apache.spark.sql.execution.ProjectExec

class BoltLiteralSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "placeholder"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  def validateOffloadResult(sql: String): Unit = {
    runQueryAndCompare(sql) {
      df =>
        val plan = df.queryExecution.executedPlan
        assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined, sql)
        assert(plan.find(_.isInstanceOf[ProjectExec]).isEmpty, sql)
    }
  }

  def validateFallbackResult(sql: String): Unit = {
    runQueryAndCompare(sql) {
      df =>
        val plan = df.queryExecution.executedPlan
        assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isEmpty, sql)
        assert(plan.find(_.isInstanceOf[ProjectExec]).isDefined, sql)
    }
  }

  test("Struct Literal") {
    validateOffloadResult("SELECT struct('Spark', 5)")
    validateOffloadResult("SELECT struct(7, struct(5, 'test'))")
    validateOffloadResult("SELECT struct(-0.1, array(5, 6))")
    validateOffloadResult("SELECT struct(7, map('red', 1, 'green', 2))")
    validateOffloadResult("SELECT struct(array(5, 6), map('red', 1, 'green', 2))")
    validateOffloadResult("SELECT struct(1.0, struct(array(5, 6), map('red', 1)))")
    validateOffloadResult("SELECT struct(5, 1S, 1Y, -1Y, true, false)")
    validateOffloadResult("SELECT struct(1D, 1F)")
    validateOffloadResult("SELECT struct(5.321E2BD, 0.1, 5.321E22BD)")
    validateOffloadResult("SELECT struct(TIMESTAMP'2020-12-31')")
    validateOffloadResult("SELECT struct(X'1234')")
    validateOffloadResult("SELECT struct(DATE'2020-12-31')")
  }

  test("Array Literal") {
    validateOffloadResult("SELECT array()")
    validateOffloadResult("SELECT array(array())")
    validateOffloadResult("SELECT array(array(), array(1, 2))")
    validateOffloadResult("SELECT array(map())")
    validateOffloadResult("SELECT array(map(), map('red', 1))")
    validateOffloadResult("SELECT array('Spark', '5')")
    validateOffloadResult("SELECT array(5, 1, -1)")
    validateOffloadResult("SELECT array(5S, 1S, -1S)")
    validateOffloadResult("SELECT array(5Y, 1Y, -1Y)")
    validateOffloadResult("SELECT array(true, false)")
    validateOffloadResult("SELECT array(1D, -1D)")
    validateOffloadResult("SELECT array(1F, -1F)")
    validateOffloadResult("SELECT array(1.0, 5.321)")
    validateOffloadResult("SELECT array(5.321E2BD, 5.321E2BD)")
    validateOffloadResult("SELECT array(5.321E2BD, 0.1, 5.321E22BD)")
    validateOffloadResult("SELECT array(TIMESTAMP'2020-12-31', TIMESTAMP'2020-12-30')")
    validateOffloadResult("SELECT array(X'1234', X'a')")
    validateOffloadResult("SELECT array(DATE'2020-12-31', DATE'2020-12-30')")
    validateOffloadResult("SELECT array(array(3, 4), array(5, 6))")
    validateOffloadResult("SELECT array(map(1,2,3,4))")
    validateOffloadResult("SELECT array(map('red', 1), map('green', 2))")
    validateOffloadResult("SELECT array(struct(6, 'test1'), struct(5, 'test'))")
  }

  test("Map Literal") {
    validateOffloadResult("SELECT map()")
    validateOffloadResult("SELECT map(1, array())")
    validateOffloadResult("SELECT map(1, map())")
    validateOffloadResult("SELECT map('b', 'a', 'e', 'e')")
    validateOffloadResult("SELECT map(1D, 'a', 2D, 'e')")
    validateOffloadResult("SELECT map(1.0, map(1, 2, 3, 4))")
    validateOffloadResult("SELECT map(array(1, 2 ,3), array(1))")
    validateOffloadResult("SELECT map(array(1, 2), map(false, 2))")
    validateOffloadResult("SELECT map(struct(1, 2), struct(1, 2))")
  }

  test("Null Literal") {
    validateOffloadResult("SELECT cast(null as int)")
    validateOffloadResult("SELECT cast(null as decimal)")
    validateOffloadResult("SELECT array(5, 1, null)")
    validateOffloadResult("SELECT array(5.321E2BD, 0.1, null)")
    validateOffloadResult("SELECT struct('Spark', cast(null as int))")
    validateOffloadResult("SELECT struct(cast(null as decimal))")
    validateOffloadResult("SELECT map('b', 'a', 'e', null)")
    validateOffloadResult("SELECT array(null)")
    validateOffloadResult("SELECT array(cast(null as int))")
    validateOffloadResult("SELECT map(1, null)")
  }

  test("Scalar Type Literal") {
    validateOffloadResult("SELECT 'Spark', ''")
    validateOffloadResult("SELECT 5, 1, -1")
    validateOffloadResult("SELECT 5S, 1S, -1S")
    validateOffloadResult("SELECT 5Y, 1Y, -1Y")
    validateOffloadResult("SELECT true, false")
    validateOffloadResult("SELECT 1D, -1D")
    validateOffloadResult("SELECT 1F, -1F")
    validateOffloadResult("SELECT 5.321E2BD, 0.1, 5.321E22BD")
    validateOffloadResult("SELECT TIMESTAMP'2020-12-31', TIMESTAMP'2020-12-30'")
    validateOffloadResult("SELECT X'1234', X'a'")
    validateOffloadResult("SELECT DATE'2020-12-31', DATE'2020-12-30'")
  }

  test("Literal Fallback") {
    validateFallbackResult("SELECT struct(cast(null as struct<a: string>))")
    validateFallbackResult("SELECT array(struct(1, 'a'), null)")
  }
}
