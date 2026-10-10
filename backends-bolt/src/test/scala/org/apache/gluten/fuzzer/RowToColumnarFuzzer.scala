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
package org.apache.gluten.fuzzer

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.RowToBoltColumnarExec
import org.apache.gluten.fuzzer.FuzzerResult.Successful
import org.apache.gluten.tags.{FuzzerTest, SkipTest}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

@FuzzerTest
@SkipTest
class RowToColumnarFuzzer extends FuzzerBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, "false")
  }

  private def checkOperators(df: DataFrame): Unit = {
    checkGlutenPlan[RowToBoltColumnarExec](df)
  }

  private val TEST_ROW_TO_COLUMNAR = "row to columnar"

  private val ROW_TO_COLUMNAR_SQL = "select * from tbl where f_1 is null or f_1 is not null"

  private val testR2C: () => Unit = () => {
    dataGenerator.generateRandomData(spark).createOrReplaceTempView("tbl")
    runQueryAndCompare(ROW_TO_COLUMNAR_SQL, noFallBack = false)(checkOperators)
  }

  test(TEST_ROW_TO_COLUMNAR) {
    repeat(10, TEST_ROW_TO_COLUMNAR, defaultRunner(testR2C))
  }

  test("reproduce") {
    // Replace seed '0L' with the actual failed seed.
    Seq(1712027684444L).foreach {
//    Seq(1711691870863L).foreach {
      seed =>
        dataGenerator.reFake(seed)
        logWarning(
          s"==============================> " +
            s"Started reproduction (seed: ${dataGenerator.getSeed})")
        val result = defaultRunner(testR2C)()
        assert(result.isInstanceOf[Successful], s"Failed to run 'reproduce' with seed: $seed")
    }
  }
}
