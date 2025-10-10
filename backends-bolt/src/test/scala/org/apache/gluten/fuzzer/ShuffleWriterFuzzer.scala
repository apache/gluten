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

import org.apache.gluten.fuzzer.FuzzerResult.Successful
import org.apache.gluten.tags.{FuzzerTest, SkipTest}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec

@FuzzerTest
@SkipTest
class ShuffleWriterFuzzer extends FuzzerBase {
  private val REPARTITION_SQL = (numPartitions: Int) =>
    s"select /*+ REPARTITION($numPartitions) */ * from tbl"
  private val AGG_REPARTITION_SQL =
    """select count(*) as cnt, f_1, f_2, f_3, f_4, f_5, f_6
      |from tbl group by f_1, f_2, f_3, f_4, f_5, f_6
      |order by cnt, f_1, f_2, f_3, f_4, f_5, f_6""".stripMargin

  private val outputPath = getClass.getResource("/").getPath + "fuzzer_output.parquet"

  private def checkOperators(df: DataFrame): Unit = {
    checkGlutenPlan[ColumnarShuffleExchangeExec](df)
  }

  def testShuffle(sql: String): () => Unit =
    () => {
      dataGenerator.generateRandomData(spark, Some(outputPath))
      spark.read.format("parquet").load(outputPath).createOrReplaceTempView("tbl")
      runQueryAndCompare(sql, noFallBack = false)(checkOperators)
    }

  private val TEST_REPARTITION = (numPartitions: Int) => s"repartition - $numPartitions"
  for (numPartitions <- Seq(1, 3, 10, 100, 1000, 4000, 8000)) {
    val testName = TEST_REPARTITION(numPartitions)
    test(testName) {
      repeat(10, testName, defaultRunner(testShuffle(REPARTITION_SQL(numPartitions))))
    }
  }

  private val TEST_AGG = "with aggregation"
  ignore(TEST_AGG) {
    repeat(10, TEST_AGG, defaultRunner(testShuffle(AGG_REPARTITION_SQL)))
  }

  ignore("reproduce") {
    // Replace sql with the actual failed sql.
    val sql = REPARTITION_SQL(1)
    // Replace seed '0L' with the actual failed seed.
    Seq(0L).foreach {
      seed =>
        dataGenerator.reFake(seed)
        logWarning(
          s"==============================> " +
            s"Started reproduction (seed: ${dataGenerator.getSeed})")
        val result = defaultRunner(testShuffle(sql))()
        assert(result.isInstanceOf[Successful], s"Failed to run 'reproduce' with seed: $seed")
    }
  }
}
