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
package org.apache.spark.sql.streaming

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.REQUIRED_BY_STATEFUL_OPERATOR
import org.apache.spark.sql.execution.streaming.MemoryStream

class GlutenStreamingQuerySuite extends StreamingQuerySuite with GlutenSQLTestsTrait {

  import testImplicits._

  testGluten("SPARK-49905") {
    val inputData = MemoryStream[Int]

    // Use the streaming aggregation as an example - all stateful operators are using the same
    // distribution, named `StatefulOpClusteredDistribution`.
    val df = inputData.toDF().groupBy("value").count()

    testStream(df, OutputMode.Update())(
      AddData(inputData, 1, 2, 3, 1, 2, 3),
      CheckAnswer((1, 2), (2, 2), (3, 2)),
      Execute {
        qe =>
          val shuffleOpt = qe.lastExecution.executedPlan.collect {
            case s: ColumnarShuffleExchangeExec => s
          }

          assert(shuffleOpt.nonEmpty, "No shuffle exchange found in the query plan")
          assert(shuffleOpt.head.shuffleOrigin === REQUIRED_BY_STATEFUL_OPERATOR)
      }
    )
  }
}
