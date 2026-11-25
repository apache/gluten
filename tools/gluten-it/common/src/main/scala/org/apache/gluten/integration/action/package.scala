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
package org.apache.gluten.integration

import org.apache.spark.sql.RunResult

package object action {

  implicit class QueryResultsOps(results: Iterable[QueryRunner.QueryResult]) {
    def asSuccesses(): Iterable[QueryRunner.Success] = {
      results.map(_.asSuccess())
    }

    def asFailures(): Iterable[QueryRunner.Failure] = {
      results.map(_.asFailure())
    }
  }

  implicit class CompletedOps(completed: Iterable[QueryRunner.Success]) {
    def agg(name: String): Option[QueryRunner.Success] = {
      completed.reduceOption {
        (c1, c2) =>
          QueryRunner.Success(
            name,
            RunResult(
              c1.runResult.rows ++ c2.runResult.rows,
              c1.runResult.planningTimeMillis + c2.runResult.planningTimeMillis,
              c1.runResult.executionTimeMillis + c2.runResult.executionTimeMillis,
              c1.runResult.sqlMetrics ++ c2.runResult.sqlMetrics,
              (c1.runResult.executorMetrics, c2.runResult.executorMetrics).sumUp
            )
          )
      }
    }
  }

  implicit class DualMetricsOps(value: (Map[String, Long], Map[String, Long])) {
    def sumUp: Map[String, Long] = {
      assert(value._1.keySet == value._2.keySet)
      value._1.map { case (k, v) => k -> (v + value._2(k)) }
    }
  }
}
