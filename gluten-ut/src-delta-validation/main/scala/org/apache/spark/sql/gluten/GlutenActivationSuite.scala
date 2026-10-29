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
package org.apache.spark.sql.gluten

import org.apache.gluten.execution.WholeStageTransformer

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class GlutenActivationSuite extends QueryTest with SharedSparkSession {
  // Every fork runs this suite. ScalaTest uses suiteId for the XML file name.
  override def suiteId: String = s"${super.suiteId}-${GlutenValidation.processId}"

  test(GlutenValidation.canaryTest) {
    GlutenValidation.nativeCanary(spark) {
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        val query = spark.range(20).selectExpr("id % 3 AS k", "id AS v").groupBy("k").sum("v")
        val rows = query.collect().toSet
        assert(rows == Set(Row(0L, 63L), Row(1L, 70L), Row(2L, 57L)))
        assert(
          query.queryExecution.executedPlan.exists(_.isInstanceOf[WholeStageTransformer]),
          s"No native Gluten stage in the canary plan:\n${query.queryExecution.executedPlan}"
        )
      }
    }
  }
}
