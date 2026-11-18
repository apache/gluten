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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig

class GlutenSparkSessionExtensionSuite
  extends SparkSessionExtensionSuite
  with GlutenTestsCommonTrait {

  testGluten("customColumnarOp") {
    val extensions = DummyFilterColmnarHelper.create {
      extensions => extensions.injectPlannerStrategy(_ => DummyFilterColumnarStrategy)
    }
    DummyFilterColmnarHelper.withSession(extensions) {
      session =>
        try {
          session.range(2).write.format("parquet").mode("overwrite").saveAsTable("a")
          def testWithFallbackSettings(scanFallback: Boolean, aggFallback: Boolean): Unit = {
            session.sessionState.conf
              .setConfString(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, scanFallback.toString)
            session.sessionState.conf
              .setConfString(GlutenConfig.COLUMNAR_HASHAGG_ENABLED.key, aggFallback.toString)
            val df = session.sql("SELECT max(id) FROM a")
            val newDf = DummyFilterColmnarHelper.dfWithDummyFilterColumnar(
              session,
              df.queryExecution.optimizedPlan)
            val result = newDf.collect
            newDf.explain(true)
            assert(result(0).getLong(0) == 1)
          }
          testWithFallbackSettings(true, true)
          testWithFallbackSettings(true, false)
          testWithFallbackSettings(false, true)
          testWithFallbackSettings(false, false)
        } finally {
          session.sql(s"DROP TABLE IF EXISTS a")
        }
    }
  }
}
