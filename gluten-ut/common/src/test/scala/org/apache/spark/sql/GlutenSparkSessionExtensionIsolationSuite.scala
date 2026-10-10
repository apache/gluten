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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.classic.ClassicTypes.ClassicSparkSession

class GlutenSparkSessionExtensionIsolationSuite extends SparkFunSuite {
  private def builder: SparkSession.Builder = SparkSession
    .builder()
    .master("local[1]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled", "false")
    .config("spark.plugins", "")

  test("custom planner extensions do not reuse a session created during suite discovery") {
    val existing = builder.getOrCreate()
    try {
      val fresh = builder.withExtensions {
        extensions => extensions.injectPlannerStrategy(_ => DummyFilterColumnarStrategy)
      }
      DummyFilterColmnarHelper.withNewSession(fresh) {
        session =>
          assert(session ne existing)
          assert(existing.sparkContext.isStopped)
          assert(session.asInstanceOf[ClassicSparkSession]
            .sessionState.planner.strategies.contains(DummyFilterColumnarStrategy))
      }
      assert(SparkSession.getActiveSession.isEmpty)
      assert(SparkSession.getDefaultSession.isEmpty)
    } finally {
      existing.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  test("custom planner sessions are stopped when the test body fails") {
    var created: SparkSession = null
    val error = intercept[IllegalStateException] {
      DummyFilterColmnarHelper.withNewSession(builder) {
        session =>
          created = session
          throw new IllegalStateException("test body failed")
      }
    }
    assert(error.getMessage == "test body failed")
    assert(created.sparkContext.isStopped)
    assert(SparkSession.getActiveSession.isEmpty)
    assert(SparkSession.getDefaultSession.isEmpty)
  }
}
