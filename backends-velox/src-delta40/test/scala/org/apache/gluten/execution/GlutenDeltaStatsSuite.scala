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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

import java.io.File

/**
 * Regression test for the Gluten Delta per-file statistics tracker.
 *
 * Writing a Delta table whose collected min/max statistics cannot be offloaded to Velox -- for
 * example over a TIMESTAMP_NTZ column -- used to crash the write task with a ClassCastException
 * (ProjectExec cannot be cast to WholeStageTransformer), because the native stats tracker assumed
 * the statistics aggregation always collapses into a WholeStageTransformer. The tracker must now
 * fall back to row-based statistics collection instead of crashing.
 */
class GlutenDeltaStatsSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  import testImplicits._

  test("TIMESTAMP_NTZ stats fall back instead of crashing the write") {
    withTempDir {
      dir =>
        val path = new File(dir, "ntz-stats").getCanonicalPath
        // Collecting min/max statistics over a TIMESTAMP_NTZ column is what cannot be offloaded to
        // Velox (a type limitation, independent of the value), so any in-range timestamp exercises
        // the same fallback path. Use a normal timestamp to keep the test focused on the fallback
        // rather than on timestamp-overflow edge cases.
        val micros = 1704067200000000L // 2024-01-01T00:00:00Z
        val data = Seq(micros)
          .toDF("micros")
          .selectExpr("micros AS id", "CAST(TIMESTAMP_MICROS(micros) AS TIMESTAMP_NTZ) AS ts")

        // Without the fix this write fails with a ClassCastException (ProjectExec cannot be cast
        // to WholeStageTransformer) while collecting statistics. With the fix it succeeds via the
        // row-based fallback tracker. A count avoids materializing the TIMESTAMP_NTZ column, which
        // is an unrelated read-path limitation.
        data.coalesce(1).write.format("delta").save(path)

        assert(spark.read.format("delta").load(path).count() === 1)
    }
  }
}
