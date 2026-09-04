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
package org.apache.spark.sql.delta

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

class GlutenDeltaStatsSuite extends DeltaSQLCommandTest {

  import testImplicits._

  test("collect TIMESTAMP_NTZ statistics natively") {
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val data = Seq(
            "1969-12-31 23:59:59.999999",
            "2024-01-01 00:00:00.123456"
          ).toDF("input")
            .selectExpr(
              "cast(input as timestamp_ntz) as ts",
              "struct(cast(input as timestamp_ntz) as ts) as nested")

          data.coalesce(1).write.format("delta").save(path)

          val actual = spark.read.format("delta").load(path)
          assert(actual.collect().toSet == data.collect().toSet)

          val addFiles = DeltaLog.forTable(spark, path).update().allFiles.collect()
          assert(addFiles.length == 1)
          val stats = addFiles.head.stats
          assert(stats != null)
          val statsValues = Seq(stats)
            .toDF("stats")
            .selectExpr(
              "get_json_object(stats, '$.minValues.ts')",
              "get_json_object(stats, '$.minValues.nested.ts')",
              "get_json_object(stats, '$.maxValues.ts')",
              "get_json_object(stats, '$.maxValues.nested.ts')"
            )
            .head()
          assert(
            statsValues == Row(
              "1969-12-31T23:59:59.999",
              "1969-12-31T23:59:59.999",
              "2024-01-01T00:00:00.123",
              "2024-01-01T00:00:00.123"),
            stats)
      }
    }
  }
}
