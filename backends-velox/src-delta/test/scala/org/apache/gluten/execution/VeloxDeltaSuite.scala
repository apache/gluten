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

import org.apache.gluten.config.VeloxDeltaConfig

import org.apache.spark.sql.Row

class VeloxDeltaSuite extends DeltaSuite {
  testWithMinSparkVersion("delta: change data feed scan offload can be disabled", "3.2") {
    withTable("delta_cdf_disabled") {
      spark.sql("""
                  |create table delta_cdf_disabled (id int, name string) using delta
                  |tblproperties ("delta.enableChangeDataFeed" = "true")
                  |""".stripMargin)
      spark.sql("""
                  |insert into delta_cdf_disabled values (1, "v1"), (2, "v2")
                  |""".stripMargin)

      withSQLConf(VeloxDeltaConfig.ENABLE_CHANGE_DATA_FEED_SCAN.key -> "false") {
        val df = spark.sql("""
                             |select id, name, _change_type, _commit_version
                             |from table_changes('delta_cdf_disabled', 1)
                             |""".stripMargin)
        checkAnswer(
          df,
          Seq(
            Row(1, "v1", "insert", 1L),
            Row(2, "v2", "insert", 1L)))
        assert(
          df.queryExecution.executedPlan.collect {
            case scan: DeltaScanTransformer => scan
          }.isEmpty,
          df.queryExecution.executedPlan)
      }
    }
  }
}
