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

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation

import java.time.LocalDateTime

class GlutenCachedTableSuite
  extends CachedTableSuite
  with GlutenSQLTestsTrait
  with AdaptiveSparkPlanHelper {
  // for temporarily disable the columnar table cache globally.
  sys.props.put(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.shuffle.partitions", "5")
    super.sparkConf.set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
  }

  testGluten("InMemoryRelation statistics") {
    sql("CACHE TABLE testData")
    spark.table("testData").queryExecution.withCachedData.collect {
      case cached: InMemoryRelation =>
        assert(cached.stats.sizeInBytes === 1132)
    }
  }

  testGluten("SPARK-36120: Support cache/uncache table with TimestampNTZ type") {
    val tableName = "ntzCache"
    withTable(tableName) {
      sql(s"CACHE TABLE $tableName AS SELECT TIMESTAMP_NTZ'2021-01-01 00:00:00'")
      checkAnswer(spark.table(tableName), Row(LocalDateTime.parse("2021-01-01T00:00:00")))
      spark.table(tableName).queryExecution.withCachedData.collect {
        case cached: InMemoryRelation =>
          assert(cached.stats.sizeInBytes === 60)
      }
      sql(s"UNCACHE TABLE $tableName")
    }
  }
}
