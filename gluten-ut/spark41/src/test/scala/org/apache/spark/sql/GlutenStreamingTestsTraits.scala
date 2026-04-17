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

import org.apache.spark.SparkConf

private[sql] object GlutenStreamingTestConf {
  def withFallbackToVanilla(conf: SparkConf): SparkConf = {
    conf
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.shuffle.manager", "sort")
      .set("spark.gluten.sql.columnar.batchscan", "false")
      .set("spark.gluten.sql.columnar.filescan", "false")
      .set("spark.gluten.sql.columnar.project", "false")
      .set("spark.gluten.sql.columnar.filter", "false")
      .set("spark.gluten.sql.columnar.sort", "false")
      .set("spark.gluten.sql.columnar.window", "false")
      .set("spark.gluten.sql.columnar.union", "false")
      .set("spark.gluten.sql.columnar.expand", "false")
      .set("spark.gluten.sql.columnar.generate", "false")
      .set("spark.gluten.sql.columnar.coalesce", "false")
      .set("spark.gluten.sql.columnar.range", "false")
      .set("spark.gluten.sql.columnar.shuffle", "false")
      .set("spark.gluten.sql.columnar.hashagg", "false")
      .set("spark.gluten.sql.columnar.shuffledHashJoin", "false")
      .set("spark.gluten.sql.columnar.sortMergeJoin", "false")
      .set("spark.gluten.sql.columnar.broadcastExchange", "false")
      .set("spark.gluten.sql.columnar.broadcastJoin", "false")
      .set("spark.gluten.sql.columnar.appendData", "false")
      .set("spark.gluten.sql.columnar.writeToDataSourceV2", "false")
      .set("spark.gluten.sql.native.writer.enabled", "false")
      .set("spark.gluten.sql.columnar.query.fallback.threshold", "0")
      .set("spark.gluten.sql.columnar.wholeStage.fallback.threshold", "0")
      .set("spark.gluten.sql.columnar.fallback.expressions.threshold", "0")
      .set("spark.gluten.sql.columnar.fallback.preferColumnar", "false")
      .set("spark.gluten.expression.blacklist", "collect_list,collect_set")
  }
}

trait GlutenStreamingSQLTestsTrait extends GlutenSQLTestsTrait {
  override def sparkConf: SparkConf = {
    GlutenStreamingTestConf.withFallbackToVanilla(super.sparkConf)
  }
}

trait GlutenStreamingVanillaFallbackTestsTrait extends GlutenStreamingSQLTestsTrait {
  override def sparkConf: SparkConf = {
    super.sparkConf
  }
}
