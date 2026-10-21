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
package org.apache.spark.sql.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait

class GlutenDeltaBasedMergeIntoTableUpdateAsDeleteAndInsertSuite
  extends DeltaBasedMergeIntoTableUpdateAsDeleteAndInsertSuite
  with GlutenSQLTestsBaseTrait
  with GlutenMergeIntoTableSuiteBase {
  // Spark's own tests use 5 shuffle partitions. With 1, the write's clustering by _partition is a
  // SinglePartition shuffle, so AQE may replace the write's input with an EmptyRelation when the
  // MERGE writes no rows. That drops MergeRowsExec, so the commit has no MERGE summary.
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.shuffle.partitions", "5")
  }
}
