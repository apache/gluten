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
package org.apache.spark.sql.sources

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.execution.FileSourceScanExec

class GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite
  extends DisableUnnecessaryBucketedScanWithoutHiveSupportSuite
  with GlutenSQLTestsBaseTrait {}

class GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE
  extends DisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE
  with GlutenSQLTestsBaseTrait {

  // The executed plan contains Gluten's FileSourceScanExecTransformer instead of the vanilla
  // FileSourceScanExec, so count bucketed scans of both.
  override protected def checkNumBucketedScan(query: String, expectedNumBucketedScan: Int): Unit = {
    val df = sql(query)
    df.collect()
    val plan = df.queryExecution.executedPlan
    val bucketedScan = collect(plan) {
      case s: FileSourceScanExec if s.bucketedScan => s
      case s: FileSourceScanExecTransformer if s.bucketedScan => s
    }
    assert(bucketedScan.length == expectedNumBucketedScan)
  }
}
