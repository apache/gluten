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

import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.exchange.ValidateRequirements
import org.apache.spark.sql.internal.SQLConf

/**
 * Gluten plan stability suites verify that Gluten-transformed plans satisfy Spark's distribution
 * and ordering requirements (via ValidateRequirements). Golden file comparison against vanilla
 * Spark plans is skipped because Gluten intentionally produces different physical plans using
 * native Transformer operators.
 */
trait GlutenPlanStabilityTestTrait {
  self: PlanStabilitySuite =>

  override protected def testQuery(tpcdsGroup: String, query: String, suffix: String = ""): Unit = {
    val queryString = resourceToString(
      s"$tpcdsGroup/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    withSQLConf(
      SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
      SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val qe = sql(queryString).queryExecution
      val plan = qe.executedPlan
      assert(
        ValidateRequirements.validate(plan),
        s"ValidateRequirements failed for $tpcdsGroup/$query$suffix")
    }
  }
}

class GlutenTPCDSV1_4_PlanStabilitySuite
  extends TPCDSV1_4_PlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSV1_4_PlanStabilityWithStatsSuite
  extends TPCDSV1_4_PlanStabilityWithStatsSuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSV2_7_PlanStabilitySuite
  extends TPCDSV2_7_PlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSV2_7_PlanStabilityWithStatsSuite
  extends TPCDSV2_7_PlanStabilityWithStatsSuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSModifiedPlanStabilitySuite
  extends TPCDSModifiedPlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSModifiedPlanStabilityWithStatsSuite
  extends TPCDSModifiedPlanStabilityWithStatsSuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCHPlanStabilitySuite
  extends TPCHPlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}
