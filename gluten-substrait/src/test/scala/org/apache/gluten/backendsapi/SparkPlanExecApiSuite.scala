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
package org.apache.gluten.backendsapi

import org.apache.spark.sql.execution.SparkPlan

import org.mockito.Answers.CALLS_REAL_METHODS
import org.mockito.Mockito.mock
import org.scalatest.funsuite.AnyFunSuite

class SparkPlanExecApiSuite extends AnyFunSuite {

  test("genOneRowRelationExecTransformer preserves unsupported backend plans") {
    val api = mock(classOf[TestSparkPlanExecApi], CALLS_REAL_METHODS)
    val plan = mock(classOf[SparkPlan])

    assert(api.genOneRowRelationExecTransformer(plan) eq plan)
  }

  abstract class TestSparkPlanExecApi extends SparkPlanExecApi
}
