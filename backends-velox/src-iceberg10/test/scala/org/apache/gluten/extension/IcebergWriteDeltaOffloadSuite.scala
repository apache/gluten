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
package org.apache.gluten.extension

import org.apache.spark.sql.connector.write.DeltaWrite
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.WriteDeltaExec

import org.mockito.Mockito.{mock, verify, verifyZeroInteractions, when}
import org.scalatest.funsuite.AnyFunSuite

class IcebergWriteDeltaOffloadSuite extends AnyFunSuite {

  test("enhanced-feature gate preserves WriteDeltaExec without inspecting it") {
    val writeDelta = mock(classOf[WriteDeltaExec])

    assert(IcebergWriteDeltaOffload.offload(
      writeDelta,
      enhancedFeaturesEnabled = false) eq writeDelta)
    verifyZeroInteractions(writeDelta)
  }

  test("unsupported WriteDeltaExec falls back when enhanced features are enabled") {
    val write = mock(classOf[DeltaWrite])
    val writeDelta = mock(classOf[WriteDeltaExec])
    when(writeDelta.write).thenReturn(write)

    assert(IcebergWriteDeltaOffload.offload(
      writeDelta,
      enhancedFeaturesEnabled = true) eq writeDelta)
    verify(writeDelta).write
  }

  test("non-WriteDelta plans are preserved") {
    val plan = mock(classOf[SparkPlan])

    assert(IcebergWriteDeltaOffload.offload(plan, enhancedFeaturesEnabled = true) eq plan)
    verifyZeroInteractions(plan)
  }
}
