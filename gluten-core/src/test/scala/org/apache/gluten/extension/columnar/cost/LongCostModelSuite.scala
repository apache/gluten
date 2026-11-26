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
package org.apache.gluten.extension.columnar.cost

import org.apache.gluten.component.WithDummyBackend
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.cost.LongCostModel.{Legacy, Rough}

import org.apache.spark.sql.execution.SparkPlan

import org.scalatest.funsuite.AnyFunSuite

class LongCostModelSuite extends AnyFunSuite with WithDummyBackend {

  private def legacyOnlyRegistry(): LongCostModel.Registry = {
    val registry = LongCostModel.registry()
    registry.register(new LongCoster {
      override def kind(): LongCostModel.Kind = Legacy
      override def selfCostOf(node: SparkPlan): Option[Long] = Some(1)
    })
    registry
  }

  // Keep first: the last test touches Rough, which registers it and would hide a
  // Kind.values() that misses it.
  test("a built-in cost model the backend has no coster for is rejected by name") {
    // The dummy backend only registers a legacy coster.
    val error = intercept[GlutenException](GlutenCostModel.find("rough"))
    assert(error.getMessage.contains("'rough'"))
    assert(error.getMessage.contains("spark.gluten.costModel"))
  }

  test("a cost model with registered costers is returned") {
    assert(legacyOnlyRegistry().get(Legacy) != null)
  }

  test("a cost model without registered costers is rejected with its name") {
    val error = intercept[GlutenException](legacyOnlyRegistry().get(Rough))
    assert(error.getMessage.contains("'rough'"))
    assert(error.getMessage.contains("spark.gluten.costModel"))
  }
}
