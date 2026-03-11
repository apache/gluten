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
package org.apache.gluten.component

import org.apache.gluten.backend.Backend
import org.apache.gluten.extension.columnar.cost.{LongCoster, LongCostModel}
import org.apache.gluten.extension.columnar.cost.LongCostModel.Legacy
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan

import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithDummyBackend extends BeforeAndAfterAll {
  this: Suite =>
  import WithDummyBackend._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    DummyBackend.ensureRegistered()
  }
}

object WithDummyBackend {
  object DummyBackend extends Backend {
    override def name(): String = "dummy-backend"
    override def injectRules(injector: Injector): Unit = {}
    override def costers(): Seq[LongCoster] = Seq(new LongCoster {
      override def kind(): LongCostModel.Kind = Legacy
      override def selfCostOf(node: SparkPlan): Option[Long] = Some(1)
    })
  }
}
