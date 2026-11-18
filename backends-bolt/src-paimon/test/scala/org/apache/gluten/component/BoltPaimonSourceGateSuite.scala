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

import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

class BoltPaimonSourceGateSuite extends AnyFunSuite {
  private val key = "spark.gluten.paimon.native.source.enabled"
  private case class TestPlan(label: String) extends LeafExecNode {
    override def output: Seq[Attribute] = Seq(AttributeReference(label, IntegerType)())
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Rule-only regression plan")
  }
  private val original = TestPlan("original")
  private val transformed = TestPlan("native")
  private val delegate = new OffloadSingleNode {
    override def offload(plan: SparkPlan): SparkPlan = transformed
  }

  test("Bolt Paimon source defaults to existing native offload") {
    SQLConf.withExistingConf(new SQLConf) {
      assert(OffloadBoltPaimonScan(delegate).offload(original) eq transformed)
    }
  }

  test("disabled Bolt Paimon source preserves original Spark plan") {
    val conf = new SQLConf
    conf.setConfString(key, "false")
    SQLConf.withExistingConf(conf) {
      assert(OffloadBoltPaimonScan(delegate).offload(original) eq original)
    }
  }

  test("Bolt Paimon source reads current configuration when rule executes") {
    val conf = new SQLConf
    val rule = OffloadBoltPaimonScan(delegate)
    SQLConf.withExistingConf(conf) {
      conf.setConfString(key, "false")
      assert(rule.offload(original) eq original)
      conf.setConfString(key, "true")
      assert(rule.offload(original) eq transformed)
      conf.setConfString(key, "false")
      assert(rule.offload(original) eq original)
    }
  }

  test("one Bolt Paimon rule respects each current SQLConf context") {
    val rule = OffloadBoltPaimonScan(delegate)
    val disabled = new SQLConf
    disabled.setConfString(key, "false")
    SQLConf.withExistingConf(disabled)(assert(rule.offload(original) eq original))
    SQLConf.withExistingConf(new SQLConf)(assert(rule.offload(original) eq transformed))
  }
}
