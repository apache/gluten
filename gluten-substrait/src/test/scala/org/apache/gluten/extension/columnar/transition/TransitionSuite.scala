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
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.component.WithDummyBackend
import org.apache.gluten.exception.GlutenException

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.SharedSparkSession

class TransitionSuite extends SharedSparkSession with TransitionSuiteBase with WithDummyBackend {

  import TransitionSuite._
  import TransitionSuiteBase._

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.ui.enabled", "false")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Convention.ensureSparkRowAndBatchTypesRegistered()
    RowTypeA.ensureRegistered()
    BatchTypeA.ensureRegistered()
    BatchTypeB.ensureRegistered()
    BatchTypeC.ensureRegistered()
    BatchTypeD.ensureRegistered()
    RowTypeB.ensureRegistered()
  }

  test("Trivial C2R") {
    val in = BatchLeaf(BatchTypeA)
    val out = insertTransitions(in, ConventionReq.ofRow(ConventionReq.RowType.Is(RowTypeA)))
    assert(out == BatchToRow(BatchTypeA, RowTypeA, BatchLeaf(BatchTypeA)))
  }

  test("Insert C2R") {
    val in = RowUnary(RowTypeA, BatchLeaf(BatchTypeA))
    val out = insertTransitions(in, ConventionReq.ofRow(ConventionReq.RowType.Is(RowTypeA)))
    assert(out == RowUnary(RowTypeA, BatchToRow(BatchTypeA, RowTypeA, BatchLeaf(BatchTypeA))))
  }

  test("Insert R2C") {
    val in = BatchUnary(BatchTypeA, RowLeaf(RowTypeA))
    val out = insertTransitions(in, ConventionReq.ofRow(ConventionReq.RowType.Is(RowTypeA)))
    assert(
      out == BatchToRow(
        BatchTypeA,
        RowTypeA,
        BatchUnary(BatchTypeA, RowToBatch(RowTypeA, BatchTypeA, RowLeaf(RowTypeA)))))
  }

  test("Insert C2R2C") {
    val in = BatchUnary(BatchTypeA, BatchLeaf(BatchTypeB))
    val out = insertTransitions(in, ConventionReq.ofRow(ConventionReq.RowType.Is(RowTypeA)))
    assert(
      out == BatchToRow(
        BatchTypeA,
        RowTypeA,
        BatchUnary(
          BatchTypeA,
          RowToBatch(
            RowTypeA,
            BatchTypeA,
            BatchToRow(BatchTypeB, RowTypeA, BatchLeaf(BatchTypeB))))))
  }

  test("Insert C2C") {
    val in = BatchUnary(BatchTypeA, BatchLeaf(BatchTypeC))
    val out = insertTransitions(in, ConventionReq.ofRow(ConventionReq.RowType.Is(RowTypeA)))
    assert(
      out == BatchToRow(
        BatchTypeA,
        RowTypeA,
        BatchUnary(
          BatchTypeA,
          BatchToBatch(from = BatchTypeC, to = BatchTypeA, BatchLeaf(BatchTypeC)))))
  }

  test("Insert R2C2R") {
    val in = RowUnary(RowTypeB, RowLeaf(RowTypeA))
    val out = insertTransitions(in, ConventionReq.ofRow(ConventionReq.RowType.Is(RowTypeA)))
    assert(
      out == BatchToRow(
        BatchTypeB,
        RowTypeA,
        RowToBatch(
          RowTypeB,
          BatchTypeB,
          RowUnary(
            RowTypeB,
            BatchToRow(BatchTypeA, RowTypeB, RowToBatch(RowTypeA, BatchTypeA, RowLeaf(RowTypeA)))))
      ))
  }

  test("No transitions found") {
    val in = BatchUnary(BatchTypeA, BatchLeaf(BatchTypeD))
    assertThrows[GlutenException] {
      insertTransitions(in, ConventionReq.ofRow(ConventionReq.RowType.Is(RowTypeA)))
    }
  }
}

object TransitionSuite extends TransitionSuiteBase {
  import TransitionSuiteBase._

  private def insertTransitions(plan: SparkPlan, req: ConventionReq): SparkPlan = {
    InsertTransitions(req).apply(plan)
  }

  object RowTypeA extends Convention.RowType {
    override protected[this] def registerTransitions(): Unit = {}
  }

  object BatchTypeA extends Convention.BatchType {
    override protected[this] def registerTransitions(): Unit = {
      fromRow(RowTypeA, RowToBatch(RowTypeA, this, _))
      toRow(RowTypeA, BatchToRow(this, RowTypeA, _))
    }
  }

  object BatchTypeB extends Convention.BatchType {
    override protected[this] def registerTransitions(): Unit = {
      fromRow(RowTypeA, RowToBatch(RowTypeA, this, _))
      toRow(RowTypeA, BatchToRow(this, RowTypeA, _))
    }
  }

  object BatchTypeC extends Convention.BatchType {
    override protected[this] def registerTransitions(): Unit = {
      fromRow(RowTypeA, RowToBatch(RowTypeA, this, _))
      toRow(RowTypeA, BatchToRow(this, RowTypeA, _))
      fromBatch(BatchTypeA, BatchToBatch(BatchTypeA, this, _))
      toBatch(BatchTypeA, BatchToBatch(this, BatchTypeA, _))
    }
  }

  object BatchTypeD extends Convention.BatchType {
    override protected[this] def registerTransitions(): Unit = {}
  }

  object RowTypeB extends Convention.RowType {
    override protected[this] def registerTransitions(): Unit = {
      fromBatch(BatchTypeA, BatchToRow(BatchTypeA, this, _))
      toBatch(BatchTypeB, RowToBatch(this, BatchTypeB, _))
    }
  }
}
