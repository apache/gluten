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
package org.apache.spark.sql.execution.python

import org.apache.gluten.execution.{ColumnarToRowExecBase, FilterExecTransformer, RowToColumnarExecBase, WholeStageTransformer}

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GreaterThan, In}
import org.apache.spark.sql.execution.{ColumnarInputAdapter, InputIteratorTransformer}

class GlutenBatchEvalPythonExecSuite extends BatchEvalPythonExecSuite with GlutenSQLTestsBaseTrait {

  import testImplicits._

  testGluten("Python UDF: push down deterministic FilterExecTransformer predicates") {
    val df = Seq(("Hello", 4))
      .toDF("a", "b")
      .where("dummyPythonUDF(b) and dummyPythonUDF(a) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExecTransformer(
            And(_: AttributeReference, _: AttributeReference),
            InputIteratorTransformer(ColumnarInputAdapter(r: RowToColumnarExecBase)))
          if r.child.isInstanceOf[BatchEvalPythonExec] =>
        f
      case b @ BatchEvalPythonExec(_, _, c: ColumnarToRowExecBase) =>
        c.child match {
          case WholeStageTransformer(FilterExecTransformer(_: In, _), _) => b
        }
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  testGluten("Nested Python UDF: push down deterministic FilterExecTransformer predicates") {
    val df = Seq(("Hello", 4))
      .toDF("a", "b")
      .where("dummyPythonUDF(a, dummyPythonUDF(a, b)) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExecTransformer(
            _: AttributeReference,
            InputIteratorTransformer(ColumnarInputAdapter(r: RowToColumnarExecBase)))
          if r.child.isInstanceOf[BatchEvalPythonExec] =>
        f
      case b @ BatchEvalPythonExec(_, _, c: ColumnarToRowExecBase) =>
        c.child match {
          case WholeStageTransformer(FilterExecTransformer(_: In, _), _) => b
        }
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  testGluten("Python UDF: no push down on non-deterministic") {
    val df = Seq(("Hello", 4))
      .toDF("a", "b")
      .where("b > 4 and dummyPythonUDF(a) and rand() > 0.3")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExecTransformer(
            And(_: AttributeReference, _: GreaterThan),
            InputIteratorTransformer(ColumnarInputAdapter(r: RowToColumnarExecBase)))
          if r.child.isInstanceOf[BatchEvalPythonExec] =>
        f
      case b @ BatchEvalPythonExec(_, _, c: ColumnarToRowExecBase) =>
        c.child match {
          case WholeStageTransformer(_: FilterExecTransformer, _) => b
        }
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  testGluten(
    "Python UDF: push down on deterministic predicates after the first non-deterministic") {
    val df = Seq(("Hello", 4))
      .toDF("a", "b")
      .where("dummyPythonUDF(a) and rand() > 0.3 and b > 4")

    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExecTransformer(
            And(_: AttributeReference, _: GreaterThan),
            InputIteratorTransformer(ColumnarInputAdapter(r: RowToColumnarExecBase)))
          if r.child.isInstanceOf[BatchEvalPythonExec] =>
        f
      case b @ BatchEvalPythonExec(_, _, c: ColumnarToRowExecBase) =>
        c.child match {
          case WholeStageTransformer(_: FilterExecTransformer, _) => b
        }
    }
    assert(qualifiedPlanNodes.size == 2)
  }
}
