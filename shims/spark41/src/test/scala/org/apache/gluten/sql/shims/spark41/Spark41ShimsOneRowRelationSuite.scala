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
package org.apache.gluten.sql.shims.spark41

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, LocalTableScanExec, OneRowRelationExec => SparkOneRowRelationExec}
import org.apache.spark.sql.test.SharedSparkSession

private case class OneRowRelationExec() extends LeafExecNode {
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("test-only plan")
}

class Spark41ShimsOneRowRelationSuite extends SharedSparkSession {
  private val shims = new Spark41Shims()

  test("recognizes OneRowRelationExec") {
    assert(shims.isOneRowRelationExec(SparkOneRowRelationExec()))
  }

  test("does not recognize an unrelated zero-column leaf") {
    assert(!shims.isOneRowRelationExec(LocalTableScanExec(Nil, Seq(InternalRow.empty), None)))
  }

  test("does not recognize a different plan with the same simple class name") {
    val sameNamePlan = OneRowRelationExec()

    assert(sameNamePlan.getClass.getSimpleName == "OneRowRelationExec")
    assert(!shims.isOneRowRelationExec(sameNamePlan))
  }
}
