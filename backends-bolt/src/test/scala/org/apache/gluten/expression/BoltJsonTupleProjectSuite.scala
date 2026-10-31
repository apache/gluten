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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.bolt.BoltSparkPlanExecApi
import org.apache.gluten.execution.PullOutGenerateProjectHelper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{GenerateExec, LeafExecNode, ProjectExec}
import org.apache.spark.sql.types.{StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class BoltJsonTupleProjectSuite extends AnyFunSuite {
  private case class InputPlan(attr: Attribute) extends LeafExecNode {
    override def output: Seq[Attribute] = Seq(attr)
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Plan-only regression")
  }
  for {
    keys <- Seq(Seq("test"), Seq("first", "second", "third"))
    outer <- Seq(false, true)
  } {
    test(
      s"json_tuple with ${keys.size} fields and outer=$outer projects native row fields") {
      val json = AttributeReference("json", StringType)()
      val output = keys.indices.map(i => AttributeReference(s"c$i", StringType)())
      val original = GenerateExec(
        JsonTuple(Seq(json) ++ keys.map(Literal(_))),
        Nil,
        outer,
        output,
        InputPlan(json))
      val pre = PullOutGenerateProjectHelper.pullOutPreProject(original).asInstanceOf[GenerateExec]
      assert(pre.generator.isInstanceOf[JsonTupleExplode])
      val post = new BoltSparkPlanExecApi().genPostProjectForGenerate(pre)
      assert(post.isInstanceOf[ProjectExec])
      val project = post.asInstanceOf[ProjectExec]
      val native = project.child.asInstanceOf[GenerateExec]
      assert(native.generatorOutput.size == 1)
      assert(native.generatorOutput.head.dataType.isInstanceOf[StructType])
      assert(project.output.map(_.dataType) == output.map(_.dataType))
      assert(project.output.map(_.exprId) == output.map(_.exprId))
      assert(project.projectList.map(
        _.asInstanceOf[Alias].child.asInstanceOf[GetStructField].ordinal) == keys.indices)
    }
  }
  test("ordinary explode retains existing native generator output order") {
    val array = AttributeReference("array", org.apache.spark.sql.types.ArrayType(StringType))()
    val original = GenerateExec(
      Explode(array),
      Nil,
      false,
      Seq(AttributeReference("value", StringType)()),
      InputPlan(array))
    assert(new BoltSparkPlanExecApi().genPostProjectForGenerate(original) eq original)
  }
}
