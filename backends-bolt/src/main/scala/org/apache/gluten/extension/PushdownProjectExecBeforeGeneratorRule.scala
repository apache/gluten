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

import org.apache.gluten.config.BoltConfig
import org.apache.gluten.execution.{FilterExecTransformer, GenerateExecTransformer, ProjectExecTransformer}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Cast, Coalesce, Expression, Literal, Size}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType}

case class PushdownProjectExecBeforeGeneratorRule(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!BoltConfig.get.enableColumnarProjectPushdown) {
      return plan
    }
    plan.transformUp {
      case project @ ProjectExecTransformer(
            _,
            filter @ FilterExecTransformer(_, generate: GenerateExecTransformer)) =>
        pushProject(project, generate, Some(filter))
      case project @ ProjectExecTransformer(_, generate: GenerateExecTransformer) =>
        pushProject(project, generate, None)
    }
  }

  private def pushProject(
      project: ProjectExecTransformer,
      generate: GenerateExecTransformer,
      filter: Option[FilterExecTransformer]): SparkPlan = {
    // Moving a nondeterministic expression below Generate changes its evaluation frequency.
    if (!project.projectList.forall(_.deterministic)) {
      return project
    }
    val generateOut = generate.generatorOutput.toSet
    val independentProject = project.projectList
      .filter(p => p.references.toSet.intersect(generateOut).isEmpty)
      .filterNot(isAliasOrAttribute(_))
    if (independentProject.isEmpty || !independentProject.forall(isSafeToEvaluate)) {
      return project
    }
    val newProjectList = project.projectList.map(
      pj =>
        if (independentProject.contains(pj)) { pj.toAttribute }
        else { pj })
    val requiredOutput = AttributeSet(newProjectList.flatMap(_.references)) ++
      filter.map(_.condition.references).getOrElse(AttributeSet.empty)
    val requiredChildOutput = generate.requiredChildOutput.filter(requiredOutput.contains) ++
      independentProject.map(_.toAttribute)
    // Preserve shared generator/filter/pass-through bindings,
    // without keeping unused output columns.
    val requiredInput = generate.generator.references ++ AttributeSet(requiredChildOutput)
    val newChild = ProjectExecTransformer(
      independentProject ++ generate.child.output.filter(requiredInput.contains),
      generate.child)
    val newGenerator = generate.copy(requiredChildOutput = requiredChildOutput, child = newChild)
    val newFilter = filter.map(f => f.copy(child = newGenerator))
    val newProject = ProjectExecTransformer(newProjectList, newFilter.getOrElse(newGenerator))
    if (
      newGenerator.generator.references.subsetOf(newChild.outputSet) &&
      newGenerator.requiredChildOutput.forall(a => newChild.outputSet.contains(a)) &&
      newFilter.forall(f => f.condition.references.subsetOf(newGenerator.outputSet)) &&
      newProject.output == project.output &&
      newChild.doValidate().ok() && newGenerator.doValidate().ok() &&
      newFilter.forall(_.doValidate().ok()) && newProject.doValidate().ok()
    ) {
      newProject
    } else {
      project
    }
  }
  // Generate can produce no rows and a subsequent Filter can discard them all. Evaluating an
  // expression earlier must not introduce errors for rows where Spark never evaluates it.
  // Keep unknown expression trees in their original position.
  private def isSafeToEvaluate(expression: Expression): Boolean = expression match {
    case Alias(child, _) => isSafeToEvaluate(child)
    case _: Attribute | _: Literal => true
    case c: Cast => isSafeToEvaluate(c.child) && isSafeWidening(c.child.dataType, c.dataType)
    case c: Coalesce => c.children.forall(isSafeToEvaluate)
    case s: Size =>
      isSafeToEvaluate(s.child) && (s.child.dataType match {
        case _: ArrayType | _: MapType => true
        case _ => false
      })
    case _ => false
  }

  private def isSafeWidening(from: DataType, to: DataType): Boolean = {
    val integral = Seq(ByteType, ShortType, IntegerType, LongType)
    val fromIntegral = integral.indexOf(from)
    val toIntegral = integral.indexOf(to)
    from == to ||
    (fromIntegral >= 0 && toIntegral >= fromIntegral) ||
    (fromIntegral >= 0 && (to == FloatType || to == DoubleType)) ||
    (from == FloatType && to == DoubleType)
  }

  private def isAliasOrAttribute(project: Expression): Boolean = project match {
    case Alias(child, _) => isAliasOrAttribute(child)
    case _: Attribute => true
    case _ => false
  }
}
