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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Generator}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
object RemoveProjectExecBeforeGeneratorRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!BoltConfig.get.enableColumnarProjectRemove) {
      return plan
    }
    plan.transformUp {
      case project @ ProjectExecTransformer(_, generate: GenerateExecTransformer) =>
        removeProject(project, generate, None)
      case project @ ProjectExecTransformer(
            _,
            filter @ FilterExecTransformer(_, generate: GenerateExecTransformer)) =>
        removeProject(project, generate, Some(filter))
    }
  }

  private def removeProject(
      project: ProjectExecTransformer,
      generate: GenerateExecTransformer,
      filter: Option[FilterExecTransformer]): SparkPlan = {
    val references = project.projectList.map {
      case attribute: Attribute => Some(attribute)
      case Alias(attribute: Attribute, _) => Some(attribute)
      case _ => None
    }
    val originalOutput = references.flatten
    val output = project.output
    // Only a one-to-one rename in the generator's original output order is removable.
    // Reordering, duplication, or computation needs its original projection.
    if (
      references.exists(_.isEmpty) || originalOutput != generate.output ||
      originalOutput.map(_.exprId).distinct.size != originalOutput.size ||
      output.map(_.exprId).distinct.size != output.size
    ) {
      return project
    }
    val renamed = originalOutput.zip(output).map { case (from, to) => from.exprId -> to }.toMap
    if (filter.exists(f => f.condition.references.exists(a => !renamed.contains(a.exprId)))) {
      return project
    }
    def renameReferences(expression: Expression): Expression = expression.transform {
      case attribute: Attribute => renamed.getOrElse(attribute.exprId, attribute)
    }

    val inputProjects = project.projectList.take(generate.requiredChildOutput.size)
    val inputAliases = generate.requiredChildOutput.zip(inputProjects)
      .collect { case (from, to: Alias) => from.exprId -> to }.toMap
    val inputProject = if (inputAliases.isEmpty) {
      None
    } else {
      Some(ProjectExecTransformer(
        generate.child.output.map(a => inputAliases.getOrElse(a.exprId, a)),
        generate.child))
    }
    val rewrittenGenerator = generate.copy(
      generator = renameReferences(generate.generator).asInstanceOf[Generator],
      requiredChildOutput = inputProjects.map(_.toAttribute),
      generatorOutput = output.drop(generate.requiredChildOutput.size),
      child = inputProject.getOrElse(generate.child)
    )
    val replacement: SparkPlan = filter match {
      case Some(original) =>
        original.copy(renameReferences(original.condition), rewrittenGenerator)
      case None => rewrittenGenerator
    }
    val sameOutput = replacement.output == output && replacement.output.zip(output).forall {
      case (actual, expected) =>
        actual.name == expected.name && actual.dataType == expected.dataType &&
        actual.nullable == expected.nullable && actual.exprId == expected.exprId &&
        actual.qualifier == expected.qualifier && actual.metadata == expected.metadata
    }
    val validFilter = replacement match {
      case rewritten: FilterExecTransformer => rewritten.doValidate().ok()
      case _ => true
    }
    if (
      sameOutput && inputProject.forall(_.doValidate().ok()) &&
      rewrittenGenerator.doValidate().ok() && validFilter
    ) {
      replacement
    } else {
      project
    }
  }
}
