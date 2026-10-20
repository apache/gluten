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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class PushdownProjectExecBeforeGeneratorRule(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!BoltConfig.get.enableColumnarProjectPushdown) {
      return plan
    }
    plan.transformUp {
      case project @ ProjectExecTransformer(
            _,
            filter @ FilterExecTransformer(_, generate: GenerateExecTransformer)) =>
        val generateOut = generate.generatorOutput.toSet
        val independentProject = project.projectList
          .filter(p => p.references.toSet.intersect(generateOut).isEmpty)
          .filterNot(isAliasOrAttribute(_))
        if (independentProject.isEmpty) {
          return plan
        }
        val independentProjectReferences = independentProject.flatMap(p => p.references).toSet
        val newGeneratorExec = generate.copy(
          generator = generate.generator,
          requiredChildOutput = generate.requiredChildOutput.filterNot(
            p => independentProjectReferences.contains(p)) ++ independentProject.map(
            pj => pj.toAttribute),
          outer = generate.outer,
          generatorOutput = generate.generatorOutput,
          child = ProjectExecTransformer(
            independentProject ++ generate.inputSet.seq.filterNot(
              p => independentProjectReferences.contains(p)),
            generate.child)
        )
        val newFilter = filter.copy(filter.condition, newGeneratorExec)
        val newProject =
          ProjectExecTransformer(
            project.projectList.map(
              pj =>
                if (pj.references.toSet.intersect(generateOut).isEmpty) { pj.toAttribute }
                else { pj }),
            newFilter)
        val validationResult = newProject.doValidate()
        if (validationResult.ok()) {
          newProject
        } else {
          project
        }
      case project @ ProjectExecTransformer(_, generate: GenerateExecTransformer) =>
        // reorder project and generator
        val generateOut = generate.generatorOutput.toSet
        val independentProject = project.projectList
          .filter(p => p.references.toSet.intersect(generateOut).isEmpty)
          .filterNot(isAliasOrAttribute(_))
        if (independentProject.isEmpty) {
          return plan
        }
        val independentProjectReferences = independentProject.flatMap(p => p.references).toSet
        val newGeneratorExec = generate.copy(
          generator = generate.generator,
          requiredChildOutput = generate.requiredChildOutput.filterNot(
            p => independentProjectReferences.contains(p)) ++ independentProject.map(
            pj => pj.toAttribute),
          outer = generate.outer,
          generatorOutput = generate.generatorOutput,
          child = ProjectExecTransformer(
            independentProject ++ generate.inputSet.seq.filterNot(
              p => independentProjectReferences.contains(p)),
            generate.child)
        )
        val newProject =
          ProjectExecTransformer(
            project.projectList.map(
              pj =>
                if (pj.references.toSet.intersect(generateOut).isEmpty) { pj.toAttribute }
                else { pj }),
            newGeneratorExec)
        val validationResult = newProject.doValidate()
        if (validationResult.ok()) {
          newProject
        } else {
          project
        }
    }
  }
  private def isAliasOrAttribute(project: Expression): Boolean = project match {
    case Alias(child, _) => isAliasOrAttribute(child)
    case _: Attribute => true
    case _ => false
  }
}
