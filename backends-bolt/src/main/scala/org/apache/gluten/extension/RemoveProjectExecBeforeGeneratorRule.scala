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
import org.apache.gluten.execution.{GenerateExecTransformer, ProjectExecTransformer}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
object RemoveProjectExecBeforeGeneratorRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!BoltConfig.get.enableColumnarProjectRemove) {
      return plan
    }
    plan.transformUp {
      case project @ ProjectExecTransformer(_, generate: GenerateExecTransformer) =>
        if (project.output.size != generate.output.size) {
          return plan
        }
        val generateOut = generate.generatorOutput.toSet
        val genInputProject =
          project.projectList
            .filterNot(p => p.references.toSet.intersect(generateOut).isEmpty)
            .filter(
              p =>
                (p.isInstanceOf[Alias] && p.asInstanceOf[Alias].child.isInstanceOf[Attribute]) || p
                  .isInstanceOf[Attribute])
        if (genInputProject.isEmpty) {
          return plan
        }
        var newGeneratorOutput = generate.generatorOutput
        if (genInputProject.flatMap(p => p.references).equals(generate.generatorOutput)) {
          newGeneratorOutput = genInputProject.map(p => p.toAttribute)
        } else {
          return plan
        }
        val newGeneratorExec = generate.copy(
          generator = generate.generator,
          requiredChildOutput = generate.requiredChildOutput,
          outer = generate.outer,
          generatorOutput = newGeneratorOutput,
          child = generate.child
        )
        val newProject =
          ProjectExecTransformer(project.projectList.map(pj => pj.toAttribute), newGeneratorExec)
        val validationResult = newProject.doValidate()
        if (validationResult.ok() && newGeneratorExec.output.equals(newProject.output)) {
          newGeneratorExec
        } else {
          project
        }
    }
  }
}
