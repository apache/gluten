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

import org.apache.gluten.execution.{BroadcastHashJoinExecTransformer, FilterExecTransformer, LimitExecTransformer, ProjectExecTransformer}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeSet, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

import scala.collection.mutable.ArrayBuffer

/**
 * Bolt does not allow duplicate projections in HashProbe and FilterProject, this rule pull out
 * duplicate projections to a new project outside.
 */
object PullOutDuplicateProject extends Rule[SparkPlan] with PredicateHelper {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case l @ LimitExecTransformer(p: ProjectExecTransformer, _, _) =>
      val duplicates = calculateDuplicates(p, AttributeSet.empty)
      if (duplicates.isEmpty) {
        l
      } else {
        val pullOutAliases = new ArrayBuffer[Alias]()
        val newChild = rewriteProject(p, AttributeSet.empty, pullOutAliases, duplicates)
        outerProject(l.copy(child = newChild), l.output, pullOutAliases)
      }
    case p @ ProjectExecTransformer(_, child: ProjectExecTransformer) =>
      val duplicates = calculateDuplicates(child, AttributeSet.empty)
      if (duplicates.isEmpty) {
        p
      } else {
        val pullOutAliases = new ArrayBuffer[Alias]()
        val newChild = rewriteProject(child, AttributeSet.empty, pullOutAliases, duplicates)
        val aliasMap = AttributeMap(pullOutAliases.map(a => a.toAttribute -> a))
        val newProjectList = p.projectList.map(replaceAliasButKeepName(_, aliasMap))
        ProjectExecTransformer(newProjectList, newChild)
      }
    case f @ FilterExecTransformer(_, child: ProjectExecTransformer) =>
      val duplicates = calculateDuplicates(child, f.references)
      if (duplicates.isEmpty) {
        f
      } else {
        val pullOutAliases = new ArrayBuffer[Alias]()
        val newChild = rewriteProject(child, f.references, pullOutAliases, duplicates)
        outerProject(f.copy(child = newChild), f.output, pullOutAliases)
      }
    case bhj: BroadcastHashJoinExecTransformer
        if bhj.streamedPlan.isInstanceOf[ProjectExecTransformer] =>
      val duplicates =
        calculateDuplicates(bhj.streamedPlan.asInstanceOf[ProjectExecTransformer], bhj.references)
      if (duplicates.isEmpty) {
        bhj
      } else {
        val pullOutAliases = new ArrayBuffer[Alias]()
        val newStreamedPlan = rewriteProject(
          bhj.streamedPlan.asInstanceOf[ProjectExecTransformer],
          bhj.references,
          pullOutAliases,
          duplicates)
        val newBhj = bhj.joinBuildSide match {
          case BuildLeft => bhj.copy(right = newStreamedPlan)
          case BuildRight => bhj.copy(left = newStreamedPlan)
        }
        outerProject(newBhj, bhj.output, pullOutAliases)
      }
  }

  private def outerProject(
      child: SparkPlan,
      output: Seq[Attribute],
      pullOutAliases: ArrayBuffer[Alias]): ProjectExecTransformer = {
    val aliasMap = AttributeMap(pullOutAliases.map(a => a.toAttribute -> a))
    val newProjectList = output.map(attr => aliasMap.getOrElse(attr, attr))
    ProjectExecTransformer(newProjectList, child)
  }

  /** Calculate the original attributes corresponding to duplicate projections. */
  private def calculateDuplicates(
      project: ProjectExecTransformer,
      references: AttributeSet): AttributeSet = {
    val projectList = project.projectList
    AttributeSet(
      projectList
        .collect {
          case attr: Attribute if !references.contains(attr) => attr
          case a @ Alias(attr: Attribute, _)
              if !references.contains(a) && !references.contains(attr) =>
            attr
        }
        .groupBy(_.exprId)
        .filter(_._2.size > 1)
        .map(_._2.head))
  }

  /**
   * If there are duplicate projections and not refer to parent, only the original attribute is kept
   * in the project.
   */
  private def rewriteProject(
      project: ProjectExecTransformer,
      references: AttributeSet,
      pullOutAliases: ArrayBuffer[Alias],
      duplicates: AttributeSet): SparkPlan = {
    val projectList = project.projectList
    val newProjectList = projectList.distinct.filter {
      case a @ Alias(attr: Attribute, _) if !references.contains(a) && duplicates.contains(attr) =>
        pullOutAliases.append(a)
        false
      case _ => true
    } ++ duplicates.filter(!project.outputSet.contains(_)).toSeq
    val newProject = project.copy(projectList = newProjectList)
    newProject.copyTagsFrom(project)
    // If the output of the new project is the same as the child, delete it to simplify the plan.
    if (newProject.outputSet.equals(project.child.outputSet)) {
      project.child
    } else {
      newProject

    }
  }
}
