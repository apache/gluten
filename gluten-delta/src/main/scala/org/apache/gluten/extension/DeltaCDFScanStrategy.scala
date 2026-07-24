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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class DeltaCDFScanStrategy(spark: SparkSession, offloadEnabled: () => Boolean)
  extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!offloadEnabled()) {
      return Nil
    }

    plan match {
      case PhysicalOperation(projects, filters, relation: LogicalRelation) =>
        relation.relation match {
          case cdfRelation: CDCReader.DeltaCDFRelation
              if !changesContainDeletionVectors(cdfRelation) =>
            planCDFRelation(relation, cdfRelation, projects, filters).map(planLater).toSeq
          case _ => Nil
        }
      case _ => Nil
    }
  }

  // Delta CDF over a commit that uses deletion vectors needs Delta's DV-aware, row-level
  // reconciliation: a deleted row stays physically in its data file and is masked by a DV, so the
  // CDF `delete` rows for a commit are only the rows the DV newly marks. Re-planning the analyzed
  // CDF batch plan here loses that reconciliation on some Delta versions (e.g. Delta 2.4 / Spark
  // 3.4), where the remove side would then surface every row of a logically-removed file as a
  // `delete` change row instead of just the DV-marked ones.
  //
  // The table property is not a reliable guard: it controls whether future writes may create DVs,
  // while existing DVs can remain after the property is disabled. Inspect the AddFile/RemoveFile
  // actions in the requested CDF interval instead. Decline to intercept only ranges that actually
  // contain DVs and let Delta's DeltaCDFRelation apply them correctly. The matching physical-scan
  // guard in DeltaScanTransformer remains as a backstop.
  private def changesContainDeletionVectors(
      cdfRelation: CDCReader.DeltaCDFRelation): Boolean = {
    cdfRelation.startingVersion.exists {
      startVersion =>
        val changes =
          cdfRelation.snapshotWithSchemaMode.snapshot.deltaLog.getChanges(startVersion)
        val changesInRange = cdfRelation.endingVersion match {
          case Some(endVersion) => changes.takeWhile { case (version, _) => version <= endVersion }
          case None => changes
        }
        changesInRange.exists {
          case (_, actions) =>
            actions.exists {
              case file: AddFile => file.deletionVector != null
              case file: RemoveFile => file.deletionVector != null
              case _ => false
            }
        }
    }
  }

  private def planCDFRelation(
      relation: LogicalRelation,
      cdfRelation: CDCReader.DeltaCDFRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): Option[LogicalPlan] = {
    if (cdfRelation.startingVersion.isEmpty) {
      return Some(projectAndFilter(LocalRelation(relation.output), projects, filters))
    }

    val cdfPlan =
      DeltaCDFRelationHelper.changesToBatchDF(cdfRelation, spark).queryExecution.analyzed
    val cdfOutput = cdfPlan.output
    val rewrittenFilters = filters.map(rewriteExpression(_, cdfOutput))
    val rewrittenProjects = projects.map(rewriteProject(_, cdfOutput))

    // This strategy expands the CDF relation during physical planning, after Spark's normal
    // optimizer pass. Optimize the resolved child so predicates can reach its file scans, but keep
    // the output-restoring projection outside the optimizer: parent operators were planned against
    // the original relation's expression IDs, and Spark 3.3 can remove these aliases as redundant.
    val optimizedChild =
      spark.sessionState.optimizer.execute(filter(cdfPlan, rewrittenFilters))
    Some(Project(rewrittenProjects, optimizedChild))
  }

  private def projectAndFilter(
      child: LogicalPlan,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): LogicalPlan = {
    Project(projects, filter(child, filters))
  }

  private def filter(
      child: LogicalPlan,
      filters: Seq[Expression]): LogicalPlan = {
    val filtered = filters.reduceOption(org.apache.spark.sql.catalyst.expressions.And) match {
      case Some(condition) => Filter(condition, child)
      case None => child
    }
    filtered
  }

  private def rewriteProject(
      project: NamedExpression,
      cdfOutput: Seq[Attribute]): NamedExpression = {
    project match {
      case attr: AttributeReference =>
        Alias(
          resolveCDFAttribute(attr, cdfOutput),
          attr.name)(
          exprId = attr.exprId,
          qualifier = attr.qualifier,
          explicitMetadata = Some(attr.metadata))
      case other =>
        rewriteExpression(other, cdfOutput).asInstanceOf[NamedExpression]
    }
  }

  private def rewriteExpression(expr: Expression, cdfOutput: Seq[Attribute]): Expression = {
    expr.transform {
      case attr: AttributeReference => resolveCDFAttribute(attr, cdfOutput)
    }
  }

  private def resolveCDFAttribute(
      attr: AttributeReference,
      cdfOutput: Seq[Attribute]): Attribute = {
    cdfOutput
      .find(output => spark.sessionState.conf.resolver(output.name, attr.name))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unable to resolve CDF attribute ${attr.name} from " +
            s"${cdfOutput.map(_.name).mkString("[", ", ", "]")}"))
  }
}
