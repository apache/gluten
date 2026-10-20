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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.util.control.NonFatal

/**
 * Expands Delta's `DeltaCDFRelation` into the underlying change-data plan so the Delta file scans
 * it reads become visible to Gluten's normal scan offload rules. Without this, Spark plans the CDF
 * relation as a `RowDataSourceScanExec` whose file scans are hidden inside an RDD, and no columnar
 * rule can reach them.
 *
 * This runs as a logical optimizer rule. Only an output-restoring projection is needed: the
 * replacement keeps the original relation's expression IDs, and because the rule participates in
 * the operator optimization batch, Spark's own optimizer then pushes the surrounding filters into
 * the expanded file scans. Expanding later -- during physical planning -- would require re-running
 * the optimizer out of band and hand-preserving expression IDs against operators that were already
 * planned, which is fragile (a collapsed alias there surfaces as `Couldn't find <attr>` at shuffle
 * binding time).
 *
 * The phase also has to be no earlier than the optimizer. Analysis runs eagerly when a Dataset is
 * created, while optimization is deferred to the first action, and Delta resolves an open-ended CDF
 * range (no ending version) to the latest version at execution time. Expanding during analysis
 * would snapshot that range at Dataset creation and silently drop commits made before the action --
 * see `DeltaCDCSQLSuite."ending version not specified resolves to latest at execution time"`.
 */
case class DeltaCDFScanRule(spark: SparkSession, offloadEnabled: () => Boolean)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!offloadEnabled()) {
      return plan
    }

    // transformUp, not resolveOperatorsUp: by the optimizer the plan is already marked analyzed,
    // and resolveOperatorsUp skips analyzed subtrees, so it would never reach the relation. The
    // analyzer's assertion against transformUp does not apply outside the analyzer. Expansion is
    // idempotent -- the replacement holds no DeltaCDFRelation -- so re-running the operator
    // optimization batch to fixed point cannot expand twice.
    plan.transformUp {
      case relation: LogicalRelation =>
        relation.relation match {
          case cdfRelation: CDCReader.DeltaCDFRelation =>
            expandOrDecline(relation, cdfRelation)
          case _ => relation
        }
    }
  }

  // Leave the relation untouched on any failure so Delta's own CDF path runs. This rule executes
  // during analysis, so an escaping exception would fail the query outright -- including for
  // metadata-only calls such as `explain()` -- where vanilla Spark would have succeeded. Offload is
  // an optimization; it must never be the reason a CDF query stops working.
  private def expandOrDecline(
      relation: LogicalRelation,
      cdfRelation: CDCReader.DeltaCDFRelation): LogicalPlan = {
    try {
      if (changesContainDeletionVectors(cdfRelation)) {
        relation
      } else {
        expand(relation, cdfRelation)
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to expand Delta CDF relation, falling back to Delta's CDF scan.", e)
        relation
    }
  }

  // Delta CDF over a commit that uses deletion vectors needs Delta's DV-aware, row-level
  // reconciliation: a deleted row stays physically in its data file and is masked by a DV, so the
  // CDF `delete` rows for a commit are only the rows the DV newly marks. Expanding the analyzed
  // CDF batch plan here loses that reconciliation on some Delta versions (e.g. Delta 2.4 / Spark
  // 3.4), where the remove side would then surface every row of a logically-removed file as a
  // `delete` change row instead of just the DV-marked ones.
  //
  // The table property is not a reliable guard: it controls whether future writes may create DVs,
  // while existing DVs can remain after the property is disabled. Inspect the AddFile/RemoveFile
  // actions in the requested CDF interval instead. Decline to expand only ranges that actually
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

  private def expand(
      relation: LogicalRelation,
      cdfRelation: CDCReader.DeltaCDFRelation): LogicalPlan = {
    // Delta represents an empty CDF range with `emptyCDFRelation`, whose `startingVersion` is unset
    // and whose scan is an empty RDD. There is nothing to expand, and asking Delta for the changes
    // would fail on `startingVersion.get`.
    if (cdfRelation.startingVersion.isEmpty) {
      return LocalRelation(relation.output)
    }

    val cdfPlan =
      DeltaCDFRelationHelper.changesToBatchDF(cdfRelation, spark).queryExecution.analyzed
    val cdfOutput = cdfPlan.output
    // Bind the expanded plan back to the expression IDs the rest of the query already references.
    val projects: Seq[NamedExpression] = relation.output.map {
      attr =>
        Alias(resolveCDFAttribute(attr, cdfOutput), attr.name)(
          exprId = attr.exprId,
          qualifier = attr.qualifier,
          explicitMetadata = Some(attr.metadata))
    }
    Project(projects, cdfPlan)
  }

  private def resolveCDFAttribute(attr: Attribute, cdfOutput: Seq[Attribute]): Attribute = {
    cdfOutput
      .find(output => spark.sessionState.conf.resolver(output.name, attr.name))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unable to resolve CDF attribute ${attr.name} from " +
            s"${cdfOutput.map(_.name).mkString("[", ", ", "]")}"))
  }
}
