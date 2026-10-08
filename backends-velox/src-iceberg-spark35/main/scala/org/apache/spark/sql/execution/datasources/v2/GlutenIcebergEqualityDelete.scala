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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.gluten.config.GlutenIcebergConfig
import org.apache.gluten.connector.write.{IcebergEqualityDeleteWrite, IcebergRowDeltaWrite}
import org.apache.gluten.execution.VeloxIcebergEqualityDeleteExec

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LogicalPlan, Project, UnaryCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.classic.ClassicTypes.ClassicSparkSession
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.iceberg.{BaseTable, FileFormat, Schema, TableProperties, TableUtil}
import org.apache.iceberg.encryption.PlaintextEncryptionManager
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.{SparkFilters, SparkReadOptions, SparkWriteConf}
import org.apache.iceberg.spark.source.{GlutenEqualityDeleteWriterUtil, SparkTable}
import org.apache.iceberg.types.Types.NestedField

import java.util.{Collections, HashMap, Locale}

import scala.collection.JavaConverters._

case class GlutenIcebergEqualityDelete(
    child: LogicalPlan,
    originalTable: DataSourceV2Relation,
    write: IcebergEqualityDeleteWrite)
  extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

case class RewriteGlutenIcebergEqualityDelete(spark: SparkSession) extends Rule[LogicalPlan] {
  private def supportedColumn(field: NestedField): Boolean =
    IcebergRowDeltaWrite.supportsNativeEqualityType(field.`type`())

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = new GlutenIcebergConfig(spark.sessionState.conf)
    if (!conf.enableNativeWrite || !conf.enableNativeEqualityDelete) {
      return plan
    }
    plan.resolveOperatorsUp {
      case delete: DeleteFromTable if delete.resolved =>
        rewrite(
          delete).orElse(new GlutenIcebergRowDeltaRewrite(spark).rewrite(delete)).getOrElse(delete)
      case command: org.apache.spark.sql.catalyst.plans.logical.UpdateTable =>
        new GlutenIcebergRowDeltaRewrite(spark).rewrite(command).getOrElse(command)
      case command: org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable =>
        new GlutenIcebergRowDeltaRewrite(spark).rewrite(command).getOrElse(command)
    }
  }

  private def rewrite(delete: DeleteFromTable): Option[LogicalPlan] = {
    val condition = delete.condition
    if (
      !condition.deterministic || SubqueryExpression.hasSubquery(condition) ||
      condition.references.isEmpty
    ) {
      return None
    }
    val relation = EliminateSubqueryAliases(delete.table) match {
      case r: DataSourceV2Relation => r
      case _ => return None
    }
    val sparkTable = relation.table match {
      case t: SparkTable if t.snapshotId() == null && t.branch() == null => t
      case _ => return None
    }
    val table = sparkTable.table()
    if (
      !table.isInstanceOf[BaseTable] || !Set(2, 3).contains(TableUtil.formatVersion(table)) ||
      !table.encryption().isInstanceOf[PlaintextEncryptionManager]
    ) {
      return None
    }
    val properties = table.properties()
    if (
      properties.getOrDefault(TableProperties.DELETE_MODE, "copy-on-write") != "merge-on-read" ||
      properties.getOrDefault(TableProperties.DELETE_ISOLATION_LEVEL, "serializable") !=
        "serializable" ||
        properties.getOrDefault(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "false").toBoolean ||
        spark.conf.getOption("spark.wap.branch").exists(_.nonEmpty)
    ) {
      return None
    }
    val writeConf = new SparkWriteConf(spark, table, Collections.emptyMap[String, String]()) {
      // The v3 default, Puffin, cannot encode equality deletes.
      override def deleteFileFormat(): FileFormat =
        Option(properties.get(TableProperties.DELETE_DEFAULT_FILE_FORMAT))
          .map(FileFormat.fromString).getOrElse(dataFileFormat())
    }
    if (writeConf.deleteFileFormat() != FileFormat.PARQUET || writeConf.mergeSchema()) {
      return None
    }
    val spec = table.spec()
    if (spec.isPartitioned && table.specs().size() != 1) {
      return None
    }
    val schema = table.schema()
    val columns = schema.columns().asScala.map(f => f.name() -> f).toMap
    val keyAttrs = relation.output.filter(condition.references.contains)
    if (
      keyAttrs.size != condition.references.size || keyAttrs.exists {
        attr => !columns.get(attr.name).exists(supportedColumn)
      }
    ) {
      return None
    }
    val partitionIds = spec.fields().asScala.map(_.sourceId()).toSet
    val partitionColumns = schema.columns().asScala.filter(f => partitionIds.contains(f.fieldId()))
    if (
      partitionColumns.size != partitionIds.size || !partitionColumns.forall(supportedColumn) ||
      spec.fields().asScala.exists(_.transform().isVoid)
    ) {
      return None
    }
    val names = keyAttrs.map(_.name).toSet ++ partitionColumns.map(_.name())
    val attrs = relation.output.filter(a => names.contains(a.name))
    val codec = writeConf.writeProperties().getOrDefault(
      TableProperties.DELETE_PARQUET_COMPRESSION,
      TableProperties.PARQUET_COMPRESSION_DEFAULT)
      .toLowerCase(Locale.ROOT)
    if (!Set("snappy", "gzip", "zstd", "lz4", "lz4_raw", "none", "uncompressed").contains(codec)) {
      return None
    }
    val snapshot = table.currentSnapshot()
    if (snapshot == null) {
      return None
    }
    // Identifier fields alone could also delete rows that fail the predicate.
    val equalityIds = keyAttrs.map(a => columns(a.name).fieldId())
    val deleteSchema = new Schema(attrs.map(a => columns(a.name)).asJava)
    val conflictFilter = DataSourceStrategy.translateFilter(condition, true)
      .flatMap(f => Option(SparkFilters.convert(f))).getOrElse(Expressions.alwaysTrue())
    val write = new IcebergEqualityDeleteWrite(
      table,
      snapshot.snapshotId(),
      deleteSchema,
      spec,
      equalityIds,
      conflictFilter,
      writeConf,
      codec)
    // SparkTable.equals ignores snapshot IDs; scan options prevent stale cache matches.
    val options = new HashMap[String, String](relation.options.asCaseSensitiveMap())
    options.put(SparkReadOptions.SNAPSHOT_ID, snapshot.snapshotId().toString)
    val scan = relation.copy(
      table = GlutenEqualityDeleteWriterUtil.scanTable(table),
      options = new CaseInsensitiveStringMap(options))
    Some(GlutenIcebergEqualityDelete(Project(attrs, Filter(condition, scan)), relation, write))
  }
}

case class GlutenIcebergEqualityDeleteStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case delete: GlutenIcebergEqualityDelete =>
      Seq(VeloxIcebergEqualityDeleteExec(
        planLater(delete.child),
        () =>
          spark.sharedState.cacheManager.recacheByPlan(
            spark.asInstanceOf[ClassicSparkSession],
            delete.originalTable),
        delete.write))
    case _ => Nil
  }
}
