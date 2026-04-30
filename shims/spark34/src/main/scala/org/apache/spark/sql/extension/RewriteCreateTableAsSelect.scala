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
package org.apache.spark.sql.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, FileSourceMetadataAttribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, METADATA_COL_ATTR_KEY}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Util.isSessionCatalog
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, LeafV2CommandExec}
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

case class RewriteCreateTableAsSelect(session: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case CreateTableAsSelect(
            ResolvedIdentifier(catalog, ident),
            parts,
            query,
            tableSpec: TableSpec,
            options,
            ifNotExists,
            analyzedQuery) if analyzedQuery.isDefined && !supportsV1Command(catalog) =>
        catalog match {
          case staging: StagingTableCatalog =>
            AtomicCreateTableAsSelectExec(
              staging,
              ident,
              parts,
              analyzedQuery.get,
              qualifyLocInTableSpec(tableSpec),
              options,
              ifNotExists) :: Nil
          case _ =>
            CreateTableAsSelectExec(
              catalog.asTableCatalog,
              ident,
              parts,
              analyzedQuery.get,
              qualifyLocInTableSpec(tableSpec),
              options,
              ifNotExists) :: Nil
        }

      case _ => Nil
    }
  }

  private def supportsV1Command(catalog: CatalogPlugin): Boolean = {
    isSessionCatalog(catalog) && catalog.isInstanceOf[CatalogExtension]
  }

  private def qualifyLocInTableSpec(tableSpec: TableSpec): TableSpec = {
    tableSpec.copy(location = tableSpec.location.map(makeQualifiedDBObjectPath))
  }

  private def makeQualifiedDBObjectPath(location: String): String = {
    CatalogUtils.makeQualifiedDBObjectPath(
      session.sharedState.conf.get(WAREHOUSE_PATH),
      location,
      session.sharedState.hadoopConf)
  }
}

/** Port from Spark 3.5. */
case class AtomicCreateTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    ifNotExists: Boolean)
  extends V2CreateTableAsSelectBaseExec {

  val properties: Map[String, String] = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }
      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }

    val stagedTable = catalog.stageCreate(
      ident,
      getV2Columns(query.schema),
      partitioning.toArray,
      properties.asJava)
    writeToTable(catalog, stagedTable, writeOptions, ident, query)
  }
}

case class CreateTableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    ifNotExists: Boolean)
  extends V2CreateTableAsSelectBaseExec {

  val properties: Map[String, String] = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }
      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }
    val table = catalog.createTable(
      ident,
      getV2Columns(query.schema),
      partitioning.toArray,
      properties.asJava)
    writeToTable(catalog, table, writeOptions, ident, query)
  }
}

trait V2CreateTableAsSelectBaseExec extends LeafV2CommandExec {
  override def output: Seq[Attribute] = Nil

  protected def getV2Columns(schema: StructType): Array[Column] = {
    val rawSchema = CharVarcharUtils.getRawSchema(removeInternalMetadata(schema), conf)
    CatalogV2Util.structTypeToV2Columns(rawSchema.asNullable)
  }

  protected def writeToTable(
      catalog: TableCatalog,
      table: Table,
      writeOptions: Map[String, String],
      ident: Identifier,
      query: LogicalPlan): Seq[InternalRow] = {
    Utils.tryWithSafeFinallyAndFailureCallbacks {
      val relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
      val append = AppendData.byPosition(relation, query, writeOptions)
      val qe = session.sessionState.executePlan(append)
      qe.assertCommandExecuted()

      table match {
        case st: StagedTable => st.commitStagedChanges()
        case _ =>
      }

      Nil
    }(catchBlock = {
      table match {
        // Failure rolls back the staged writes and metadata changes.
        case st: StagedTable => st.abortStagedChanges()
        case _ => catalog.dropTable(ident)
      }
    })
  }

  val INTERNAL_METADATA_KEYS = Seq(
    "__autoGeneratedAlias",
    METADATA_COL_ATTR_KEY,
    "__qualified_access_only",
    FileSourceMetadataAttribute.FILE_SOURCE_METADATA_COL_ATTR_KEY,
    "__file_source_constant_metadata_col",
    "__file_source_generated_metadata_col"
  )

  private def removeInternalMetadata(schema: StructType): StructType = {
    StructType(schema.map {
      field =>
        var builder = new MetadataBuilder().withMetadata(field.metadata)
        INTERNAL_METADATA_KEYS.foreach(key => builder = builder.remove(key))
        field.copy(metadata = builder.build())
    })
  }
}
