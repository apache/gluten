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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.datasource.ArrowCSVFileFormat
import org.apache.gluten.datasource.v2.ArrowCSVTable

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, PermissiveMode}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.csv.CSVTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil

import java.nio.charset.StandardCharsets

import scala.collection.convert.ImplicitConversions.`map AsScala`

/**
 * Extracts a CSVTable from a DataSourceV2Relation.
 *
 * Only the table variable of DataSourceV2Relation is accessed to improve compatibility across
 * different Spark versions.
 * @since Spark
 *   4.1
 */
private object CSVTableExtractor {
  def unapply(relation: DataSourceV2Relation): Option[(DataSourceV2Relation, CSVTable)] = {
    relation.table match {
      case t: CSVTable =>
        Some((relation, t))
      case _ => None
    }
  }
}

@Experimental
case class ArrowConvertorRule(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!BackendsApiManager.getSettings.enableNativeArrowReadFiles()) {
      return plan
    }
    plan.resolveOperators {
      case l: LogicalRelation =>
        l.relation match {
          case r @ HadoopFsRelation(_, _, dataSchema, _, _: CSVFileFormat, options)
              if validate(session, dataSchema, options) =>
            val csvOptions = new CSVOptions(
              options,
              columnPruning = session.sessionState.conf.csvColumnPruning,
              session.sessionState.conf.sessionLocalTimeZone)
            l.copy(relation = r.copy(fileFormat = new ArrowCSVFileFormat(csvOptions))(session))
          case _ => l
        }
      case CSVTableExtractor(d, t)
          if validate(session, t.dataSchema, t.options.asCaseSensitiveMap().toMap) =>
        d.copy(table = ArrowCSVTable(
          "arrow" + t.name,
          t.sparkSession,
          t.options,
          t.paths,
          t.userSpecifiedSchema,
          t.fallbackFileFormat))
      case r =>
        r
    }
  }

  private def validate(
      session: SparkSession,
      dataSchema: StructType,
      options: Map[String, String]): Boolean = {
    val csvOptions = new CSVOptions(
      options,
      columnPruning = session.sessionState.conf.csvColumnPruning,
      session.sessionState.conf.sessionLocalTimeZone)
    SparkArrowUtil.checkSchema(dataSchema) &&
    checkCsvOptions(csvOptions, session.sessionState.conf.sessionLocalTimeZone) &&
    dataSchema.nonEmpty
  }

  private def checkCsvOptions(csvOptions: CSVOptions, timeZone: String): Boolean = {
    val default = new CSVOptions(CaseInsensitiveMap(Map()), csvOptions.columnPruning, timeZone)
    csvOptions.headerFlag && !csvOptions.multiLine &&
    csvOptions.delimiter.length == 1 &&
    csvOptions.quote == '\"' &&
    csvOptions.escape == '\\' &&
    csvOptions.lineSeparator.isEmpty &&
    csvOptions.charset == StandardCharsets.UTF_8.name() &&
    csvOptions.parseMode == PermissiveMode && !csvOptions.inferSchemaFlag &&
    csvOptions.nullValue == "" &&
    csvOptions.emptyValueInRead == "" && csvOptions.comment == '\u0000' &&
    csvOptions.columnPruning &&
    csvOptions.dateFormatInRead == default.dateFormatInRead &&
    csvOptions.timestampFormatInRead == default.timestampFormatInRead &&
    csvOptions.timestampNTZFormatInRead == default.timestampNTZFormatInRead
  }

}
