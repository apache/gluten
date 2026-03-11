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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.V1WritesUtils.isOrderingMatched

case class DeltaV1Writes(
    spark: SparkSession,
    query: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec = Map.empty) {

  require(fileFormat != null, "FileFormat is required to write files.")
  require(BackendsApiManager.getSettings.enableNativeWriteFiles())

  private lazy val requiredOrdering: Seq[SortOrder] =
    V1WritesUtils.getSortOrder(
      query.output,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions.size)

  lazy val sortPlan: SparkPlan = {
    val outputOrdering = query.outputOrdering
    val orderingMatched = isOrderingMatched(requiredOrdering.map(_.child), outputOrdering)
    if (orderingMatched) {
      query
    } else {
      SortExec(requiredOrdering, global = false, query)
    }
  }

  lazy val writePlan: SparkPlan =
    WriteFilesExec(
      sortPlan,
      fileFormat = fileFormat,
      partitionColumns = partitionColumns,
      bucketSpec = bucketSpec,
      options = options,
      staticPartitions = staticPartitions)

  lazy val executedPlan: SparkPlan =
    CallTransformer(spark, writePlan).executedPlan
}

case class CallTransformer(spark: SparkSession, physicalPlan: SparkPlan)
  extends QueryExecution(spark, LocalRelation()) {
  override lazy val sparkPlan: SparkPlan = physicalPlan
}
